# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ruff: noqa

"""
This component is derived from the notebook:
https://github.com/GoogleCloudPlatform/generative-ai/blob/main/gemini/use-cases/retrieval-augmented_generation/scalable_rag_with_bigframes.ipynb

It leverages BigQuery for data processing. We also suggest looking at remote functions for enhanced scalability.
"""

from kfp.dsl import Dataset, Output, component

@component(
    base_image="us-docker.pkg.dev/production-ai-template/starter-pack/data_processing:0.2",
    packages_to_install=[]  # No need to install packages as they're in the base image
)
def process_data(
    project_id: str,
    schedule_time: str,
    output_files: Output[Dataset],
    confluence_domain: str,
    confluence_email: str,
    confluence_token: str,
    confluence_space_key: str = "",      # NEW
    confluence_page_ids: str = "",       # Still supported
    chunk_size: int = 1500,
    chunk_overlap: int = 20,
    location: str = "us-central1",
    embedding_column: str = "embedding",
    is_incremental: bool = True,  # Added back for future use
) -> None:
    """Process Confluence pages by:
    1. Fetching all page IDs from a space (if space key provided) or using provided page IDs
    2. Fetching page content
    3. Splitting text into chunks
    4. Generating embeddings
    5. Exporting to JSONL

    Args:
        output_files: Output dataset path
        chunk_size: Size of text chunks
        chunk_overlap: Overlap between chunks
        confluence_space_key: Confluence space key (if provided, fetch all page IDs in this space)
        confluence_page_ids: Comma-separated list of Confluence page IDs (used if space key not provided)
        is_incremental: (Not currently used) Intended for future incremental ingestion support.
        schedule_time: (Not currently used) Intended for future incremental ingestion support.
        ... other params ...
    """
    import logging
    from datetime import datetime, timedelta
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import backoff
    import google.api_core.exceptions
    from langchain.text_splitter import RecursiveCharacterTextSplitter

    logging.basicConfig(level=logging.INFO)

    def get_all_page_ids_for_space(domain, email, token, space_key):
        url = f"https://{domain}/wiki/rest/api/content"
        auth = (email, token)
        limit = 50
        start = 0
        page_ids = []
        while True:
            params = {
                "spaceKey": space_key,
                "type": "page",
                "limit": limit,
                "start": start,
            }
            response = requests.get(url, params=params, auth=auth)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                break
            for page in results:
                page_ids.append(page["id"])
            if len(results) < limit:
                break
            start += limit
        logging.info(f"Fetched {len(page_ids)} page IDs from space '{space_key}'")
        return page_ids

    def fetch_confluence_pages(domain: str, email: str, token: str, page_ids: list[str]) -> pd.DataFrame:
        rows = []
        auth = (email, token)
        for pid in page_ids:
            url = (
                f"https://{domain}/wiki/rest/api/content/{pid}"
                "?expand=body.storage,version,space,metadata.labels"
            )
            r = requests.get(url, auth=auth, timeout=30)
            r.raise_for_status()
            html = r.json()["body"]["storage"]["value"]
            text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            rows.append(
                {
                    "id": pid,
                    "title": r.json().get("title", ""),
                    "space": r.json().get("space", {}).get("key", ""),
                    "text": text,
                }
            )
        return pd.DataFrame(rows)

    # --- Parameter handling ---
    if confluence_space_key:
        logging.info(f"Fetching all page IDs for space: {confluence_space_key}")
        page_ids = get_all_page_ids_for_space(
            confluence_domain, confluence_email, confluence_token, confluence_space_key
        )
    elif confluence_page_ids:
        logging.info(f"Using provided page IDs: {confluence_page_ids}")
        page_ids = [p.strip() for p in confluence_page_ids.split(",") if p.strip()]
    else:
        raise ValueError("Either confluence_space_key or confluence_page_ids must be provided.")

    # Fetch and preprocess data
    logging.info("Fetching and preprocessing data...")
    df_raw = fetch_confluence_pages(
        confluence_domain, confluence_email, confluence_token, page_ids
    )
    df_raw = df_raw.rename(columns={"text": "full_text_md"})
    df = df_raw[["id", "title", "full_text_md"]].copy()
    logging.info("Data fetched and preprocessed.")

    # Split text into chunks
    logging.info("Splitting text into chunks...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )

    df["text_chunk"] = (
        df["full_text_md"].apply(text_splitter.split_text)
    )
    logging.info("Text split into chunks.")

    # Create chunk IDs and explode chunks into rows
    logging.info("Creating chunk IDs and exploding chunks into rows...")
    # First, create a list of tuples with (page_id, chunk_number)
    chunk_info = []
    for _, row in df.iterrows():
        page_id = row["id"]
        num_chunks = len(row["text_chunk"])
        chunk_info.extend([(page_id, i) for i in range(num_chunks)])
    
    # Now explode the DataFrame
    df = df.explode("text_chunk").reset_index(drop=True)
    
    # Create chunk IDs using the pre-calculated chunk info
    df["chunk_id"] = [f"{page_id}__{chunk_num}" for page_id, chunk_num in chunk_info]
    logging.info("Chunk IDs created and chunks exploded.")

    from langchain_google_vertexai import VertexAIEmbeddings
    import json, pathlib, uuid
    from datetime import datetime

    # ------------------------------------------------------------
    # Generate embeddings (client-side call to Vertex AI endpoint)
    # ------------------------------------------------------------
    logging.info("Generating embeddings...")

    @backoff.on_exception(
        backoff.expo, google.api_core.exceptions.InvalidArgument, max_tries=10
    )
    def create_embedder() -> VertexAIEmbeddings:
        return VertexAIEmbeddings(
            model_name="text-embedding-005",
            project=project_id,
            location=location,
        )

    embedder = create_embedder()

    embeddings = embedder.embed_documents(df["text_chunk"].tolist())
    df["embedding"] = embeddings
    df["creation_timestamp"] = datetime.utcnow()

    logging.info("Embeddings generated.")

    # ------------------------------------------------------------
    # Export each chunk → JSONL for Vertex AI Search
    # ------------------------------------------------------------
    out_dir = pathlib.Path(output_files.path)
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_dir / f"confluence_{uuid.uuid4().hex}.jsonl"

    with jsonl_path.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            doc = {
                "id": row["chunk_id"],
                "json_data": json.dumps({
                    embedding_column: row["embedding"],
                    "content": row["text_chunk"],
                    "source_title": row["title"],
                    "source_page_id": row["id"],
                    "creation_timestamp": row["creation_timestamp"].isoformat(),
                }, ensure_ascii=False)
            }
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    # Write the JSONL file path to file_list.txt for downstream components
    file_list_path = out_dir / "file_list.txt"
    with file_list_path.open("w", encoding="utf-8") as f:
        f.write(str(jsonl_path))

    logging.info("Exported %d chunks to %s", len(df), jsonl_path)
    # Set the output_files.uri to the correct GCS URI for the JSONL file
    if output_files.uri.rstrip("/").endswith("output_files"):
        output_files.uri = output_files.uri.rstrip("/") + "/" + jsonl_path.name
    else:
        output_files.uri = output_files.uri.rstrip("/") + "/output_files/" + jsonl_path.name
    logging.info("Set output_files.uri to %s", output_files.uri)
    logging.info("Wrote file list to %s", file_list_path)


    
    
    # At the bottom of your editor / a REPL test:
# ------------------------------------------------------------------
# Quick local test – leave this at the very bottom of process_data.py
# ------------------------------------------------------------------
# if __name__ == "__main__":
#     df = fetch_confluence_pages(
#         "badal.atlassian.net",                # domain
#         "rana.hashemi@badal.io",              # e-mail
#         #add pat
#         ["887816193"]                         #  ← COMMA added ^^^
#     )
#     print(df.head())
if __name__ == "__main__":
    import types
    from kfp.dsl import Dataset
    
    # Create a simple output dataset
    output_files = types.SimpleNamespace(
        path="/tmp/confluence_test",
        uri="gs://prj-00-np-002-genai-f737-pipeline-root/test_output"
    )
# import types   # add to your imports

# # … keep everything in the file exactly as you have it …

# # ---------------- quick local test -----------------
# if __name__ == "__main__":
#     process_data.python_func(             # <-- call the raw Python fn
#         project_id="prj-00-np-002-genai-f737",
#         schedule_time="2025-04-30T00:00:00Z",
#         output_files=types.SimpleNamespace(path="/tmp"),  # simple stub
#         confluence_domain="badal.atlassian.net",
#         confluence_email="rana.hashemi@badal.io",
#         #add pat,
#         confluence_page_ids="887816193",
#     ) 
    # Call process_data with all required parameters
    process_data.python_func(
        project_id="prj-00-np-002-genai-f737",
        schedule_time="2025-04-30T00:00:00Z",
        output_files=output_files,
        confluence_domain="badal.atlassian.net",
        confluence_email="rana.hashemi@badal.io",
       # add pat here
        confluence_space_key="BE",  # Test with space key
        # confluence_page_ids="887816193",  # Commented out since we're using space key
        chunk_size=1500,
        chunk_overlap=20,
        location="us-central1",
        embedding_column="embedding",
        is_incremental=True
    )