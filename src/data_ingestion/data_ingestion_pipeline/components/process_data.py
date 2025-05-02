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
    confluence_domain: str,      # NEW
    confluence_email: str,       # NEW
    confluence_token: str,       # NEW (store in Secret Manager!)
    confluence_page_ids: str,    # NEW
    is_incremental: bool = True,
    look_back_days: int = 1,
    chunk_size: int = 1500,
    chunk_overlap: int = 20,
    destination_dataset: str = "stackoverflow_data",
    destination_table: str = "incremental_questions_embeddings",
    deduped_table: str = "questions_embeddings",
    location: str = "us-central1",
    embedding_column: str = "embedding",
) -> None:
    """Process StackOverflow questions and answers by:
    1. Fetching data from BigQuery
    2. Converting HTML to markdown
    3. Splitting text into chunks
    4. Generating embeddings
    5. Storing results in BigQuery
    6. Exporting to JSONL

    Args:
        output_files: Output dataset path
        is_incremental: Whether to process only recent data
        look_back_days: Number of days to look back for incremental processing
        chunk_size: Size of text chunks
        chunk_overlap: Overlap between chunks
        destination_dataset: BigQuery dataset for storing results
        destination_table: Table for storing incremental results
        deduped_table: Table for storing deduplicated results
        location: BigQuery location
    """
    import logging
    from datetime import datetime, timedelta

    import backoff
    import google.api_core.exceptions
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    # from markdownify import markdownify
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd

    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Set date range for data fetch
    schedule_time_dt: datetime = datetime.fromisoformat(
        schedule_time.replace("Z", "+00:00")
    )
    if schedule_time_dt.year == 1970:
        logging.warning(
            "Pipeline schedule not set. Setting schedule_time to current date."
        )
        schedule_time_dt = datetime.now()

    # Note: The following line sets the schedule time 5 years back to allow sample data to be present.
    # For your use case, please comment out the following line to use the actual schedule time.
    schedule_time_dt = schedule_time_dt - timedelta(days=5 * 365)

    START_DATE: datetime = schedule_time_dt - timedelta(
        days=look_back_days
    )  # Start date for data processing window
    END_DATE: datetime = schedule_time_dt  # End date for data processing window

    logging.info(f"Date range set: START_DATE={START_DATE}, END_DATE={END_DATE}")
    
    def fetch_confluence_pages(
        domain: str, email: str, token: str, page_ids: list[str]
    ) -> pd.DataFrame:
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

    # optional -- will investigate later!
    # def convert_html_to_markdown(html: str) -> str:
    #     """Convert HTML into Markdown for easier parsing and rendering after LLM response."""
    #     return markdownify(html).strip()

    # Fetch and preprocess data
    logging.info("Fetching and preprocessing data...")
    page_ids = [p.strip() for p in confluence_page_ids.split(",") if p.strip()]
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
    chunk_ids = [str(i) for txt in df["text_chunk"] for i in range(len(txt))]
    df = df.explode("text_chunk").reset_index(drop=True)
    df["chunk_id"] = df["id"].astype("string") + "__" + chunk_ids
    logging.info("Chunk IDs created and chunks exploded.")
#     print(df.head(3))          # see the first 3 exploded rows
    
    
#     # --- show the first 500 chars of full_text_md for 3 rows ---
#     print("\n--- full_text_md for first 3 rows ---")
#     for i, txt in enumerate(df["full_text_md"].head(3), start=1):
#         snippet = txt[:500].replace("\n", " ") + "..."
#         print(f"{i}. {snippet}\n")
    # return                      # <--- temporary early exit

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