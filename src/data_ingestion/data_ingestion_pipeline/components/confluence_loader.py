# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from typing import List

import requests
from bs4 import BeautifulSoup
from kfp.dsl import Dataset, Output, component
from langchain_core.documents import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

@component(
    base_image="us-docker.pkg.dev/production-ai-template/starter-pack/data_processing:0.2"
)
def load_confluence_data(
    output_files: Output[Dataset],
    confluence_domain: str,
    confluence_email: str,
    confluence_token: str,
    page_ids: List[str],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> None:
    """Load and process Confluence pages into documents.
    
    Args:
        output_files: Output dataset path
        confluence_domain: Confluence domain
        confluence_email: Confluence email for authentication
        confluence_token: Confluence API token
        page_ids: List of Confluence page IDs to process
        chunk_size: Size of text chunks
        chunk_overlap: Overlap between chunks
    """
    def fetch_page_content(page_id: str) -> str:
        url = f"https://{confluence_domain}/wiki/rest/api/content/{page_id}?expand=body.storage"
        response = requests.get(url, auth=(confluence_email, confluence_token))
        response.raise_for_status()
        html = response.json()["body"]["storage"]["value"]
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n", strip=True)

    def load_and_split_documents() -> List[Document]:
        docs = []
        for pid in page_ids:
            try:
                content = fetch_page_content(pid)
                docs.append(Document(page_content=content, metadata={"page_id": pid}))
                logging.info(f"Successfully loaded page {pid}")
            except Exception as e:
                logging.error(f"Error loading page {pid}: {str(e)}")
                continue

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        return splitter.split_documents(docs)

    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    
    # Load and process documents
    logging.info("Loading and processing Confluence documents...")
    doc_splits = load_and_split_documents()
    logging.info(f"Processed {len(doc_splits)} document chunks")

    # Convert to JSONL format
    import json
    output_data = []
    for doc in doc_splits:
        output_data.append({
            "id": doc.metadata["page_id"],
            "content": doc.page_content,
            "metadata": doc.metadata
        })

    # Write to output file
    output_files.uri = output_files.uri + "*.jsonl"
    with open(output_files.uri, "w") as f:
        for item in output_data:
            f.write(json.dumps(item) + "\n")
    
    logging.info("Documents processed and saved to JSONL format") 