# Copyright 2024 Google LLC
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

from data_ingestion_pipeline.components.ingest_data import ingest_data
from data_ingestion_pipeline.components.confluence_loader import load_confluence_data
from kfp import dsl


@dsl.pipeline(description="A pipeline to run ingestion of Confluence data into the datastore")
def pipeline(
    project_id: str,
    location: str,
    confluence_domain: str,
    confluence_email: str,
    confluence_token: str,
    page_ids: list,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    data_store_region: str = "",
    data_store_id: str = "",
    vector_search_index: str = "",
    vector_search_index_endpoint: str = "",
    vector_search_data_bucket_name: str = "",
    ingestion_batch_size: int = 1000,
) -> None:
    """Processes Confluence data and ingests it into a datastore for RAG Retrieval"""

    # Load and process Confluence data
    processed_data = load_confluence_data(
        confluence_domain=confluence_domain,
        confluence_email=confluence_email,
        confluence_token=confluence_token,
        page_ids=page_ids,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    ).set_retry(num_retries=2)

    # Ingest the processed data into Vertex AI Search datastore
    ingest_data(
        project_id=project_id,
        data_store_region=data_store_region,
        input_files=processed_data.output,
        data_store_id=data_store_id,
        embedding_column="embedding",
    ).set_retry(num_retries=2)

    # Ingest the processed data into Vertex AI Vector Search
    ingest_data(
        project_id=project_id,
        location=location,
        vector_search_index=vector_search_index,
        vector_search_index_endpoint=vector_search_index_endpoint,
        vector_search_data_bucket_name=vector_search_data_bucket_name,
        input_table=processed_data.output,
        schedule_time=dsl.PIPELINE_JOB_SCHEDULE_TIME_UTC_PLACEHOLDER,
        is_incremental=False,
        look_back_days=1,
        ingestion_batch_size=ingestion_batch_size,
    ).set_retry(num_retries=2)