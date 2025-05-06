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

from data_ingestion_pipeline.components.ingest_data import ingest_data
from data_ingestion_pipeline.components.process_data import process_data
from kfp import dsl


@dsl.pipeline(description="A pipeline to run ingestion of new data into the datastore")
def pipeline(
    project_id: str,
    location: str,
    confluence_domain: str,
    confluence_email: str,
    confluence_token: str,
    confluence_space_key: str = "",
    confluence_page_ids: str = "",
    is_incremental: bool = True,
    chunk_size: int = 1500,
    chunk_overlap: int = 20,
    data_store_region: str = "",
    data_store_id: str = "",
) -> None:
    """Processes data and ingests it into a datastore for RAG Retrieval"""

    # Process the data and generate embeddings
    process_data_task = process_data(
        project_id=project_id,
        schedule_time=dsl.PIPELINE_JOB_SCHEDULE_TIME_UTC_PLACEHOLDER,
        confluence_domain=confluence_domain,
        confluence_email=confluence_email,
        confluence_token=confluence_token,
        confluence_space_key=confluence_space_key,
        confluence_page_ids=confluence_page_ids,
        is_incremental=is_incremental,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        location=location,
        embedding_column="embedding",
    ).set_retry(num_retries=2).set_env_variable("PIP_DISABLE_PIP_VERSION_CHECK", "1")

    # Ingest the processed data into Vertex AI Search datastore
    ingest_data_task = ingest_data(
        project_id=project_id,
        data_store_region=data_store_region,
        input_files=process_data_task.output,
        data_store_id=data_store_id,
        embedding_column="embedding",
    ).set_retry(num_retries=2).set_env_variable("PIP_DISABLE_PIP_VERSION_CHECK", "1")