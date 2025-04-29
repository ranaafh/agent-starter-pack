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

import os
from kfp import compiler
from pipeline import pipeline

def test_pipeline():
    # Set your test parameters here
    test_params = {
        "project_id": os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id"),
        "location": os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1"),
        "confluence_domain": os.getenv("CONFLUENCE_DOMAIN", "your-domain.atlassian.net"),
        "confluence_email": os.getenv("CONFLUENCE_EMAIL", "your-email@example.com"),
        "confluence_token": os.getenv("CONFLUENCE_TOKEN", "your-api-token"),
        "page_ids": ["123456", "789012"],  # Replace with actual test page IDs
        "chunk_size": 1000,
        "chunk_overlap": 200,
        "data_store_region": "us-central1",
        "data_store_id": "test-datastore",
        "vector_search_index": "test-index",
        "vector_search_index_endpoint": "test-endpoint",
        "vector_search_data_bucket_name": "test-bucket",
        "ingestion_batch_size": 1000
    }

    # Compile the pipeline
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path="confluence_pipeline.json"
    )

    print("Pipeline compiled successfully. You can now run it using:")
    print("kfp run create --package-path confluence_pipeline.json --arguments " + 
          "project_id={project_id} " +
          "location={location} " +
          "confluence_domain={confluence_domain} " +
          "confluence_email={confluence_email} " +
          "confluence_token={confluence_token} " +
          "page_ids={page_ids} " +
          "chunk_size={chunk_size} " +
          "chunk_overlap={chunk_overlap} " +
          "data_store_region={data_store_region} " +
          "data_store_id={data_store_id} " +
          "vector_search_index={vector_search_index} " +
          "vector_search_index_endpoint={vector_search_index_endpoint} " +
          "vector_search_data_bucket_name={vector_search_data_bucket_name} " +
          "ingestion_batch_size={ingestion_batch_size}".format(**test_params))

if __name__ == "__main__":
    test_pipeline() 