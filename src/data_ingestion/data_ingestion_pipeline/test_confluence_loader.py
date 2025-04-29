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
import tempfile
from data_ingestion_pipeline.components.confluence_loader import load_confluence_data

def test_confluence_loader():
    # Create a temporary directory for output
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = os.path.join(temp_dir, "output.jsonl")
        
        # Test parameters
        test_params = {
            "confluence_domain": os.getenv("CONFLUENCE_DOMAIN"),
            "confluence_email": os.getenv("CONFLUENCE_EMAIL"),
            "confluence_token": os.getenv("CONFLUENCE_TOKEN"),
            "page_ids": ["123456"],  # Replace with a test page ID
            "chunk_size": 1000,
            "chunk_overlap": 200,
            "output_files": output_path
        }
        
        # Run the component
        load_confluence_data(**test_params)
        
        # Verify the output
        if os.path.exists(output_path):
            print("Test successful! Output file created at:", output_path)
            with open(output_path, 'r') as f:
                content = f.read()
                print("Output content preview:", content[:500])
        else:
            print("Test failed: Output file not created")

if __name__ == "__main__":
    test_confluence_loader() 