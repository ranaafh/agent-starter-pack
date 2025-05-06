from data_ingestion_pipeline.pipeline import pipeline
from kfp import compiler
from google.cloud import aiplatform
import os

# Set your GCP project ID
PROJECT_ID = "prj-00-np-002-genai-f737"  # Your project ID
LOCATION = "us-central1"  # Vertex AI supported region

# Confluence credentials
CONFLUENCE_DOMAIN = "badal.atlassian.net"  # Your Confluence domain
CONFLUENCE_EMAIL = "rana.hashemi@badal.io"  # Your Confluence email
CONFLUENCE_SPACE_KEY = "BE" 
# CONFLUENCE_PAGE_IDS = "887816193"  # Optionally keep for single-page test

# Pipeline parameters
PIPELINE_PARAMS = {
    "project_id": PROJECT_ID,
    "location": LOCATION,
    "confluence_domain": CONFLUENCE_DOMAIN,
    "confluence_email": CONFLUENCE_EMAIL,
    "confluence_token": CONFLUENCE_TOKEN,
    "confluence_space_key": CONFLUENCE_SPACE_KEY,
    # "confluence_page_ids": CONFLUENCE_PAGE_IDS,  # Optionally keep for single-page test
    "is_incremental": True,
    "chunk_size": 1500,
    "chunk_overlap": 20,
    "data_store_region": "us",  # Discovery Engine multi-region
    "data_store_id": "my-agentic-rag-datastore",  # Using existing datastore
}

# Initialize Vertex AI
aiplatform.init(project=PROJECT_ID, location=LOCATION)

# Compile the pipeline
compiler.Compiler().compile(
    pipeline_func=pipeline,
    package_path="pipeline.json"
)

# Submit the pipeline
job = aiplatform.PipelineJob(
    display_name="confluence-ingestion-pipeline",
    template_path="pipeline.json",
    pipeline_root=f"gs://{PROJECT_ID}-pipeline-root",  # You'll need to create this bucket
    parameter_values=PIPELINE_PARAMS
)

# Run the pipeline
job.run()

print("\nPipeline submitted successfully!")
print("You can monitor the pipeline in the Vertex AI Pipelines section of the Google Cloud Console.") 