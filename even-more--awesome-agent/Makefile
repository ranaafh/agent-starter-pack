install:
	@command -v uv >/dev/null 2>&1 || { echo "uv is not installed. Installing uv..."; curl -LsSf https://astral.sh/uv/0.6.12/install.sh | sh; source ~/.bashrc; }
	uv sync --dev --extra jupyter --frozen

test:
	uv run pytest tests/unit && uv run pytest tests/integration

playground:
	@echo "\033[1;32m===============================================================================\033[0m"
	@echo "\033[1;32m| Starting ADK Web Server via 'adk web' command.                              |\033[0m"
	@echo "\033[1;32m|                                                                             |\033[0m"
	@echo "\033[1;32m| IMPORTANT: Select the 'app' folder to interact with your agent.             |\033[0m"
	@echo "\033[1;32m===============================================================================\033[0m"
	uv run adk web --port 8501

backend:
	PROJECT_ID=$$(gcloud config get-value project) && \
	gcloud run deploy even-more--awesome-agent \
		--source . \
		--memory "4Gi" \
		--project $$PROJECT_ID \
		--region "us-central1" \
		--no-allow-unauthenticated \
		--labels "created-by=adk" \
		--set-env-vars \
		"COMMIT_SHA=$(shell git rev-parse HEAD),DATA_STORE_ID=my-agentic-rag-datastore,DATA_STORE_REGION=us"

local-backend:
	uv run uvicorn app.server:app --host 0.0.0.0 --port 8000 --reload

setup-dev-env:
	PROJECT_ID=$$(gcloud config get-value project) && \
	(cd deployment/terraform/dev && terraform init && terraform apply --var-file vars/env.tfvars --var dev_project_id=$$PROJECT_ID --auto-approve)

data-ingestion:
	PROJECT_ID=$$(gcloud config get-value project) && \
	(cd data_ingestion && uv run data_ingestion_pipeline/submit_pipeline.py \
		--project-id=$$PROJECT_ID \
		--region="us-central1" \
		--data-store-id="even-more--awesome-agent-datastore" \
		--data-store-region="us" \
		--service-account="even-more--awesome-agent-rag@$$PROJECT_ID.iam.gserviceaccount.com" \
		--pipeline-root="gs://$$PROJECT_ID-even-more--awesome-agent-rag" \
		--pipeline-name="data-ingestion-pipeline")

lint:
	uv run codespell
	uv run ruff check . --diff
	uv run ruff format . --check --diff
	uv run mypy .
