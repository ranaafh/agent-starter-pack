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
	# Export dependencies to requirements file using uv export.
	uv export --no-hashes --no-header --no-dev --no-emit-project --no-annotate --frozen > .requirements.txt 2>/dev/null || \
	uv export --no-hashes --no-header --no-dev --no-emit-project --frozen > .requirements.txt && \
	uv run app/agent_engine_app.py --set-env-vars "DATA_STORE_ID=my-agentic-rag-datastore,DATA_STORE_REGION=us"

deploy-ui:
	PROJECT_ID=$$(gcloud config get-value project) && \
	gcloud builds submit --config cloudbuild.yaml --substitutions=_PROJECT_ID=$$PROJECT_ID .

setup-dev-env:
	PROJECT_ID=$$(gcloud config get-value project) && \
	(cd deployment/terraform/dev && terraform init && terraform apply --var-file vars/env.tfvars --var dev_project_id=$$PROJECT_ID --auto-approve)

data-ingestion:
	PROJECT_ID=$$(gcloud config get-value project) && \
	(cd data_ingestion && uv run data_ingestion_pipeline/submit_pipeline.py \
		--project-id=$$PROJECT_ID \
		--region="us-central1" \
		--data-store-id="my-confluence-agent-datastore" \
		--data-store-region="us" \
		--service-account="my-confluence-agent-rag@$$PROJECT_ID.iam.gserviceaccount.com" \
		--pipeline-root="gs://$$PROJECT_ID-my-confluence-agent-rag" \
		--pipeline-name="data-ingestion-pipeline")

lint:
	uv run codespell
	uv run ruff check . --diff
	uv run ruff format . --check --diff
	uv run mypy .
