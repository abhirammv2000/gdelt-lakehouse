# GDELT Lakehouse — developer entrypoints
.DEFAULT_GOAL := help
.PHONY: help install fmt lint test build up down ps logs ingest backfill spark-build spark-test silver maintain dbt-build dbt-docs clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install the package with dev + spark + stream extras
	pip install -e ".[dev,spark,stream]"
	pre-commit install

fmt: ## Auto-format code
	ruff format src tests
	ruff check --fix src tests

lint: ## Lint + type-check
	ruff check src tests
	mypy src

test: ## Run the test suite with coverage
	pytest

build: ## Build the custom Airflow image
	docker compose build

up: ## Start the local stack (MinIO, Iceberg, Spark, Redpanda, Marquez, Airflow)
	docker compose up -d

down: ## Stop the local stack (add v=1 to also drop volumes: make down v=1)
	docker compose down $(if $(v),--volumes,)

ps: ## Show container status / health
	docker compose ps

logs: ## Tail all container logs
	docker compose logs -f

ingest: ## Ingest the latest GDELT update into bronze
	gdelt ingest latest

backfill: ## Backfill a window, e.g. make backfill FROM=2026-07-20 TO=2026-07-21
	gdelt backfill --start $(FROM) --end $(TO)

spark-build: ## Build the custom Spark image (boto3 + pytest)
	docker compose build spark-iceberg

spark-test: ## Run the Spark unit tests inside the container
	docker compose exec -T spark-iceberg bash -lc "cd /home/iceberg/work && python -m pytest tests -q"

silver: ## Run bronze->silver Iceberg job, e.g. make silver FEED=export
	docker compose exec -T spark-iceberg bash -c 'spark-submit /home/iceberg/work/jobs/bronze_to_silver.py --feed $(if $(FEED),$(FEED),export)'

maintain: ## Compact + expire snapshots on the silver Iceberg table
	docker compose exec -T spark-iceberg bash -c 'spark-submit /home/iceberg/work/jobs/maintain_silver.py'

dbt-build: ## Build the gold star schema + run dbt tests (reads silver Iceberg)
	dbt build --project-dir dbt --profiles-dir dbt

dbt-docs: ## Generate + serve the dbt docs site for the gold layer
	dbt docs generate --project-dir dbt --profiles-dir dbt && dbt docs serve --project-dir dbt --profiles-dir dbt

clean: ## Remove caches and local warehouse artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache spark-warehouse metastore_db derby.log
	find . -type d -name __pycache__ -exec rm -rf {} +
