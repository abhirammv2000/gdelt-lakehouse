# GDELT Lakehouse — developer entrypoints
.DEFAULT_GOAL := help
.PHONY: help install fmt lint test up down logs ingest backfill dbt-build clean

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

up: ## Start the local stack (MinIO, Airflow, Spark, DuckDB, Redpanda, Marquez)
	docker compose up -d

down: ## Stop the local stack
	docker compose down

logs: ## Tail all container logs
	docker compose logs -f

ingest: ## Ingest the latest GDELT update into bronze
	gdelt ingest latest

backfill: ## Backfill a window, e.g. make backfill FROM=2026-07-20 TO=2026-07-21
	gdelt backfill --start $(FROM) --end $(TO)

dbt-build: ## Run dbt build (models + tests) in the gold layer
	cd dbt && dbt build

clean: ## Remove caches and local warehouse artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache spark-warehouse metastore_db derby.log
	find . -type d -name __pycache__ -exec rm -rf {} +
