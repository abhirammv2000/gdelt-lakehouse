# Roadmap

Built in verifiable increments — each phase runs and is tested before the next.

| Phase | Scope | Status |
|---|---|---|
| **0** | Repo scaffold: package layout, `pyproject`, ruff/mypy/pre-commit, Makefile, README | ✅ Done |
| **1** | GDELT ingestion: poll feed, MD5-verify, idempotent checkpointing, land raw to bronze (S3/MinIO); CLI `gdelt`; unit + integration tests | ✅ Done |
| **2** | Local stack via Docker Compose: MinIO, Iceberg REST, Spark, Redpanda, Marquez, Airflow | ✅ Done |
| **3** | PySpark bronze→silver: parse 61-col schema, type/clean/dedup, write **Iceberg** table + data-quality gate | ✅ Done |
| **4** | dbt gold layer: star schema (`fact_events` + `dim_actor`/`dim_geography`/`dim_date`/`dim_cameo_event`), tests, docs | ✅ Done |
| **5** | Airflow DAGs: 15-min incremental + parameterized backfill, wiring ingest→spark→dbt with retries/SLAs | ✅ Done |
| **6** | Data quality gates + streaming: Kafka/Redpanda producer & consumer for the live event stream | ✅ Done |
| **7** | Terraform (AWS: S3, Glue/Iceberg, IAM, MWAA/Snowflake), GitHub Actions CI/CD, OpenLineage observability, polished README | ⏳ Next |

## Design principles

- **Idempotent & retry-safe** at every stage (checkpoints, existence checks, Iceberg MERGE).
- **Same code, two targets** — `GDELT_ENV=local|aws` swaps MinIO/DuckDB for S3/Snowflake.
- **Everything tested** — no phase is "done" until it runs and has a test.
- **Medallion** bronze → silver → gold with clear contracts between layers.
