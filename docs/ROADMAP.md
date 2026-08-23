# Roadmap

Built in verifiable increments - each phase runs and is tested before the next.

| Phase | Scope | Status |
|---|---|---|
| **0** | Repo scaffold: package layout, `pyproject`, ruff/mypy/pre-commit, Makefile, README | done |
| **1** | GDELT ingestion: poll feed, MD5-verify, idempotent checkpointing, land raw to bronze (S3/MinIO); CLI `gdelt`; unit + integration tests | done |
| **2** | Local stack via Docker Compose: MinIO, Iceberg REST, Spark, Redpanda, Marquez, Airflow | done |
| **3** | PySpark bronze->silver: parse 61-col schema, type/clean/dedup, write **Iceberg** table + data-quality gate | done |
| **4** | dbt gold layer: star schema (`fact_events` + `dim_actor`/`dim_geography`/`dim_date`/`dim_cameo_event`), tests, docs | done |
| **5** | Airflow DAGs: 15-min incremental + parameterized backfill, wiring ingest->spark->dbt with retries/SLAs | done |
| **6** | Data quality gates + streaming: Kafka/Redpanda producer & consumer for the live event stream | done, later removed |
| **7** | Terraform (AWS: S3, Glue/Iceberg, IAM), GitHub Actions CI, OpenLineage observability, polished README | done |

Phase 6's streaming half was built, ran, and was then deliberately removed: the
batch pipeline never depended on it and its one real payoff, the alerts topic, had
no consumer. Why, in [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md#why-is-there-no-kafka-when-this-used-to-have-one).

## Design principles

- **Idempotent & retry-safe** at every stage (checkpoints, existence checks, Iceberg MERGE).
- **Same code, two targets** - environment config (endpoints, credentials, catalog type) swaps MinIO/DuckDB/REST for S3/BigQuery/Glue; the code stays the same.
- **Everything tested** - no phase is "done" until it runs and has a test.
- **Medallion** bronze -> silver -> gold with clear contracts between layers.
