# GDELT Global Events Lakehouse

[![CI](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml)

An end-to-end data pipeline for the GDELT 2.0 global events feed. GDELT publishes a
new batch of world-news events every 15 minutes as tab-delimited files with no
header, 61 columns, and the usual real-world mess (nulls, encoding issues, the
occasional malformed row). This project ingests that feed, cleans and models it,
and serves a tested star schema.

The same code runs locally (MinIO, DuckDB, Redpanda) or on AWS (S3, Glue,
Snowflake). What changes between them is configuration - endpoints, credentials,
and the catalog type - read from the environment, not the code.

## Architecture

A bronze / silver / gold medallion layout:

- Ingestion (Python): poll the feed, verify the MD5 checksum, land the raw zips in
  the bronze bucket, and checkpoint so re-runs are safe.
- Bronze to silver (PySpark + Apache Iceberg): parse the 61 columns, cast and
  clean, drop duplicates, and MERGE into an Iceberg table. Rows that do not match
  the 61-field contract go to a rejects table instead of being forced into the data.
- Silver to gold (dbt): a star schema (`fact_events` plus date, actor, geography,
  and cameo-event dimensions) built in DuckDB locally or Snowflake on AWS, with
  dbt tests.
- Orchestration (Airflow): a 15-minute incremental DAG, a backfill DAG, and a
  weekly Iceberg maintenance DAG.
- Streaming (Kafka/Redpanda): a producer publishes each batch to a topic; a
  consumer validates each event and routes bad or high-impact records to separate
  topics.
- Lineage: Airflow emits OpenLineage events to Marquez.

## Stack

| Layer | Local | AWS |
|---|---|---|
| Object storage | MinIO | S3 |
| Table format | Apache Iceberg (REST catalog) | Apache Iceberg (Glue catalog) |
| Processing | PySpark | PySpark |
| Warehouse | DuckDB | Snowflake |
| Transform | dbt | dbt |
| Orchestration | Airflow | Airflow / MWAA |
| Streaming | Redpanda | Kafka |
| Infrastructure | Terraform | Terraform |

## Quickstart

```bash
make install     # install the package and dev tooling
make up          # start the local stack
make ingest      # ingest the latest 15-minute GDELT batch into bronze
make silver      # bronze to silver: parse, clean, dedup, quality checks, MERGE
make dbt-build   # build the gold star schema and run dbt tests
make test        # run the tests
```

Backfill a date range:

```bash
make backfill FROM=2026-07-20 TO=2026-07-21
```

### Local service URLs

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Redpanda Console | http://localhost:8080 | |
| Spark UI | http://localhost:8082 | |
| Marquez (lineage) | http://localhost:3000 | |
| Iceberg REST catalog | http://localhost:8181 | |

## Repository layout

```
src/gdelt_pipeline/   Python package: config, schema, ingestion CLI, streaming
spark/                PySpark bronze-to-silver job and unit tests
dbt/                  gold star schema (models, seeds, tests)
airflow/dags/         incremental, backfill, and maintenance DAGs
docker/               custom Airflow and Spark images
docker-compose.yml    local stack
terraform/            AWS infrastructure (S3, Glue, IAM)
.github/workflows/    CI (ruff, mypy, pytest, terraform validate)
docs/                 design decisions and roadmap
```

## Notes

- Design decisions and trade-offs: [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md)
- How it was built, phase by phase: [docs/ROADMAP.md](docs/ROADMAP.md)

## License

MIT
