# GDELT Global Events Lakehouse

A production-grade, end-to-end data engineering pipeline that ingests the
[**GDELT 2.0**](https://www.gdeltproject.org/) global events feed — a new batch of
world news events published **every 15 minutes** — and turns it into a queryable,
tested, well-modeled analytics lakehouse.

> **Why GDELT and not the NYC Taxi dataset?** The taxi dataset is the "hello world"
> of data engineering — clean, static, and in every tutorial. GDELT is *large*,
> *messy* (tab-delimited, no headers, 61 columns, CAMEO codes, nulls, encoding
> quirks), and *continuously arriving*, which is what lets this project demonstrate
> real incremental ingestion, idempotency, schema evolution, data quality, and
> streaming — the things production data platforms actually deal with.

---

## Architecture

```
GDELT 2.0 feed (new batch every 15 min)
   │  lastupdate.txt / masterfilelist.txt → *.export.CSV.zip
   ▼
┌── INGESTION (Python) ──────────────┐   Airflow: 15-min incremental DAG + backfill DAG
│  poll · md5-verify · idempotent    │   OpenLineage events emitted at every hop
│  checkpoint · land raw             │──────► Kafka / Redpanda (live event stream)
└────────────────────────────────────┘                     │
   ▼ raw zips                                                ▼ streaming consumer
╔═ BRONZE  (S3 / MinIO) ═╗   raw CSV, partitioned dt=/hour=
   ▼  PySpark: parse 61-col schema · type · clean · dedup
╔═ SILVER  (Apache Iceberg on S3) ═╗   typed, ACID, time-travel · Great Expectations checks
   ▼  load
╔═ GOLD  (Snowflake / DuckDB) ═╗   dbt star schema:
      fact_events · dim_actor · dim_geography · dim_date · dim_cameo_event  (+ marts)
   ▼
   dbt tests + docs · lineage graph (Marquez) · analytics
```

## Stack

| Concern | Technology | Local dev stand-in |
|---|---|---|
| Ingestion | Python 3.11, httpx, tenacity, typer | — |
| Object storage | AWS S3 | MinIO |
| Table format | **Apache Iceberg** (ACID, time-travel, schema evolution) | Iceberg REST catalog |
| Processing | **PySpark** | Spark in Docker |
| Orchestration | **Apache Airflow** | Airflow in Docker |
| Warehouse | **Snowflake** | DuckDB |
| Transformation | **dbt** (dimensional / star schema) | dbt-duckdb |
| Data quality | **Great Expectations** + dbt tests | — |
| Streaming | **Kafka** | Redpanda |
| Lineage | **OpenLineage** → Marquez | Marquez in Docker |
| IaC | **Terraform** (AWS) | — |
| CI/CD | **GitHub Actions** | — |

Everything runs locally with `docker compose up` (no cloud account needed) and
deploys to AWS via Terraform — the same code, switched by a single `GDELT_ENV` flag.

## Quickstart

```bash
# 1. Install the package + dev tooling
make install

# 2. Bring up the local lakehouse (MinIO, Airflow, Spark, DuckDB, Redpanda, Marquez)
make up

# 3. Ingest the latest 15-minute GDELT batch into the bronze layer
make ingest

# 4. Backfill a historical window
make backfill FROM=2026-07-20 TO=2026-07-21

# 5. Run the tests
make test
```

### Local stack services

`make up` builds the Airflow image and starts everything (`make ps` shows health).
Once up, these are reachable on the host:

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Redpanda Console | http://localhost:8080 | — |
| Spark master UI | http://localhost:8082 | — |
| Spark / Jupyter | http://localhost:8888 | — |
| Marquez (lineage) UI | http://localhost:3000 | — |
| Iceberg REST catalog | http://localhost:8181 | — |

The bronze/silver MinIO buckets are created automatically on startup. Trigger the
`stack_healthcheck` DAG in Airflow to smoke-test that the pipeline package is
importable and object storage is reachable. DuckDB is an embedded library (used by
dbt in Phase 4), so it has no container of its own.

## Repository layout

```
src/gdelt_pipeline/       Python package
  config.py                 env-driven settings (local ⇄ aws)
  schema/events.py          authoritative 61-column GDELT event schema
  ingestion/                poll · download · verify · land · checkpoint  (CLI: `gdelt`)
docker/airflow/            custom Airflow image (deps + providers)   (Phase 2)
docker-compose.yml         local lakehouse stack                     (Phase 2)
spark/                     PySpark bronze→silver Iceberg jobs        (Phase 3)
dbt/                       gold-layer star schema + tests            (Phase 4)
airflow/dags/              incremental + backfill DAGs               (Phase 5)
great_expectations/        data-quality suites                       (Phase 6)
streaming/                 Kafka producer/consumer                   (Phase 6)
terraform/                 AWS infrastructure                        (Phase 7)
.github/workflows/         CI/CD                                     (Phase 7)
tests/                     unit + integration tests
```

## Engineering practices on display

- **Idempotency** — checkpointing + object-existence checks make every DAG run
  safe to retry; re-ingesting a batch is a no-op (proven by test).
- **Data integrity** — MD5 verification on every downloaded file.
- **Medallion architecture** — bronze (raw) → silver (typed/clean Iceberg) → gold (modeled).
- **Testing** — unit tests with mocked HTTP (`respx`) and mocked S3 (`moto`); dbt tests; DQ suites.
- **Type safety & linting** — `ruff`, `mypy --strict`, `sqlfluff`, enforced via `pre-commit` and CI.
- **Observability** — structured JSON logs and end-to-end column-level lineage.

## Build status

Built in phases — see the roadmap in [`docs/ROADMAP.md`](docs/ROADMAP.md).
Phases 0 (scaffold), 1 (ingestion), and 2 (local Docker Compose stack) are complete.

## License

MIT
