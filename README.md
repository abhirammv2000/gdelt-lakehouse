# GDELT Global Events Lakehouse

[![CI](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml)

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
╔═ SILVER  (Apache Iceberg on S3) ═╗   typed, ACID, time-travel · data-quality checks
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
| Data quality | **PySpark expectation gate** + dbt tests | — |
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

# 5. Transform bronze -> silver (typed, deduped Iceberg table)
make silver

# 6. Build the gold star schema + run dbt tests
make dbt-build

# 7. Run the tests
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

### Bronze → silver (PySpark + Iceberg)

The silver layer turns the raw bronze zips into a typed, deduplicated **Iceberg**
table via a PySpark job:

```bash
make ingest         # land a raw GDELT batch into bronze (MinIO)
make spark-test     # run the Spark unit tests inside the container
make silver         # bronze -> silver: parse 61 cols, type/clean/dedup, DQ gate, MERGE
```

What the job does, in one pass:

- **Reads** every `*.CSV.zip` under the bronze prefix with boto3 (no `s3a` needed),
  unzips and splits the tab-delimited 61-column rows on the executors.
- **Types & cleans** each column from the authoritative `EVENT_COLUMNS` schema —
  empty strings become `NULL`, dates/timestamps are parsed, numerics cast.
- **De-duplicates** on `global_event_id`, keeping the most recently added record.
- **Validates** a data-quality suite (unique/not-null keys, `quad_class ∈ {1..4}`,
  Goldstein/tone/geo ranges); `error`-severity failures abort before any write.
- **MERGEs** into `lakehouse.gdelt.events`, partitioned by `days(sql_date)`. The
  merge keys on the event id and only overwrites with newer records, so re-running
  the job on the same bronze data is a **no-op** — a new Iceberg snapshot with zero
  net row change. Query `lakehouse.gdelt.events.snapshots` to see the history.

## Repository layout

```
src/gdelt_pipeline/       Python package
  config.py                 env-driven settings (local ⇄ aws)
  schema/events.py          authoritative 61-column GDELT event schema
  ingestion/                poll · download · verify · land · checkpoint  (CLI: `gdelt`)
docker/                    custom Airflow + Spark images             (Phase 2-3)
docker-compose.yml         local lakehouse stack                     (Phase 2)
spark/                     PySpark bronze→silver Iceberg job         (Phase 3)
  gdelt_spark/               read (boto3+unzip) · transform · validate · iceberg
  jobs/bronze_to_silver.py   spark-submit entrypoint (idempotent MERGE)
  tests/                     Spark unit tests (run in-container)
dbt/                       gold-layer star schema + tests            (Phase 4)
  models/staging/            stg_events (reads the Iceberg silver table)
  models/marts/              fact_events + dim_date/actor/geography/cameo_event
  seeds/                     CAMEO root-code lookup
airflow/dags/              incremental + backfill DAGs               (Phase 5)
  gdelt_incremental.py       15-min ingest→spark→dbt + parallel stream publish
  gdelt_backfill.py          parameterized window backfill
  gdelt_maintenance.py       weekly Iceberg compaction / snapshot expiry
  gdelt_lib.py               execs Spark jobs in the Spark container
src/gdelt_pipeline/streaming/  Kafka producer/consumer + DQ gate       (Phase 6)
  producer.py                fan a bronze batch onto gdelt.events.raw
  consumer.py                DQ-gate → dead-letter / alert (consumer group)
terraform/                 AWS infra: S3 + Glue Iceberg + IAM         (Phase 7)
.github/workflows/ci.yml   CI: ruff · mypy · pytest · tf validate     (Phase 7)
tests/                     unit + integration tests
```

## Observability & lineage

Every Airflow task emits **OpenLineage** events to **Marquez** (http://localhost:3000),
so each run's jobs and datasets — and their upstream/downstream edges — are
captured automatically. Query the lineage API, e.g. the jobs in the namespace:

```bash
curl -s localhost:5000/api/v1/namespaces/gdelt-lakehouse/jobs
```

## Engineering practices

- **Idempotency** — checkpointing + object-existence checks make every DAG run
  safe to retry; re-ingesting a batch is a no-op (proven by test).
- **Data integrity** — MD5 verification on every downloaded file.
- **Schema-drift handling** — a 61-field contract quarantines non-conformant rows to a
  rejects table, fails fast on a malformed-rate spike, and absorbs additive drift via
  Iceberg schema evolution (never silently corrupts good data).
- **Medallion architecture** — bronze (raw) → silver (typed/clean Iceberg) → gold (modeled).
- **Testing** — unit tests with mocked HTTP (`respx`) and mocked S3 (`moto`); Spark tests; dbt tests.
- **Type safety & linting** — `ruff` and `mypy --strict`, enforced via `pre-commit` and CI.
- **Observability** — structured JSON logs and run-level lineage (OpenLineage → Marquez).

## Status

Built in phases (see [`docs/ROADMAP.md`](docs/ROADMAP.md)); the full local stack
runs and is tested end to end, and the same code targets AWS via `GDELT_ENV=aws`.
Design rationale and trade-offs are in
[`docs/DESIGN_DECISIONS.md`](docs/DESIGN_DECISIONS.md).

## License

MIT
