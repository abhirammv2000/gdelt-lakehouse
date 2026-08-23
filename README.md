# GDELT Global Events Lakehouse

[![CI](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/abhirammv2000/gdelt-lakehouse/actions/workflows/ci.yml)

An end-to-end lakehouse for the GDELT 2.0 global events feed, built with the patterns
you would use in production and run locally on Docker. GDELT publishes a new batch of
world-news events every 15 minutes as tab-delimited files with no header, 61 columns,
and the usual real-world mess (nulls, encoding quirks, the occasional malformed row).
This project ingests that feed, cleans and models it, and serves a tested star schema.

The same code runs against the local stack or against cloud services. What changes is
configuration (endpoints, credentials, catalog type) read from the environment, not
the code.

## Architecture

A bronze / silver / gold medallion layout:

- Ingestion (Python): poll the feed, verify the MD5 checksum, land the raw zips in
  the bronze bucket, and checkpoint so re-runs are safe.
- Bronze to silver (PySpark + Apache Iceberg): parse the 61 columns, cast and
  clean, drop duplicates, and MERGE into an Iceberg table. Rows that do not match
  the 61-field contract go to a rejects table instead of being forced into the data.
- Silver to gold (dbt): a star schema (`fact_events` plus date, actor, geography,
  and cameo-event dimensions) with 19 dbt tests. The same models run on DuckDB
  locally and on BigQuery (`dbt build --target bq`).
- Orchestration (Airflow): a 15-minute incremental DAG, a backfill DAG, and a
  weekly Iceberg maintenance DAG.
- Streaming (Kafka/Redpanda): a producer publishes each batch to a topic; a
  consumer validates each event and routes bad or high-impact records to separate
  topics.
- Lineage: Airflow emits OpenLineage events to Marquez.

## Stack

| Layer | Local | Cloud |
|---|---|---|
| Object storage | MinIO | AWS S3 |
| Table format | Apache Iceberg (REST catalog) | Apache Iceberg (AWS Glue catalog) |
| Processing | PySpark | PySpark |
| Warehouse | DuckDB | Athena (AWS) or BigQuery |
| Transform | dbt | dbt |
| Orchestration | Airflow | Airflow (MWAA not run) |
| Streaming | Redpanda | Kafka (not run) |
| Infrastructure | Docker Compose | Terraform (S3, Glue, IAM) |

Everything in the Local column runs with `make up`. The whole pipeline has also been
run on AWS as one continuous chain: ingestion into S3, the Spark bronze-to-silver job
writing Iceberg through the Glue catalog, and the dbt gold models built by Athena
reading that same Glue table, so gold is created in place with no data movement. The
same models also build on BigQuery (`--target bq`).

Two honest limits. Spark, dbt, and Airflow run as local processes pointed at AWS
rather than on EMR and MWAA, which are a deployment target rather than a code change
but have not been run. Managed Kafka has not been run either. The AWS resources are
created with Terraform and destroyed after each run, so nothing is standing.

## Results

Numbers below are from an actual run, not estimates. Reproduce with the Quickstart.

**Volume and runtime** (laptop, Docker Desktop, single Spark container)

| Stage | Work | Wall time |
|---|---|---|
| Ingest one batch | 1 zip, 54 KB | 8.3 s |
| Bronze to silver | 9 batch files, 10,163 rows parsed, deduped, quality-gated, merged | 39.9 s |
| dbt build (gold) | 7 models, 1 snapshot, 1 seed, 19 tests | 15.4 s |

A single 15-minute GDELT batch held 614 to 2,743 events (median 888) across the nine
batches loaded. Spark time is dominated by JVM startup at this size; the job is built
for the shape of the work, not this volume (see
[docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md#7-honest-answers-to-the-obvious-questions)).

**Data quality**: 10,163 of 10,163 rows matched the 61-field contract (0 quarantined),
and all 9 silver expectations plus all 19 dbt tests passed (`PASS=28 ERROR=0`).

**Gold star schema**: 10,163 facts, 546 actors, 1,134 geographies, 150 event types,
23 dates.

![Events by CAMEO event type](docs/images/gold_top_event_types.png)

Three queries against the gold layer:

```sql
-- 1. Conflict vs cooperation mix
select c.quad_class_name, count(*) events,
       round(avg(f.goldstein_scale), 2) avg_goldstein
from fact_events f join dim_cameo_event c using (cameo_key)
group by 1 order by events desc;
```

| quad_class_name | events | % | avg_goldstein |
|---|---|---|---|
| Verbal Cooperation | 5,381 | 52.9 | 2.01 |
| Material Cooperation | 1,741 | 17.1 | 5.48 |
| Verbal Conflict | 1,609 | 15.8 | -3.39 |
| Material Conflict | 1,432 | 14.1 | -8.34 |

The Goldstein scale runs +5.48 for material cooperation down to -8.34 for material
conflict, which is what CAMEO defines it to do. That the dimension join reproduces it
is a useful check that the keys are right.

```sql
-- 2. Where events happen, and how negative the coverage is
select g.country_code, count(*) events, round(avg(f.avg_tone), 2) avg_tone
from fact_events f join dim_geography g using (geo_key)
group by 1 order by events desc limit 5;
```

| country_code | events | avg_tone |
|---|---|---|
| US | 4,413 | -2.24 |
| IN | 514 | -3.46 |
| IS | 380 | -3.46 |
| UK | 332 | -1.75 |
| NI | 324 | -0.82 |

```sql
-- 3. Most common event types (the chart above)
select c.root_description, count(*) events, round(avg(f.avg_tone), 2) avg_tone
from fact_events f join dim_cameo_event c using (cameo_key)
group by 1 order by events desc limit 3;
```

| root_description | events | avg_tone |
|---|---|---|
| Consult | 2,467 | -1.44 |
| Make Public Statement | 1,621 | -2.25 |
| Engage in Diplomatic Cooperation | 818 | 0.16 |

Regenerate the chart with `python scripts/plot_top_event_types.py`.

### The same pipeline on AWS

Run end to end against real AWS, with Terraform creating the buckets, the Glue
database, and a least-privilege IAM role first.

| Stage | Where it ran | Result |
|---|---|---|
| Ingest | S3 bronze bucket | 1 batch landed, 18 KB |
| Bronze to silver | Spark, Iceberg on the Glue catalog | 317 rows, 0 non-conformant, 9/9 quality checks |
| Gold | dbt via Athena on the same Glue tables | `PASS=28 WARN=0 ERROR=0` |

All five gold tables register in Glue as `table_type=ICEBERG` backed by S3, and the
quad-class query reproduces the same Goldstein polarity as the local run (+5.79 for
material cooperation, -7.85 for material conflict) on independently ingested data.

The gold models therefore run unchanged on three warehouses: DuckDB, BigQuery, and
Athena. The dialect differences (hashing, date formatting, incremental strategy) go
through adapter-dispatched macros in `dbt/macros/`.

**Orchestration and lineage.** The `gdelt_incremental` DAG runs ingest, bronze to
silver, and dbt build, with the stream publish in parallel. Airflow emits OpenLineage
events to Marquez, which records 12 jobs across the three DAGs with their run states.

<!-- Screenshots: run `make up`, open the two URLs below, and save the images to these
     paths, then delete this comment and uncomment the two lines under it.
       Airflow graph view  http://localhost:8081  ->  docs/images/airflow_dag.png
       Marquez lineage     http://localhost:3000  ->  docs/images/marquez_lineage.png -->
<!-- ![gdelt_incremental DAG in Airflow](docs/images/airflow_dag.png) -->
<!-- ![Job lineage in Marquez](docs/images/marquez_lineage.png) -->

### Is Spark even the right tool here?

Probably not, at this volume, and the numbers say so rather than my opinion. Running
the same bronze-to-silver transform over the same files, one machine, median of three:

| Input rows | DuckDB | Polars | PySpark |
|---|---|---|---|
| 2,278 (one 15-minute batch) | **0.26 s** | 0.40 s | 14.06 s |
| 25,017 (six hours) | **0.98 s** | 1.99 s | 18.48 s |
| 103,858 (one day) | **1.39 s** | 8.25 s | 22.49 s |

![Engine benchmark](docs/images/benchmark_engines.png)

Spark loses at every scale measured, with no crossover in sight: about 14 seconds of
that is fixed JVM start-up, paid on every scheduled run. All three engines produce
identical output row counts, which is the check that makes the comparison meaningful.
Method, caveats, and what I would change are in
[docs/BENCHMARK.md](docs/BENCHMARK.md).

### The failure modes are tested, not just claimed

The failure-mode table in the design doc is backed by tests that trigger each
failure on purpose: a corrupt file that fails its MD5 while the rest of the batch
still lands, a partial batch that re-runs and fetches only what is missing, a
mixed batch of truncated and over-wide rows, a phantom 62nd column (and the
historical trailing tab that must *not* be mistaken for one), a replayed batch
that tries to move the checkpoint backwards.

```bash
pytest tests/test_failure_modes.py     # 7 passed
```

These run in CI on every push. Four rows in that table have no automated proof and
are labelled as such rather than left to look covered.

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

The window is capped at 30 days (`GDELT_MAX_BACKFILL_DAYS`) so a mistyped range can't
turn one run into a load against GDELT's full archive back to 2015. Widening it is a
config change, not something a trigger typo can do by accident.

Build the same gold models on BigQuery:

```bash
gcloud auth application-default login
dbt build --target bq --project-dir dbt --profiles-dir dbt
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
dbt/                  gold star schema (models, seeds, snapshots, tests)
airflow/dags/         incremental, backfill, and maintenance DAGs
scripts/              chart generation for the README
docker/               custom Airflow and Spark images
docker-compose.yml    local stack
terraform/            AWS infrastructure (S3, Glue, IAM)
.github/workflows/    CI (ruff, mypy, pytest, terraform validate)
docs/                 design decisions and roadmap
```

## Notes

- Design decisions, trade-offs, and honest answers to the obvious questions:
  [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md)
- How it was built, phase by phase: [docs/ROADMAP.md](docs/ROADMAP.md)

## License

MIT
