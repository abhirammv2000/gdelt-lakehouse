# Design decisions and trade-offs

Notes on why the project is built the way it is: the main choices, what I
considered instead, how it fails and recovers, how it would scale, and what it
does not do yet.

## 1. The dataset

GDELT 2.0 publishes a batch of world-news events every 15 minutes. The files are
tab-delimited, have no header, and carry 61 fixed columns, with plenty of nulls,
CAMEO codes, and encoding quirks. It is a good dataset to build on because it is
messy and always arriving, so it needs real incremental ingestion, idempotency,
schema handling, data quality, and streaming rather than a one-off load. One row
is one event.

## 2. Architecture

Ingest (Python) -> bronze (raw zips in S3/MinIO) -> PySpark -> silver (Iceberg,
typed and deduped) -> dbt -> gold (star schema in DuckDB/Snowflake). Airflow runs
the schedule, a Kafka/Redpanda path runs off ingestion for the streaming side, and
Marquez collects lineage. Moving to AWS is a change of configuration, not code:
real S3 endpoints and the Glue catalog in place of MinIO and the REST catalog.

The layers follow the medallion pattern. Bronze is the raw feed, unchanged. Silver
is typed, cleaned, and deduped. Gold is modeled for querying. Keeping them separate
means I only reprocess downstream of wherever data changed, and each layer can be
inspected on its own.

## 3. Component choices

### Ingestion (Python, httpx, tenacity)
- Idempotent: before landing a file it checks whether the object already exists,
  and after landing it records the last timestamp per feed. Re-running does
  nothing, which matters because the 15-minute schedule needs to be safe to retry.
- MD5 from GDELT's index is checked on every download.
- One bad file is logged and skipped instead of failing the whole batch.
- Trade-off: backfill reads GDELT's full `masterfilelist.txt` and filters it in
  Python, which is simple but slow (tens of minutes). A bigger system would cache
  that list or list by date prefix.

### Object storage: S3, with MinIO locally
One storage layer (`fsspec`/`s3fs`) talks to MinIO locally and S3 on AWS. The only
difference is the endpoint and credentials, read from the environment. I did not
want two code paths for storage, and this keeps the local and AWS runs identical.

### Bronze to silver: PySpark and Apache Iceberg
- Why Iceberg over Hive tables or plain Parquet: it gives ACID commits, snapshots
  (time travel and rollback), hidden partitioning, and schema evolution on top of
  object storage. Plain Parquet has no transactions, and Hive tables have the
  small-file and partition-listing problems Iceberg avoids.
- Idempotent MERGE: `MERGE INTO ... WHEN MATCHED AND source.date_added >=
  target.date_added THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. Running the same
  bronze again changes nothing; a re-published event with a newer `date_added`
  overwrites the old one. This is the main correctness property.
- Dedup: a window over `global_event_id` ordered by `date_added` descending keeps
  the newest record before the MERGE, since GDELT re-publishes events across batches.
- Reading zipped CSV without S3A: the Spark image has Iceberg's `S3FileIO` for
  table data but no `hadoop-aws`/`s3a` filesystem, so Spark can't read `s3a://...zip`
  directly. I list and fetch the objects with boto3 on the executors and unzip in
  Python. The alternative, adding the hadoop-aws and matching aws-sdk jars, means
  fragile version matching; boto3 is lighter and behaves the same locally and on AWS.
- Catalog: the same job writes to the Iceberg REST catalog locally or the Glue Data
  Catalog on AWS, chosen by `GDELT_ICEBERG_CATALOG_TYPE`. Table data goes through
  `S3FileIO` either way, so only the catalog wiring changes between the two targets.
- Partitioning by `days(sql_date)`, because that is how the data gets queried and
  it keeps partitions a reasonable size.
- Schema-drift handling: each row is checked against the 61-field contract (a lone
  trailing tab is normalized first). Rows that don't match go to a `..._rejects`
  table with the raw line and the field count, so they are never padded or shifted
  into the good data. If too many rows fail (default 5%), the job stops with
  `SchemaDriftError` before writing, since a spike usually means GDELT changed its
  layout and a person should look. A new column that GDELT adds is handled with
  `ALTER TABLE ADD COLUMN`, which Iceberg applies without rewriting data.

### Data quality: a write-time gate (silver) and tests (gold)
- Silver gate: a small set of checks (not-null, unique, in-set, between) with
  `error` or `warn` severity, run as one Spark aggregation. An `error` failure stops
  the write before the MERGE, so bad data never reaches silver. I wrote a small
  engine instead of pulling in a bigger framework because it is deterministic, runs
  in one job, and is easy to test.
- Gold tests: dbt tests for uniqueness, not-null, relationships, and accepted
  values. The relationship tests matter most, since they prove every fact foreign
  key points at a real dimension row.

### Gold: dbt star schema on DuckDB (Snowflake on AWS)
- A Kimball star: `fact_events` at one row per event, plus date, actor, geography,
  and cameo-event dimensions. Surrogate keys are `md5(natural_key)` and the same
  macro builds them on both the fact and the dimensions, so the relationship tests
  line up.
- dbt reads the Iceberg silver table directly through the REST catalog (DuckDB
  `iceberg` + `httpfs` extensions). I did not export silver to Parquet for dbt
  because that duplicates the data and breaks the idea that silver is the Iceberg
  table. One quirk: the REST catalog defaults to `oauth2`, so DuckDB needs
  `AUTHORIZATION_TYPE 'none'`.
- `fact_events` is incremental (`delete+insert` on `event_key`, using `date_added`
  as the high-water mark), so each run only handles new events and re-published
  events overwrite. A full rebuild every 15 minutes would not scale.
- `dim_actor` is SCD Type 2 (a dbt snapshot) so actor history is kept.
  `dim_geography` and `dim_cameo_event` are Type-1 reference data.
- A seed maps the 20 CAMEO root codes to readable labels.

### Orchestration: Airflow
- Three DAGs: `gdelt_incremental` (every 15 minutes, `catchup=False`,
  `max_active_runs=1`, retries with backoff, an SLA), `gdelt_backfill` (manual, with
  a start/end window), and a weekly `gdelt_maintenance` DAG for Iceberg upkeep.
- `catchup=False` and `max_active_runs=1` because GDELT is a live feed: I want the
  latest batch, not a backlog of runs racing on the same table.
- The DAGs stay thin. Ingest runs in-process (the package is importable in
  Airflow), Spark is triggered by running `spark-submit` in the Spark container over
  the Docker socket, and dbt runs from its own virtualenv so its dependency versions
  don't clash with Airflow's. Each step is easy to swap later:
  `SparkKubernetesOperator` or `EmrAddStepsOperator` for Spark, MWAA for Airflow.

### Streaming: Kafka/Redpanda
- The producer sends a landed batch to `gdelt.events.raw`, one message per event,
  keyed by country so events for a place land on the same partition, with an
  idempotent `acks=all` producer. A long-running consumer reads it as a consumer
  group with manual offset commits (at-least-once), runs the per-event checks,
  sends failures to a dead-letter topic, and sends high-impact conflict events to an
  alerts topic.
- It is a separate path from the batch pipeline so the streaming pieces are real:
  partitions, consumer groups, offsets, a dead-letter queue, and routing.
- Trade-off: JSON on the wire, which is easy to read and needs few dependencies.
  For production I would switch to Avro with a Schema Registry (Redpanda ships one,
  and the port is already wired).

### Infrastructure, CI, lineage
- Terraform creates versioned, encrypted S3 buckets (public access blocked, old
  versions expired), a Glue database for the Iceberg catalog, and an IAM role scoped
  to just those buckets and that database. It is `fmt`/`validate`-clean.
- GitHub Actions runs ruff, `mypy --strict`, pytest, `docker compose config`, and
  `terraform validate` on every push and PR.
- Airflow emits OpenLineage events to Marquez at the run/job level.

## 4. Failure modes

| Failure | What happens |
|---|---|
| Ingest crashes mid-batch | Checkpoint and existence checks make a re-run safe; landed files are skipped |
| GDELT returns a corrupt file | MD5 mismatch raises, the file is logged as failed, the batch continues |
| Silver job re-run on same data | Recency-guarded MERGE does nothing |
| Bad data in a batch | Silver checks stop the write before MERGE; the stream dead-letters the event |
| A few malformed rows | Sent to the rejects table by the field-count check; good rows still load |
| GDELT changes its column layout | Malformed rate spikes, the job stops before writing; a new column is added with `ALTER TABLE ADD COLUMN` |
| Catalog restart | SQLite-on-a-volume (WAL + busy_timeout) survives; on AWS this is Glue |
| Spark task fails in a DAG | Airflow retries with backoff; the MERGE is safe to repeat |
| Consumer crashes | Offsets are committed only after handling, so messages are redelivered |

## 5. Scaling

- Small files and snapshot buildup from frequent MERGEs: the weekly maintenance
  DAG compacts files and expires old snapshots.
- Gold rebuilds: the fact table is already incremental; the dimensions could move to
  incremental or snapshots too.
- Slow backfill (the masterfilelist scan): cache or index the list, or list by date.
- One Spark container: submit to a real cluster (EMR or Kubernetes); the DAG's Spark
  step is already isolated for that swap.
- Stream throughput: add topic partitions and more consumers in the group; the
  country key already supports spreading the load.
- DuckDB is single-node: Snowflake handles concurrent querying at scale on the AWS
  target.

## 6. What it does not do yet

- `dim_geography` and `dim_cameo_event` are Type-1, so they keep no history. That is
  fine for reference data that rarely changes; `dim_actor` already keeps history.
- Streaming is at-least-once, not exactly-once. The dead-letter/alerts writes are not
  in the same transaction as the offset commit, so a crash can re-emit a message.
  Kafka transactions or an idempotent sink would fix that.
- The Snowflake gold target is written but not run; only the DuckDB target has been
  used.
- Lineage is run/job level. Column-level lineage would need the OpenLineage Spark
  listener and `openlineage-dbt`.
- There are no pipeline metrics beyond Airflow SLAs. Prometheus/Grafana or an
  Airflow callback would add that.
