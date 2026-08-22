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
typed and deduped) -> dbt -> gold (star schema in DuckDB or BigQuery). Airflow runs
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

### Gold: dbt star schema on DuckDB, and on BigQuery
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
- Warehouse-portable: the same models run on DuckDB, BigQuery, and Athena. The few
  dialect points (surrogate-key hashing, date formatting, the incremental strategy,
  the seed column type) go through adapter-dispatched macros, so `--target bq` and
  `--target athena` build the same star schema and pass the same tests.
- Athena is the AWS target and the tidiest of the three, because it reads the same
  Glue/Iceberg silver table the Spark job writes and creates the gold tables as
  Iceberg in S3. Nothing is copied or exported: silver and gold are the same catalog.
  BigQuery cannot read the Iceberg REST catalog, so that path loads silver into a
  BigQuery dataset first, which is a real disadvantage compared with Athena.

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

Each row links to the test that triggers the failure on purpose and asserts the
behaviour. The FM-n tests live in `tests/test_failure_modes.py` and run in CI on
every push; the Spark-side ones run in the Spark container via `make spark-test`.
Rows marked "not automated" are claims I have exercised by hand but have not
turned into a test, and I would rather say so than imply coverage I do not have.

| Failure | What happens | Proof |
|---|---|---|
| GDELT returns a corrupt file | MD5 mismatch raises, the file is logged as failed, the batch continues | FM-1 |
| Ingest crashes mid-batch | Existence checks skip what landed, so a re-run fetches only what is missing | FM-2 |
| A few malformed rows | Separated by the field-count check and quarantined; good rows still load | FM-3 |
| GDELT changes its column layout | A real extra column is flagged while the historical trailing tab is not; above the malformed-rate threshold the job stops before writing | FM-4, `spark/tests` |
| Bad data in a batch | Silver expectations stop the write before the MERGE; the stream gate names each violation so the consumer dead-letters it | FM-5, `spark/tests/test_validate.py` |
| Bucket in a non-default region | The region is always passed to fsspec; without it s3fs signs for us-east-1 and the bucket answers a bare 403 | FM-6 |
| Out-of-order or replayed batch | The checkpoint is monotonic and never moves backwards | FM-7 |
| Silver job re-run on same data | Recency-guarded MERGE leaves the table unchanged | Observed in the runs in the README; not automated |
| Spark task fails in a DAG | Airflow retries with backoff, and the MERGE is safe to repeat | Not automated |
| Consumer crashes | Offsets are committed only after handling, so messages are redelivered | Not automated |
| Catalog restart | SQLite on a volume (WAL + busy_timeout) survives; on AWS this is Glue | Not automated |

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
- DuckDB is single-node: the BigQuery target handles concurrent querying at scale.

## 6. What it does not do yet

- `dim_geography` and `dim_cameo_event` are Type-1, so they keep no history. That is
  fine for reference data that rarely changes; `dim_actor` already keeps history.
- Streaming is at-least-once, not exactly-once. The dead-letter/alerts writes are not
  in the same transaction as the offset commit, so a crash can re-emit a message.
  Kafka transactions or an idempotent sink would fix that.
- The gold models have been run on DuckDB (local) and BigQuery (`--target bq`). A
  Snowflake target would follow the same adapter-dispatch pattern but is not wired
  yet.
- Lineage is run/job level. Column-level lineage would need the OpenLineage Spark
  listener and `openlineage-dbt`.
- There are no pipeline metrics beyond Airflow SLAs. Prometheus/Grafana or an
  Airflow callback would add that.

## 7. Honest answers to the obvious questions

### Why Spark, when the data fits in memory?

At the volume shown in the README (about 900 events per 15-minute batch, 10,163 rows
total) Spark is the wrong tool and I would not defend it on performance. Most of the
40 seconds is JVM startup. Pandas or DuckDB would finish in under a second.

Two reasons it is still here. First, the same job has to run over a backfill window as
well as a single batch, and GDELT's full history is roughly 96 files a day going back
to 2015, which is hundreds of millions of events. I did not want one code path for
"small" and another for "large." Second, Iceberg's most complete engine integration is
Spark: `MERGE INTO`, schema evolution, and the maintenance procedures
(`rewrite_data_files`, `expire_snapshots`) are all first-class there and partial
elsewhere.

The honest version of this trade-off: if I were running only the 15-minute incremental
path in production, I would move it to DuckDB or Polars and keep Spark for backfills
and compaction. The current design pays a fixed startup cost every 15 minutes to avoid
maintaining two implementations, and at this volume that is a bad trade. It becomes a
good trade somewhere around the point where a batch no longer fits comfortably on one
machine.

### Why Kafka, when the source is a 15-minute batch feed?

The batch pipeline does not need it, and this is the component I would cut first.
GDELT publishes on a fixed 15-minute cadence, so there is no continuous stream to
consume. What arrives is a file, and treating it as a stream is a choice, not a
requirement.

What the streaming path does earn: the consumer applies a per-event quality gate,
dead-letters what fails, and routes high-impact conflict events to an alerts topic.
If something downstream needs to react to an individual event rather than wait for the
next warehouse build, a topic decouples that consumer from the pipeline better than
polling gold on a schedule. That is a real pattern, and the implementation exercises
the parts that matter: keyed partitioning so one country's events stay ordered,
a consumer group with manual offset commits, and a dead-letter topic.

But it is a parallel path, not a dependency. The batch pipeline is correct with the
streaming stack turned off. Calling this "real-time" would be a stretch: the freshest
an event can be is however long ago GDELT published, which averages 7.5 minutes.

### What would break first at 100x?

100x here means roughly 90,000 events per 15-minute batch instead of 900, with the
table growing proportionally.

**The copy-on-write MERGE, and it breaks quietly rather than loudly.** The silver table
is created with `write.merge.mode = copy-on-write`, so every merge rewrites whole data
files that contain any touched row. The incoming batch stays a constant size, but the
files it touches grow with the table, so per-batch merge time climbs without bound even
at a constant ingest rate. Eventually a 15-minute batch takes longer than 15 minutes and
the schedule collapses backwards. The fix is `merge-on-read` plus frequent compaction,
trading read amplification for write cost.

In roughly the order they would hurt after that:

1. **Small-file accumulation.** 96 merges a day across date partitions produces
   thousands of small Parquet files. The maintenance DAG compacts weekly, which is
   already generous and would need to be hourly at 100x.
2. **Driver-side object listing.** `list_bronze_keys` pages the whole bronze prefix with
   boto3 on the driver and parallelizes the key list. That is fine for hundreds of
   objects and a bottleneck at hundreds of thousands. It needs date-prefix pruning or a
   manifest instead of a full listing.
3. **The dedup shuffle.** Deduplication is a window over `global_event_id` ordered by
   `date_added`, which is a full shuffle of the batch. It scales horizontally, so this
   costs money rather than correctness.
4. **DuckDB gold.** Single node, single file, one writer. This is why the BigQuery
   target exists and has been run.
5. **Backfill.** Already the slowest path, since it scans GDELT's full
   `masterfilelist.txt` and filters in Python. Linear in history length.

What would not break: the ingestion checkpoint, the schema contract, and the quality
gate are all per-batch and constant-cost, and the Iceberg MERGE stays correct under
retries regardless of size.
