# Design decisions, trade-offs & interview walkthrough

A deep tour of *why* the GDELT lakehouse is built the way it is — the decision
behind each component, the alternatives considered, how it fails and recovers,
how it scales, and where the honest limits are. Pair with
[`RESUME_AND_CONCEPTS.md`](RESUME_AND_CONCEPTS.md).

---

## 1. The dataset, and why it's a good choice

GDELT 2.0 publishes a batch of world-news events **every 15 minutes**: tab-
delimited, **no header, 61 columns**, CAMEO codes, nulls, encoding quirks,
continuously arriving. That's deliberately unlike the clean/static "taxi" tutorial
dataset — it forces real incremental ingestion, idempotency, schema handling, data
quality, and streaming. Grain: one row = one geopolitical event.

---

## 2. Architecture in one breath

`ingest (Python) → bronze (raw zips, S3/MinIO) → PySpark → silver (Iceberg, typed
& deduped) → dbt → gold (star schema, DuckDB/Snowflake)`, orchestrated by Airflow,
with a **parallel streaming path** (Kafka/Redpanda) off ingestion, lineage to
Marquez, and an AWS/Terraform target selected by one `GDELT_ENV` flag.

**Medallion** is the backbone: each layer has a contract (bronze = faithful raw,
silver = typed/clean/conformed, gold = modeled for consumption). You only
reprocess *downstream* of where data changed, and each layer is independently
debuggable.

---

## 3. Decisions & trade-offs, component by component

### Ingestion (Python, httpx, tenacity)
- **Idempotent by construction.** Before landing a file we check object existence;
  after landing we advance a per-feed checkpoint (last timestamp). Re-running is a
  no-op (proven by test). *Why:* the 15-min schedule must be safe to retry.
- **Integrity:** MD5 from GDELT's index is verified on every download.
- **Failure isolation:** one bad file is recorded and skipped, not fatal to the batch.
- **Trade-off:** backfill reads GDELT's full `masterfilelist.txt` (huge) and filters
  in Python — simple but slow (~tens of minutes). A production system would cache /
  index that list or list by date prefix.

### Object storage — S3 with a MinIO local stand-in
- One storage abstraction (`fsspec`/`s3fs`); the *same code* talks to MinIO locally
  and S3 in AWS. The only difference is an endpoint + credentials, resolved from env
  (`GDELT_ENV`). *Alternative:* separate code paths — rejected; env-switching is the
  whole "two targets, one codebase" story.

### Bronze → silver — **PySpark + Apache Iceberg**
- **Why Iceberg (vs Hive tables / raw Parquet):** ACID commits, snapshots
  (time-travel + rollback), hidden partitioning, and schema evolution over object
  storage. Raw Parquet gives none of the transactional guarantees; Hive tables have
  the small-files/partition-listing problems Iceberg fixes.
- **Idempotent MERGE:** `MERGE INTO … WHEN MATCHED AND source.date_added >=
  target.date_added THEN UPDATE … WHEN NOT MATCHED THEN INSERT`. Re-running the same
  bronze is a no-op (identical rows re-MERGE'd); a re-published event (newer
  `date_added`) upserts. This is the core correctness property.
- **Dedup:** a window over `global_event_id` ordered by `date_added desc` keeps the
  most-recent record before the MERGE (GDELT re-publishes events across batches).
- **Reading zipped CSV without S3A:** the image ships Iceberg's `S3FileIO` (for table
  data) but **no `hadoop-aws`/`s3a` filesystem**, so Spark can't read `s3a://…zip`.
  We list + fetch the objects with **boto3** on the executors and unzip in Python.
  *Alternative:* add the hadoop-aws + matching aws-sdk jars — brittle version
  matching; boto3 is dependency-light and dual-target.
- **Partitioning:** `days(sql_date)` — matches how the data is queried (by date) and
  keeps partitions a sensible size.

### Data quality — a write-time gate (silver) + warehouse tests (gold)
- **Silver gate:** a small single-pass expectation engine (not-null, unique, in-set,
  between) with `error`/`warn` severity. Error failures **abort the write** before
  the MERGE — bad data never reaches silver. *Why not Great Expectations?* GE-on-Spark
  is heavy and version-fragile; a focused engine is deterministic, one Spark job, and
  trivially unit-tested. The interface mirrors GE names so it reads familiarly.
- **Gold tests:** 19 dbt tests (unique / not-null / **relationships** / accepted-
  values). The relationship tests are the important ones — they prove every fact
  foreign key resolves to a dimension member.

### Gold — **dbt** star schema on DuckDB (Snowflake in AWS)
- **Kimball star:** `fact_events` (grain = event) + conformed dims (date, actor,
  geography, cameo_event). **Surrogate keys** are `md5(natural_key)` computed by the
  *same macro* on both fact and dim, so relationship tests hold by construction.
- **dbt reads the Iceberg silver table directly** through the REST/Glue catalog
  (DuckDB `iceberg` + `httpfs` extensions). *Alternative:* export silver to plain
  Parquet for dbt — rejected; it duplicates data and breaks the "silver = the Iceberg
  table" contract. (Bridge quirk: the REST catalog defaults to `oauth2`; DuckDB needs
  `AUTHORIZATION_TYPE 'none'`.)
- **`fact_events` is incremental** (`delete+insert` on `event_key`, high-water-marked
  by `date_added`): each run processes only newly-added events, and re-published
  events upsert. *Why:* a full rebuild every 15 minutes doesn't scale.
- **CAMEO seed:** conformed reference data (the 20 CAMEO root categories) joins codes
  to human labels — the classic seed/lookup pattern.

### Orchestration — **Airflow**
- Two DAGs: `gdelt_incremental` (`*/15`, `catchup=False`, `max_active_runs=1`, retries
  w/ backoff, an SLA) and `gdelt_backfill` (manual, parameterized window). A weekly
  `gdelt_maintenance` DAG runs Iceberg upkeep.
- **`catchup=False` + `max_active_runs=1`:** GDELT is a live feed — we want the latest
  batch, never a backlog of overlapping runs racing on the same table.
- **Thin orchestration:** ingest runs *in-process* (the package is importable in
  Airflow); Spark is *triggered* by exec'ing `spark-submit` in the Spark container
  over the Docker socket; dbt runs from an **isolated venv** (dbt and Airflow have
  clashing pins — the standard isolation pattern). Each is a clean swap point:
  `SparkKubernetesOperator`/`EmrAddStepsOperator` for Spark, MWAA for Airflow itself.

### Streaming — **Kafka/Redpanda**, a parallel real-time path
- The producer fans a landed batch onto `gdelt.events.raw` (one message/event,
  **keyed by country** for stable partitioning, **idempotent `acks=all`**). An
  always-on consumer runs a **consumer group** with **manual offset commits**
  (at-least-once), applies the per-event DQ gate, **dead-letters** failures to
  `…dlq`, and routes high-impact conflict events to `…alerts`.
- **Why a separate path** (not just the batch): demonstrates the streaming
  fundamentals — partitions (parallelism + per-key order), consumer groups (scale-out
  + rebalancing), offsets (progress), DLQ (quarantine), content routing.
- **Trade-off:** JSON on the wire (readable, dependency-light). Production would use
  **Avro + Schema Registry** for enforced, evolvable contracts (Redpanda ships a
  registry; the port is wired).

### IaC / CI / lineage (Phase 7)
- **Terraform:** versioned + encrypted S3 buckets (public access blocked, lifecycle
  expiry), a **Glue** database as the production Iceberg catalog, **least-privilege
  IAM** scoped to exactly those buckets + DB. `fmt`/`validate`-clean.
- **CI:** GitHub Actions runs ruff, `mypy --strict`, pytest, `docker compose config`,
  and `terraform validate` on every push/PR.
- **Lineage:** every Airflow task emits **OpenLineage** to **Marquez** (run/job level),
  captured automatically.

---

## 4. Failure modes & recovery

| Failure | What happens |
|---|---|
| Ingest crashes mid-batch | Checkpoint + existence checks → safe re-run; landed files skipped |
| GDELT returns a corrupt file | MD5 mismatch raises; that file is recorded failed, batch continues |
| Silver job re-run on same data | Recency-guarded MERGE → no-op (idempotent) |
| Bad data in a batch | Silver DQ gate aborts the write before MERGE; stream DLQs the event |
| Catalog restart | Persistent SQLite-on-volume (WAL + busy_timeout) survives; prod = Glue |
| Spark task fails in a DAG | Airflow retries with backoff; MERGE idempotency makes retries safe |
| Consumer crashes | Offsets committed only after handling → at-least-once redelivery |

---

## 5. Scaling — what breaks first, and the fix

- **Small files / snapshot bloat** from frequent MERGEs → the `gdelt_maintenance`
  DAG (compaction, `expire_snapshots`, `rewrite_manifests`, `remove_orphan_files`).
- **Full-refresh gold** → already **incremental** on the fact; dims would move to
  incremental/snapshot too.
- **Backfill masterfilelist scan** (slow) → cache/index the file list; list by date.
- **Single Spark container** → submit to a real cluster (EMR/K8s); the DAG's Spark
  step is already abstracted for that swap.
- **Stream throughput** → more topic partitions + more consumers in the group
  (horizontal scale via rebalancing); the keying already supports it.
- **DuckDB gold** (single-node) → Snowflake for concurrent BI at scale (the
  `GDELT_ENV=aws` target).

---

## 6. Honest limitations & what I'd do next

- **Dimensions are Type-1** (`max()` per natural key) — no history. Actors change
  attributes over time; **SCD Type 2** (dbt snapshots) is the next step for `dim_actor`.
- **Streaming is at-least-once, not exactly-once.** The DLQ/alerts produce isn't
  transactional with the offset commit, so a crash can re-emit. Kafka transactions
  (EOS) or an idempotent downstream sink would close this.
- **Gold on Snowflake is designed, not built** — only the DuckDB target exists;
  the profile/warehouse wiring is the remaining work.
- **Lineage is run-level** (Airflow→Marquez). Column-level lineage would need the
  OpenLineage Spark listener + `openlineage-dbt`.
- **No pipeline metrics/alerting** beyond Airflow SLAs — a Prometheus/Grafana or
  Airflow-callback layer would add operational observability.
- **JSON streaming contract** — Avro + Schema Registry for real contracts.

---

## 7. Likely interview questions (with the short answers)

- **"How is this idempotent end-to-end?"** Ingest: checkpoint + existence check.
  Silver: recency-guarded Iceberg MERGE. Gold: incremental `delete+insert` on the
  key. Stream: commit offsets after processing. Re-running any stage converges to
  the same state.
- **"Why Iceberg over Delta/Hudi/plain Parquet?"** ACID + snapshots + hidden
  partitioning + schema evolution over object storage, engine-agnostic (Spark writes,
  DuckDB/Trino/Snowflake read the same table via a catalog). Delta/Hudi are valid
  peers; Iceberg's catalog story (Glue/REST) fit the "two targets" goal.
- **"How does dbt read an Iceberg table?"** DuckDB `iceberg`+`httpfs` extensions
  attach the REST/Glue catalog; the silver table *is* the dbt source — no copy.
- **"What's your partitioning / small-files strategy?"** Partition by `days(sql_date)`;
  copy-on-write MERGE keeps partitions compact; a scheduled maintenance DAG compacts
  and expires snapshots.
- **"At-least-once vs exactly-once?"** Manual offset commit after processing =
  at-least-once; I'd reach for Kafka transactions or idempotent sinks for EOS.
- **"How would this run in the cloud?"** `GDELT_ENV=aws`: S3 + Glue catalog +
  Snowflake, Terraform-provisioned, orchestrated by MWAA, Spark on EMR/K8s — the
  app code is unchanged; only operators and endpoints swap.
- **"What would you change with more time?"** SCD2 dims, exactly-once streaming,
  Snowflake gold, column-level lineage, and pipeline metrics (see §6).
