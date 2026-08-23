# Spark against DuckDB and Polars

The pipeline uses Spark. At one 15-minute batch that is hard to justify, so this
measures it instead of arguing about it. The result does not flatter the choice.

## What was measured

Each engine does the same bronze-to-silver work on the same local files: read the
tab-delimited rows, cast the 61 columns to the silver types (trim, empty string to
NULL), drop rows with no `global_event_id`, keep the newest row per id by
`date_added`, write Parquet.

Three scales, all real GDELT export files from 2026-08-21:

| Scale | Files | Input rows |
|---|---|---|
| One 15-minute batch | 1 | 2,278 |
| Six hours | 24 | 25,017 |
| One day | 96 | 103,858 |

Method notes, because they change the answer:

- The zips are expanded to TSV once up front. No engine pays the unzip cost.
- Each engine reads the TSV with its own native CSV reader, the way you would
  actually write it for that engine.
- **One engine, one scale, one process.** Interpreter and JVM start-up land on the
  engine that pays them. Start-up is a real per-batch cost here, since each
  scheduled run is a fresh `spark-submit`, so it is included deliberately.
- Median of three runs.
- Every engine's output row count is recorded. They agree exactly at every scale
  (2,278 / 25,017 / 103,858). If they disagreed the benchmark would be measuring
  three different jobs.

Hardware is one laptop, Spark in `local[*]` inside a CPU-limited Docker container.

Reproduce with `spark/bench/benchmark_engines.py`; chart with
`scripts/plot_benchmark.py`.

## Results

Seconds, median of 3.

| Input rows | DuckDB | Polars | PySpark | Spark vs DuckDB |
|---|---|---|---|---|
| 2,278 (1 batch) | **0.26** | 0.40 | 14.06 | 54x slower |
| 25,017 (6 hours) | **0.98** | 1.99 | 18.48 | 19x slower |
| 103,858 (1 day) | **1.39** | 8.25 | 22.49 | 16x slower |

![Benchmark](images/benchmark_engines.png)

## What it says

**Spark loses at every scale measured, and there is no crossover in sight.**
Extrapolating the two larger points, DuckDB's marginal cost is about 5 microseconds
per row and Spark's about 51. Spark is not merely paying a fixed start-up penalty
it would amortise away; in this range its per-row cost is higher too, so the lines
diverge rather than converge. Spark carries roughly 14 seconds of fixed start-up,
which is most of its time at one batch and still half of it at one day.

For the 15-minute incremental path this is decisive. A batch of about 900 to 2,700
events is DuckDB work. Running Spark for it costs 14 seconds of JVM start-up every
quarter hour to process something a single process finishes in under a second.

Polars is interesting and not in the way I expected: fastest-growing of the three
between six hours and one day (79 microseconds per row), slower than DuckDB
throughout. Concatenating 96 frames and sorting to deduplicate is doing it no
favours; a lazy scan would likely close much of that gap. I did not tune it, and I
would not present these Polars numbers as its ceiling.

## What it does not say

The benchmark is single-node, and horizontal scale is the entire reason Spark
exists. It measures nothing about a cluster.

The largest scale here is one day. The full GDELT history is 396,183 files, 37 GB
compressed, and roughly 700 million rows. At that size the questions change from
throughput to memory, shuffle, fault tolerance, and whether a failure at hour four
costs you the whole run. None of that shows up at 103,858 rows.

Spark also brings the most complete Iceberg integration: `MERGE INTO`, schema
evolution, and the maintenance procedures the pipeline uses for compaction and
snapshot expiry. That is a real reason to keep it that has nothing to do with speed.

## What I would change

Keep Spark for backfill and for Iceberg maintenance, where the data is large enough
to need it and where its Iceberg support is doing the work. Move the 15-minute
incremental path to DuckDB, which would cut per-batch latency from about 22 seconds
to about 1 and remove a JVM from the hot path.

I have not made that change. It splits the pipeline into two implementations, which
is the cost the current single-code-path design was buying, and that trade deserves
its own decision rather than being smuggled in on the back of a benchmark. But the
numbers are the numbers, and the honest summary is that the current design pays a
real and measurable price every 15 minutes for a uniformity benefit that only starts
to pay off at volumes this project has not yet reached.
