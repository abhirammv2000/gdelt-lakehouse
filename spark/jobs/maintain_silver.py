"""Silver table maintenance: compaction, history expiry, orphan cleanup.

A MERGE-heavy table accumulates many small data files (one write per run, per
partition) and a growing version history. Left alone, reads get slower and
catalog metadata bloats. This job runs the maintenance a real lakehouse
schedules (e.g. weekly).

Iceberg does it with stored procedures:

  * rewrite_data_files  - bin-pack small files into larger ones (compaction)
  * rewrite_manifests   - keep the manifest list tidy
  * expire_snapshots    - drop old snapshots (bounds time-travel + metadata size)
  * remove_orphan_files - delete files no live snapshot references

Delta does the same work with two SQL commands:

  * OPTIMIZE            - compaction, the rewrite_data_files equivalent
  * VACUUM              - drops files no longer referenced by a retained version,
                          collapsing expire_snapshots and remove_orphan_files

There is no Delta counterpart to rewrite_manifests: Delta's transaction log is
compacted automatically into checkpoints, so that upkeep is the engine's job
rather than a scheduled one.

    spark-submit maintain_silver.py --table lakehouse.gdelt.events --retain-last 5
"""

from __future__ import annotations

import argparse
import sys

from gdelt_spark.session import DELTA, CatalogConfig, build_spark
from pyspark.sql import SparkSession


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GDELT silver table maintenance")
    parser.add_argument("--table", default="lakehouse.gdelt.events")
    parser.add_argument("--retain-last", type=int, default=5, help="Iceberg snapshots to keep")
    parser.add_argument("--min-input-files", type=int, default=2, help="compact partitions with >= N files")
    parser.add_argument(
        "--retain-hours",
        type=int,
        default=168,
        help="Delta history to keep when vacuuming (default 168, Delta's own floor)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    catalog = CatalogConfig.from_env()
    spark = build_spark("gdelt-silver-maintenance", catalog)
    spark.sparkContext.setLogLevel("WARN")

    if catalog.table_format == DELTA:
        _maintain_delta(spark, args.table, args.retain_hours)
    else:
        _maintain_iceberg(spark, args.table, catalog.name, args.retain_last, args.min_input_files)

    spark.stop()
    return 0


def _maintain_iceberg(
    spark: SparkSession, table: str, cat: str, retain_last: int, min_input_files: int
) -> None:
    # Procedures take the identifier without the catalog prefix (namespace.table).
    table_id = table.split(".", 1)[1]

    def call(proc: str, sql_args: str) -> None:
        rows = spark.sql(f"CALL {cat}.system.{proc}({sql_args})").collect()
        print(f"[maintenance] {proc}: {rows[0].asDict() if rows else 'ok'}")

    call("rewrite_data_files", f"table => '{table_id}', options => map('min-input-files','{min_input_files}')")
    call("rewrite_manifests", f"table => '{table_id}'")
    call("expire_snapshots", f"table => '{table_id}', retain_last => {retain_last}")
    call("remove_orphan_files", f"table => '{table_id}'")


def _maintain_delta(spark: SparkSession, table: str, retain_hours: int) -> None:
    rows = spark.sql(f"OPTIMIZE {table}").collect()
    print(f"[maintenance] optimize: {rows[0].asDict() if rows else 'ok'}")

    # Delta refuses a retention below 168 hours by default, because vacuuming
    # files newer than the longest running reader can pull data out from under a
    # query in flight. Overriding that check is possible and is the wrong move.
    spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
    print(f"[maintenance] vacuum: retained {retain_hours}h of history")


if __name__ == "__main__":
    sys.exit(main())
