"""Silver table maintenance: compaction, history cleanup and orphan file removal.

A table that is MERGEd often collects many small files (one write per run per partition)
and a growing history. Left alone, reads slow down and metadata grows. This job does the
maintenance a real lakehouse runs on a schedule, for example weekly.

Iceberg does it with stored procedures:

  * rewrite_data_files  - pack small files into bigger ones (compaction)
  * rewrite_manifests   - keep the manifest list tidy
  * expire_snapshots    - drop old snapshots (limits time travel and metadata size)
  * remove_orphan_files - delete files no live snapshot uses

Delta does the same with two SQL commands:

  * OPTIMIZE - compaction, like rewrite_data_files
  * VACUUM   - drops files no retained version uses, which covers expire_snapshots and
               remove_orphan_files

Delta has no rewrite_manifests. Its transaction log is compacted into checkpoints
automatically, so there is nothing to schedule.

    spark-submit maintain_silver.py --table lakehouse.gdelt.events --retain-last 5
"""

from __future__ import annotations

import argparse
import sys

from gdelt_spark.session import DELTA, CatalogConfig, build_spark, stop_spark
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

    stop_spark(spark, catalog)
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
