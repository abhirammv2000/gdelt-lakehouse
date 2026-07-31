"""Silver Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup.

A MERGE-heavy table accumulates many small data files (one write per run, per
partition) and a growing snapshot log. Left alone, reads get slower and catalog
metadata bloats. This job runs Iceberg's maintenance procedures — the operational
upkeep a real lakehouse schedules (e.g. weekly):

  * rewrite_data_files  — bin-pack small files into larger ones (compaction)
  * rewrite_manifests   — keep the manifest list tidy
  * expire_snapshots    — drop old snapshots (bounds time-travel + metadata size)
  * remove_orphan_files — delete files no live snapshot references

    spark-submit maintain_silver.py --table lakehouse.gdelt.events --retain-last 5
"""

from __future__ import annotations

import argparse
import sys

from gdelt_spark.session import CatalogConfig, build_spark


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GDELT silver Iceberg maintenance")
    parser.add_argument("--table", default="lakehouse.gdelt.events")
    parser.add_argument("--retain-last", type=int, default=5, help="snapshots to keep")
    parser.add_argument("--min-input-files", type=int, default=2, help="compact partitions with >= N files")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    catalog = CatalogConfig.from_env()
    spark = build_spark("gdelt-silver-maintenance", catalog)
    spark.sparkContext.setLogLevel("WARN")

    # Procedures take the identifier without the catalog prefix (namespace.table).
    table_id = args.table.split(".", 1)[1]
    cat = catalog.name

    def call(proc: str, sql_args: str) -> None:
        rows = spark.sql(f"CALL {cat}.system.{proc}({sql_args})").collect()
        print(f"[maintenance] {proc}: {rows[0].asDict() if rows else 'ok'}")

    call("rewrite_data_files", f"table => '{table_id}', options => map('min-input-files','{args.min_input_files}')")
    call("rewrite_manifests", f"table => '{table_id}'")
    call("expire_snapshots", f"table => '{table_id}', retain_last => {args.retain_last}")
    call("remove_orphan_files", f"table => '{table_id}'")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
