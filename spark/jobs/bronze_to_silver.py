"""spark-submit entrypoint: bronze GDELT zips -> typed, deduped silver Iceberg table.

    spark-submit /home/iceberg/work/jobs/bronze_to_silver.py \
        --feed export --table lakehouse.gdelt.events

Reads config from the environment (see ``.env.example``), so the same job runs
against local MinIO or real S3 by swapping endpoints. Re-running is idempotent.
"""

from __future__ import annotations

import argparse
import os
import sys

from gdelt_spark.iceberg import rejects_table_name, write_rejects, write_silver
from gdelt_spark.read import S3Location, read_bronze
from gdelt_spark.session import CatalogConfig, build_spark
from gdelt_spark.transform import to_silver
from gdelt_spark.validate import SILVER_EXPECTATIONS, run_suite
from pyspark.sql import functions as F

from gdelt_pipeline.schema.events import EXPECTED_COLUMN_COUNT


class SchemaDriftError(RuntimeError):
    """Raised when too many rows violate the 61-field schema contract."""


def _s3_location() -> S3Location:
    return S3Location(
        bucket=os.environ.get("GDELT_BRONZE_BUCKET", "gdelt-bronze"),
        endpoint_url=os.environ.get("GDELT_S3_ENDPOINT_URL", "http://minio:9000"),
        access_key=os.environ.get("GDELT_S3_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("GDELT_S3_SECRET_KEY", "minioadmin"),
        region=os.environ.get("GDELT_S3_REGION", "us-east-1"),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GDELT bronze -> silver Iceberg job")
    parser.add_argument("--feed", default=os.environ.get("GDELT_FEEDS", "export").split(",")[0])
    parser.add_argument("--table", default="lakehouse.gdelt.events")
    parser.add_argument(
        "--prefix",
        default=None,
        help="Bronze key prefix to read (default: '<feed>/'). Narrow it to backfill one day.",
    )
    parser.add_argument(
        "--max-malformed-rate",
        type=float,
        default=float(os.environ.get("GDELT_MAX_MALFORMED_RATE", "0.05")),
        help="Abort if the share of non-conformant rows exceeds this (default 0.05).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    prefix = args.prefix if args.prefix is not None else f"{args.feed}/"

    spark = build_spark("gdelt-bronze-to-silver", CatalogConfig.from_env())
    spark.sparkContext.setLogLevel("WARN")

    loc = _s3_location()
    print(f"[bronze->silver] reading s3://{loc.bucket}/{prefix}")
    raw = read_bronze(spark, loc, prefix).cache()
    total = raw.count()
    if total == 0:
        print("[bronze->silver] no bronze files found — nothing to do")
        spark.stop()
        return 0

    # ── Schema-contract validation (drift detection) ────────────────────────────
    # A conformant row has exactly EXPECTED_COLUMN_COUNT fields. Non-conformant
    # rows are quarantined (never silently coerced into good data); a spike in the
    # malformed rate means GDELT changed its layout, so we abort before writing.
    conformant = raw.filter(F.col("_field_count") == EXPECTED_COLUMN_COUNT)
    rejects = raw.filter(F.col("_field_count") != EXPECTED_COLUMN_COUNT)
    malformed = rejects.count()
    rate = malformed / total
    print(
        f"[bronze->silver] schema contract ({EXPECTED_COLUMN_COUNT} fields/row): "
        f"{total} rows, {malformed} non-conformant ({rate:.2%})"
    )

    dist: dict[int, int] = {}
    if malformed:
        dist = {r["_field_count"]: r["count"] for r in rejects.groupBy("_field_count").count().collect()}
        print(f"[bronze->silver] non-conformant field-count distribution: {dist}")
        write_rejects(spark, rejects, rejects_table_name(args.table), EXPECTED_COLUMN_COUNT)
        print(f"[bronze->silver] quarantined {malformed} rows to {rejects_table_name(args.table)}")

    if rate > args.max_malformed_rate:
        raise SchemaDriftError(
            f"malformed rate {rate:.2%} exceeds threshold {args.max_malformed_rate:.2%} — "
            f"likely GDELT schema drift (field-count distribution: {dist}). Aborting before write."
        )

    silver = to_silver(conformant).cache()
    silver_count = silver.count()
    print(f"[bronze->silver] {total - malformed} conformant rows -> {silver_count} deduped events")

    suite = run_suite(silver, SILVER_EXPECTATIONS)
    print("[bronze->silver] data-quality results:")
    print(suite.summary())
    suite.raise_for_errors()  # aborts before writing if any error-severity rule failed

    write_silver(spark, silver, args.table)
    table_count = spark.table(args.table).count()
    print(f"[bronze->silver] merged into {args.table}; table now holds {table_count} events")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
