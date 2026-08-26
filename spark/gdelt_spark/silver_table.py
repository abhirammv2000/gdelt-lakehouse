"""Create the silver table and MERGE the silver frame into it.

The MERGE keys on ``global_event_id`` and only overwrites when the incoming
record is at least as recent as the stored one, so re-running the job on the same
bronze data is a no-op - the core idempotency guarantee of the pipeline.

Two table formats, because the pipeline targets two clouds: Iceberg on AWS and
locally, Delta on Databricks. Only the DDL differs. The MERGE statement is
byte-for-byte identical on both, which is the reason this is one module with a
format argument rather than two implementations that have to be kept in step.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gdelt_pipeline.schema.events import EVENT_COLUMNS
from gdelt_spark.transform import EVENT_KEY, RECENCY_COLUMN, SPARK_TYPES

ICEBERG = "iceberg"
DELTA = "delta"

_METADATA_COLUMNS = [("_source_file", "string"), ("_ingested_at", "timestamp")]


def silver_ddl(table: str, table_format: str = ICEBERG) -> str:
    columns = [(name, SPARK_TYPES[sem]) for name, sem in EVENT_COLUMNS] + _METADATA_COLUMNS
    body = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in columns)
    head = f"CREATE TABLE IF NOT EXISTS {table} (\n  {body}\n)\n"

    if table_format == DELTA:
        return (
            f"{head}USING delta\n"
            # sql_date is already day-granular, so partitioning on it directly is
            # the equivalent of Iceberg's days() transform, not a coarser choice.
            # Delta has no hidden partitioning, so the column is named as it is.
            "PARTITIONED BY (sql_date)\n"
            # Delta merges copy-on-write by default, matching write.merge.mode
            # below. optimizeWrite bin-packs on the way in, which is the
            # small-file defence a table that takes a MERGE per run needs.
            "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')"
        )

    return (
        f"{head}USING iceberg\n"
        "PARTITIONED BY (days(sql_date))\n"
        "TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='copy-on-write')"
    )


def ensure_silver_table(spark: SparkSession, table: str, table_format: str = ICEBERG) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(silver_ddl(table, table_format))


def merge_silver(spark: SparkSession, table: str, updates_view: str) -> None:
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {updates_view} s
        ON t.{EVENT_KEY} = s.{EVENT_KEY}
        WHEN MATCHED AND s.{RECENCY_COLUMN} >= t.{RECENCY_COLUMN} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def write_silver(
    spark: SparkSession, df: DataFrame, table: str, table_format: str = ICEBERG
) -> None:
    ensure_silver_table(spark, table, table_format)
    view = "gdelt_silver_updates"
    df.createOrReplaceTempView(view)
    merge_silver(spark, table, view)


def rejects_table_name(silver_table: str) -> str:
    """Quarantine table alongside the silver table, e.g. ...events -> ...events_rejects."""
    return f"{silver_table}_rejects"


def ensure_rejects_table(spark: SparkSession, table: str, table_format: str = ICEBERG) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    # format-version is an Iceberg property; Delta rejects unknown ones.
    properties = "" if table_format == DELTA else " TBLPROPERTIES ('format-version'='2')"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        "  _raw_line string,\n"
        "  _field_count int,\n"
        "  _expected_count int,\n"
        "  _source_file string,\n"
        "  _quarantined_at timestamp\n"
        f") USING {table_format}{properties}"
    )


def write_rejects(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    expected_count: int,
    table_format: str = ICEBERG,
) -> None:
    """Append non-conformant rows to the quarantine table for inspection/replay."""
    ensure_rejects_table(spark, table, table_format)
    (
        df.select("_raw_line", "_field_count", "_source_file")
        .withColumn("_expected_count", F.lit(expected_count))
        .withColumn("_quarantined_at", F.current_timestamp())
        .select("_raw_line", "_field_count", "_expected_count", "_source_file", "_quarantined_at")
        .writeTo(table)
        .append()
    )
