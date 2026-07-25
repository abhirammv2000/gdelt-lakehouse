"""Create the silver Iceberg table and MERGE the silver frame into it.

The MERGE keys on ``global_event_id`` and only overwrites when the incoming
record is at least as recent as the stored one, so re-running the job on the same
bronze data is a no-op — the core idempotency guarantee of the pipeline.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from gdelt_pipeline.schema.events import EVENT_COLUMNS
from gdelt_spark.transform import EVENT_KEY, RECENCY_COLUMN, SPARK_TYPES

_METADATA_COLUMNS = [("_source_file", "string"), ("_ingested_at", "timestamp")]


def silver_ddl(table: str) -> str:
    columns = [(name, SPARK_TYPES[sem]) for name, sem in EVENT_COLUMNS] + _METADATA_COLUMNS
    body = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {body}\n)\n"
        "USING iceberg\n"
        "PARTITIONED BY (days(sql_date))\n"
        "TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='copy-on-write')"
    )


def ensure_silver_table(spark: SparkSession, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(silver_ddl(table))


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


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    ensure_silver_table(spark, table)
    view = "gdelt_silver_updates"
    df.createOrReplaceTempView(view)
    merge_silver(spark, table, view)
