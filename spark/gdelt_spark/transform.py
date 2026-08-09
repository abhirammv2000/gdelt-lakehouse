"""Cast, clean, and de-duplicate raw bronze rows into the silver contract.

Uses ``EVENT_COLUMNS`` as the column list, so it is defined in one place.
Output columns: the 61 typed event columns, then ``_source_file`` and
``_ingested_at``. Order matches :func:`gdelt_spark.iceberg.silver_ddl` so the
Iceberg ``MERGE ... INSERT *`` lines up by position and name.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gdelt_pipeline.schema.events import EVENT_COLUMNS

# GDELT's primary key, and the column we dedup on.
EVENT_KEY = "global_event_id"
# When the same event id recurs across batches, the newest record wins.
RECENCY_COLUMN = "date_added"

# Semantic type (from the schema) -> Spark SQL type used in DDL and casts.
SPARK_TYPES: dict[str, str] = {
    "long": "bigint",
    "int": "int",
    "double": "double",
    "string": "string",
    "date_yyyymmdd": "date",
    "timestamp_yyyymmddhhmmss": "timestamp",
}


def _clean(name: str) -> Column:
    """Trim, then map GDELT's empty-string fields to real NULLs."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", None).otherwise(trimmed)


def _cast_expr(name: str, semantic: str) -> Column:
    cleaned = _clean(name)
    if semantic == "date_yyyymmdd":
        return F.to_date(cleaned, "yyyyMMdd").alias(name)
    if semantic == "timestamp_yyyymmddhhmmss":
        return F.to_timestamp(cleaned, "yyyyMMddHHmmss").alias(name)
    if semantic == "string":
        return cleaned.alias(name)
    return cleaned.cast(SPARK_TYPES[semantic]).alias(name)


def to_silver(raw: DataFrame) -> DataFrame:
    """Type + clean + dedup a raw bronze frame into the silver frame."""
    typed = raw.select(*[_cast_expr(n, s) for n, s in EVENT_COLUMNS], F.col("_source_file"))

    # A row without the primary key can't be modeled or merged - drop it.
    typed = typed.filter(F.col(EVENT_KEY).isNotNull())

    # Keep the most-recently-added record per event id (idempotent across reruns).
    newest = Window.partitionBy(EVENT_KEY).orderBy(F.col(RECENCY_COLUMN).desc_nulls_last())
    deduped = (
        typed.withColumn("_rn", F.row_number().over(newest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return deduped.withColumn("_ingested_at", F.current_timestamp())
