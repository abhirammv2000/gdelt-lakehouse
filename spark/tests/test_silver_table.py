"""The silver DDL differs per table format; the MERGE deliberately does not.

silver_ddl is a pure string builder, so these need no SparkSession.
"""

from __future__ import annotations

from gdelt_spark.session import CatalogConfig
from gdelt_spark.silver_table import DELTA, ICEBERG, merge_silver, silver_ddl

TABLE = "lakehouse.gdelt.events"


def test_iceberg_ddl_is_unchanged_by_delta_support() -> None:
    ddl = silver_ddl(TABLE)

    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (days(sql_date))" in ddl
    assert "'write.merge.mode'='copy-on-write'" in ddl


def test_delta_ddl_partitions_on_the_date_column_itself() -> None:
    """Delta has no hidden partitioning, so days(sql_date) has no counterpart.

    sql_date is already day-granular, so naming it directly is equivalent
    rather than a coarser partitioning choice.
    """
    ddl = silver_ddl(TABLE, DELTA)

    assert "USING delta" in ddl
    assert "PARTITIONED BY (sql_date)" in ddl
    assert "days(" not in ddl
    # format-version is an Iceberg property and Delta rejects unknown ones.
    assert "format-version" not in ddl


def test_both_formats_declare_the_same_columns() -> None:
    """The format is a storage decision; the schema is not affected by it."""
    iceberg_head = silver_ddl(TABLE, ICEBERG).split("USING")[0]
    delta_head = silver_ddl(TABLE, DELTA).split("USING")[0]

    assert iceberg_head == delta_head


def test_the_merge_is_identical_on_both_formats() -> None:
    """The reason this is one module rather than two implementations.

    merge_silver takes no format argument at all, so there is no second MERGE
    that could drift from this one. Asserting the recency guard is present
    catches anyone adding a format branch that quietly drops it.
    """
    captured: list[str] = []
    spark_stub = type("S", (), {"sql": lambda self, q: captured.append(q)})()

    merge_silver(spark_stub, TABLE, "updates")

    (sql,) = captured
    assert "MERGE INTO lakehouse.gdelt.events t" in sql
    assert "WHEN MATCHED AND s.date_added >= t.date_added THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_catalog_type_decides_the_format() -> None:
    """Not independent knobs: Unity Catalog is Delta, Glue and REST are Iceberg."""
    assert CatalogConfig(catalog_type="databricks").table_format == DELTA
    assert CatalogConfig(catalog_type="glue").table_format == ICEBERG
    assert CatalogConfig(catalog_type="rest").table_format == ICEBERG
