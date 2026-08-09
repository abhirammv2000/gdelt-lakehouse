"""Unit tests for the bronze -> silver casting, cleaning, and dedup."""

from __future__ import annotations

import datetime as dt

from gdelt_spark.read import _raw_schema
from gdelt_spark.transform import to_silver
from pyspark.sql import SparkSession

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES


def _raw_row(source: str = "f.zip", **overrides: str) -> tuple:
    values = [str(overrides.get(name, "")) for name in EVENT_COLUMN_NAMES]
    # match _raw_schema: 61 fields + _source_file, _field_count, _raw_line
    return (*values, source, len(EVENT_COLUMN_NAMES), "\t".join(values))


def _raw_df(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(rows, _raw_schema())


def test_types_and_empty_string_to_null(spark: SparkSession) -> None:
    raw = _raw_df(
        spark,
        [
            _raw_row(
                global_event_id="100",
                sql_date="20260724",
                date_added="20260724120000",
                quad_class="1",
                goldstein_scale="",  # empty -> NULL
                avg_tone="1.5",
            )
        ],
    )
    row = to_silver(raw).collect()[0]

    assert row["global_event_id"] == 100
    assert row["sql_date"] == dt.date(2026, 7, 24)
    assert row["date_added"] == dt.datetime(2026, 7, 24, 12, 0, 0)
    assert row["goldstein_scale"] is None
    assert abs(row["avg_tone"] - 1.5) < 1e-9
    assert row["_source_file"] == "f.zip"
    assert row["_ingested_at"] is not None


def test_dedup_keeps_latest_by_date_added(spark: SparkSession) -> None:
    raw = _raw_df(
        spark,
        [
            _raw_row(global_event_id="1", sql_date="20260724", date_added="20260724120000", avg_tone="1.0"),
            _raw_row(global_event_id="1", sql_date="20260724", date_added="20260724130000", avg_tone="9.0"),
        ],
    )
    result = to_silver(raw)
    assert result.count() == 1
    assert abs(result.collect()[0]["avg_tone"] - 9.0) < 1e-9  # newer record won


def test_rows_without_primary_key_are_dropped(spark: SparkSession) -> None:
    raw = _raw_df(
        spark,
        [
            _raw_row(global_event_id="", sql_date="20260724", date_added="20260724120000"),
            _raw_row(global_event_id="2", sql_date="20260101", date_added="20260101000000"),
        ],
    )
    result = to_silver(raw)
    ids = [r["global_event_id"] for r in result.collect()]
    assert ids == [2]


def test_output_column_order_matches_contract(spark: SparkSession) -> None:
    raw = _raw_df(spark, [_raw_row(global_event_id="1", date_added="20260101000000")])
    expected = [*EVENT_COLUMN_NAMES, "_source_file", "_ingested_at"]
    assert to_silver(raw).columns == expected
