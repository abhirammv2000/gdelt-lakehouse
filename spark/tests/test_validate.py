"""Unit tests for the data-quality expectations engine."""

from __future__ import annotations

import datetime as dt

import pytest
from gdelt_spark.validate import (
    SILVER_EXPECTATIONS,
    DataQualityError,
    Expectation,
    run_suite,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

_SCHEMA = StructType(
    [
        StructField("global_event_id", LongType()),
        StructField("sql_date", DateType()),
        StructField("quad_class", IntegerType()),
        StructField("goldstein_scale", DoubleType()),
        StructField("avg_tone", DoubleType()),
        StructField("num_articles", IntegerType()),
        StructField("action_geo_lat", DoubleType()),
        StructField("action_geo_long", DoubleType()),
    ]
)

_D = dt.date(2026, 7, 24)


def _df(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(rows, _SCHEMA)


def _good(event_id: int) -> tuple:
    return (event_id, _D, 1, 2.5, 10.0, 3, 40.0, -70.0)


def test_clean_data_passes(spark: SparkSession) -> None:
    suite = run_suite(_df(spark, [_good(1), _good(2)]), SILVER_EXPECTATIONS)
    assert suite.failed == []
    suite.raise_for_errors()  # must not raise


def test_error_violations_flagged_and_raise(spark: SparkSession) -> None:
    rows = [
        _good(1),
        (1, _D, 1, 2.5, 10.0, 3, 40.0, -70.0),  # duplicate id -> unique fails
        (None, _D, 9, 2.5, 10.0, 3, 40.0, -70.0),  # null id + quad_class 9 -> two errors
    ]
    suite = run_suite(_df(spark, rows), SILVER_EXPECTATIONS)
    failed_names = {r.expectation.name for r in suite.failed_errors}
    assert "expect_global_event_id_not_null" in failed_names
    assert "expect_global_event_id_unique" in failed_names
    assert "expect_quad_class_in_set" in failed_names
    with pytest.raises(DataQualityError):
        suite.raise_for_errors()


def test_warn_only_failure_does_not_raise(spark: SparkSession) -> None:
    # goldstein_scale 99 is out of [-10, 10] but that rule is WARN severity.
    rows = [(1, _D, 1, 99.0, 10.0, 3, 40.0, -70.0)]
    suite = run_suite(_df(spark, rows), SILVER_EXPECTATIONS)
    assert any(r.expectation.name == "expect_goldstein_scale_between_-10.0_and_10.0" for r in suite.failed)
    assert suite.failed_errors == []
    suite.raise_for_errors()  # WARN failures never abort


def test_single_pass_evaluation(spark: SparkSession) -> None:
    # A trivial suite still returns one result per expectation.
    suite = run_suite(_df(spark, [_good(1)]), [Expectation("not_null", "global_event_id")])
    assert len(suite.results) == 1
    assert suite.results[0].passed
