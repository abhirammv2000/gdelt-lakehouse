"""Shared fixtures for the Spark unit tests.

They run inside the spark-iceberg container (make spark-test) on a local SparkSession.
No S3 or Iceberg catalog is needed, so they are fast and offline.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("gdelt-spark-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # The base image defaults to a REST catalog ("demo"); these unit tests
        # need no catalog, so pin to the built-in session catalog to stay offline.
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
