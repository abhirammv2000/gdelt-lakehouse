"""Build a SparkSession wired to the Iceberg REST catalog on MinIO/S3.

Only Iceberg's ``S3FileIO`` is used for table data (shipped in the base image's
iceberg-aws-bundle) — no ``hadoop-aws``/``s3a`` filesystem is required, because
bronze objects are fetched with boto3 (see ``read.py``), not through Spark's FS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class CatalogConfig:
    name: str = "lakehouse"
    rest_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://gdelt-silver/warehouse"
    s3_endpoint: str = "http://minio:9000"

    @classmethod
    def from_env(cls) -> CatalogConfig:
        return cls(
            name=os.environ.get("GDELT_ICEBERG_CATALOG", "lakehouse"),
            rest_uri=os.environ.get("GDELT_ICEBERG_CATALOG_URI", cls.rest_uri),
            warehouse=os.environ.get("GDELT_ICEBERG_WAREHOUSE", cls.warehouse),
            s3_endpoint=os.environ.get("GDELT_S3_ENDPOINT_URL", cls.s3_endpoint),
        )


def build_spark(app_name: str, catalog: CatalogConfig) -> SparkSession:
    c = catalog.name
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{c}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{c}.type": "rest",
        f"spark.sql.catalog.{c}.uri": catalog.rest_uri,
        f"spark.sql.catalog.{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{c}.warehouse": catalog.warehouse,
        f"spark.sql.catalog.{c}.s3.endpoint": catalog.s3_endpoint,
        f"spark.sql.catalog.{c}.s3.path-style-access": "true",
        "spark.sql.defaultCatalog": c,
    }
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
