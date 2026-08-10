"""Build a SparkSession wired to the Iceberg catalog.

Two catalog backends, selected by ``GDELT_ICEBERG_CATALOG_TYPE``:

  - ``rest``  (local)  the Iceberg REST fixture, with data in MinIO.
  - ``glue``  (AWS)    the Glue Data Catalog, with data in S3.

Either way only Iceberg's ``S3FileIO`` handles table data (shipped in the base
image's iceberg-aws-bundle) - no ``hadoop-aws``/``s3a`` filesystem is required,
because bronze objects are fetched with boto3 (see ``read.py``), not Spark's FS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class CatalogConfig:
    name: str = "lakehouse"
    catalog_type: str = "rest"  # "rest" (local fixture) | "glue" (AWS Glue)
    rest_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://gdelt-silver/warehouse"
    # MinIO endpoint locally; None on real AWS (the SDK resolves the S3 endpoint).
    s3_endpoint: str | None = "http://minio:9000"

    @classmethod
    def from_env(cls) -> CatalogConfig:
        return cls(
            name=os.environ.get("GDELT_ICEBERG_CATALOG", cls.name),
            catalog_type=os.environ.get("GDELT_ICEBERG_CATALOG_TYPE", cls.catalog_type),
            rest_uri=os.environ.get("GDELT_ICEBERG_CATALOG_URI", cls.rest_uri),
            warehouse=os.environ.get("GDELT_ICEBERG_WAREHOUSE", cls.warehouse),
            # Empty string (real AWS S3) collapses to None so no endpoint is set.
            s3_endpoint=os.environ.get("GDELT_S3_ENDPOINT_URL", cls.s3_endpoint) or None,
        )


def build_spark(app_name: str, catalog: CatalogConfig) -> SparkSession:
    c = catalog.name
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{c}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{c}.warehouse": catalog.warehouse,
        "spark.sql.defaultCatalog": c,
    }
    if catalog.catalog_type == "glue":
        # AWS: namespaces map to Glue databases; region and credentials come from
        # the AWS SDK default chain (env vars / profile / instance role).
        conf[f"spark.sql.catalog.{c}.catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
    else:
        conf[f"spark.sql.catalog.{c}.type"] = "rest"
        conf[f"spark.sql.catalog.{c}.uri"] = catalog.rest_uri

    # Path-style access + an explicit endpoint are only needed for MinIO. Real S3
    # resolves its own regional endpoint, so set these only when one is configured.
    if catalog.s3_endpoint:
        conf[f"spark.sql.catalog.{c}.s3.endpoint"] = catalog.s3_endpoint
        conf[f"spark.sql.catalog.{c}.s3.path-style-access"] = "true"

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
