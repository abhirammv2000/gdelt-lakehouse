"""Read zipped GDELT export CSVs from the bronze layer into a raw-string frame.

GDELT ships ``*.CSV.zip`` (tab-delimited, no header, 61 columns). Spark's CSV
reader cannot see inside a zip either way, but how the bytes are reached differs
by platform, and the difference is not cosmetic:

  * S3 / MinIO (``S3Location``): the local Spark image has no ``s3a`` filesystem,
    so objects are listed with boto3 on the driver, then the keys are
    parallelized and each is fetched and unzipped on an executor.

  * ADLS Gen2 (``AdlsLocation``): Databricks Runtime ships the ABFS driver and
    the cluster already holds the Unity Catalog credential, so Spark reads the
    files itself with the ``binaryFile`` source. No SDK, no credentials in the
    closure, and the listing is distributed rather than a driver-side paginate.

Both end up at the same place: a DataFrame of 61 string columns plus
``_source_file``, ``_field_count``, and ``_raw_line``.

Row parsing and the 61-field contract check live in
``gdelt_pipeline.schema.parse``; this module is only the Spark plumbing.

Casting/cleaning happens later in ``transform.py``; bronze stays an unmodified copy.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES

# The contract parsing lives in the package (pure Python, no Spark) so the
# schema-drift behaviour is testable in CI. Re-exported here because this is
# where the Spark side of the job reaches for it.
from gdelt_pipeline.schema.parse import records_from_zip

__all__ = [
    "AdlsLocation",
    "S3Location",
    "list_bronze_keys",
    "read_bronze",
    "records_from_zip",
]


@dataclass(frozen=True)
class S3Location:
    """Everything an executor needs to pull an object - must stay picklable."""

    bucket: str
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str = "us-east-1"

    def client(self):  # type: ignore[no-untyped-def]
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )


@dataclass(frozen=True)
class AdlsLocation:
    """Bronze in ADLS Gen2, read through Spark's own ABFS driver.

    Deliberately carries no credentials. On Databricks the cluster is already
    authorized for this container through the Unity Catalog storage credential
    backed by the access connector, so putting a key in here would both be
    unnecessary and ship a secret to every executor.
    """

    account: str
    container: str

    @property
    def base_url(self) -> str:
        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net"


def _raw_schema() -> StructType:
    fields = [StructField(name, StringType(), nullable=True) for name in EVENT_COLUMN_NAMES]
    fields.append(StructField("_source_file", StringType(), nullable=True))
    # Schema-drift signal: the actual (contract-normalized) field count of the row,
    # and the raw line kept verbatim so non-conformant rows can be quarantined.
    fields.append(StructField("_field_count", IntegerType(), nullable=False))
    fields.append(StructField("_raw_line", StringType(), nullable=True))
    return StructType(fields)


def list_bronze_keys(loc: S3Location, prefix: str) -> list[str]:
    paginator = loc.client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=loc.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".CSV.zip"):
                keys.append(obj["Key"])
    return keys


def _fetch_and_unzip(key: str, loc: S3Location) -> Iterator[tuple[str | None, ...]]:
    body = loc.client().get_object(Bucket=loc.bucket, Key=key)["Body"].read()
    source_file = key.rsplit("/", 1)[-1]
    yield from records_from_zip(source_file, body)


def read_bronze(
    spark: SparkSession, loc: S3Location | AdlsLocation, prefix: str
) -> DataFrame:
    """Return a raw-string DataFrame for every ``*.CSV.zip`` under ``prefix``."""
    if isinstance(loc, AdlsLocation):
        return _read_bronze_adls(spark, loc, prefix)

    keys = list_bronze_keys(loc, prefix)
    if not keys:
        return spark.createDataFrame([], _raw_schema())
    slices = min(len(keys), 16)
    rdd = spark.sparkContext.parallelize(keys, numSlices=slices).flatMap(
        lambda key: _fetch_and_unzip(key, loc)
    )
    return spark.createDataFrame(rdd, _raw_schema())


def _read_bronze_adls(spark: SparkSession, loc: AdlsLocation, prefix: str) -> DataFrame:
    """Read bronze zips with Spark's binaryFile source.

    ``pathGlobFilter`` restricts to the zips while ``recursiveFileLookup`` walks
    the dt=/hour= partition directories, so the prefix stays a plain path rather
    than something that has to encode the partition depth.
    """
    files = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.CSV.zip")
        .option("recursiveFileLookup", "true")
        .load(f"{loc.base_url}/{prefix}")
        .select("path", "content")
    )
    if files.isEmpty():
        return spark.createDataFrame([], _raw_schema())

    rdd = files.rdd.flatMap(
        lambda row: records_from_zip(row.path.rsplit("/", 1)[-1], row.content)
    )
    return spark.createDataFrame(rdd, _raw_schema())
