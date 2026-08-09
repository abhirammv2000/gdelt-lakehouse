"""Read zipped GDELT export CSVs from the bronze bucket into a raw-string frame.

GDELT ships ``*.CSV.zip`` (tab-delimited, no header, 61 columns). Spark's CSV
reader can't see inside a zip, and this image has no ``s3a`` filesystem, so we:

  1. list the bronze objects with boto3 (driver side),
  2. parallelize the keys and fetch+unzip each on the executors,
  3. build a DataFrame of 61 string columns (+ ``_source_file``).

Casting/cleaning happens later in ``transform.py``; bronze stays an unmodified copy.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES

_N_COLS = len(EVENT_COLUMN_NAMES)  # 61 - the schema contract


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


def _raw_schema() -> StructType:
    fields = [StructField(name, StringType(), nullable=True) for name in EVENT_COLUMN_NAMES]
    fields.append(StructField("_source_file", StringType(), nullable=True))
    # Schema-drift signal: the actual (contract-normalized) field count of the row,
    # and the raw line kept verbatim so non-conformant rows can be quarantined.
    fields.append(StructField("_field_count", IntegerType(), nullable=False))
    fields.append(StructField("_raw_line", StringType(), nullable=True))
    return StructType(fields)


def records_from_zip(source_file: str, content: bytes) -> Iterator[tuple[object, ...]]:
    """Yield one row-tuple per data line: 61 fields + source, field count, raw line.

    Fields are padded/truncated to 61 so the good path keeps a fixed schema, but
    ``_field_count`` carries the *actual* count (after normalizing the historical
    single trailing-tab quirk) so the job can detect and quarantine drifted rows.
    Decoding uses UTF-8 with replacement to survive GDELT's encoding quirks.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()
        if not names:
            return
        text = zf.read(names[0]).decode("utf-8", errors="replace")
    for line in text.split("\n"):
        stripped = line.rstrip("\r")
        if not stripped.strip():
            continue
        parts = stripped.split("\t")
        # Normalize the historical trailing-tab quirk: a lone empty trailing field.
        if len(parts) == _N_COLS + 1 and parts[-1] == "":
            parts = parts[:-1]
        field_count = len(parts)
        fields: list[str | None] = (parts + [None] * _N_COLS)[:_N_COLS]  # type: ignore[assignment]
        yield (*fields, source_file, field_count, stripped)


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


def read_bronze(spark: SparkSession, loc: S3Location, prefix: str) -> DataFrame:
    """Return a raw-string DataFrame for every ``*.CSV.zip`` under ``prefix``."""
    keys = list_bronze_keys(loc, prefix)
    if not keys:
        return spark.createDataFrame([], _raw_schema())
    slices = min(len(keys), 16)
    rdd = spark.sparkContext.parallelize(keys, numSlices=slices).flatMap(
        lambda key: _fetch_and_unzip(key, loc)
    )
    return spark.createDataFrame(rdd, _raw_schema())
