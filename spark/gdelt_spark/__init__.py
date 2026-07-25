"""Bronze -> silver PySpark transforms for the GDELT lakehouse.

The modules here are deliberately import-light (PySpark + stdlib + boto3) so they
can be unit-tested against a local ``SparkSession`` without S3 or an Iceberg
catalog. The ``jobs/`` entrypoint wires them to the real REST catalog + MinIO.
"""
