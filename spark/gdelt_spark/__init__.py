"""Bronze to silver PySpark transforms for the GDELT lakehouse.

These modules only need PySpark, the standard library and boto3, so they can be tested
against a local SparkSession without S3 or an Iceberg catalog. The jobs/ entrypoint
connects them to the real REST catalog and MinIO.
"""
