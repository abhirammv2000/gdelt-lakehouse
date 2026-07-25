# Spark + Iceberg image for the GDELT lakehouse.
# Base already bundles Spark 3.5.5, Iceberg 1.8.1 runtime, and the Iceberg AWS
# bundle (S3FileIO). We add only what the bronze->silver job and its tests need:
#   - boto3  : list + fetch the zipped bronze CSVs from MinIO/S3 (no hadoop-aws jar)
#   - pytest : run the Spark unit tests inside the container
FROM tabulario/spark-iceberg:3.5.5_1.8.1

COPY docker/spark/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
