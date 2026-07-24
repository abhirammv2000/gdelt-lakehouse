"""A minimal DAG that verifies the local stack is wired correctly.

It confirms two things the rest of the platform depends on:
  1. the ``gdelt_pipeline`` package is importable inside Airflow, and
  2. Airflow can reach MinIO and see the bronze/silver buckets.

Trigger it once from the Airflow UI after ``make up`` to smoke-test Phase 2.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ops", "healthcheck"],
)
def stack_healthcheck() -> None:
    @task
    def check_package() -> str:
        from gdelt_pipeline.config import get_settings

        settings = get_settings()
        return f"gdelt_pipeline OK (env={settings.env}, endpoint={settings.s3_endpoint_url})"

    @task
    def check_buckets() -> list[str]:
        import s3fs

        from gdelt_pipeline.config import get_settings

        settings = get_settings()
        fs = s3fs.S3FileSystem(**settings.storage_options)
        buckets = [settings.bronze_bucket, settings.silver_bucket]
        for bucket in buckets:
            if not fs.exists(bucket):
                raise RuntimeError(f"expected bucket missing: {bucket}")
        return buckets

    check_package()
    check_buckets()


stack_healthcheck()
