"""Incremental GDELT pipeline. Runs every 15 minutes.

It runs the whole chain for the current batch:

    ingest (poll and land bronze)  ->  bronze_to_silver (PySpark MERGE into Iceberg)
                                   ->  dbt_build (gold star schema and tests)

Notes:
- catchup=False and max_active_runs=1, because GDELT is a live feed. We want the latest
  batch, not a queue of overlapping runs.
- Every stage is safe to run twice (checkpointed ingest, a MERGE that only takes newer
  rows, a dbt rebuild), so the automatic retries are safe.
- Silver only reprocesses the partition for the run's date (dt={{ ds }}).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

_DBT = "/opt/dbt-venv/bin/dbt"
_DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="gdelt_incremental",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    tags=["gdelt", "incremental", "medallion"],
    doc_md=__doc__,
)
def gdelt_incremental() -> None:
    @task(sla=timedelta(minutes=10))
    def ingest() -> dict:
        """Poll the GDELT feed and land the latest batch into bronze (idempotent)."""
        from gdelt_pipeline.ingestion.service import IngestService

        return IngestService().ingest_latest().summary

    @task
    def bronze_to_silver() -> None:
        """PySpark: parse/clean/dedup this date's bronze into the Iceberg silver table."""
        from gdelt_lib import run_bronze_to_silver

        ds = get_current_context()["ds"]
        run_bronze_to_silver(f"--feed export --prefix export/dt={ds}/")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{_DBT} build --project-dir {_DBT_DIR} --profiles-dir {_DBT_DIR}",
    )

    ingest() >> bronze_to_silver() >> dbt_build


gdelt_incremental()
