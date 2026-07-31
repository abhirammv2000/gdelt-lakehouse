"""Incremental GDELT pipeline — runs every 15 minutes.

Wires the medallion end to end for the current batch:

    ingest (poll + land bronze)  ->  bronze_to_silver (PySpark MERGE into Iceberg)
                                 ->  dbt_build (gold star schema + tests)

Design notes:
- ``catchup=False`` + ``max_active_runs=1`` — GDELT is a live feed; we always want
  the latest batch, never a backlog of overlapping runs.
- Every stage is idempotent (checkpointed ingest, recency-guarded Iceberg MERGE,
  dbt rebuild), so the automatic retries below are always safe.
- silver reprocesses only the logical date's partition (``dt={{ ds }}``); the
  MERGE makes re-runs a no-op.
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

    @task
    def publish_stream() -> dict:
        """Fan the landed batch onto Kafka for the real-time consumer (parallel path)."""
        from gdelt_pipeline.streaming.producer import GdeltStreamProducer

        return {"published": GdeltStreamProducer().publish_bronze_latest("export")}

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{_DBT} build --project-dir {_DBT_DIR} --profiles-dir {_DBT_DIR}",
    )

    # Batch path (silver -> gold) and streaming path run in parallel after ingest.
    landed = ingest()
    landed >> bronze_to_silver() >> dbt_build
    landed >> publish_stream()


gdelt_incremental()
