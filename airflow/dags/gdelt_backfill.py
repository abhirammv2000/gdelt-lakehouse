"""Parameterized GDELT backfill — triggered manually for a historical window.

Same medallion wiring as the incremental DAG, but the ingest step pulls every
15-minute file whose timestamp falls in ``[start, end)`` from GDELT's master file
list. Trigger with a config like::

    {"start": "2026-07-20", "end": "2026-07-22"}

Because ingest is checkpoint/existence-guarded and the Iceberg MERGE is
recency-guarded, re-running a backfill over an already-loaded window is a no-op.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

_DBT = "/opt/dbt-venv/bin/dbt"
_DBT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="gdelt_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        "start": Param("2026-07-27", type="string", description="inclusive UTC date/datetime"),
        "end": Param("2026-07-29", type="string", description="exclusive UTC date/datetime"),
    },
    tags=["gdelt", "backfill", "medallion"],
    doc_md=__doc__,
)
def gdelt_backfill() -> None:
    @task
    def ingest_window() -> dict:
        """Land every GDELT file with a timestamp in [start, end) into bronze."""
        from gdelt_pipeline.ingestion.service import IngestService

        params = get_current_context()["params"]
        start = datetime.fromisoformat(params["start"]).replace(tzinfo=UTC)
        end = datetime.fromisoformat(params["end"]).replace(tzinfo=UTC)
        return IngestService().backfill(start, end).summary

    @task
    def bronze_to_silver() -> None:
        """PySpark: MERGE the full export feed into the Iceberg silver table."""
        from gdelt_lib import run_bronze_to_silver

        run_bronze_to_silver("--feed export")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{_DBT} build --project-dir {_DBT_DIR} --profiles-dir {_DBT_DIR}",
    )

    ingest_window() >> bronze_to_silver() >> dbt_build


gdelt_backfill()
