"""Backfill DAG for a past date range. Trigger it by hand.

It wires the pipeline the same way as the incremental DAG, but ingest pulls every
15-minute file between start and end (end not included) from GDELT's master file list.
Trigger it with a config like:

    {"start": "2026-07-20", "end": "2026-07-22"}

Ingest is checkpointed and the Iceberg MERGE only overwrites with newer rows, so running
a window that is already loaded again does nothing.

The window is capped at GDELT_MAX_BACKFILL_DAYS (30 by default), so a typo in the config
can't start a load of the whole archive back to 2015. ingest_window stops with a clear
error if the window is backwards or too wide. It does not shorten it quietly.
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
