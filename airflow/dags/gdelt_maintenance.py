"""Weekly Iceberg silver-table maintenance.

The 15-minute MERGE pipeline steadily adds small data files and snapshots. This
DAG runs Iceberg's maintenance procedures (compaction, manifest + snapshot
cleanup, orphan removal) on a low-traffic weekly slot so reads stay fast and the
catalog metadata stays small - separated from the hot path on purpose.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="gdelt_maintenance",
    schedule="0 3 * * 0",  # 03:00 UTC every Sunday
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gdelt", "maintenance", "iceberg"],
    doc_md=__doc__,
)
def gdelt_maintenance() -> None:
    @task
    def maintain_silver() -> None:
        """Compact + expire snapshots + remove orphans on the silver Iceberg table."""
        from gdelt_lib import run_maintenance

        run_maintenance("--retain-last 5")

    maintain_silver()


gdelt_maintenance()
