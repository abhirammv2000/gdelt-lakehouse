"""Shared helpers for the GDELT DAGs.

The bronze to silver step runs PySpark in its own container. Airflow reaches it through
the mounted Docker socket and runs spark-submit inside the spark-iceberg service, which
already has the code, jars and network set up. On a cloud setup this one function would
become a SparkKubernetesOperator, EmrAddStepsOperator or DatabricksSubmitRunOperator, and
the DAGs would stay the same.
"""

from __future__ import annotations

import socket

from airflow.exceptions import AirflowException

_SPARK_SERVICE = "spark-iceberg"
_JOBS_DIR = "/home/iceberg/work/jobs"
_SILVER_JOB = f"{_JOBS_DIR}/bronze_to_silver.py"
_MAINTENANCE_JOB = f"{_JOBS_DIR}/maintain_silver.py"


def _find_spark_container(client):  # type: ignore[no-untyped-def]
    """Locate the running Spark container in this compose project."""
    project = None
    try:
        me = client.containers.get(socket.gethostname())
        project = me.labels.get("com.docker.compose.project")
    except Exception:  # noqa: BLE001 (best-effort; fall back to service-only match)
        pass

    candidates = client.containers.list(
        filters={"label": f"com.docker.compose.service={_SPARK_SERVICE}", "status": "running"}
    )
    if project:
        scoped = [c for c in candidates if c.labels.get("com.docker.compose.project") == project]
        candidates = scoped or candidates
    if not candidates:
        raise AirflowException(f"No running '{_SPARK_SERVICE}' container found")
    return candidates[0]


def run_spark_job(job_path: str, args: str = "") -> None:
    """Exec ``spark-submit <job_path> <args>`` in the Spark container; raise on failure."""
    import docker

    client = docker.from_env()
    container = _find_spark_container(client)
    cmd = ["bash", "-c", f"spark-submit {job_path} {args}".rstrip()]
    print(f"[airflow->spark] exec in {container.name}: spark-submit {job_path} {args}")

    exit_code, output = container.exec_run(cmd, demux=False)
    if output:
        print(output.decode("utf-8", errors="replace"))
    if exit_code != 0:
        raise AirflowException(f"spark-submit {job_path} failed (exit {exit_code})")


def run_bronze_to_silver(spark_args: str) -> None:
    run_spark_job(_SILVER_JOB, spark_args)


def run_maintenance(spark_args: str = "") -> None:
    run_spark_job(_MAINTENANCE_JOB, spark_args)
