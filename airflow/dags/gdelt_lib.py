"""Shared helpers for the GDELT DAGs.

The bronze->silver step runs PySpark, which lives in its own container. Airflow
reaches it over the mounted Docker socket and execs ``spark-submit`` in the
already-running ``spark-iceberg`` service (its code mounts, jars, and network are
already correct). In a cloud deployment this single function would be swapped for
a SparkKubernetesOperator / EmrAddStepsOperator / DatabricksSubmitRunOperator —
the DAGs wouldn't change.
"""

from __future__ import annotations

import socket

from airflow.exceptions import AirflowException

_SPARK_SERVICE = "spark-iceberg"
_SPARK_JOB = "/home/iceberg/work/jobs/bronze_to_silver.py"


def _find_spark_container(client):  # type: ignore[no-untyped-def]
    """Locate the running Spark container in this compose project."""
    project = None
    try:
        me = client.containers.get(socket.gethostname())
        project = me.labels.get("com.docker.compose.project")
    except Exception:  # noqa: BLE001 - best-effort; fall back to service-only match
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


def run_bronze_to_silver(spark_args: str) -> None:
    """Exec the bronze->silver job in the Spark container; raise on failure."""
    import docker

    client = docker.from_env()
    container = _find_spark_container(client)
    cmd = ["bash", "-c", f"spark-submit {_SPARK_JOB} {spark_args}"]
    print(f"[airflow->spark] exec in {container.name}: spark-submit {_SPARK_JOB} {spark_args}")

    exit_code, output = container.exec_run(cmd, demux=False)
    if output:
        print(output.decode("utf-8", errors="replace"))
    if exit_code != 0:
        raise AirflowException(f"bronze->silver spark-submit failed (exit {exit_code})")
