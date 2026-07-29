# Airflow image for the GDELT lakehouse.
# The gdelt_pipeline package itself is mounted at /opt/airflow/src (PYTHONPATH),
# so code edits are live — only its dependencies + Airflow providers are baked in.
FROM apache/airflow:2.9.3-python3.11

COPY docker/airflow/requirements.txt /requirements.txt

# Pin against the matching Airflow constraints file to keep the provider set
# consistent with the base image.
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# dbt lives in its own virtualenv so its pinned deps (jinja2, click, ...) can't
# clash with Airflow's. The dbt tasks invoke /opt/dbt-venv/bin/dbt directly.
# Root is needed to write under /opt; the venv is world-executable afterwards.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb>=1.9"
USER airflow

