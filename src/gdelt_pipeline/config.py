"""Central, environment-driven configuration.

Every knob is read from the environment (or a local ``.env``) so the exact same
code runs against local MinIO or real AWS S3 with only ``GDELT_ENV`` changing.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GDELT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Literal["local", "aws"] = "local"

    # GDELT source feed
    base_url: str = "http://data.gdeltproject.org/gdeltv2"
    feeds: str = "export"  # comma-separated subset of export,mentions,gkg

    # Object storage (S3 / MinIO)
    s3_endpoint_url: str | None = None  # None => real AWS S3
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"
    bronze_bucket: str = "gdelt-bronze"
    silver_bucket: str = "gdelt-silver"

    # Iceberg REST catalog
    iceberg_catalog_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://gdelt-silver/warehouse"

    # Kafka / Redpanda (streaming, Phase 6)
    kafka_bootstrap: str = "localhost:19092"
    kafka_topic: str = "gdelt.events.raw"
    kafka_dlq_topic: str = "gdelt.events.dlq"
    kafka_alerts_topic: str = "gdelt.events.alerts"
    kafka_consumer_group: str = "gdelt-stream-consumer"

    # HTTP behaviour
    http_timeout_seconds: float = 60.0
    max_retries: int = 5

    # Backfill scope. GDELT's full history runs back to 2015, which is hundreds
    # of millions of rows; nothing about the code stops a `gdelt_backfill` trigger
    # from being pointed at all of it. This caps the window one backfill run can
    # cover so that "backfill" means a deliberate, bounded catch-up, not an accidental
    # full-archive load. A full-archive backfill is still possible, but it takes
    # raising this number on purpose, not a typo in an Airflow trigger config.
    max_backfill_days: int = 30

    @property
    def enabled_feeds(self) -> list[str]:
        return [f.strip() for f in self.feeds.split(",") if f.strip()]

    @property
    def storage_options(self) -> dict[str, object]:
        """fsspec/s3fs kwargs.

        Credentials are omitted when unset so real AWS falls back to the default
        chain (profile, env vars, or an instance role). The region is always sent:
        without it s3fs signs for us-east-1 and a bucket in another region answers
        403, which surfaces as a bare "Forbidden" that looks like a permissions bug.
        """
        opts: dict[str, object] = {}
        if self.s3_access_key and self.s3_secret_key:
            opts["key"] = self.s3_access_key
            opts["secret"] = self.s3_secret_key
        client_kwargs: dict[str, object] = {"region_name": self.s3_region}
        if self.s3_endpoint_url:  # MinIO / localstack
            client_kwargs["endpoint_url"] = self.s3_endpoint_url
        opts["client_kwargs"] = client_kwargs
        return opts


@lru_cache
def get_settings() -> Settings:
    return Settings()
