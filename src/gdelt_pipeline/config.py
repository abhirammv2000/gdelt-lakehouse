"""Central, environment-driven configuration.

Every knob is read from the environment (or a local ``.env``) so the exact same
code runs against local MinIO or real AWS S3 with only ``GDELT_ENV`` changing.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
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
    iceberg_catalog_uri: str = "http://localhost:8181/api/catalog"
    iceberg_warehouse: str = "s3://gdelt-silver/warehouse"

    # HTTP behaviour
    http_timeout_seconds: float = 60.0
    max_retries: int = 5

    @property
    def enabled_feeds(self) -> list[str]:
        return [f.strip() for f in self.feeds.split(",") if f.strip()]

    @property
    def storage_options(self) -> dict[str, object]:
        """fsspec/s3fs kwargs. Empty when running against real AWS with an IAM role."""
        opts: dict[str, object] = {}
        if self.s3_access_key and self.s3_secret_key:
            opts["key"] = self.s3_access_key
            opts["secret"] = self.s3_secret_key
        if self.s3_endpoint_url:  # MinIO / localstack
            opts["client_kwargs"] = {
                "endpoint_url": self.s3_endpoint_url,
                "region_name": self.s3_region,
            }
        return opts


@lru_cache
def get_settings() -> Settings:
    return Settings()
