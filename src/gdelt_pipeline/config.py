"""Central, environment-driven configuration.

Every knob is read from the environment (or a local ``.env``) so the exact same
code runs against local MinIO, real AWS S3, or Azure ADLS Gen2 with only
``GDELT_ENV`` changing.
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

    env: Literal["local", "aws", "azure"] = "local"

    # GDELT source feed
    base_url: str = "http://data.gdeltproject.org/gdeltv2"
    feeds: str = "export"  # comma-separated subset of export,mentions,gkg

    # Object storage. bronze_bucket/silver_bucket name an S3 bucket on AWS and a
    # blob container on Azure. The concept is identical and the code only ever
    # needs the name, so one pair of settings covers both.
    s3_endpoint_url: str | None = None  # None => real AWS S3
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"
    bronze_bucket: str = "gdelt-bronze"
    silver_bucket: str = "gdelt-silver"

    # Azure ADLS Gen2. The account name is the globally unique storage account
    # Terraform creates. Leaving the key unset is the better path: adlfs then uses
    # DefaultAzureCredential, which picks up `az login`, so no secret is stored
    # anywhere. Terraform grants that identity Storage Blob Data Contributor for
    # exactly this reason. The key exists as a fallback for Spark running in a
    # container that has no logged-in Azure CLI to borrow credentials from.
    azure_storage_account: str | None = None
    azure_storage_key: str | None = None

    # Iceberg REST catalog
    iceberg_catalog_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://gdelt-silver/warehouse"

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
    def lake_protocol(self) -> str:
        """fsspec protocol backing the lake: ``s3`` or ``abfs`` (ADLS Gen2)."""
        return "abfs" if self.env == "azure" else "s3"

    def lake_uri(self, container: str, *parts: str) -> str:
        """fsspec URI for an object in the lake.

        Both protocols spell this the same way, ``<protocol>://<container>/<path>``,
        so callers do not branch on the cloud. The Azure account name is not in the
        URI because adlfs takes it from ``storage_options`` instead.
        """
        suffix = "/".join(p.strip("/") for p in parts if p)
        root = f"{self.lake_protocol}://{container}"
        return f"{root}/{suffix}" if suffix else root

    def lake_url(self, container: str) -> str:
        """Fully qualified URL, as Spark and Delta need it.

        This is deliberately not ``lake_uri``. Hadoop's ABFS driver requires the
        account in the authority (``abfss://container@account.dfs.core.windows.net``)
        while adlfs wants it as a separate argument, so the same object has two
        correct spellings depending on who is reading it.
        """
        if self.env == "azure":
            return f"abfss://{container}@{self.azure_storage_account}.dfs.core.windows.net"
        return f"s3://{container}"

    @property
    def storage_options(self) -> dict[str, object]:
        """fsspec kwargs for the configured lake backend.

        Azure: the account key is omitted when unset so adlfs falls back to
        DefaultAzureCredential, which picks up an `az login` session. That keeps
        the working path secret-free.

        S3: credentials are omitted when unset so real AWS falls back to the default
        chain (profile, env vars, or an instance role). The region is always sent:
        without it s3fs signs for us-east-1 and a bucket in another region answers
        403, which surfaces as a bare "Forbidden" that looks like a permissions bug.
        """
        if self.env == "azure":
            if not self.azure_storage_account:
                raise ValueError(
                    "GDELT_ENV=azure requires GDELT_AZURE_STORAGE_ACCOUNT. Set it to the "
                    "storage account name, which `terraform output -raw storage_account` prints."
                )
            azure_opts: dict[str, object] = {"account_name": self.azure_storage_account}
            if self.azure_storage_key:
                azure_opts["account_key"] = self.azure_storage_key
            else:
                azure_opts["anon"] = False  # => DefaultAzureCredential
            return azure_opts

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
