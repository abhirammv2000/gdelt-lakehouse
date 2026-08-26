"""The lake backend is configuration, not code: same pipeline, three targets.

These pin the parts that are easy to get quietly wrong when a second cloud is
added. Every Settings here passes ``_env_file=None`` so a developer's local .env
cannot leak into an assertion.
"""

from __future__ import annotations

import pytest

from gdelt_pipeline.config import Settings


def _azure(**kw: object) -> Settings:
    base: dict[str, object] = {
        "_env_file": None,
        "env": "azure",
        "azure_storage_account": "gdeltlakehousedev123456",
    }
    base.update(kw)
    return Settings(**base)  # type: ignore[arg-type]


def test_protocol_follows_the_target() -> None:
    assert _azure().lake_protocol == "abfs"
    assert Settings(_env_file=None, env="aws").lake_protocol == "s3"
    assert Settings(_env_file=None, env="local").lake_protocol == "s3"


def test_lake_uri_is_spelled_the_same_way_on_both_clouds() -> None:
    """Callers build paths without branching, which is the point of the seam."""
    parts = ("export", "dt=2026-08-21/hour=12", "20260821121500.export.CSV.zip")

    assert _azure().lake_uri("bronze", *parts) == (
        "abfs://bronze/export/dt=2026-08-21/hour=12/20260821121500.export.CSV.zip"
    )
    assert Settings(_env_file=None, env="aws").lake_uri("bronze", *parts) == (
        "s3://bronze/export/dt=2026-08-21/hour=12/20260821121500.export.CSV.zip"
    )


def test_lake_uri_with_no_path_is_the_container_root() -> None:
    assert _azure().lake_uri("silver") == "abfs://silver"
    assert Settings(_env_file=None, env="aws").lake_uri("silver") == "s3://silver"


def test_spark_url_carries_the_account_but_the_fsspec_uri_does_not() -> None:
    """The same object has two correct spellings and mixing them up fails late.

    Hadoop's ABFS driver needs the account in the authority; adlfs takes it as a
    separate argument and rejects it in the URI. On AWS both spellings coincide,
    which is exactly why this is easy to miss until it fails against real Azure.
    """
    azure = _azure()
    assert azure.lake_url("silver") == (
        "abfss://silver@gdeltlakehousedev123456.dfs.core.windows.net"
    )
    assert "@" not in azure.lake_uri("silver")

    aws = Settings(_env_file=None, env="aws")
    assert aws.lake_url("silver") == aws.lake_uri("silver") == "s3://silver"


def test_azure_without_a_key_uses_the_az_login_credential() -> None:
    """No key set means adlfs falls back to DefaultAzureCredential.

    That is the intended path: Terraform grants the logged-in identity Storage
    Blob Data Contributor, so nothing has to store a secret.
    """
    opts = _azure().storage_options

    assert opts == {"account_name": "gdeltlakehousedev123456", "anon": False}
    assert "account_key" not in opts


def test_azure_with_a_key_uses_it() -> None:
    opts = _azure(azure_storage_key="deadbeef==").storage_options

    assert opts["account_key"] == "deadbeef=="
    assert "anon" not in opts


def test_azure_without_an_account_fails_with_an_actionable_message() -> None:
    """A missing account otherwise surfaces deep inside adlfs as an auth error."""
    settings = Settings(_env_file=None, env="azure", azure_storage_account=None)

    with pytest.raises(ValueError, match="GDELT_AZURE_STORAGE_ACCOUNT"):
        _ = settings.storage_options


def test_adding_azure_did_not_change_the_s3_options() -> None:
    """Regression guard on the region fix: s3fs must always be told the region,
    or it signs for us-east-1 and another region answers a bare 403."""
    aws = Settings(_env_file=None, env="aws", s3_endpoint_url=None, s3_region="us-east-2")

    assert aws.storage_options["client_kwargs"] == {"region_name": "us-east-2"}
