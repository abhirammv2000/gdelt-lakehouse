"""Integration test for IngestService against a real moto S3 *server* and a
mocked GDELT feed.

We use ``ThreadedMotoServer`` (real HTTP) rather than the in-process ``mock_aws``
decorator because ``s3fs`` talks to S3 through ``aiobotocore``, which does not
compose with moto's in-process patching. Pointing s3fs at the moto server also
mirrors exactly how the code talks to MinIO / real S3.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

import boto3
import httpx
import pytest
import respx
from moto.server import ThreadedMotoServer

from gdelt_pipeline.config import Settings
from gdelt_pipeline.ingestion.service import IngestService

BASE = "http://data.gdeltproject.org/gdeltv2"


@pytest.fixture(scope="module")
def moto_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    _, port = server.get_host_and_port()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def settings(moto_endpoint: str) -> Settings:
    return Settings(
        env="aws",
        base_url=BASE,
        feeds="export",
        s3_endpoint_url=moto_endpoint,
        s3_access_key="test",
        s3_secret_key="test",
        bronze_bucket="gdelt-bronze",
    )


@pytest.fixture(autouse=True)
def _reset_s3fs() -> None:
    """s3fs caches filesystem instances process-wide; clear it between tests."""
    import s3fs

    s3fs.S3FileSystem.clear_instance_cache()


def _feed_line(payload: bytes, ts: str = "20260723121500") -> tuple[str, str]:
    url = f"{BASE}/{ts}.export.CSV.zip"
    md5 = hashlib.md5(payload).hexdigest()
    return url, f"{len(payload)} {md5} {url}"


@respx.mock
def test_ingest_latest_lands_and_is_idempotent(settings: Settings, moto_endpoint: str) -> None:
    boto3.client(
        "s3",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    ).create_bucket(Bucket="gdelt-bronze")

    payload = b"gdelt\trow\n"
    url, line = _feed_line(payload)
    respx.get(f"{BASE}/lastupdate.txt").mock(return_value=httpx.Response(200, text=line))
    respx.get(url).mock(return_value=httpx.Response(200, content=payload))

    service = IngestService(settings)

    first = service.ingest_latest()
    assert first.summary == {"landed": 1, "skipped": 0, "failed": 0}

    # Re-running the same batch must skip - proves idempotency.
    second = service.ingest_latest()
    assert second.summary == {"landed": 0, "skipped": 1, "failed": 0}
