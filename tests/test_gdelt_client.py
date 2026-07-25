"""Unit tests for the GDELT feed client (network mocked with respx)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import httpx
import pytest
import respx

from gdelt_pipeline.ingestion.gdelt_client import GdeltClient, _parse_line

BASE = "http://data.gdeltproject.org/gdeltv2"


def test_parse_line_extracts_timestamp_and_feed() -> None:
    line = f"227762 abc123 {BASE}/20260723121500.export.CSV.zip"
    f = _parse_line(line)
    assert f is not None
    assert f.feed == "export"
    assert f.size == 227762
    assert f.timestamp == datetime(2026, 7, 23, 12, 15, 0, tzinfo=UTC)
    assert f.partition_path == "dt=2026-07-23/hour=12"


@pytest.mark.parametrize("bad", ["", "only two", "a b c d", "227762 md5 http://x/nope.txt"])
def test_parse_line_rejects_malformed(bad: str) -> None:
    assert _parse_line(bad) is None


@respx.mock
def test_latest_filters_to_requested_feeds() -> None:
    body = "\n".join(
        [
            f"1 h1 {BASE}/20260723121500.export.CSV.zip",
            f"2 h2 {BASE}/20260723121500.mentions.CSV.zip",
            f"3 h3 {BASE}/20260723121500.gkg.csv.zip",
        ]
    )
    respx.get(f"{BASE}/lastupdate.txt").mock(return_value=httpx.Response(200, text=body))
    with GdeltClient(BASE) as client:
        files = client.latest(["export"])
    assert [f.feed for f in files] == ["export"]


@respx.mock
def test_download_verifies_md5() -> None:
    payload = b"tab\tseparated\tgdelt\trow\n"
    good_md5 = hashlib.md5(payload).hexdigest()
    url = f"{BASE}/20260723121500.export.CSV.zip"
    line = f"{len(payload)} {good_md5} {url}"
    respx.get(url).mock(return_value=httpx.Response(200, content=payload))

    with GdeltClient(BASE) as client:
        file = _parse_line(line)
        assert file is not None
        assert client.download(file) == payload


@respx.mock
def test_download_raises_on_md5_mismatch() -> None:
    url = f"{BASE}/20260723121500.export.CSV.zip"
    line = f"10 deadbeef {url}"
    respx.get(url).mock(return_value=httpx.Response(200, content=b"corrupted"))
    with GdeltClient(BASE) as client:
        file = _parse_line(line)
        assert file is not None
        with pytest.raises(ValueError, match="MD5 mismatch"):
            client.download(file)
