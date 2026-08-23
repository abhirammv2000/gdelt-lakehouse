"""Executable proof for the failure-mode table in docs/DESIGN_DECISIONS.md.

Every test here triggers a documented failure on purpose and asserts the pipeline
behaves the way the table claims. The table lists behaviours; this file is the
evidence that they fire. Test ids are FM-n and the table cites them.

These run in CI (no Spark, no Docker). The Spark-side gates (the silver quality
suite aborting a write, SchemaDriftError above the malformed-rate threshold) are
covered by spark/tests, which run in the Spark container via `make spark-test`.
"""

from __future__ import annotations

import hashlib
import io
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime

import boto3
import httpx
import pytest
import respx
from moto.server import ThreadedMotoServer

from gdelt_pipeline.config import Settings
from gdelt_pipeline.ingestion.checkpoint import Checkpoint
from gdelt_pipeline.ingestion.service import IngestService
from gdelt_pipeline.schema.events import EXPECTED_COLUMN_COUNT

BASE = "http://data.gdeltproject.org/gdeltv2"

# Column offsets of the trailing metadata that read.py appends after the 61 fields.
FIELD_COUNT_IDX = EXPECTED_COLUMN_COUNT + 1
RAW_LINE_IDX = EXPECTED_COLUMN_COUNT + 2


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
        bronze_bucket="fm-bronze",
    )


@pytest.fixture(autouse=True)
def _reset_s3fs() -> None:
    """s3fs caches filesystem instances process-wide; clear it between tests."""
    import s3fs

    s3fs.S3FileSystem.clear_instance_cache()


def _make_bucket(endpoint: str, name: str) -> None:
    boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    ).create_bucket(Bucket=name)


def _index_line(payload: bytes, ts: str, md5: str | None = None) -> tuple[str, str]:
    """Build one feed-index row: size, checksum, url."""
    url = f"{BASE}/{ts}.export.CSV.zip"
    digest = md5 if md5 is not None else hashlib.md5(payload).hexdigest()
    return url, f"{len(payload)} {digest} {url}"


def _zip_of(rows: list[str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("export.CSV", "\n".join(rows) + "\n")
    return buf.getvalue()


def _row(fields: int, event_id: str) -> str:
    return "\t".join([event_id] + [f"c{i}" for i in range(1, fields)])


# ---------------------------------------------------------------------------
# FM-1  A corrupt file is recorded as failed and the rest of the batch lands.
# ---------------------------------------------------------------------------
@respx.mock
def test_fm1_corrupt_file_fails_alone_and_batch_continues(
    settings: Settings, moto_endpoint: str
) -> None:
    _make_bucket(moto_endpoint, "fm-bronze")

    good, bad = b"good-payload\n", b"bad-payload\n"
    good_url, good_line = _index_line(good, "20260101000000")
    # Advertise a checksum the payload cannot match: simulated corruption.
    bad_url, bad_line = _index_line(bad, "20260101001500", md5="0" * 32)

    respx.get(f"{BASE}/lastupdate.txt").mock(
        return_value=httpx.Response(200, text=f"{good_line}\n{bad_line}")
    )
    respx.get(good_url).mock(return_value=httpx.Response(200, content=good))
    respx.get(bad_url).mock(return_value=httpx.Response(200, content=bad))

    result = IngestService(settings).ingest_latest()

    assert result.summary == {"landed": 1, "skipped": 0, "failed": 1}
    assert result.landed == ["20260101000000.export.CSV.zip"]
    assert result.failed == ["20260101001500.export.CSV.zip"]


# ---------------------------------------------------------------------------
# FM-2  After a partial batch, a re-run lands only what is missing.
# ---------------------------------------------------------------------------
@respx.mock
def test_fm2_rerun_lands_only_the_missing_file(settings: Settings, moto_endpoint: str) -> None:
    _make_bucket(moto_endpoint, "fm-bronze")

    first, second = b"first\n", b"second\n"
    first_url, first_line = _index_line(first, "20260202000000")
    second_url, second_line = _index_line(second, "20260202001500")

    respx.get(f"{BASE}/lastupdate.txt").mock(
        return_value=httpx.Response(200, text=f"{first_line}\n{second_line}")
    )
    respx.get(first_url).mock(return_value=httpx.Response(200, content=first))
    # Pass 1: the second file arrives corrupt, so only the first lands.
    respx.get(second_url).mock(return_value=httpx.Response(200, content=b"corrupted"))

    service = IngestService(settings)
    assert service.ingest_latest().summary == {"landed": 1, "skipped": 0, "failed": 1}

    # Pass 2: the source is healthy. The landed file is skipped rather than
    # re-fetched, and only the previously failed one is written.
    respx.get(second_url).mock(return_value=httpx.Response(200, content=second))
    assert service.ingest_latest().summary == {"landed": 1, "skipped": 1, "failed": 0}

    # Pass 3: nothing left to do.
    assert service.ingest_latest().summary == {"landed": 0, "skipped": 2, "failed": 0}


# ---------------------------------------------------------------------------
# FM-3  Malformed rows stay separable from good rows in the same batch.
# ---------------------------------------------------------------------------
def test_fm3_malformed_rows_are_separable_from_good_rows() -> None:
    from gdelt_pipeline.schema.parse import records_from_zip

    rows = [
        _row(EXPECTED_COLUMN_COUNT, "good-1"),
        _row(EXPECTED_COLUMN_COUNT - 3, "short-1"),
        _row(EXPECTED_COLUMN_COUNT, "good-2"),
        _row(EXPECTED_COLUMN_COUNT + 5, "wide-1"),
    ]
    parsed = list(records_from_zip("batch.zip", _zip_of(rows)))
    counts = [r[FIELD_COUNT_IDX] for r in parsed]

    assert len(parsed) == 4
    assert counts.count(EXPECTED_COLUMN_COUNT) == 2, "good rows survive alongside bad ones"
    assert sorted(c for c in counts if c != EXPECTED_COLUMN_COUNT) == [
        EXPECTED_COLUMN_COUNT - 3,
        EXPECTED_COLUMN_COUNT + 5,
    ]
    # The raw line is kept verbatim so a rejected row can be inspected or replayed.
    assert all(isinstance(r[RAW_LINE_IDX], str) and r[RAW_LINE_IDX] for r in parsed)


# ---------------------------------------------------------------------------
# FM-4  A phantom 62nd column is flagged; the historical trailing tab is not.
# ---------------------------------------------------------------------------
def test_fm4_phantom_column_detected_and_trailing_tab_is_not() -> None:
    from gdelt_pipeline.schema.parse import records_from_zip

    phantom = _row(EXPECTED_COLUMN_COUNT, "drift-1") + "\tEXTRA_COLUMN_DATA"
    trailing_tab = _row(EXPECTED_COLUMN_COUNT, "quirk-1") + "\t"

    parsed = list(records_from_zip("drift.zip", _zip_of([phantom, trailing_tab])))
    counts = [r[FIELD_COUNT_IDX] for r in parsed]

    assert counts[0] == EXPECTED_COLUMN_COUNT + 1, "a real extra column must be flagged"
    assert counts[1] == EXPECTED_COLUMN_COUNT, "a lone trailing tab must be normalised away"


# ---------------------------------------------------------------------------
# FM-5  The region reaches fsspec even with no endpoint or explicit credentials.
#       Without it s3fs signs for us-east-1 and another region answers a bare 403.
# ---------------------------------------------------------------------------
def test_fm5_region_is_always_passed_to_fsspec() -> None:
    # _env_file=None keeps a developer's local .env out of the assertion, so this
    # tests the code rather than whatever happens to be configured on the machine.
    aws = Settings(_env_file=None, env="aws", s3_endpoint_url=None, s3_region="us-east-2")
    opts = aws.storage_options
    assert opts["client_kwargs"] == {"region_name": "us-east-2"}
    assert "key" not in opts, "real AWS falls back to the default credential chain"

    minio = Settings(
        _env_file=None,
        env="local",
        s3_endpoint_url="http://localhost:9000",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_region="us-east-1",
    )
    assert minio.storage_options["client_kwargs"] == {
        "region_name": "us-east-1",
        "endpoint_url": "http://localhost:9000",
    }


# ---------------------------------------------------------------------------
# FM-6  The checkpoint is monotonic: a replayed batch cannot move it backwards.
# ---------------------------------------------------------------------------
def test_fm6_checkpoint_never_moves_backwards(settings: Settings, moto_endpoint: str) -> None:
    _make_bucket(moto_endpoint, "fm-bronze")
    cp = Checkpoint(settings)

    newer = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    older = datetime(2026, 8, 22, 11, 0, tzinfo=UTC)

    cp.advance("export", newer)
    assert cp.last_timestamp("export") == newer

    cp.advance("export", older)
    assert cp.last_timestamp("export") == newer, "checkpoint must not regress"
