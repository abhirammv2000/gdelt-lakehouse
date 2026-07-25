"""Client for the GDELT 2.0 file feed.

GDELT publishes a new batch every 15 minutes. Two index files drive ingestion:

* ``lastupdate.txt``      — the 3 most recent files (export, mentions, gkg)
* ``masterfilelist.txt``  — every file ever published (used for backfill)

Each line has the shape::

    <size_bytes> <md5> <url>

e.g. ``227762 6f9a...e1 http://data.gdeltproject.org/gdeltv2/20260723121500.export.CSV.zip``
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from gdelt_pipeline.logging import get_logger

log = get_logger(__name__)

# Matches e.g. 20260723121500.export.CSV.zip  ->  (timestamp, feed)
_FILENAME_RE = re.compile(r"(\d{14})\.(export|mentions|gkg)\.", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class GdeltFile:
    """One downloadable GDELT artifact."""

    url: str
    size: int
    md5: str
    timestamp: datetime  # UTC, parsed from the 14-digit filename prefix
    feed: str  # export | mentions | gkg

    @property
    def filename(self) -> str:
        return self.url.rsplit("/", 1)[-1]

    @property
    def partition_path(self) -> str:
        """Hive-style partition used in bronze: dt=YYYY-MM-DD/hour=HH."""
        return f"dt={self.timestamp:%Y-%m-%d}/hour={self.timestamp:%H}"


def _parse_line(line: str) -> GdeltFile | None:
    parts = line.split()
    if len(parts) != 3:
        return None
    size, md5, url = parts
    m = _FILENAME_RE.search(url)
    if not m:
        return None
    ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    return GdeltFile(url=url, size=int(size), md5=md5, timestamp=ts, feed=m.group(2).lower())


class GdeltClient:
    def __init__(self, base_url: str, timeout: float = 60.0, max_retries: int = 5) -> None:
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._client = httpx.Client(timeout=timeout, follow_redirects=True)

    def __enter__(self) -> GdeltClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _index(self, name: str) -> list[GdeltFile]:
        resp = self._get(f"{self._base_url}/{name}")
        files = [f for line in resp.text.splitlines() if (f := _parse_line(line))]
        log.info("fetched_index", index=name, count=len(files))
        return files

    def latest(self, feeds: list[str]) -> list[GdeltFile]:
        """The current 15-minute batch, filtered to the requested feeds."""
        return [f for f in self._index("lastupdate.txt") if f.feed in feeds]

    def list_all(self, feeds: list[str]) -> list[GdeltFile]:
        """Every published file (for backfill), sorted oldest-first."""
        files = [f for f in self._index("masterfilelist.txt") if f.feed in feeds]
        return sorted(files, key=lambda f: f.timestamp)

    def download(self, file: GdeltFile) -> bytes:
        """Download and verify the MD5 checksum GDELT ships in the index."""
        resp = self._get(file.url)
        payload = resp.content
        digest = hashlib.md5(payload).hexdigest()  # noqa: S324 - integrity, not security
        if file.md5 and digest != file.md5:
            raise ValueError(f"MD5 mismatch for {file.filename}: {digest} != {file.md5}")
        log.info("downloaded", file=file.filename, bytes=len(payload))
        return payload

    def _get(self, url: str) -> httpx.Response:
        @retry(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            reraise=True,
        )
        def _do() -> httpx.Response:
            resp = self._client.get(url)
            resp.raise_for_status()
            return resp

        return _do()
