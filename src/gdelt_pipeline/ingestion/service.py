"""Ingestion orchestration: fetch -> (skip if present) -> download+verify -> land -> checkpoint."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from gdelt_pipeline.config import Settings, get_settings
from gdelt_pipeline.ingestion.checkpoint import Checkpoint
from gdelt_pipeline.ingestion.gdelt_client import GdeltClient, GdeltFile
from gdelt_pipeline.ingestion.storage import BronzeStorage
from gdelt_pipeline.logging import get_logger

log = get_logger(__name__)


@dataclass
class IngestResult:
    landed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    @property
    def summary(self) -> dict[str, int]:
        return {"landed": len(self.landed), "skipped": len(self.skipped), "failed": len(self.failed)}


class IngestService:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._storage = BronzeStorage(self._settings)
        self._checkpoint = Checkpoint(self._settings)

    def ingest_latest(self) -> IngestResult:
        """Ingest the current 15-minute batch."""
        with GdeltClient(
            self._settings.base_url,
            timeout=self._settings.http_timeout_seconds,
            max_retries=self._settings.max_retries,
        ) as client:
            files = client.latest(self._settings.enabled_feeds)
            return self._ingest_files(client, files)

    def backfill(self, start: datetime, end: datetime) -> IngestResult:
        """Ingest every published file with a timestamp in [start, end)."""
        with GdeltClient(
            self._settings.base_url,
            timeout=self._settings.http_timeout_seconds,
            max_retries=self._settings.max_retries,
        ) as client:
            files = [
                f
                for f in client.list_all(self._settings.enabled_feeds)
                if start <= f.timestamp < end
            ]
            log.info("backfill_window", start=start.isoformat(), end=end.isoformat(), files=len(files))
            return self._ingest_files(client, files)

    def _ingest_files(self, client: GdeltClient, files: list[GdeltFile]) -> IngestResult:
        self._storage.ensure_bucket()
        result = IngestResult()
        for file in files:
            uri = self._storage.key(file.feed, file.partition_path, file.filename)
            if self._storage.exists(uri):
                result.skipped.append(file.filename)
                continue
            try:
                payload = client.download(file)
                self._storage.write(uri, payload)
                self._checkpoint.advance(file.feed, file.timestamp)
                result.landed.append(file.filename)
            except Exception as exc:  # noqa: BLE001 - record & continue, don't fail the batch
                log.error("ingest_failed", file=file.filename, error=str(exc))
                result.failed.append(file.filename)
        log.info("ingest_complete", **result.summary)
        return result
