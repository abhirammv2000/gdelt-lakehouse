"""Idempotency checkpoint.

The ingest job records the last GDELT timestamp it successfully landed, per feed,
in a small JSON object next to the data. Re-running is therefore a no-op for
already-ingested batches — the property that makes the 15-minute Airflow schedule
safe to retry.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import fsspec

from gdelt_pipeline.config import Settings
from gdelt_pipeline.logging import get_logger

log = get_logger(__name__)


class Checkpoint:
    def __init__(self, settings: Settings) -> None:
        self._uri = f"s3://{settings.bronze_bucket}/_meta/checkpoint.json"
        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem("s3", **settings.storage_options)

    def _load(self) -> dict[str, str]:
        if not self._fs.exists(self._uri):
            return {}
        with self._fs.open(self._uri, "r") as fh:
            return dict(json.load(fh))

    def last_timestamp(self, feed: str) -> datetime | None:
        raw = self._load().get(feed)
        return datetime.fromisoformat(raw) if raw else None

    def advance(self, feed: str, ts: datetime) -> None:
        state = self._load()
        current = state.get(feed)
        if current and datetime.fromisoformat(current) >= ts:
            return
        state[feed] = ts.astimezone(timezone.utc).isoformat()
        parent = self._uri.rsplit("/", 1)[0]
        self._fs.makedirs(parent, exist_ok=True)
        with self._fs.open(self._uri, "w") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
        log.info("checkpoint_advanced", feed=feed, timestamp=state[feed])
