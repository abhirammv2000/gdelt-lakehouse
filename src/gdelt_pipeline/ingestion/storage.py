"""Thin storage layer over fsspec so bronze writes work identically on
local disk, MinIO, real AWS S3, and Azure ADLS Gen2.

The protocol and URI both come from ``Settings``, so nothing in here knows
which cloud it is talking to.
"""

from __future__ import annotations

import fsspec

from gdelt_pipeline.config import Settings
from gdelt_pipeline.logging import get_logger

log = get_logger(__name__)


class BronzeStorage:
    """Writes raw GDELT artifacts to the bronze container, laid out as::

        <s3|abfs>://<bronze>/<feed>/dt=YYYY-MM-DD/hour=HH/<filename>
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._bucket = settings.bronze_bucket
        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(
            settings.lake_protocol, **settings.storage_options
        )

    def key(self, feed: str, partition_path: str, filename: str) -> str:
        return self._settings.lake_uri(self._bucket, feed, partition_path, filename)

    def exists(self, uri: str) -> bool:
        return bool(self._fs.exists(uri))

    def write(self, uri: str, payload: bytes) -> None:
        parent = uri.rsplit("/", 1)[0]
        self._fs.makedirs(parent, exist_ok=True)
        with self._fs.open(uri, "wb") as fh:
            fh.write(payload)
        log.info("bronze_write", uri=uri, bytes=len(payload))

    def ensure_bucket(self) -> None:
        root = self._settings.lake_uri(self._bucket)
        if not self._fs.exists(root):
            self._fs.mkdir(root)
