"""Publish a landed bronze batch onto Kafka/Redpanda, one message per event.

Uses an idempotent, ``acks=all`` producer so retries never duplicate or lose
messages, and keys each event by country for stable partitioning.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from typing import Any

import fsspec

from gdelt_pipeline.config import Settings, get_settings
from gdelt_pipeline.logging import get_logger
from gdelt_pipeline.streaming.event import event_from_row, partition_key, to_json

log = get_logger(__name__)

_N_COLS = 61


def _iter_events(fs: fsspec.AbstractFileSystem, uri: str) -> Iterator[dict[str, Any]]:
    with fs.open(uri, "rb") as fh:
        content = fh.read()
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()
        if not names:
            return
        text = zf.read(names[0]).decode("utf-8", errors="replace")
    for line in text.split("\n"):
        if not line.strip():
            continue
        row = line.rstrip("\r").split("\t")
        row = (row + [""] * _N_COLS)[:_N_COLS]
        yield event_from_row(row)


class GdeltStreamProducer:
    def __init__(self, settings: Settings | None = None) -> None:
        from confluent_kafka import Producer

        self._settings = settings or get_settings()
        self._producer = Producer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
            }
        )

    def publish_events(self, events: Iterator[dict[str, Any]], topic: str | None = None) -> int:
        topic = topic or self._settings.kafka_topic
        count = 0
        for event in events:
            self._producer.produce(topic, key=partition_key(event), value=to_json(event))
            count += 1
            if count % 1000 == 0:
                self._producer.poll(0)  # serve delivery callbacks, apply backpressure
        self._producer.flush(30)
        log.info("stream_published", topic=topic, events=count)
        return count

    def publish_bronze_latest(self, feed: str | None = None, topic: str | None = None) -> int:
        """Find the newest bronze ``*.CSV.zip`` for the feed and stream its events."""
        feed = feed or self._settings.enabled_feeds[0]
        fs = fsspec.filesystem("s3", **self._settings.storage_options)
        matches = fs.glob(f"s3://{self._settings.bronze_bucket}/{feed}/**/*.CSV.zip")
        if not matches:
            log.info("stream_no_bronze", feed=feed)
            return 0
        latest = max(matches)  # timestamped, zero-padded paths sort chronologically
        uri = latest if str(latest).startswith("s3://") else f"s3://{latest}"
        log.info("stream_source", uri=uri)
        return self.publish_events(_iter_events(fs, uri), topic)
