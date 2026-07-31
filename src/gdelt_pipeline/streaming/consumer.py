"""Stream processor: DQ-gate every event, dead-letter failures, alert on impact.

Demonstrates the streaming fundamentals a real consumer needs:
- a **consumer group** with **manual offset commits** (at-least-once delivery),
- a **dead-letter topic** for events that fail deserialization or the DQ gate,
- content-based **routing/alerting** (high-impact conflict events fan out to an
  alerts topic downstream systems can subscribe to).
"""

from __future__ import annotations

from typing import Any

from gdelt_pipeline.config import Settings, get_settings
from gdelt_pipeline.logging import get_logger
from gdelt_pipeline.streaming.dq import is_high_impact, validate_event
from gdelt_pipeline.streaming.event import from_json, to_json

log = get_logger(__name__)


class GdeltStreamConsumer:
    def __init__(self, settings: Settings | None = None) -> None:
        from confluent_kafka import Consumer, Producer

        self._settings = settings or get_settings()
        self._consumer = Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap,
                "group.id": self._settings.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # commit only after we've handled the message
            }
        )
        # Reuse an idempotent producer to emit to the DLQ / alerts topics.
        self._producer = Producer(
            {"bootstrap.servers": self._settings.kafka_bootstrap, "enable.idempotence": True, "acks": "all"}
        )

    def run(self, max_messages: int | None = None, idle_timeout: float | None = None) -> dict[str, int]:
        """Poll and process until ``max_messages`` handled or idle for ``idle_timeout`` s."""
        self._consumer.subscribe([self._settings.kafka_topic])
        stats = {"consumed": 0, "valid": 0, "dead_lettered": 0, "alerts": 0}
        idle = 0.0
        try:
            while max_messages is None or stats["consumed"] < max_messages:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    idle += 1.0
                    if idle_timeout is not None and idle >= idle_timeout:
                        break
                    continue
                idle = 0.0
                if msg.error():
                    log.error("stream_consume_error", error=str(msg.error()))
                    continue
                self._handle(msg, stats)
                self._consumer.commit(message=msg, asynchronous=False)
        finally:
            self._producer.flush(10)
            self._consumer.close()
        log.info("stream_consume_complete", **stats)
        return stats

    def _handle(self, msg: Any, stats: dict[str, int]) -> None:
        stats["consumed"] += 1
        try:
            event = from_json(msg.value())
        except (ValueError, TypeError):
            self._dead_letter(msg.key(), msg.value())
            stats["dead_lettered"] += 1
            return

        violations = validate_event(event)
        if violations:
            self._dead_letter(msg.key(), to_json({**event, "_violations": violations}))
            stats["dead_lettered"] += 1
            return

        stats["valid"] += 1
        if is_high_impact(event):
            self._producer.produce(self._settings.kafka_alerts_topic, key=msg.key(), value=msg.value())
            stats["alerts"] += 1
        self._producer.poll(0)

    def _dead_letter(self, key: bytes | None, value: bytes) -> None:
        self._producer.produce(self._settings.kafka_dlq_topic, key=key, value=value)
