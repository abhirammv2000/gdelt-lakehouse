"""Per-event data-quality gate for the stream.

Mirrors the batch silver expectations ([[gdelt_spark.validate]] in spirit) but at
single-event granularity: pure functions, no Spark. Events that fail are
dead-lettered by the consumer; high-impact events are routed to an alerts topic.
"""

from __future__ import annotations

from typing import Any

_QUAD_CLASSES = {1, 2, 3, 4}

# A conflict event is "high impact" when it is very escalatory (Goldstein) or a
# well-corroborated material-conflict event.
_ALERT_GOLDSTEIN = -8.0
_ALERT_MIN_MENTIONS = 10


def validate_event(event: dict[str, Any]) -> list[str]:
    """Return a list of violation codes; empty means the event passes the gate."""
    violations: list[str] = []

    if event.get("global_event_id") is None:
        violations.append("global_event_id_missing")
    if not event.get("event_day"):
        violations.append("event_day_missing")

    quad_class = event.get("quad_class")
    if quad_class is not None and quad_class not in _QUAD_CLASSES:
        violations.append("quad_class_out_of_range")

    goldstein = event.get("goldstein_scale")
    if goldstein is not None and not -10.0 <= goldstein <= 10.0:
        violations.append("goldstein_out_of_range")

    tone = event.get("avg_tone")
    if tone is not None and not -100.0 <= tone <= 100.0:
        violations.append("avg_tone_out_of_range")

    lat = event.get("action_lat")
    if lat is not None and not -90.0 <= lat <= 90.0:
        violations.append("action_lat_out_of_range")

    long = event.get("action_long")
    if long is not None and not -180.0 <= long <= 180.0:
        violations.append("action_long_out_of_range")

    return violations


def is_high_impact(event: dict[str, Any]) -> bool:
    goldstein = event.get("goldstein_scale")
    if goldstein is not None and goldstein <= _ALERT_GOLDSTEIN:
        return True
    return event.get("quad_class") == 4 and (event.get("num_mentions") or 0) >= _ALERT_MIN_MENTIONS
