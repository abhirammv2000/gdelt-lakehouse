"""The streaming event: a small typed subset of the 61 GDELT columns.

Kept small and JSON-serializable so the stream is easy to work with. Field order is
taken from the schema module so the two stay in sync.
"""

from __future__ import annotations

import json
from typing import Any

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES

_IDX = {name: i for i, name in enumerate(EVENT_COLUMN_NAMES)}


def _raw(row: list[str], name: str) -> str | None:
    value = row[_IDX[name]].strip()
    return value or None


def _as_int(row: list[str], name: str) -> int | None:
    value = _raw(row, name)
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _as_float(row: list[str], name: str) -> float | None:
    value = _raw(row, name)
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def event_from_row(row: list[str]) -> dict[str, Any]:
    """Build a streaming event dict from a raw 61-field GDELT export row."""
    sql_date = _raw(row, "sql_date")
    event_day = (
        f"{sql_date[:4]}-{sql_date[4:6]}-{sql_date[6:8]}"
        if sql_date and len(sql_date) == 8
        else None
    )
    return {
        "global_event_id": _as_int(row, "global_event_id"),
        "event_day": event_day,
        "actor1_name": _raw(row, "actor1_name"),
        "actor2_name": _raw(row, "actor2_name"),
        "actor1_country_code": _raw(row, "actor1_country_code"),
        "actor2_country_code": _raw(row, "actor2_country_code"),
        "event_code": _raw(row, "event_code"),
        "event_root_code": _raw(row, "event_root_code"),
        "quad_class": _as_int(row, "quad_class"),
        "goldstein_scale": _as_float(row, "goldstein_scale"),
        "num_mentions": _as_int(row, "num_mentions"),
        "avg_tone": _as_float(row, "avg_tone"),
        "action_country_code": _raw(row, "action_geo_country_code"),
        "action_location": _raw(row, "action_geo_fullname"),
        "action_lat": _as_float(row, "action_geo_lat"),
        "action_long": _as_float(row, "action_geo_long"),
        "date_added": _raw(row, "date_added"),
        "source_url": _raw(row, "source_url"),
    }


def to_json(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def from_json(data: bytes) -> dict[str, Any]:
    parsed = json.loads(data)
    if not isinstance(parsed, dict):
        raise ValueError("event payload is not a JSON object")
    return parsed


def partition_key(event: dict[str, Any]) -> bytes | None:
    """Key by country so events for one place land on the same partition (ordered)."""
    key = event.get("action_country_code") or event.get("actor1_country_code")
    return key.encode("utf-8") if key else None
