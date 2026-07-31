"""Unit tests for the streaming event contract and per-event DQ gate."""

from __future__ import annotations

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES
from gdelt_pipeline.streaming.dq import is_high_impact, validate_event
from gdelt_pipeline.streaming.event import (
    event_from_row,
    from_json,
    partition_key,
    to_json,
)

_IDX = {name: i for i, name in enumerate(EVENT_COLUMN_NAMES)}


def _row(**overrides: str) -> list[str]:
    row = [""] * 61
    for name, value in overrides.items():
        row[_IDX[name]] = value
    return row


def test_event_from_row_types_and_derives_day() -> None:
    event = event_from_row(
        _row(
            global_event_id="42",
            sql_date="20260729",
            quad_class="4",
            goldstein_scale="-9.5",
            avg_tone="-3.2",
            action_geo_country_code="US",
        )
    )
    assert event["global_event_id"] == 42
    assert event["event_day"] == "2026-07-29"
    assert event["quad_class"] == 4
    assert event["goldstein_scale"] == -9.5
    assert event["action_country_code"] == "US"
    # empty GDELT fields become None, not ""
    assert event["actor1_name"] is None


def test_json_round_trips() -> None:
    event = event_from_row(_row(global_event_id="1", sql_date="20260101"))
    assert from_json(to_json(event)) == event


def test_partition_key_prefers_action_country() -> None:
    assert partition_key({"action_country_code": "FR", "actor1_country_code": "US"}) == b"FR"
    assert partition_key({"actor1_country_code": "US"}) == b"US"
    assert partition_key({}) is None


def test_dq_passes_clean_event() -> None:
    event = event_from_row(_row(global_event_id="1", sql_date="20260101", quad_class="2"))
    assert validate_event(event) == []


def test_dq_flags_missing_key_and_bad_ranges() -> None:
    bad = {
        "global_event_id": None,
        "event_day": None,
        "quad_class": 9,
        "goldstein_scale": 50.0,
        "avg_tone": -250.0,
        "action_lat": 100.0,
        "action_long": 5.0,
    }
    violations = set(validate_event(bad))
    assert "global_event_id_missing" in violations
    assert "event_day_missing" in violations
    assert "quad_class_out_of_range" in violations
    assert "goldstein_out_of_range" in violations
    assert "avg_tone_out_of_range" in violations
    assert "action_lat_out_of_range" in violations
    assert "action_long_out_of_range" not in violations  # 5.0 is valid


def test_high_impact_detection() -> None:
    assert is_high_impact({"goldstein_scale": -9.0}) is True
    assert is_high_impact({"quad_class": 4, "num_mentions": 15}) is True
    assert is_high_impact({"quad_class": 4, "num_mentions": 3}) is False
    assert is_high_impact({"goldstein_scale": 1.0, "quad_class": 1}) is False
