"""Unit tests for zip parsing + schema-contract classification (pure Python)."""

from __future__ import annotations

import io
import zipfile

from gdelt_spark.read import records_from_zip

# Tuple layout: 61 fields, then _source_file, _field_count, _raw_line.
_SRC = 61
_FIELD_COUNT = 62
_RAW_LINE = 63


def _make_zip(lines: list[str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("20260724.export.CSV", "\n".join(lines))
    return buf.getvalue()


def _row(n: int) -> str:
    return "\t".join(str(i) for i in range(n))


def test_conformant_row_maps_to_61_fields_and_reports_count() -> None:
    (record,) = records_from_zip("20260724.export.CSV.zip", _make_zip([_row(61)]))
    assert len(record) == 64  # 61 columns + source + field_count + raw_line
    assert record[0] == "0"
    assert record[60] == "60"
    assert record[_SRC] == "20260724.export.CSV.zip"
    assert record[_FIELD_COUNT] == 61  # conformant
    assert record[_RAW_LINE] == _row(61)


def test_blank_lines_skipped_and_short_rows_flagged() -> None:
    records = list(records_from_zip("f.zip", _make_zip([_row(61), "", "a\tb\tc", "   "])))
    assert len(records) == 2  # blank / whitespace-only lines dropped
    short = records[1]
    assert short[:3] == ("a", "b", "c")
    assert short[3] is None  # padded so the tuple shape stays fixed
    assert short[_FIELD_COUNT] == 3  # but the real count is preserved -> non-conformant


def test_trailing_tab_is_normalized_to_conformant() -> None:
    (record,) = records_from_zip("f.zip", _make_zip([_row(61) + "\t"]))
    assert record[60] == "60"
    assert record[_FIELD_COUNT] == 61  # lone trailing empty field dropped


def test_extra_columns_flagged_as_non_conformant() -> None:
    # Schema drift: GDELT adds a real 62nd column.
    (record,) = records_from_zip("f.zip", _make_zip([_row(62)]))
    assert record[_FIELD_COUNT] == 62  # detected, not silently truncated-away


def test_missing_column_flagged_as_non_conformant() -> None:
    (record,) = records_from_zip("f.zip", _make_zip([_row(60)]))
    assert record[_FIELD_COUNT] == 60
