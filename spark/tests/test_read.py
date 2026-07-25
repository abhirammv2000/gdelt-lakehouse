"""Unit tests for zip parsing (pure Python, no Spark needed)."""

from __future__ import annotations

import io
import zipfile

from gdelt_spark.read import records_from_zip


def _make_zip(lines: list[str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("20260724.export.CSV", "\n".join(lines))
    return buf.getvalue()


def test_full_row_maps_to_61_fields_plus_source() -> None:
    full = "\t".join(str(i) for i in range(61))
    (record,) = records_from_zip("20260724.export.CSV.zip", _make_zip([full]))
    assert len(record) == 62  # 61 columns + _source_file
    assert record[0] == "0"
    assert record[60] == "60"
    assert record[61] == "20260724.export.CSV.zip"


def test_blank_lines_skipped_and_short_rows_padded() -> None:
    full = "\t".join(str(i) for i in range(61))
    short = "a\tb\tc"
    records = list(records_from_zip("f.zip", _make_zip([full, "", short, "   "])))
    assert len(records) == 2  # blank / whitespace-only lines dropped
    padded = records[1]
    assert padded[:3] == ("a", "b", "c")
    assert padded[3] is None  # short row padded with NULLs
    assert padded[61] == "f.zip"


def test_trailing_tab_does_not_create_62nd_column() -> None:
    row_with_trailing_tab = "\t".join(str(i) for i in range(61)) + "\t"
    (record,) = records_from_zip("f.zip", _make_zip([row_with_trailing_tab]))
    assert len(record) == 62
    assert record[60] == "60"
