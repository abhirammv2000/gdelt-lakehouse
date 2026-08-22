"""Parse GDELT export rows against the 61-field schema contract.

This is the heart of the schema-drift handling, and it is deliberately pure
Python: no Spark, no I/O. The Spark job wraps it (see ``gdelt_spark.read``), but
keeping the contract logic here means the drift behaviour can be tested without a
JVM, so it runs in CI rather than only inside the Spark container.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES

_N_COLS = len(EVENT_COLUMN_NAMES)  # 61 - the schema contract


def normalize_fields(line: str) -> list[str]:
    """Split one raw line and undo GDELT's historical trailing-tab quirk.

    Some GDELT rows end with a single empty trailing field. That is a formatting
    artefact, not a 62nd column, so it is dropped before the field count is taken.
    A trailing field with actual content is left alone, because that is real drift.
    """
    parts = line.split("\t")
    if len(parts) == _N_COLS + 1 and parts[-1] == "":
        parts = parts[:-1]
    return parts


def records_from_zip(source_file: str, content: bytes) -> Iterator[tuple[object, ...]]:
    """Yield one row-tuple per data line: 61 fields + source, field count, raw line.

    Fields are padded or truncated to 61 so the good path keeps a fixed schema,
    while ``_field_count`` carries the actual count so the caller can quarantine
    rows that do not match the contract. Decoding uses UTF-8 with replacement to
    survive GDELT's encoding quirks.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()
        if not names:
            return
        text = zf.read(names[0]).decode("utf-8", errors="replace")
    for line in text.split("\n"):
        stripped = line.rstrip("\r")
        if not stripped.strip():
            continue
        parts = normalize_fields(stripped)
        fields: list[str | None] = (parts + [None] * _N_COLS)[:_N_COLS]
        yield (*fields, source_file, len(parts), stripped)
