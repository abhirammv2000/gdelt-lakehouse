"""``gdelt`` command-line entrypoint (installed via project.scripts)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import typer

from gdelt_pipeline.ingestion.service import IngestService
from gdelt_pipeline.logging import configure_logging

app = typer.Typer(help="GDELT lakehouse ingestion CLI", no_args_is_help=True)

_DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]


@app.callback()
def _init(
    log_level: Annotated[str, typer.Option(envvar="GDELT_LOG_LEVEL")] = "INFO",
) -> None:
    configure_logging(log_level)


@app.command()
def ingest(
    target: Annotated[str, typer.Argument(help="Only 'latest' is supported here.")] = "latest",
) -> None:
    """Ingest the current 15-minute GDELT batch into bronze."""
    if target != "latest":
        raise typer.BadParameter("Use 'gdelt ingest latest' or the 'backfill' command.")
    result = IngestService().ingest_latest()
    typer.echo(result.summary)


@app.command()
def backfill(
    start: Annotated[datetime, typer.Option(formats=_DATE_FORMATS)],
    end: Annotated[datetime, typer.Option(formats=_DATE_FORMATS)],
) -> None:
    """Backfill all GDELT files in the [start, end) window into bronze."""
    result = IngestService().backfill(start.replace(tzinfo=UTC), end.replace(tzinfo=UTC))
    typer.echo(result.summary)


if __name__ == "__main__":
    app()
