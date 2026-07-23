"""``gdelt`` command-line entrypoint (installed via project.scripts)."""

from __future__ import annotations

from datetime import datetime, timezone

import typer

from gdelt_pipeline.ingestion.service import IngestService
from gdelt_pipeline.logging import configure_logging

app = typer.Typer(help="GDELT lakehouse ingestion CLI", no_args_is_help=True)


@app.callback()
def _init(log_level: str = typer.Option("INFO", envvar="GDELT_LOG_LEVEL")) -> None:
    configure_logging(log_level)


@app.command()
def ingest(target: str = typer.Argument("latest", help="Only 'latest' is supported here.")) -> None:
    """Ingest the current 15-minute GDELT batch into bronze."""
    if target != "latest":
        raise typer.BadParameter("Use 'gdelt ingest latest' or the 'backfill' command.")
    result = IngestService().ingest_latest()
    typer.echo(result.summary)


@app.command()
def backfill(
    start: datetime = typer.Option(..., formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    end: datetime = typer.Option(..., formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
) -> None:
    """Backfill all GDELT files in the [start, end) window into bronze."""
    start_utc = start.replace(tzinfo=timezone.utc)
    end_utc = end.replace(tzinfo=timezone.utc)
    result = IngestService().backfill(start_utc, end_utc)
    typer.echo(result.summary)


if __name__ == "__main__":
    app()
