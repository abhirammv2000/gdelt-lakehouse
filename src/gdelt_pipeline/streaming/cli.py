"""``gdelt stream`` subcommands (imports are lazy so the base CLI needs no Kafka)."""

from __future__ import annotations

from typing import Annotated

import typer

stream_app = typer.Typer(help="Kafka/Redpanda streaming for the live event stream")


@stream_app.command()
def produce(
    feed: Annotated[str | None, typer.Option(help="Feed to stream (default: first enabled).")] = None,
) -> None:
    """Publish the latest landed bronze batch onto the raw events topic."""
    from gdelt_pipeline.streaming.producer import GdeltStreamProducer

    published = GdeltStreamProducer().publish_bronze_latest(feed)
    typer.echo({"published": published})


@stream_app.command()
def consume(
    max_messages: Annotated[int | None, typer.Option(help="Stop after N messages (default: run forever).")] = None,
    idle_timeout: Annotated[float | None, typer.Option(help="Stop after N idle seconds.")] = None,
) -> None:
    """Consume the raw topic: DQ-gate, dead-letter failures, alert on high impact."""
    from gdelt_pipeline.streaming.consumer import GdeltStreamConsumer

    stats = GdeltStreamConsumer().run(max_messages=max_messages, idle_timeout=idle_timeout)
    typer.echo(stats)
