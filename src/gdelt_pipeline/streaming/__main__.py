"""Run the stream consumer as a long-lived service: ``python -m gdelt_pipeline.streaming``."""

from __future__ import annotations

from gdelt_pipeline.logging import configure_logging
from gdelt_pipeline.streaming.consumer import GdeltStreamConsumer


def main() -> None:
    configure_logging()
    GdeltStreamConsumer().run()


if __name__ == "__main__":
    main()
