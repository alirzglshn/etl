# sync_etl/main.py
"""
Container entrypoint with explicit ``daemon`` and ``once`` modes. Compose
always passes ``daemon`` so an environment variable cannot accidentally turn
the service into a one-shot process that Docker repeatedly restarts.
"""

import argparse
import sys

from sync_etl.config import load_settings_or_exit
from sync_etl.logging_config import configure_logging, get_logger
from sync_etl.pipeline import run_once
from sync_etl.scheduler import run_forever


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", nargs="?", choices=("daemon", "once"), default="daemon")
    args = parser.parse_args()
    settings = load_settings_or_exit()
    configure_logging(settings.log_level)
    logger = get_logger()

    logger.info(
        "Sync ETL starting. engine=%s host=%s db=%s api_url=%s mode=%s",
        settings.db_engine, settings.db_host, settings.db_name,
        settings.api_url, args.mode,
    )

    if args.mode == "once":
        outcome = run_once(settings)
        sys.exit(0 if outcome.status in ("success", "partial") else 1)

    run_forever(settings)


if __name__ == "__main__":
    main()
