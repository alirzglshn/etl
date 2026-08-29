# sync_etl/main.py
"""
Container entrypoint. Two modes:

    RUN_ONCE=true   — run a single sync cycle and exit (exit code 0 on
                       success/partial, 1 on failure). Useful for manual
                       testing, or if you'd rather drive scheduling from
                       an external cron/orchestrator instead of the
                       built-in sleep-loop scheduler.

    (default)       — start the long-running scheduler (scheduler.run_forever),
                       which is what docker-compose.yml runs by default.
"""

import sys

from sync_etl.config import load_settings_or_exit
from sync_etl.logging_config import configure_logging, get_logger
from sync_etl.pipeline import run_once
from sync_etl.scheduler import run_forever


def main() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings.log_level)
    logger = get_logger()

    logger.info(
        "Sync ETL starting. engine=%s host=%s db=%s api_url=%s run_once=%s",
        settings.db_engine, settings.db_host, settings.db_name,
        settings.api_url, settings.run_once,
    )

    if settings.run_once:
        outcome = run_once(settings)
        sys.exit(0 if outcome.status in ("success", "partial") else 1)

    run_forever(settings)


if __name__ == "__main__":
    main()
