# sync_etl/scheduler.py
"""
Runs pipeline.run_once() every SYNC_INTERVAL_HOURS. Deliberately simple
(a sleep loop, not a cron daemon or APScheduler) — the container has
exactly one job, and a sleep loop is easier to reason about, log, and
debug in a single-purpose container than a general-purpose scheduler
library would be. Swappable for APScheduler later without touching
pipeline.py if more sophisticated scheduling (specific times of day,
multiple schedules) is ever needed.
"""

import time
import uuid

from sync_etl.config import Settings
from sync_etl.logging_config import get_logger
from sync_etl.pipeline import run_once

logger = get_logger()


def run_forever(settings: Settings) -> None:
    instance_id = uuid.uuid4()
    interval_seconds = settings.sync_interval_hours * 3600
    logger.info(
        "Scheduler starting — sync will run every %d hour(s).",
        settings.sync_interval_hours,
    )

    while True:
        try:
            run_once(settings, instance_id=instance_id)
        except Exception:  # noqa: BLE001
            # A single cycle crashing must never kill the container — the
            # next cycle should still be attempted 24h later. Full
            # traceback goes to the logs either way via logger.exception.
            logger.exception("Unhandled exception during sync cycle.")

        logger.info("Sleeping for %d hour(s) until next cycle.", settings.sync_interval_hours)
        time.sleep(interval_seconds)
