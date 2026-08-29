# sync_etl/logging_config.py
"""
Structured-ish logging: consistent format, one logger name ("sync_etl")
used throughout so a client's log aggregator (if any) can filter on it
easily. Not JSON-structured by default (keeps container logs readable via
plain `docker logs`), but every log line includes a cycle_id so all lines
from one sync cycle can be grepped together.
"""

import logging
import sys


def configure_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("sync_etl")
    logger.setLevel(level)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger("sync_etl")
