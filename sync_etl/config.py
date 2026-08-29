# sync_etl/config.py
"""
All configuration comes from environment variables. Per the agreed
architecture, this container is deliberately "thin": it knows only how to
reach the client's database and how to reach our API. Everything else
(table names, column names, batch size) is fetched fresh from our API at
the start of every cycle — see api_client.SyncApiClient.fetch_config().

Required env vars:
    DB_ENGINE       — one of: postgresql, mysql, mariadb, mssql
    DB_HOST
    DB_PORT
    DB_NAME
    DB_USERNAME
    DB_PASSWORD

    API_URL         — base URL of our DRF API, e.g. https://api.retano360.com/api/v1
    API_KEY         — tenant's sync API key, generated via تولید API

Optional env vars:
    SYNC_INTERVAL_HOURS   — default 24
    REQUEST_TIMEOUT_SECONDS — default 30
    MAX_RETRIES           — default 5
    RETRY_BACKOFF_SECONDS — default 10 (doubles each retry, capped)
    LOG_LEVEL             — default INFO
    RUN_ONCE              — if "true", run a single cycle and exit instead
                             of starting the scheduler. Useful for manual
                             testing/debugging and for running via an
                             external cron instead of the built-in scheduler.
"""

import os
import sys
from dataclasses import dataclass

SUPPORTED_ENGINES = {"postgresql", "mysql", "mariadb", "mssql"}


class ConfigError(Exception):
    """Raised when required environment variables are missing or invalid."""


@dataclass(frozen=True)
class Settings:
    db_engine: str
    db_host: str
    db_port: int
    db_name: str
    db_username: str
    db_password: str

    api_url: str
    api_key: str

    sync_interval_hours: int
    request_timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: int
    log_level: str
    run_once: bool


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ConfigError(f"Required environment variable {name} is not set.")
    return value.strip()


def _optional_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"Environment variable {name} must be an integer, got: {raw!r}")


def load_settings() -> Settings:
    db_engine = _require("DB_ENGINE").lower()
    if db_engine not in SUPPORTED_ENGINES:
        raise ConfigError(
            f"DB_ENGINE must be one of {sorted(SUPPORTED_ENGINES)}, got: {db_engine!r}"
        )

    settings = Settings(
        db_engine=db_engine,
        db_host=_require("DB_HOST"),
        db_port=_optional_int("DB_PORT", 5432 if db_engine == "postgresql" else 3306),
        db_name=_require("DB_NAME"),
        db_username=_require("DB_USERNAME"),
        db_password=_require("DB_PASSWORD"),
        api_url=_require("API_URL").rstrip("/"),
        api_key=_require("API_KEY"),
        sync_interval_hours=_optional_int("SYNC_INTERVAL_HOURS", 24),
        request_timeout_seconds=_optional_int("REQUEST_TIMEOUT_SECONDS", 30),
        max_retries=_optional_int("MAX_RETRIES", 5),
        retry_backoff_seconds=_optional_int("RETRY_BACKOFF_SECONDS", 10),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        run_once=os.environ.get("RUN_ONCE", "false").lower() in ("1", "true", "yes"),
    )
    return settings


def load_settings_or_exit() -> Settings:
    """Used by main.py — a config error at startup should fail loudly and
    immediately, not partway through a scheduled cycle."""
    try:
        return load_settings()
    except ConfigError as exc:
        print(f"[FATAL] Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)
