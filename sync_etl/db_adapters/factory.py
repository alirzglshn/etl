# sync_etl/db_adapters/factory.py
"""
Single point of extension for new engines. Adding support for, say,
Oracle later means: write oracle.py implementing DatabaseAdapter, add one
line here, add "oracle" to config.SUPPORTED_ENGINES. Nothing else in the
ETL changes.
"""

from sync_etl.config import Settings
from sync_etl.db_adapters.base import DatabaseAdapter
from sync_etl.db_adapters.mssql import MSSQLAdapter
from sync_etl.db_adapters.mysql import MySQLAdapter
from sync_etl.db_adapters.postgres import PostgresAdapter

_ADAPTERS: dict[str, type[DatabaseAdapter]] = {
    "postgresql": PostgresAdapter,
    "mysql": MySQLAdapter,
    "mariadb": MySQLAdapter,
    "mssql": MSSQLAdapter,
}


def build_adapter(settings: Settings) -> DatabaseAdapter:
    adapter_cls = _ADAPTERS.get(settings.db_engine)
    if adapter_cls is None:
        raise ValueError(f"No adapter registered for DB_ENGINE={settings.db_engine!r}")

    return adapter_cls(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        username=settings.db_username,
        password=settings.db_password,
    )
