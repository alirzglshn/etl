# sync_etl/db_adapters/postgres.py
from urllib.parse import quote_plus

from sync_etl.db_adapters.base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    def build_connection_url(self) -> str:
        return (
            f"postgresql+psycopg2://{quote_plus(self.username)}:"
            f"{quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
        )
