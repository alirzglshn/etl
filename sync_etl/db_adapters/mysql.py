# sync_etl/db_adapters/mysql.py
from urllib.parse import quote_plus

from sync_etl.db_adapters.base import DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    """
    Covers both DB_ENGINE=mysql and DB_ENGINE=mariadb — MariaDB is
    wire-compatible with MySQL and both work fine through PyMySQL, so a
    single adapter class serves both rather than duplicating one for a
    difference that doesn't actually exist at the query layer we use.
    """

    def build_connection_url(self) -> str:
        return (
            f"mysql+pymysql://{quote_plus(self.username)}:"
            f"{quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
        )
