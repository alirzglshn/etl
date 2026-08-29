# sync_etl/db_adapters/base.py
"""
Abstract interface every engine adapter implements. Adding support for a
new SQL engine means writing one small adapter class here, nothing else
in the pipeline changes — this is the "database access should be
abstracted so adding another SQL database requires minimal changes"
requirement from the spec.

All adapters are built on SQLAlchemy Core (not the ORM — we have no
models for the client's arbitrary schema, just raw table/column names
discovered at runtime), so the actual querying/introspection logic is
shared in this base class; subclasses only supply the connection URL and
any dialect-specific quirks.
"""

from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine


class DatabaseConnectionError(Exception):
    """Raised when the adapter cannot connect to the client's database at all."""


class DatabaseAdapter(ABC):
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._engine: Engine | None = None

    @abstractmethod
    def build_connection_url(self) -> str:
        """Return a SQLAlchemy connection URL for this engine."""

    def connect(self) -> None:
        try:
            self._engine = sa.create_engine(
                self.build_connection_url(),
                pool_pre_ping=True,
                pool_recycle=300,
            )
            # Force an actual connection attempt now, not lazily on first
            # query — we want connection failures caught explicitly and
            # early, in the pipeline's "connect" step, not buried inside
            # whatever query happens to run first.
            with self._engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Could not connect to {self.database}@{self.host}:{self.port}: {exc}"
            ) from exc

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("connect() must be called before using the adapter.")
        return self._engine

    # ── schema introspection ────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        inspector = sa.inspect(self.engine)
        return table_name in inspector.get_table_names()

    def column_exists(self, table_name: str, column_name: str) -> bool:
        inspector = sa.inspect(self.engine)
        try:
            columns = inspector.get_columns(table_name)
        except sa.exc.NoSuchTableError:
            return False
        return any(col["name"] == column_name for col in columns)

    # ── data access ─────────────────────────────────────────────────────────

    def fetch_rows(
        self, table_name: str, column_names: list[str]
    ) -> list[dict[str, Any]]:
        """
        Full-snapshot read of the given columns from the given table.
        Column/table names are validated to exist by schema_validator.py
        BEFORE this is ever called — this method trusts its inputs and
        does not re-validate, since re-validating per-call would be
        wasteful for tables with millions of rows queried in the same run.

        Uses SQLAlchemy's quoted_name / identifier quoting (via sa.column
        / sa.table) rather than naive string interpolation, so table and
        column names are properly escaped for the target dialect even
        though they come from user-entered config rather than our own
        code — this is metadata-level dynamic SQL (identifiers), not
        parameter-level, so classic bind-parameter escaping does not apply
        here; SQLAlchemy's identifier quoting is the correct defense for
        this specific case (a mismatched/invalid identifier will raise a
        clear database error rather than being silently misinterpreted).
        """
        table = sa.table(table_name, *[sa.column(c) for c in column_names])
        stmt = sa.select(*[sa.column(c) for c in column_names]).select_from(table)

        rows = []
        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(stmt)
            for row in result:
                rows.append(dict(zip(column_names, row)))
        return rows

    def fetch_rows_batched(
        self, table_name: str, column_names: list[str], batch_size: int = 5000
    ):
        """
        Generator version for large tables — yields lists of row-dicts of
        at most batch_size rows at a time, so millions of rows never need
        to be held fully in memory at once. Prefer this over fetch_rows()
        in the pipeline for anything beyond small lookup tables.
        """
        table = sa.table(table_name, *[sa.column(c) for c in column_names])
        stmt = sa.select(*[sa.column(c) for c in column_names]).select_from(table)

        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(stmt)
            while True:
                chunk = result.fetchmany(batch_size)
                if not chunk:
                    break
                yield [dict(zip(column_names, row)) for row in chunk]
