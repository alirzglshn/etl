# sync_etl/schema_validator.py
"""
Pre-flight schema check, run BEFORE any query against the client's real
data and BEFORE any call to our data-ingest endpoints.

Failure rules (per explicit product decision — these are strict and
intentionally asymmetric):

    1. TABLE mismatch (a table referenced anywhere in the mapping does
       not exist in the client's database) → ALWAYS FATAL. The entire
       sync run aborts immediately. This applies uniformly to every
       table, with no exceptions — including tables that would only
       have supplied first_product_attribute/second_product_attribute.

    2. COLUMN mismatch → FATAL for every field EXCEPT
       first_product_attribute and second_product_attribute. For those
       two specifically, a missing/misnamed column is NOT fatal: the
       field is simply treated as absent for this run (the pipeline
       will send null for it), and the sync continues normally.

    3. Any other column mismatch (i.e. anything other than the two
       exempt fields) → ALWAYS FATAL, same as a table mismatch.

This module ONLY determines pass/fail and produces a precise,
machine-readable reason. It does not query actual row data — that is
query_planner.py / pipeline.py's job, and only runs after this check
passes cleanly.
"""

from dataclasses import dataclass

from sync_etl.api_client import SyncConfigResponse
from sync_etl.db_adapters.base import DatabaseAdapter
from sync_etl.logging_config import get_logger

logger = get_logger()


@dataclass
class SchemaValidationResult:
    ok: bool
    failure_stage: str | None = None  # "schema_table" | "schema_column"
    failure_detail: str = ""
    # Fields whose column was missing but are exempt (nullable) — the
    # pipeline needs this list to know which fields to skip when building
    # its per-table SELECTs, without treating their absence as an error.
    degraded_fields: dict[str, set[str]] | None = None  # entity -> field_names


def validate_schema(
    adapter: DatabaseAdapter, config: SyncConfigResponse
) -> SchemaValidationResult:
    degraded: dict[str, set[str]] = {"user": set(), "product": set()}

    # ── Step 1: every distinct table referenced anywhere MUST exist ────────
    all_tables: set[str] = set()
    for entity in ("user", "product"):
        all_tables |= config.tables_for_entity(entity)

    for table_name in sorted(all_tables):
        if not adapter.table_exists(table_name):
            logger.error("Schema check failed: table %r does not exist.", table_name)
            return SchemaValidationResult(
                ok=False,
                failure_stage="schema_table",
                failure_detail=f"Table '{table_name}' does not exist in the client database.",
            )

    # Each cursor is transport metadata on the entity's single source table.
    for entity in ("user", "product"):
        cursor = config.cursors.get(entity)
        tables = config.tables_for_entity(entity)
        if cursor is None or not cursor.column:
            return SchemaValidationResult(
                ok=False, failure_stage="schema_column",
                failure_detail=f"No incremental cursor configured for '{entity}'.",
            )
        if len(tables) != 1:
            return SchemaValidationResult(
                ok=False, failure_stage="schema_table",
                failure_detail=(
                    f"Entity '{entity}' must map to exactly one table before "
                    "its incremental cursor can be validated."
                ),
            )
        table_name = next(iter(tables))
        if not adapter.column_exists(table_name, cursor.column):
            return SchemaValidationResult(
                ok=False, failure_stage="schema_column",
                failure_detail=(
                    f"Cursor column '{cursor.column}' does not exist in "
                    f"table '{table_name}' for entity '{entity}'."
                ),
            )

    # ── Step 2: every column must exist, except the two exempt fields ──────
    for entity in ("user", "product"):
        nullable_names = config.nullable_fields.get(entity, set())

        for field_name, field_cfg in config.mapping.get(entity, {}).items():
            if not field_cfg.client_table or not field_cfg.client_column:
                # Should not happen once تولید API has validated
                # completeness on the backend, but treat as a schema
                # failure defensively rather than silently querying a
                # None column.
                if field_name in nullable_names:
                    degraded[entity].add(field_name)
                    continue
                return SchemaValidationResult(
                    ok=False,
                    failure_stage="schema_column",
                    failure_detail=(
                        f"No table/column configured for required field "
                        f"'{entity}.{field_name}'."
                    ),
                )

            column_exists = adapter.column_exists(
                field_cfg.client_table, field_cfg.client_column
            )
            if not column_exists:
                if field_name in nullable_names:
                    logger.warning(
                        "Column '%s.%s' (field '%s') not found — exempt field, "
                        "will be sent as null for this run.",
                        field_cfg.client_table, field_cfg.client_column, field_name,
                    )
                    degraded[entity].add(field_name)
                    continue

                logger.error(
                    "Schema check failed: column '%s.%s' (field '%s') does not exist.",
                    field_cfg.client_table, field_cfg.client_column, field_name,
                )
                return SchemaValidationResult(
                    ok=False,
                    failure_stage="schema_column",
                    failure_detail=(
                        f"Column '{field_cfg.client_column}' not found in table "
                        f"'{field_cfg.client_table}' for required field "
                        f"'{entity}.{field_name}'."
                    ),
                )

    return SchemaValidationResult(ok=True, degraded_fields=degraded)
