# sync_etl/stitcher.py
"""
Per explicit product decision, this ETL supports exactly ONE client
table per entity ("user" fields must all come from one table; "product"
fields must all come from one table). There is no cross-table join logic
here or anywhere else in this codebase.

Why: joining tables generically, without real foreign-key metadata from
the client's schema, is inherently a heuristic that can silently produce
WRONG results (mismatched rows) rather than a clean failure — and the
cost of getting that wrong (bad data quietly flowing into Retano360) is
worse than asking the client's technical team to do a small amount of
one-time SQL work up front.

If a tenant's data for one entity genuinely lives across multiple
tables, the correct fix is on THEIR side: create a single SQL VIEW in
their own database that joins/flattens what we need into one row-per-
entity shape (they know their own foreign keys; we deliberately don't
collect that information), then point the "تنظیم API" mapping at that
view's name instead of a raw table. From our side a view behaves
identically to a table for both schema validation and SELECT queries —
nothing else in this pipeline needs to know the difference.

This module's only remaining job is therefore trivial: rename a table's
raw rows from CLIENT column names to OUR canonical field names. If more
than one distinct table is referenced for a single entity, that is a
configuration error, raised clearly rather than guessed at.
"""

from typing import Any

from sync_etl.query_planner import TableQueryPlan


class StitchError(Exception):
    """
    Raised when more than one distinct client table is referenced for a
    single entity's field mapping. Per product decision, cross-table
    joining is not supported — the tenant must consolidate the required
    columns into a single table or database VIEW and update their
    mapping on the تنظیم API page to point at it.
    """


def get_single_table_plan(entity: str, plans: list[TableQueryPlan]) -> TableQueryPlan | None:
    """
    Returns the one TableQueryPlan for this entity, or None if the
    entity has no configured fields at all (nothing to sync).

    Raises StitchError if more than one distinct table is referenced —
    this is treated as a hard configuration error rather than an attempt
    to guess a join.
    """
    if not plans:
        return None

    if len(plans) > 1:
        table_names = sorted(p.table_name for p in plans)
        raise StitchError(
            f"Fields for entity '{entity}' are mapped across {len(plans)} "
            f"different tables ({table_names}), but this sync system only "
            f"supports a single table per entity. Please consolidate the "
            f"required columns into one table or database view, and update "
            f"the mapping on the تنظیم API page to reference it."
        )

    return plans[0]


def rename_rows(rows: list[dict[str, Any]], plan: TableQueryPlan) -> list[dict[str, Any]]:
    """rows are keyed by CLIENT column name; returns rows keyed by OUR
    canonical field name, per plan.field_to_column."""
    column_to_field = {col: field_name for field_name, col in plan.field_to_column.items()}
    return [
        {
            column_to_field[col]: value
            for col, value in row.items()
            if col in column_to_field
        }
        for row in rows
    ]
