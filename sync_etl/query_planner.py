# sync_etl/query_planner.py
"""
Groups our canonical fields by which client table they're mapped to, so
the adapter runs ONE query per distinct client table rather than one
tiny query per field — critical given "millions of rows" in the source
spec. Also excludes any field that schema_validator.py flagged as
degraded (column missing but exempt) from the SELECT entirely, since
querying a nonexistent column would simply fail.
"""

from dataclasses import dataclass

from sync_etl.api_client import SyncConfigResponse


@dataclass
class TableQueryPlan:
    table_name: str
    # field_name -> client_column, for every field on this table that
    # should actually be queried (degraded fields excluded).
    field_to_column: dict[str, str]

    @property
    def column_names(self) -> list[str]:
        return list(self.field_to_column.values())


def build_query_plans(
    entity: str,
    config: SyncConfigResponse,
    degraded_fields: set[str],
) -> list[TableQueryPlan]:
    """
    Returns one TableQueryPlan per distinct client table referenced by
    this entity's (non-degraded) field mappings.
    """
    plans_by_table: dict[str, dict[str, str]] = {}

    for field_name, field_cfg in config.mapping.get(entity, {}).items():
        if field_name in degraded_fields:
            continue  # column doesn't exist — never query it
        if not field_cfg.client_table or not field_cfg.client_column:
            continue  # defensive — schema_validator should already exclude these

        plans_by_table.setdefault(field_cfg.client_table, {})
        plans_by_table[field_cfg.client_table][field_name] = field_cfg.client_column

    return [
        TableQueryPlan(table_name=table, field_to_column=fields)
        for table, fields in plans_by_table.items()
    ]
