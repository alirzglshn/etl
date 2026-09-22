# sync_etl/pipeline.py
"""
Orchestrates one full sync cycle, per the agreed architecture:

    1. Authenticate (implicit — every API call carries the Bearer key)
    2. GET /api/sync/config/
    3. Connect to client's database
    4. Pre-flight schema validation (schema_validator.py)
         → any hard failure: report + abort, no data endpoints ever called
    5. Read the configured table for each entity (query_planner.py /
       stitcher.get_single_table_plan) — exactly ONE table per entity is
       supported; a mapping spanning multiple tables for one entity is a
       configuration error (see stitcher.py for why cross-table joining
       is intentionally not supported).
    6. Stream rows straight through to the API in batches (bounded
       memory regardless of table size).
    7. Report success/partial/failure.

Row-level type coercion/validation happens SERVER-SIDE (our DRF API), not
here — this module ships raw values as-is. The only local "correctness"
work this module does is schema-level (do the configured table/column
names exist at all) and structural (exactly one table per entity),
never data-level validation.
"""

from dataclasses import dataclass, field
from typing import Any
import uuid

from sync_etl.api_client import ApiClientError, ApiConnectionError, SyncApiClient
from sync_etl.config import Settings
from sync_etl.cursors import CursorError, decode_cursor, encode_cursor, json_safe
from sync_etl.db_adapters.base import DatabaseConnectionError
from sync_etl.db_adapters.factory import build_adapter
from sync_etl.logging_config import get_logger
from sync_etl.query_planner import build_query_plans
from sync_etl.schema_validator import validate_schema
from sync_etl.stitcher import StitchError, get_single_table_plan, rename_rows

logger = get_logger()


@dataclass
class EntityRunOutcome:
    rows_received: int = 0
    rows_accepted: int = 0
    rows_rejected: int = 0


@dataclass
class CycleOutcome:
    status: str = "running"  # success | partial | failed
    failure_stage: str | None = None
    failure_detail: str = ""
    users: EntityRunOutcome = field(default_factory=EntityRunOutcome)
    products: EntityRunOutcome = field(default_factory=EntityRunOutcome)

    def report_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status}
        if self.failure_stage:
            payload["failure_stage"] = self.failure_stage
            payload["failure_detail"] = self.failure_detail
        payload.update(
            {
                "users_rows_received": self.users.rows_received,
                "users_rows_accepted": self.users.rows_accepted,
                "users_rows_rejected": self.users.rows_rejected,
                "products_rows_received": self.products.rows_received,
                "products_rows_accepted": self.products.rows_accepted,
                "products_rows_rejected": self.products.rows_rejected,
            }
        )
        return payload


def _run_entity(
    entity: str,
    adapter,
    api_client: SyncApiClient,
    config,
    degraded_fields: set[str],
    batch_size: int,
) -> EntityRunOutcome:
    outcome = EntityRunOutcome()

    plans = build_query_plans(entity, config, degraded_fields)
    plan = get_single_table_plan(entity, plans)  # raises StitchError if >1 table
    if plan is None:
        logger.info("No table configured for entity '%s' — skipping.", entity)
        return outcome

    logger.info(
        "Entity '%s': querying table '%s', columns: %s",
        entity, plan.table_name, plan.column_names,
    )

    cursor_config = config.cursors[entity]
    after_value = decode_cursor(cursor_config.value)
    previous_cursor = after_value
    batch_number = 0

    # Single table → stream rows straight through to the API in batches,
    # without ever holding the full table in memory. This is what makes
    # "millions of rows" tractable: memory usage is bounded by batch_size,
    # not by table size.
    for raw_chunk in adapter.fetch_incremental_rows_batched(
        plan.table_name, plan.column_names, cursor_config.column,
        after_value=after_value, batch_size=batch_size,
    ):
        for raw_row in raw_chunk:
            cursor_value = raw_row[cursor_config.column]
            if cursor_value is None:
                raise CursorError(
                    f"NULL cursor in {plan.table_name}.{cursor_config.column}."
                )
            if previous_cursor is not None and cursor_value <= previous_cursor:
                raise CursorError(
                    f"Cursor {plan.table_name}.{cursor_config.column} must be unique "
                    "and strictly increasing."
                )
            previous_cursor = cursor_value

        renamed_chunk = [json_safe(row) for row in rename_rows(raw_chunk, plan)]
        outcome.rows_received += len(renamed_chunk)
        batch_number += 1
        response = api_client.post_data_batch(
            entity, renamed_chunk, batch_number, encode_cursor(previous_cursor)
        )
        outcome.rows_accepted += response.get("rows_accepted", 0)
        outcome.rows_rejected += response.get("rows_rejected", 0)
        if response.get("rejections"):
            for rejection in response["rejections"][:20]:  # cap noisy logs
                logger.warning("Row rejected (%s): %s", entity, rejection)

    logger.info(
        "Entity '%s': finished — received=%d accepted=%d rejected=%d",
        entity, outcome.rows_received, outcome.rows_accepted, outcome.rows_rejected,
    )

    return outcome


def run_once(settings: Settings, instance_id: uuid.UUID | None = None) -> CycleOutcome:
    outcome = CycleOutcome()
    instance_id = instance_id or uuid.uuid4()
    run_id = uuid.uuid4()
    api_client = SyncApiClient(settings, str(instance_id), str(run_id))

    logger.info("Sync cycle starting.")

    try:
        config = api_client.fetch_config()
    except (ApiClientError, ApiConnectionError) as exc:
        logger.error("Could not fetch sync configuration: %s", exc)
        outcome.status = "failed"
        outcome.failure_stage = "unknown"
        outcome.failure_detail = f"Could not fetch configuration: {exc}"
        return outcome

    adapter = build_adapter(settings)
    try:
        adapter.connect()
    except DatabaseConnectionError as exc:
        logger.error("Could not connect to client database: %s", exc)
        outcome.status = "failed"
        outcome.failure_stage = "connection"
        outcome.failure_detail = str(exc)
        _safe_report(api_client, outcome)
        return outcome

    try:
        validation = validate_schema(adapter, config)
        if not validation.ok:
            logger.error(
                "Pre-flight schema check failed [%s]: %s",
                validation.failure_stage, validation.failure_detail,
            )
            outcome.status = "failed"
            outcome.failure_stage = validation.failure_stage
            outcome.failure_detail = validation.failure_detail
            _safe_report(api_client, outcome)
            return outcome

        degraded = validation.degraded_fields or {"user": set(), "product": set()}

        try:
            outcome.users = _run_entity(
                "user", adapter, api_client, config, degraded["user"], config.batch_size
            )
            outcome.products = _run_entity(
                "product", adapter, api_client, config, degraded["product"], config.batch_size
            )
        except (StitchError, CursorError) as exc:
            logger.error("Row stitching failed: %s", exc)
            outcome.status = "failed"
            outcome.failure_stage = "schema_column"
            outcome.failure_detail = str(exc)
            _safe_report(api_client, outcome)
            return outcome
        except (ApiClientError, ApiConnectionError) as exc:
            logger.error("Data ingest call failed: %s", exc)
            outcome.status = "failed"
            outcome.failure_stage = "unknown"
            outcome.failure_detail = f"Data ingest failed: {exc}"
            _safe_report(api_client, outcome)
            return outcome
        except Exception as exc:  # database/query failure after run acquisition
            logger.exception("Incremental source read failed.")
            outcome.status = "failed"
            outcome.failure_stage = "unknown"
            outcome.failure_detail = f"Incremental source read failed: {exc}"
            _safe_report(api_client, outcome)
            return outcome

    finally:
        adapter.dispose()

    total_rejected = outcome.users.rows_rejected + outcome.products.rows_rejected
    outcome.status = "partial" if total_rejected > 0 else "success"

    logger.info(
        "Sync cycle finished: status=%s users(received=%d accepted=%d rejected=%d) "
        "products(received=%d accepted=%d rejected=%d)",
        outcome.status,
        outcome.users.rows_received, outcome.users.rows_accepted, outcome.users.rows_rejected,
        outcome.products.rows_received, outcome.products.rows_accepted, outcome.products.rows_rejected,
    )

    _safe_report(api_client, outcome)
    return outcome


def _safe_report(api_client: SyncApiClient, outcome: CycleOutcome) -> None:
    """Reporting failures should never crash the pipeline or mask the
    original outcome — log and move on."""
    try:
        api_client.post_report(outcome.report_payload())
    except Exception as exc:  # noqa: BLE001 — reporting is best-effort
        logger.error("Failed to report sync outcome to API: %s", exc)
