# sync_etl/api_client.py
"""
Thin HTTP client for the three/four sync endpoints on our DRF backend:

    GET  {API_URL}/sync/config/
    POST {API_URL}/sync/data/users/
    POST {API_URL}/sync/data/products/
    POST {API_URL}/sync/report/

Every request carries `Authorization: Bearer <API_KEY>`. Retries apply
only to transient failures (network errors, 5xx, timeouts) — 4xx
responses (bad auth, bad payload) are never retried, since retrying a
request the server has already rejected as invalid cannot succeed.
"""

import time
from dataclasses import dataclass
from typing import Any

import requests

from sync_etl.config import Settings
from sync_etl.logging_config import get_logger

logger = get_logger()


class ApiClientError(Exception):
    """Raised for non-retryable API failures (4xx, malformed responses)."""


class ApiConnectionError(Exception):
    """Raised when the API could not be reached after all retries."""


@dataclass
class SyncFieldConfig:
    """One field's mapping, as received from GET /sync/config/."""

    field_name: str
    client_table: str | None
    client_column: str | None


@dataclass
class SyncCursorConfig:
    column: str
    value: dict[str, Any] | None


@dataclass
class SyncConfigResponse:
    batch_size: int
    mapping: dict[str, dict[str, SyncFieldConfig]]  # entity -> field_name -> config
    nullable_fields: dict[str, set[str]]  # entity -> set of field names
    cursors: dict[str, SyncCursorConfig]

    def tables_for_entity(self, entity: str) -> set[str]:
        """Distinct client table names referenced by this entity's mapping,
        excluding fields with no table (should not normally happen once
        تولید API has been used, but defensive nonetheless)."""
        return {
            f.client_table
            for f in self.mapping.get(entity, {}).values()
            if f.client_table
        }

    def fields_for_table(self, entity: str, table: str) -> list[SyncFieldConfig]:
        return [
            f for f in self.mapping.get(entity, {}).values() if f.client_table == table
        ]


class SyncApiClient:
    def __init__(self, settings: Settings, instance_id: str, run_id: str):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.api_key}",
                "Content-Type": "application/json",
                "X-Sync-Instance-ID": instance_id,
                "X-Sync-Run-ID": run_id,
            }
        )

    # ── internal request helper with retry/backoff ─────────────────────────

    def _request(
        self, method: str, path: str, json_body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict:
        url = f"{self.settings.api_url}{path}"
        backoff = self.settings.retry_backoff_seconds
        last_exc: Exception | None = None

        for attempt in range(1, self.settings.max_retries + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    json=json_body,
                    headers=headers,
                    timeout=self.settings.request_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Request %s %s failed (attempt %d/%d): %s",
                    method, url, attempt, self.settings.max_retries, exc,
                )
                time.sleep(min(backoff, 60))
                backoff *= 2
                continue

            if response.status_code >= 500:
                last_exc = ApiConnectionError(
                    f"{response.status_code} from {url}: {response.text[:500]}"
                )
                logger.warning(
                    "Server error %s %s (attempt %d/%d): %s",
                    method, url, attempt, self.settings.max_retries, response.text[:300],
                )
                time.sleep(min(backoff, 60))
                backoff *= 2
                continue

            if 400 <= response.status_code < 500:
                # Not retryable — the request itself is wrong (bad auth,
                # bad payload, tenant disabled, etc.)
                raise ApiClientError(
                    f"{response.status_code} from {url}: {response.text[:1000]}"
                )

            try:
                return response.json() if response.content else {}
            except ValueError as exc:
                raise ApiClientError(f"Invalid JSON from {url}: {exc}")

        raise ApiConnectionError(
            f"Failed to reach {url} after {self.settings.max_retries} attempts: {last_exc}"
        )

    # ── public API ──────────────────────────────────────────────────────────

    def fetch_config(self) -> SyncConfigResponse:
        raw = self._request("GET", "/sync/config/")

        mapping: dict[str, dict[str, SyncFieldConfig]] = {}
        for entity, fields in raw.get("mapping", {}).items():
            mapping[entity] = {
                field_name: SyncFieldConfig(
                    field_name=field_name,
                    client_table=info.get("table"),
                    client_column=info.get("column"),
                )
                for field_name, info in fields.items()
            }

        nullable_fields = {
            entity: set(names) for entity, names in raw.get("nullable_fields", {}).items()
        }
        cursors = {
            entity: SyncCursorConfig(
                column=info.get("column") or "", value=info.get("value")
            )
            for entity, info in raw.get("cursors", {}).items()
        }

        return SyncConfigResponse(
            batch_size=raw.get("batch_size", 1000),
            mapping=mapping,
            nullable_fields=nullable_fields,
            cursors=cursors,
        )

    def post_data_batch(
        self, entity: str, rows: list[dict[str, Any]], batch_number: int,
        cursor_after: dict[str, Any],
    ) -> dict:
        """entity is 'user' or 'product'; endpoint path is pluralized."""
        path = f"/sync/data/{entity}s/"
        return self._request(
            "POST", path,
            json_body={
                "rows": rows,
                "batch_number": batch_number,
                "cursor_after": cursor_after,
            },
            headers={
                "Idempotency-Key": (
                    f"{self.session.headers['X-Sync-Run-ID']}:{entity}:{batch_number}"
                )
            },
        )

    def post_report(self, payload: dict) -> None:
        self._request("POST", "/sync/report/", json_body=payload)
