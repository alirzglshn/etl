"""Type-safe checkpoint serialization and JSON-safe row conversion."""

import base64
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


class CursorError(ValueError):
    pass


def encode_cursor(value: Any) -> dict[str, Any]:
    if value is None:
        raise CursorError("Incremental cursor values must not be NULL.")
    if isinstance(value, bool):
        raise CursorError("Boolean columns cannot be used as incremental cursors.")
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": repr(value)}
    if isinstance(value, datetime):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, uuid.UUID):
        return {"type": "uuid", "value": str(value)}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    raise CursorError(f"Unsupported incremental cursor type: {type(value).__name__}.")


def decode_cursor(encoded: dict[str, Any] | None) -> Any:
    if encoded is None:
        return None
    if not isinstance(encoded, dict) or "type" not in encoded or "value" not in encoded:
        raise CursorError("The stored incremental checkpoint is malformed.")
    kind, value = encoded["type"], encoded["value"]
    decoders = {
        "integer": int,
        "decimal": Decimal,
        "float": float,
        "datetime": datetime.fromisoformat,
        "date": date.fromisoformat,
        "uuid": uuid.UUID,
        "string": str,
    }
    try:
        return decoders[kind](value)
    except (KeyError, TypeError, ValueError) as exc:
        raise CursorError(f"Invalid stored {kind!r} cursor value.") from exc


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return str(value)
