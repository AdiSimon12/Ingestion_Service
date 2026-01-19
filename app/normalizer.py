from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Dict, Optional

from app.models import NormalizedEvent, UnifiedEventType
from app.validators import validate_payload_is_dict, validate_minimal_required_fields
from app.mappings import PROVIDER_MAPPINGS, get_by_path, EVENT_TYPE_MAP


def _parse_timestamp(value: Any) -> datetime:
    """
    Convert common timestamp representations to a timezone-aware UTC datetime.
    """
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            raise ValueError(f"Invalid timestamp format: '{value}' (expected ISO-8601).")
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    raise ValueError(f"Invalid timestamp type: {type(value).__name__}. Expected ISO string or datetime.")


def _extract_value(payload: Dict[str, Any], path: str) -> Optional[Any]:
    """
    Extract a value by dot-path using the utility from mappings.py.
    """
    return get_by_path(payload, path)


def _to_unified_event_type(provider: str, raw_event_type: Any) -> UnifiedEventType:
    """
    Translates a provider-specific raw event type into a UnifiedEventType Enum
    using the EVENT_TYPE_MAP.
    """
    if raw_event_type is None:
        raise ValueError("Normalization failed: missing required field 'unified_event_type'.")

    s = str(raw_event_type).strip()
    if not s:
        raise ValueError("Normalization failed: missing required field 'unified_event_type'.")

    # ניסיון תרגום לפי ספק הענן
    provider_map = EVENT_TYPE_MAP.get(provider.lower(), {})
    unified_value = provider_map.get(s, s)  # אם אין תרגום, ננסה להשתמש בערך המקורי

    try:
        return UnifiedEventType(unified_value)
    except ValueError:
        allowed = ", ".join([e.value for e in UnifiedEventType])
        raise ValueError(
            f"Normalization failed: unsupported event type '{s}' for provider '{provider}'. "
            f"Allowed unified values: {allowed}"
        )


def normalize_event(provider: str, payload: Dict[str, Any]) -> NormalizedEvent:
    """
    Normalize a provider-specific raw payload into the unified internal schema.
    """
    payload = validate_payload_is_dict(payload)
    provider_clean = provider.lower().strip()

    if provider_clean not in PROVIDER_MAPPINGS:
        raise ValueError(f"Unsupported provider '{provider_clean}' for normalization.")

    validate_minimal_required_fields(provider_clean, payload)

    mapping = PROVIDER_MAPPINGS[provider_clean]

    raw_event_type = _extract_value(payload, mapping["event_type"])
    raw_timestamp = _extract_value(payload, mapping["timestamp"])
    raw_resource_id = _extract_value(payload, mapping["resource_id"])

    # Fallbacks for resource_id if missing
    if raw_resource_id is None:
        raw_resource_id = payload.get("resourceId") or payload.get("resource_id")

    # Post-normalization validation for critical fields
    if not raw_timestamp:
        raise ValueError("Normalization failed: missing required field 'timestamp_utc'.")
    if not raw_resource_id:
        raise ValueError("Normalization failed: missing required field 'resource_id'.")

    # שימוש בפונקציית התרגום המעודכנת
    unified_event_type = _to_unified_event_type(provider_clean, raw_event_type)
    timestamp_utc = _parse_timestamp(raw_timestamp)
    resource_id = str(raw_resource_id).strip()
    
    if not resource_id:
        raise ValueError("Normalization failed: missing required field 'resource_id'.")

    provider_upper = provider_clean.upper()

    normalized = NormalizedEvent(
        event_uuid=uuid4(),
        cloud_provider=provider_upper,   # must match Literal in the model
        unified_event_type=unified_event_type,
        timestamp_utc=timestamp_utc,
        resource_id=resource_id,
        raw_payload=payload,
    )

    return normalized