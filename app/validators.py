from typing import Any, Dict


def validate_payload_is_dict(payload: Any) -> Dict[str, Any]:
    """
    Ensures the incoming payload is a JSON object (Python dict).
    """
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload: expected a JSON object (dictionary).")
    return payload


def validate_minimal_required_fields(provider: str, payload: Dict[str, Any]) -> None:
    """
    Performs minimal validation to ensure the payload contains enough information
    to be normalized into the unified schema.

    Note:
    - We intentionally keep this validation lightweight and provider-aware,
      since providers use different field names and structures.
    """
    provider = provider.lower().strip()

    if provider == "aws":
        # Common fields in CloudTrail-like events (example subset)
        required = ["eventName", "eventTime"]
        missing = [k for k in required if k not in payload]
        if missing:
            raise ValueError(f"AWS payload missing required fields: {missing}")

    elif provider == "azure":
        # Common fields in Azure activity log-like events (example subset)
        required = ["operationName", "time"]
        missing = [k for k in required if k not in payload]
        if missing:
            raise ValueError(f"Azure payload missing required fields: {missing}")

    elif provider == "gcp":
        # Common fields in GCP audit log-like events (example subset)
        required = ["protoPayload", "timestamp"]
        missing = [k for k in required if k not in payload]
        if missing:
            raise ValueError(f"GCP payload missing required fields: {missing}")

    else:
        raise ValueError(f"Unsupported provider '{provider}' for validation.")