from typing import Any, Dict, Optional


def get_by_path(data: Dict[str, Any], path: str) -> Optional[Any]:
    """
    Extract a value from a nested dict or list using a dot-separated path.
    Example: path="resources.0.ARN"
    Returns None if the path does not exist or index is out of range.
    """
    current: Any = data
    for part in path.split("."):
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
            else:
                return None
        elif isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


# Minimal provider-specific mapping definitions.
# These can be extended later without changing the normalizer logic.
PROVIDER_MAPPINGS: Dict[str, Dict[str, str]] = {
    "aws": {
        "event_type": "eventName",
        "timestamp": "eventTime",
        "resource_id": "resources.0.ARN",  # example of nested path (may not exist in all events)
    },
    "azure": {
        "event_type": "operationName",
        "timestamp": "time",
        "resource_id": "resourceId",
    },
    "gcp": {
        "event_type": "protoPayload.methodName",
        "timestamp": "timestamp",
        "resource_id": "protoPayload.resourceName",
    },
}

# Unified event type translation map.
# Maps provider-specific event names to the unified internal Enum values.
EVENT_TYPE_MAP = {
    "aws": {
        "GetObject": "STORAGE_ACCESS",
        "PutObject": "STORAGE_ACCESS",
        "DeleteObject": "STORAGE_ACCESS",
        "UpdatePolicy": "POLICY_CHANGE",
    },
    "azure": {
        "Microsoft.Storage/storageAccounts/blobServices/containers/read": "STORAGE_ACCESS",
        "Microsoft.Storage/storageAccounts/blobServices/containers/write": "STORAGE_ACCESS",
    },
    "gcp": {
        "storage.objects.get": "STORAGE_ACCESS",
        "storage.objects.create": "STORAGE_ACCESS",
        "setIamPolicy": "POLICY_CHANGE",
    }
}