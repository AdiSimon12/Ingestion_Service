import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict

from app.models import NormalizedEvent

# Directory to store published events (mock event bus persistence)
PUBLISHED_DIR = Path("published_events")
PUBLISHED_DIR.mkdir(exist_ok=True)

# Directory for the Dead Letter Queue (DLQ)
DLQ_DIR = Path("failed_events")
DLQ_DIR.mkdir(exist_ok=True)


def publish_event(event: NormalizedEvent) -> None:
    """
    Mock publisher for the Event Bus.
    Converts Pydantic model to JSON-compatible dict before saving.
    """
    # שימוש ב-mode="json" הופך UUID ותאריכים לטקסט באופן אוטומטי
    event_dict = event.model_dump(mode="json")
    event_dict["_published_at_utc"] = datetime.now(timezone.utc).isoformat()

    filename = f"{event.event_uuid}.json"
    filepath = PUBLISHED_DIR / filename

    try:
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(event_dict, f, ensure_ascii=False, indent=2)
    except Exception as e:
        raise ValueError(f"Failed to publish event to mock event bus storage: {e}")


def log_to_dlq(provider: str, payload: Dict[str, Any], error_message: str) -> None:
    """
    Implements the Dead Letter Queue (DLQ) mechanism.
    Ensures all IDs are strings for JSON serialization.
    """
    import uuid
    
    dlq_entry = {
        "dlq_id": str(uuid.uuid4()), # המרה מפורשת לטקסט
        "provider": provider,
        "error_details": error_message,
        "failure_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "raw_payload": payload
    }

    filename = f"failed_{dlq_entry['dlq_id']}.json"
    filepath = DLQ_DIR / filename

    try:
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(dlq_entry, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Critical Failure: Could not write to DLQ: {e}")