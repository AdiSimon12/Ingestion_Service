import json
from pathlib import Path

import pytest

from app.normalizer import normalize_event
from app.models import NormalizedEvent


MOCK_DIR = Path("mock_events")


def load_mock(filename: str) -> dict:
    path = MOCK_DIR / filename
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize(
    "provider, filename, expected_provider",
    [
        ("aws", "aws_event.json", "AWS"),
        ("azure", "azure_event.json", "AZURE"),
        ("gcp", "gcp_event.json", "GCP"),
    ],
)
def test_normalize_event_success(provider, filename, expected_provider):
    payload = load_mock(filename)
    event = normalize_event(provider, payload)

    assert isinstance(event, NormalizedEvent)
    assert str(event.cloud_provider) == expected_provider

    # Critical fields must exist and be non-empty
    assert event.event_uuid is not None
    assert event.unified_event_type is not None and event.unified_event_type.strip() != ""
    assert event.timestamp_utc is not None
    assert event.resource_id is not None and event.resource_id.strip() != ""

    # Raw payload must be preserved for traceability
    assert isinstance(event.raw_payload, dict)
    assert len(event.raw_payload) > 0


def test_normalize_event_invalid_provider():
    payload = {"some": "data"}
    with pytest.raises(ValueError):
        normalize_event("unknown", payload)