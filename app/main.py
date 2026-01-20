from typing import Any, Dict

from fastapi import Body, FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse

from app.normalizer import normalize_event
from app.publisher import log_to_dlq, publish_event

app = FastAPI(
    title="Ingestion & Normalization Service",
    description=(
        "Receives raw provider-specific cloud events, validates basic structure, "
        "normalizes them into a unified internal schema, and publishes them to the event bus (mock)."
    ),
    version="1.0.0",
)

ALLOWED_PROVIDERS = {"aws", "azure", "gcp"}


@app.get("/health")
def health_check() -> Dict[str, str]:
    """
    Simple health endpoint to verify the service is running.
    """
    return {"status": "ok"}


@app.post("/ingest/{provider}")
def ingest_event(
    provider: str = Path(..., description="Cloud provider name: aws | azure | gcp"),
    payload: Dict[str, Any] = Body(..., description="Raw provider-specific cloud event payload (JSON)."),
):
    """
    Ingest a raw event from the given cloud provider, normalize it,
    and publish it to the event bus (mock publisher).

    Consistency note:
    - The request body is explicitly declared with Body(...) to avoid ambiguity in FastAPI parsing and OpenAPI docs.
    - Validation/normalization errors are routed to the DLQ simulation and return HTTP 422.
    """
    provider_normalized = provider.strip().lower()
    if provider_normalized not in ALLOWED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported provider '{provider}'. Allowed providers: {sorted(ALLOWED_PROVIDERS)}",
        )

    try:
        # Step 1: Normalize into the Unified Internal Schema
        normalized_event = normalize_event(provider_normalized, payload)

        # Step 2: Publish to the mock event bus (filesystem)
        publish_event(normalized_event)

        # Step 3: Return response to caller
        return JSONResponse(
            status_code=200,
            content=normalized_event.model_dump(mode="json"),
        )

    except ValueError as e:
        # Handle validation/normalization errors via DLQ
        log_to_dlq(provider_normalized, payload, str(e))
        raise HTTPException(status_code=422, detail=str(e))

    except Exception as e:
        # Catch-all for unexpected errors
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")