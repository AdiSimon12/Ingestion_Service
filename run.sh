#!/bin/bash

echo "Starting Ingestion & Normalization Service..."

# Optional: activate virtual environment if exists
if [ -d ".venv" ]; then
  source .venv/bin/activate
  echo "Virtual environment activated."
fi

# Run the FastAPI application
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000