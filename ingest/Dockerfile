FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml .

COPY common/pyproject.toml ./common/pyproject.toml
COPY ingest/pyproject.toml ./ingest/pyproject.toml
RUN uv sync --no-dev --package ingest

COPY common ./common
COPY ingest ./ingest


CMD ["uv", "run", "-m", "ingest.main"]
