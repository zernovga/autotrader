FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml .
COPY common ./common
COPY ingest ./ingest

RUN uv sync --no-dev --package ingest

CMD ["uv", "run", "-m", "ingest.main"]
