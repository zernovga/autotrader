FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml  ./

COPY common/pyproject.toml ./common/pyproject.toml
COPY tick-storage/pyproject.toml ./tick-storage/pyproject.toml

RUN uv sync --no-dev --package tick-storage

COPY common ./common
COPY tick-storage ./tick-storage


CMD ["uv", "run", "-m", "tick-storage.main"]
