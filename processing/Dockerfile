FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml .
COPY common/pyproject.toml ./common/pyproject.toml
COPY processing/pyproject.toml ./processing/pyproject.toml

RUN uv sync --no-dev --package processing

COPY common ./common
COPY processing ./processing


CMD ["uv", "run", "-m", "processing.main"]
