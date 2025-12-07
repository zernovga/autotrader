FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./

COPY common/pyproject.toml ./common/pyproject.toml
COPY tinkoff/pyproject.toml ./tinkoff/pyproject.toml 
# FIXME: remove tinkoff when it's out if quarantine again  
COPY ingest/pyproject.toml ./ingest/pyproject.toml

RUN uv sync --no-dev --package ingest 

COPY common ./common
COPY tinkoff ./tinkoff
COPY ingest ./ingest


CMD ["uv", "run", "-m", "ingest.main"]
