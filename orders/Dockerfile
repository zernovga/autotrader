FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./

COPY common/pyproject.toml ./common/pyproject.toml
COPY tinkoff/pyproject.toml ./tinkoff/pyproject.toml 
# FIXME: remove tinkoff when it's out if quarantine again  
COPY orders/pyproject.toml ./orders/pyproject.toml

RUN uv sync --no-dev --package orders 

COPY common ./common
COPY tinkoff ./tinkoff
COPY orders ./orders


CMD ["uv", "run", "-m", "orders.main"]
