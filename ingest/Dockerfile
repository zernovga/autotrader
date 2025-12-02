FROM python:3.13-slim

RUN pip install uv

WORKDIR /app

COPY ../common /common
COPY . .

RUN uv sync --no-dev

CMD ["uv", "run", "main.py"]
