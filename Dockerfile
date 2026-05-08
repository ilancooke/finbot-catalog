FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts

RUN pip install --no-cache-dir -e .

ENV FINBOT_DATA_ROOT=/data

CMD ["python", "scripts/build_catalog.py"]
