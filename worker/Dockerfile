FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY backend /app/backend
COPY worker /app/worker
COPY scripts /app/scripts

ENV PYTHONPATH=/app/backend:/app

RUN useradd --create-home --shell /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

CMD ["celery", "-A", "worker.celery_app", "worker", "-l", "INFO", "-Q", "default,ingest"]
