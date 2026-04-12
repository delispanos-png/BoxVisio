#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TENANT_SLUG="${1:-}"
MAX_JOBS="${2:-500}"

if [[ -z "$TENANT_SLUG" ]]; then
  echo "Usage: $0 <tenant_slug> [max_jobs]"
  exit 1
fi

COMPOSE="docker compose --project-directory $PROJECT_DIR -f $PROJECT_DIR/docker-compose.yml -f $PROJECT_DIR/infra/production/compose.prod.yml"

$COMPOSE exec -T worker /opt/cloudon-bi/.venv/bin/celery -A worker.celery_app call worker.tasks.drain_tenant_ingest_queue --kwargs "{\"tenant_slug\":\"$TENANT_SLUG\",\"max_jobs\":$MAX_JOBS}"

echo "Triggered drain for tenant=$TENANT_SLUG max_jobs=$MAX_JOBS"
