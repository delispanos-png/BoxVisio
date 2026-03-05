#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TENANT_SLUG="${1:-}"
LIMIT="${2:-0}"

if [[ -z "$TENANT_SLUG" ]]; then
  echo "Usage: $0 <tenant_slug> [limit]"
  echo "limit=0 means replay all"
  exit 1
fi

COMPOSE="docker compose --project-directory $PROJECT_DIR -f $PROJECT_DIR/docker-compose.yml -f $PROJECT_DIR/infra/production/compose.prod.yml"
DLQ_KEY="dlq:${TENANT_SLUG}"
INGEST_KEY="ingest:${TENANT_SLUG}"

moved=0
while true; do
  result="$($COMPOSE exec -T redis redis-cli --raw RPOPLPUSH "$DLQ_KEY" "$INGEST_KEY" | tr -d '\r')"
  if [[ "$result" == "(nil)" || -z "$result" ]]; then
    break
  fi
  moved=$((moved + 1))
  if [[ "$LIMIT" -gt 0 && "$moved" -ge "$LIMIT" ]]; then
    break
  fi
done

echo "Replayed $moved events from $DLQ_KEY to $INGEST_KEY"
