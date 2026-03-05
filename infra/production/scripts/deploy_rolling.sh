#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$PROJECT_DIR"

COMPOSE="docker compose -f docker-compose.yml -f infra/production/compose.prod.yml"
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

DB_SUPERUSER="${TENANT_DB_SUPERUSER:-postgres}"
CONTROL_DB="${CONTROL_DB_NAME:-bi_control}"

# Ensure required services are running before exec/migrations
$COMPOSE up -d postgres redis api worker nginx

# 1) Pull/build images
$COMPOSE build api worker

# 2) Run control DB migrations first
$COMPOSE exec -T api alembic -c alembic.ini upgrade head

# 3) Run tenant migrations for all active tenants
TENANTS=$($COMPOSE exec -T postgres psql -U "$DB_SUPERUSER" -d "$CONTROL_DB" -At -c "SELECT slug FROM tenants WHERE status <> 'terminated' ORDER BY id" | tr -d '\r')
while IFS= read -r slug; do
  [[ -z "$slug" ]] && continue
  $COMPOSE exec -T api python /app/scripts/run_tenant_migrations.py --tenant "$slug"
done <<< "$TENANTS"

# 4) Zero-downtime-ish rollout: scale up then recreate
$COMPOSE up -d --no-deps --scale api=2 api
$COMPOSE up -d --no-deps --scale worker=2 worker

# 5) Recreate services with new image
$COMPOSE up -d --no-deps --force-recreate api
$COMPOSE up -d --no-deps --force-recreate worker

# 6) Normalize scale if desired
$COMPOSE up -d --no-deps --scale api=1 api
$COMPOSE up -d --no-deps --scale worker=1 worker

# 7) Post checks
$COMPOSE ps
$COMPOSE exec -T api curl -fsS http://localhost:8000/ready >/dev/null
$COMPOSE exec -T nginx wget -q -O /dev/null http://localhost/health

echo "Deployment completed"
