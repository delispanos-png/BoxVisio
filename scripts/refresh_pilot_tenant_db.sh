#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_TENANT_ID="${SOURCE_TENANT_ID:-5}"
TARGET_TENANT_ID="${TARGET_TENANT_ID:-1}"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_DIR/backups/pilot_refresh}"
EXECUTE=0
STOP_SERVICES=1

usage() {
  cat <<'USAGE'
Refresh a pilot tenant database from a production tenant snapshot.

Default source/target:
  source tenant id: 5
  target tenant id: 1

Usage:
  scripts/refresh_pilot_tenant_db.sh [--source-id 5] [--target-id 1] [--backup-root DIR] [--execute]

Safety:
  - Dry-run by default.
  - Requires --execute to drop/recreate the target tenant database.
  - Always backs up the target database before replacing it.
  - Stops API/workers during restore unless --no-stop-services is used.

Examples:
  scripts/refresh_pilot_tenant_db.sh
  scripts/refresh_pilot_tenant_db.sh --execute
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-id)
      SOURCE_TENANT_ID="$2"
      shift 2
      ;;
    --target-id)
      TARGET_TENANT_ID="$2"
      shift 2
      ;;
    --backup-root)
      BACKUP_ROOT="$2"
      shift 2
      ;;
    --execute)
      EXECUTE=1
      shift
      ;;
    --no-stop-services)
      STOP_SERVICES=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$SOURCE_TENANT_ID" == "$TARGET_TENANT_ID" ]]; then
  echo "source-id and target-id must be different" >&2
  exit 2
fi

if [[ ! "$SOURCE_TENANT_ID" =~ ^[0-9]+$ || ! "$TARGET_TENANT_ID" =~ ^[0-9]+$ ]]; then
  echo "source-id and target-id must be numeric." >&2
  exit 2
fi

if [[ "$TARGET_TENANT_ID" != "1" ]]; then
  echo "Refusing to refresh target tenant $TARGET_TENANT_ID. This job is intentionally scoped to pilot tenant id 1." >&2
  exit 2
fi

cd "$PROJECT_DIR"

compose_exec() {
  docker compose exec -T "$@"
}

env_from_api() {
  local key="$1"
  compose_exec api printenv "$key" 2>/dev/null || true
}

CONTROL_DB_NAME="${CONTROL_DB_NAME:-$(env_from_api CONTROL_DB_NAME)}"
TENANT_DB_SUPERUSER="${TENANT_DB_SUPERUSER:-$(env_from_api TENANT_DB_SUPERUSER)}"

CONTROL_DB_NAME="${CONTROL_DB_NAME:-bi_control_recovery}"
TENANT_DB_SUPERUSER="${TENANT_DB_SUPERUSER:-postgres}"

psql_control() {
  compose_exec postgres psql -U "$TENANT_DB_SUPERUSER" -d "$CONTROL_DB_NAME" "$@"
}

sql_ident() {
  local value="$1"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

tenant_rows="$(
  psql_control -At -F $'\t' \
    -c "SELECT id, slug, name, db_name, db_user, status FROM tenants WHERE id IN ($SOURCE_TENANT_ID, $TARGET_TENANT_ID) ORDER BY id"
)"

source_row=""
target_row=""
while IFS=$'\t' read -r id slug name db_name db_user status; do
  [[ -z "${id:-}" ]] && continue
  if [[ "$id" == "$SOURCE_TENANT_ID" ]]; then
    source_row="$id"$'\t'"$slug"$'\t'"$name"$'\t'"$db_name"$'\t'"$db_user"$'\t'"$status"
  elif [[ "$id" == "$TARGET_TENANT_ID" ]]; then
    target_row="$id"$'\t'"$slug"$'\t'"$name"$'\t'"$db_name"$'\t'"$db_user"$'\t'"$status"
  fi
done <<< "$tenant_rows"

if [[ -z "$source_row" || -z "$target_row" ]]; then
  echo "Could not load both source and target tenants from control database." >&2
  echo "$tenant_rows" >&2
  exit 1
fi

IFS=$'\t' read -r _ source_slug source_name source_db source_user source_status <<< "$source_row"
IFS=$'\t' read -r _ target_slug target_name target_db target_user target_status <<< "$target_row"

if [[ "$source_status" != "active" || "$target_status" != "active" ]]; then
  echo "Both source and target tenants must be active. source=$source_status target=$target_status" >&2
  exit 1
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="$BACKUP_ROOT/$run_id"
target_backup="$run_dir/target_${TARGET_TENANT_ID}_${target_db}_before_refresh.dump"
source_dump="$run_dir/source_${SOURCE_TENANT_ID}_${source_db}.dump"

cat <<PLAN
Pilot tenant refresh plan
  source: id=$SOURCE_TENANT_ID slug=$source_slug db=$source_db user=$source_user
  target: id=$TARGET_TENANT_ID slug=$target_slug db=$target_db user=$target_user
  backup dir: $run_dir
  stop services: $STOP_SERVICES
  execute: $EXECUTE
PLAN

if [[ "$EXECUTE" != "1" ]]; then
  echo
  echo "Dry-run only. Re-run with --execute to perform the refresh."
  exit 0
fi

mkdir -p "$run_dir"

echo "[1/7] Backing up target tenant database..."
compose_exec postgres pg_dump -U "$TENANT_DB_SUPERUSER" -Fc -Z 9 -d "$target_db" > "$target_backup"

echo "[2/7] Dumping source tenant database..."
compose_exec postgres pg_dump -U "$TENANT_DB_SUPERUSER" -Fc -Z 9 -d "$source_db" > "$source_dump"

services_stopped=0
restart_services() {
  if [[ "$services_stopped" == "1" ]]; then
    echo "[cleanup] Starting API/workers..."
    docker compose start api worker worker_beat worker_delete >/dev/null
  fi
}
trap restart_services EXIT

if [[ "$STOP_SERVICES" == "1" ]]; then
  echo "[3/7] Stopping API/workers..."
  docker compose stop api worker worker_beat worker_delete >/dev/null
  services_stopped=1
else
  echo "[3/7] Skipping service stop by request."
fi

echo "[4/7] Recreating target database..."
target_db_ident="$(sql_ident "$target_db")"
target_user_ident="$(sql_ident "$target_user")"
target_db_literal="${target_db//\'/\'\'}"
psql_control -v ON_ERROR_STOP=1 \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$target_db_literal' AND pid <> pg_backend_pid();" \
  -c "DROP DATABASE IF EXISTS $target_db_ident;" \
  -c "CREATE DATABASE $target_db_ident OWNER $target_user_ident;"

echo "[5/7] Restoring source snapshot into target database..."
compose_exec postgres pg_restore -U "$TENANT_DB_SUPERUSER" --no-owner --role="$target_user" -d "$target_db" < "$source_dump"

echo "[6/7] Analyzing target database..."
compose_exec postgres psql -U "$TENANT_DB_SUPERUSER" -d "$target_db" -v ON_ERROR_STOP=1 -c "ANALYZE;"

echo "[7/7] Basic validation..."
compose_exec postgres psql -U "$TENANT_DB_SUPERUSER" -d "$target_db" -v ON_ERROR_STOP=1 -c "
SELECT 'fact_sales' AS table_name, count(*) FROM fact_sales
UNION ALL SELECT 'fact_purchases', count(*) FROM fact_purchases
UNION ALL SELECT 'dim_items', count(*) FROM dim_items
UNION ALL SELECT 'fact_cashflows', count(*) FROM fact_cashflows
ORDER BY table_name;
"

echo
echo "Pilot tenant refresh completed."
echo "Target backup: $target_backup"
echo "Source dump:    $source_dump"
