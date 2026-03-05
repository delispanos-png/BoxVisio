#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$PROJECT_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_DIR/backups}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
BACKUP_METRICS_FILE="${BACKUP_METRICS_FILE:-}"
CONTROL_DB_NAME="${CONTROL_DB_NAME:-bi_control}"
TENANT_DB_SUPERUSER="${TENANT_DB_SUPERUSER:-postgres}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$BACKUP_ROOT/$TS"
CONTROL_DIR="$RUN_DIR/control"
TENANT_DIR="$RUN_DIR/tenants"
MANIFEST="$RUN_DIR/manifest.txt"

mkdir -p "$CONTROL_DIR" "$TENANT_DIR"

write_backup_metrics() {
  local exit_code="$1"
  [[ -z "$BACKUP_METRICS_FILE" ]] && return 0
  mkdir -p "$(dirname "$BACKUP_METRICS_FILE")"
  local now_epoch
  now_epoch="$(date -u +%s)"
  local success=0
  if [[ "$exit_code" -eq 0 ]]; then
    success=1
  fi
  cat > "${BACKUP_METRICS_FILE}.tmp" <<EOF
# HELP cloudon_backup_last_run_timestamp_seconds Unix timestamp of latest backup run.
# TYPE cloudon_backup_last_run_timestamp_seconds gauge
cloudon_backup_last_run_timestamp_seconds ${now_epoch}
# HELP cloudon_backup_success 1 when latest backup run succeeded, 0 otherwise.
# TYPE cloudon_backup_success gauge
cloudon_backup_success ${success}
# HELP cloudon_backup_last_exit_code Exit code of latest backup run.
# TYPE cloudon_backup_last_exit_code gauge
cloudon_backup_last_exit_code ${exit_code}
EOF
  if [[ "$success" -eq 1 ]]; then
    cat >> "${BACKUP_METRICS_FILE}.tmp" <<EOF
# HELP cloudon_backup_last_success_timestamp_seconds Unix timestamp of latest successful backup.
# TYPE cloudon_backup_last_success_timestamp_seconds gauge
cloudon_backup_last_success_timestamp_seconds ${now_epoch}
EOF
  elif [[ -f "$BACKUP_METRICS_FILE" ]]; then
    grep '^cloudon_backup_last_success_timestamp_seconds ' "$BACKUP_METRICS_FILE" >> "${BACKUP_METRICS_FILE}.tmp" || true
  fi
  mv "${BACKUP_METRICS_FILE}.tmp" "$BACKUP_METRICS_FILE"
}

trap 'write_backup_metrics $?' EXIT

compose_exec() {
  docker compose --project-directory "$PROJECT_DIR" exec -T "$@"
}

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

backup_db() {
  local db_name="$1"
  local out_file="$2"
  log "backup_start db=$db_name file=$out_file"
  compose_exec postgres pg_dump -U "$TENANT_DB_SUPERUSER" -d "$db_name" -Fc -Z 9 > "$out_file"
  log "backup_done db=$db_name"
}

log "nightly_backup_started run_dir=$RUN_DIR"

backup_db "$CONTROL_DB_NAME" "$CONTROL_DIR/${CONTROL_DB_NAME}.dump"
compose_exec postgres pg_dumpall -U "$TENANT_DB_SUPERUSER" --globals-only > "$CONTROL_DIR/globals.sql"

mapfile -t TENANT_DBS < <(
  compose_exec postgres psql -U "$TENANT_DB_SUPERUSER" -d "$CONTROL_DB_NAME" -At \
    -c "SELECT db_name FROM tenants WHERE status <> 'terminated' ORDER BY id" | tr -d '\r'
)

TENANT_COUNT=0
for db_name in "${TENANT_DBS[@]}"; do
  [[ -z "$db_name" ]] && continue
  backup_db "$db_name" "$TENANT_DIR/${db_name}.dump"
  TENANT_COUNT=$((TENANT_COUNT + 1))
done

{
  echo "timestamp=$TS"
  echo "control_db=$CONTROL_DB_NAME"
  echo "tenant_count=$TENANT_COUNT"
  echo "retention_days=$BACKUP_RETENTION_DAYS"
  echo "files:"
  find "$RUN_DIR" -type f -printf '%P\n' | sort
} > "$MANIFEST"

find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -name '20*' -mtime "+$BACKUP_RETENTION_DAYS" -print0 | while IFS= read -r -d '' old_dir; do
  log "retention_delete dir=$old_dir"
  rm -rf "$old_dir"
done

log "nightly_backup_completed run_dir=$RUN_DIR tenant_count=$TENANT_COUNT"
