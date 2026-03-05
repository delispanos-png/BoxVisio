#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$PROJECT_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

TENANT_DB_SUPERUSER="${TENANT_DB_SUPERUSER:-postgres}"
DB_NAME=""
DUMP_FILE=""
DROP_CREATE="false"

usage() {
  cat <<EOF
Usage:
  $(basename "$0") --db <database_name> --file <backup_file.dump> [--drop-create]

Examples:
  $(basename "$0") --db bi_control --file /opt/cloudon-bi/backups/20260228T020000Z/control/bi_control.dump --drop-create
  $(basename "$0") --db bi_tenant_alpha --file /opt/cloudon-bi/backups/20260228T020000Z/tenants/bi_tenant_alpha.dump --drop-create
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      DB_NAME="$2"
      shift 2
      ;;
    --file)
      DUMP_FILE="$2"
      shift 2
      ;;
    --drop-create)
      DROP_CREATE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DB_NAME" || -z "$DUMP_FILE" ]]; then
  usage
  exit 1
fi

if [[ ! -f "$DUMP_FILE" ]]; then
  echo "Backup file not found: $DUMP_FILE" >&2
  exit 1
fi

compose_exec() {
  docker compose --project-directory "$PROJECT_DIR" exec -T "$@"
}

if [[ "$DROP_CREATE" == "true" ]]; then
  compose_exec postgres psql -U "$TENANT_DB_SUPERUSER" -d postgres -v ON_ERROR_STOP=1 <<SQL
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS "${DB_NAME}";
CREATE DATABASE "${DB_NAME}";
SQL
fi

cat "$DUMP_FILE" | compose_exec postgres pg_restore -U "$TENANT_DB_SUPERUSER" -d "$DB_NAME" --clean --if-exists --no-owner --no-privileges

echo "Restore completed for database: $DB_NAME"
