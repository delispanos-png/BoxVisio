#!/usr/bin/env bash
set -euo pipefail

# Conservative Docker cleanup for production hosts.
# It never prunes volumes and never removes running containers.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${DOCKER_CLEANUP_LOG_DIR:-$PROJECT_DIR/artifacts/ops}"
METRICS_FILE="${DOCKER_CLEANUP_METRICS_FILE:-}"
UNTIL_HOURS="${DOCKER_CLEANUP_UNTIL_HOURS:-168}"
DRY_RUN="${DOCKER_CLEANUP_DRY_RUN:-0}"

mkdir -p "$LOG_DIR"

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

write_metrics() {
  local exit_code="$1"
  [[ -z "$METRICS_FILE" ]] && return 0
  mkdir -p "$(dirname "$METRICS_FILE")"
  local now_epoch success
  now_epoch="$(date -u +%s)"
  success=0
  [[ "$exit_code" -eq 0 ]] && success=1
  cat > "${METRICS_FILE}.tmp" <<EOF
# HELP cloudon_docker_cleanup_last_run_timestamp_seconds Unix timestamp of latest Docker cleanup run.
# TYPE cloudon_docker_cleanup_last_run_timestamp_seconds gauge
cloudon_docker_cleanup_last_run_timestamp_seconds ${now_epoch}
# HELP cloudon_docker_cleanup_success 1 when latest Docker cleanup run succeeded, 0 otherwise.
# TYPE cloudon_docker_cleanup_success gauge
cloudon_docker_cleanup_success ${success}
# HELP cloudon_docker_cleanup_last_exit_code Exit code of latest Docker cleanup run.
# TYPE cloudon_docker_cleanup_last_exit_code gauge
cloudon_docker_cleanup_last_exit_code ${exit_code}
EOF
  mv "${METRICS_FILE}.tmp" "$METRICS_FILE"
}

trap 'write_metrics $?' EXIT

if ! command -v docker >/dev/null 2>&1; then
  log "docker_not_found"
  exit 127
fi

log "docker_cleanup_started project_dir=$PROJECT_DIR until_hours=$UNTIL_HOURS dry_run=$DRY_RUN"
log "disk_before"
docker system df || true

if [[ "$DRY_RUN" == "1" ]]; then
  log "dry_run: would run docker container prune --force --filter until=${UNTIL_HOURS}h"
  log "dry_run: would run docker image prune --force --filter until=${UNTIL_HOURS}h"
  log "dry_run: would run docker builder prune --force --filter until=${UNTIL_HOURS}h, if supported"
else
  # Remove stopped containers older than the retention window.
  docker container prune --force --filter "until=${UNTIL_HOURS}h" || true

  # Remove dangling/unused images older than the retention window.
  # No -a: keep tagged images that may be useful for rollback.
  docker image prune --force --filter "until=${UNTIL_HOURS}h" || true

  # Clean build cache when the installed Docker supports it.
  if docker builder prune --help >/dev/null 2>&1; then
    docker builder prune --force --filter "until=${UNTIL_HOURS}h" || true
  else
    log "builder_prune_not_supported"
  fi
fi

log "disk_after"
docker system df || true
log "docker_cleanup_completed"
