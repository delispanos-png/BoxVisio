#!/usr/bin/env bash
# Install the repo's git hooks. Safe to re-run.
#
# pre-commit stamps the build number into backend/app/build_info.json and stages
# it, so every commit carries its own version. Without this hook the version
# stops moving and the app keeps reporting the last stamped build.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK_DIR="$REPO_ROOT/.git/hooks"
HOOK="$HOOK_DIR/pre-commit"

mkdir -p "$HOOK_DIR"

cat > "$HOOK" <<'HOOK_EOF'
#!/usr/bin/env bash
# Managed by scripts/install_git_hooks.sh — stamps the build number.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
PY="$(command -v python3 || command -v python || true)"
[ -z "$PY" ] && { echo "[pre-commit] no python found, skipping build stamp" >&2; exit 0; }

"$PY" "$REPO_ROOT/scripts/bump_build.py" || {
  echo "[pre-commit] build stamp failed; commit aborted" >&2
  exit 1
}
git add "$REPO_ROOT/backend/app/build_info.json"
HOOK_EOF

chmod +x "$HOOK"
echo "installed: $HOOK"
