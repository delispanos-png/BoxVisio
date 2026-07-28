#!/usr/bin/env bash
# Automated Let's Encrypt renewal for the BoxVisio nginx stack.
# Renews any cert within 30 days of expiry (certbot's default) using the same
# webroot nginx already serves at /.well-known/acme-challenge/, then reloads
# nginx so the new cert is picked up without downtime.
#
# Installed as a twice-daily cron job (see /etc/cron.d/boxvisio-certbot-renew).
# Safe to run any time: certbot no-ops if nothing is due to renew.
set -euo pipefail

PROJECT_DIR=/opt/cloudon-bi
WEBROOT="${PROJECT_DIR}/infra/certbot/www"
LOG=/var/log/boxvisio-certbot-renew.log

exec >>"$LOG" 2>&1
echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') certbot renew ====="

docker run --rm \
  -v /etc/letsencrypt:/etc/letsencrypt \
  -v "${WEBROOT}:${WEBROOT}" \
  certbot/certbot renew --webroot -w "${WEBROOT}" --non-interactive

# Reload nginx unconditionally: a no-op renewal leaves certs untouched, and a
# reload is cheap and connection-safe. Guarantees a freshly renewed cert is live.
if docker ps --format '{{.Names}}' | grep -q '^cloudon_bi-nginx-1$'; then
  docker exec cloudon_bi-nginx-1 nginx -s reload && echo "nginx reloaded"
fi
echo "----- done -----"
