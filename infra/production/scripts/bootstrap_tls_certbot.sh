#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
EMAIL="${1:-}"
DOMAINS="${2:-bi.boxvisio.com,adminpanel.boxvisio.com}"

if [[ -z "$EMAIL" ]]; then
  echo "Usage: $0 <letsencrypt_email> [domains_csv]"
  exit 1
fi

IFS=',' read -r -a DOM_ARR <<< "$DOMAINS"
DOMAIN_ARGS=()
for d in "${DOM_ARR[@]}"; do
  DOMAIN_ARGS+=("-d" "$d")
done

cd "$PROJECT_DIR"

# Start stack with bootstrap (HTTP-only) nginx for challenge serving
cp infra/production/nginx/conf.d/cloudon.bootstrap.conf infra/production/nginx/conf.d/default.active.conf

docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d postgres redis api worker nginx

# Obtain certificates
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml run --rm certbot \
  certonly --webroot -w /var/www/certbot \
  --email "$EMAIL" --agree-tos --no-eff-email \
  "${DOMAIN_ARGS[@]}"

# Switch to TLS nginx config
cp infra/production/nginx/conf.d/cloudon.conf infra/production/nginx/conf.d/default.active.conf

docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T nginx nginx -t
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T nginx nginx -s reload

echo "TLS bootstrap complete"
