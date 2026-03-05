# Production TLS Setup (bi.boxvisio.com + adminpanel.boxvisio.com)

Server model: single Ubuntu server, Docker Compose stack (`nginx`, `api`, `worker`, `postgres`, `redis`).

## 1) DNS prerequisites
Run these first and wait for propagation:
- `bi.boxvisio.com` -> server public IP
- `adminpanel.boxvisio.com` -> server public IP

Quick check:
```bash
dig +short bi.boxvisio.com
dig +short adminpanel.boxvisio.com
```

## 2) Install certbot on Ubuntu
```bash
sudo apt update
sudo apt install -y certbot
```

## 3) Prepare paths used by nginx + certbot
```bash
cd /opt/cloudon-bi
mkdir -p infra/certbot/www
sudo mkdir -p /etc/letsencrypt
```

## 4) Stop nginx container temporarily (free port 80)
```bash
cd /opt/cloudon-bi
docker compose stop nginx
```

## 5) Issue certificates (Let's Encrypt)
Replace `ops@boxvisio.com` with your real mailbox.
```bash
sudo certbot certonly --standalone \
  --preferred-challenges http \
  --agree-tos --no-eff-email --email ops@boxvisio.com \
  -d bi.boxvisio.com \
  -d adminpanel.boxvisio.com
```

## 6) Start full stack with TLS nginx config
```bash
cd /opt/cloudon-bi
docker compose up -d nginx api worker postgres redis
```

## 7) Validate nginx + TLS
```bash
cd /opt/cloudon-bi
docker compose exec -T nginx nginx -t
curl -I http://bi.boxvisio.com
curl -I http://adminpanel.boxvisio.com
curl -I https://bi.boxvisio.com
curl -I https://adminpanel.boxvisio.com
```

Expected:
- `http://...` returns `301` to `https://...`
- `https://...` returns `200/302`
- Security headers present (`Strict-Transport-Security`, `X-Frame-Options`, `X-Content-Type-Options`, `Referrer-Policy`)

## 8) Configure auto-renew
Systemd timer is installed with certbot package. Verify:
```bash
systemctl list-timers | grep certbot
```

Create a deploy hook so nginx reloads after certificate renewal:
```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/reload-cloudon-nginx.sh >/dev/null <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail
cd /opt/cloudon-bi
docker compose exec -T nginx nginx -s reload
HOOK
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/reload-cloudon-nginx.sh
```

Dry-run renewal:
```bash
sudo certbot renew --dry-run
```

## 9) Security verification
```bash
curl -sI https://bi.boxvisio.com | grep -Ei 'strict-transport-security|x-frame-options|x-content-type-options|referrer-policy'
curl -sI https://adminpanel.boxvisio.com | grep -Ei 'strict-transport-security|x-frame-options|x-content-type-options|referrer-policy'
```

## Notes
- `/metrics` is intentionally internal-only (allowed only from localhost, denied for public clients).
- `/api/*` is proxied to the API upstream and `/api` prefix is stripped before forwarding.
- All other paths are routed to frontend upstream (currently the same FastAPI app).
- `adminpanel.boxvisio.com` is IP-restricted via `infra/nginx/admin_allowlist.conf`.
