# Production Deployment (Ubuntu 22.04/24.04)

## 1) Prerequisites
```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg ufw
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
```

## 2) Project folder + env
```bash
sudo mkdir -p /opt/cloudon-bi
cd /opt/cloudon-bi
cp .env.example .env
# Edit .env and replace all CHANGE_ME values
nano .env
```

Required checks:
- `.env` must not be committed (covered by `.gitignore`).
- App startup fails if `CHANGE_ME` placeholders remain.

## 3) Start stack (production compose)
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --build
```

## 4) Migrations and admin seed
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T api alembic -c alembic.ini upgrade head
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T api python /app/scripts/seed_admin.py
```

## 5) TLS setup (Let's Encrypt)
Use DNS first: point `bi.boxvisio.com` and `adminpanel.boxvisio.com` to this server.

```bash
cd /opt/cloudon-bi
chmod +x infra/production/scripts/bootstrap_tls_certbot.sh
bash infra/production/scripts/bootstrap_tls_certbot.sh you@example.com
```

This will:
- start HTTP challenge mode,
- issue certs,
- switch nginx to TLS config,
- enable 80->443 redirect + HSTS.

Enable automatic renewal container:
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml --profile tools up -d certbot
```

## 6) Firewall hardening (UFW)
```bash
cd /opt/cloudon-bi
chmod +x infra/production/scripts/ufw_hardening.sh
bash infra/production/scripts/ufw_hardening.sh 194.154.34.84
```

Expected policy:
- open: `80/tcp`, `443/tcp`
- open: `22/tcp` only from `194.154.34.84`
- closed publicly: `5432/tcp`, `6379/tcp`

## 7) Healthchecks verification
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml ps
curl -fsS http://localhost/health
curl -kfsS https://bi.boxvisio.com/health
curl -kfsS https://adminpanel.boxvisio.com/health
```

## 8) Zero-downtime-ish deployment procedure
```bash
cd /opt/cloudon-bi
chmod +x infra/production/scripts/deploy_rolling.sh
bash infra/production/scripts/deploy_rolling.sh
```

Procedure includes:
- control DB migration,
- tenant migrations,
- temporary scale-up of api/worker,
- recreate + scale normalization.

## 9) Restart policy / systemd
Compose services use `restart: unless-stopped`.

To auto-start full stack on reboot via systemd:
```bash
sudo cp infra/production/systemd/cloudon-bi.service /etc/systemd/system/cloudon-bi.service
sudo systemctl daemon-reload
sudo systemctl enable --now cloudon-bi.service
sudo systemctl status cloudon-bi.service
```

## 10) Log retention
- Container logs: capped via compose `json-file` logging options (`max-size=50m`, `max-file=10`).
- Nginx access/error logs in `/opt/cloudon-bi/infra/production/logs/nginx`.

Enable logrotate:
```bash
sudo cp infra/production/logrotate/cloudon-bi-nginx /etc/logrotate.d/cloudon-bi-nginx
sudo logrotate -f /etc/logrotate.d/cloudon-bi-nginx
```

## 11) Promotion runbook
Follow:
- `infra/production/STAGING_TO_PROD_RUNBOOK.md`
