# Go-Live Plan

## Scope
Production cutover from staging-validated release to live operation for:
- `bi.boxvisio.com` (tenant portal)
- `adminpanel.boxvisio.com` (CloudOn admin)

## Roles
- Release owner: runs deploy and rollback commands.
- Ops owner: infra checks (TLS, firewall, backups).
- Product owner: business sign-off after smoke tests.
- Support owner: customer communications and status updates.

## T-24h Preconditions
1. `STAGING_VALIDATION_REPORT.md` and `UAT_REPORT.md` reviewed.
2. DNS records for both domains point to production IP.
3. TLS certs issued and renewal enabled.
4. `.env` secrets validated (no `CHANGE_ME`).
5. Latest backup successful and restore drill passed.
6. Incident runbooks available (`infra/runbooks/*`).

## Cutover Steps (T0)

### 1) Freeze window
```bash
cd /opt/cloudon-bi
# Freeze: no schema/code changes outside release owner.
```

### 2) Pre-cutover backup
```bash
cd /opt/cloudon-bi
./scripts/nightly_backup.sh
```

### 3) Deploy release + migrations
```bash
cd /opt/cloudon-bi
bash infra/production/scripts/deploy_rolling.sh
```

### 4) Verify service health
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml ps
curl -fsS https://bi.boxvisio.com/health
curl -fsS https://adminpanel.boxvisio.com/health
```

### 5) Validate core flows
1. Admin login works on `adminpanel.boxvisio.com`.
2. Tenant dashboard loads on `bi.boxvisio.com`.
3. Ingestion queue drain works for one test tenant.
4. KPI summary returns data from Postgres.

### 6) Enable monitoring watch
1. Open Grafana dashboard `CloudOn BI - Monitoring Overview`.
2. Watch alerts for 30 minutes post-cutover.

## Rollback Plan

## Trigger Conditions
- Sustained 5xx error rate > 5% for >10 min.
- Critical data path broken (ingest or KPI endpoints unavailable).
- DB migration causes functional regression.

## Rollback Steps
1. Roll app version back to previous stable release.
```bash
cd /opt/cloudon-bi
# git checkout <previous-release-tag>
bash infra/production/scripts/deploy_rolling.sh
```
2. If schema rollback is required:
```bash
cd /opt/cloudon-bi
./scripts/restore_db.sh --db bi_control --file <control_dump> --drop-create
./scripts/restore_db.sh --db bi_tenant_<slug> --file <tenant_dump> --drop-create
```
3. Re-run health checks and smoke tests.
4. Send rollback communication template.

## Communication Templates

### Pre-maintenance (T-2h)
Subject: Scheduled Go-Live Window - CloudOn BI

Message:
- We will perform a production go-live on `<DATE>` between `<START UTC>` and `<END UTC>`.
- Expected impact: brief service instability possible during deployment.
- Dashboards and ingestion will be monitored continuously.
- Next update at `<TIME>`.

### During maintenance (status update)
Subject: Go-Live In Progress - CloudOn BI

Message:
- Go-live is in progress.
- Current step: `<deploy/migration/verification>`.
- Current status: `<on-track / delayed by X min>`.
- Next update at `<TIME>`.

### Completed successfully
Subject: Go-Live Completed - CloudOn BI

Message:
- Go-live completed successfully at `<TIME UTC>`.
- Services are healthy and monitored.
- If you observe anomalies, contact support with tenant slug and timestamp.

### Rollback notice
Subject: Go-Live Rollback Executed - CloudOn BI

Message:
- We executed rollback at `<TIME UTC>` due to `<reason>`.
- Service is restored to last stable version.
- We will share RCA and new release window by `<DATE/TIME>`.
