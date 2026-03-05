# Incident Response Runbook

## Scope
Use this for API outages, ingest stalls, DLQ spikes, backup failures, DB issues.

## 1) Triage (first 10 minutes)
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml ps
curl -fsS http://localhost/health
curl -fsS http://localhost:8000/ready
```

Check logs:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml logs --tail=200 api
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml logs --tail=200 worker
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml logs --tail=200 postgres
```

## 2) Classify severity
- `SEV-1`: login/API unavailable, postgres down, data loss risk.
- `SEV-2`: ingestion blocked for one/more tenants, high error rate.
- `SEV-3`: non-critical dashboard degradation.

## 3) Immediate mitigations
- API unstable:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --no-deps --force-recreate api
```
- Worker backlog:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --scale worker=3 worker
```
- Retry storm / repeated failed jobs:
```bash
# in /opt/cloudon-bi/.env tune these values and restart worker
# INGEST_RETRY_BACKOFF_SECONDS=10
# INGEST_DRAIN_MAX_JOBS=50
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --no-deps --force-recreate worker
```
- Postgres exporter/downstream scrape issue:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml restart postgres
```

## 4) Verify recovery
- Alert clears in Grafana/Prometheus.
- Queue depth trends down.
- `cloudon_ingest_jobs_total{status="ok"}` increases.
- `/ready` returns 200.

## 5) Post-incident
- Capture timeline, root cause, impacted tenants.
- Add remediation task and regression test.
- If issue involved onboarding/provisioning, validate `/admin/tenants` last run panel:
  - step-by-step status is rendered
  - invite token/API key are visible only immediately after successful creation
