# STAGING Validation Report (Milestone 1)

Date: 2026-02-28 (UTC)
Scope: `PRE_PRODUCTION_CHECKLIST.md` end-to-end on STAGING
Constraint: No new features; only validation + bugfix + operational wiring.

## Executive Summary

- Validation status: **BLOCKED (infrastructure build blocker)**
- Root cause: Docker image builds for `api` and `worker` fail/hang on `apt-get update` due repeated timeouts to Debian mirrors (`trixie-updates`, `trixie-security`) in this staging environment.
- Minimal fix applied: add apt retry/timeout options in Dockerfiles to improve resilience.
- End-to-end mandatory test matrix could not be completed until staging build becomes healthy.

---

## What Was Tested (Actual Execution)

### A. Staging bootstrap preflight

#### Test A1: Bring up stack
- Command:
```bash
cd /opt/cloudon-bi && docker compose up -d --build
```
- Result: **FAIL**
- Evidence snippet:
```text
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```
(then retried with elevated permissions)

#### Test A2: Bring up stack with elevated permissions
- Command:
```bash
cd /opt/cloudon-bi && docker compose up -d --build
```
- Result: **FAIL / HANG**
- Evidence snippet:
```text
RUN apt-get ... update ...
Get:2 http://deb.debian.org/debian trixie-updates InRelease [47.3 kB]
Ign:2 http://deb.debian.org/debian trixie-updates InRelease
Get:3 http://deb.debian.org/debian-security trixie-security InRelease [43.4 kB]
Ign:3 http://deb.debian.org/debian-security trixie-security InRelease
(repeats)
```

#### Test A3: Verify service status
- Command:
```bash
cd /opt/cloudon-bi && docker compose ps
```
- Result: **FAIL** (no services up)
- Evidence snippet:
```text
NAME      IMAGE     COMMAND   SERVICE   CREATED   STATUS    PORTS
```

---

## Issues Found + Fixes Applied

### Issue 1: Staging build hangs/fails on apt mirror timeouts
- Severity: High (blocks all end-to-end validation)
- Affected: `backend` and `worker` image builds
- Fix applied (minimal operational hardening):
  - Added apt retry/timeout options in both Dockerfiles.

Changed files:
- `/opt/cloudon-bi/backend/Dockerfile`
- `/opt/cloudon-bi/worker/Dockerfile`

Diff summary:
- `apt-get update` -> `apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 update`

Status after fix:
- Still blocked due persistent external mirror instability in current staging environment.

---

## Mandatory Tests Matrix

Legend: `PASS`, `FAIL`, `BLOCKED`

1. Tenant lifecycle (trial->active->past_due->suspended->unsuspended->canceled)
- Status: **BLOCKED**
- Reason: staging services not up

2. Provisioning wizard success + forced rollback at step 4/5/6
- Status: **BLOCKED**
- Reason: staging services not up

3. Ingestion SQL connector (incremental, retries, DLQ, idempotency)
- Status: **BLOCKED**
- Reason: staging services not up

4. External ingest API (signature, replay/idempotency, queueing, DLQ)
- Status: **BLOCKED**
- Reason: staging services not up

5. KPI correctness vs raw facts
- Status: **BLOCKED**
- Reason: staging services not up

6. Aggregations update + endpoint usage
- Status: **BLOCKED**
- Reason: staging services not up

7. Security (rate limit, RBAC, CSRF, refresh rotation, API key rotation)
- Status: **BLOCKED**
- Reason: staging services not up

8. Observability (/metrics, logging, Sentry opt-in, sync duration metrics)
- Status: **BLOCKED**
- Reason: staging services not up

9. Backups + restore drill (control + tenant + full clean env)
- Status: **BLOCKED**
- Reason: staging services not up

10. Load smoke (concurrent dashboard reads + ingests)
- Status: **BLOCKED**
- Reason: staging services not up

---

## Exact Commands to Reproduce Each Mandatory Test

> Run after staging stack is healthy.

### 0) Bootstrap
```bash
cd /opt/cloudon-bi
cp .env.example .env
# fill secrets in .env
docker compose up -d --build
make migrate-control
make seed-admin
```

### 1) Tenant lifecycle
```bash
# Login as cloudon_admin
curl -s -X POST http://localhost:8000/v1/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"admin@cloudon.local","password":"<ADMIN_PASSWORD>"}'

# Create tenant via wizard
curl -s -X POST http://localhost:8000/v1/admin/tenants/wizard \
  -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' \
  -d '{"name":"Stage Pharmacy","slug":"stage-pharma","admin_email":"owner@stage.local","plan":"standard","source":"external","subscription_status":"trial","trial_days":14}'

# Move through statuses
curl -s -X PATCH http://localhost:8000/v1/admin/tenants/<TENANT_ID>/subscription \
  -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' \
  -d '{"status":"active"}'
curl -s -X PATCH http://localhost:8000/v1/admin/tenants/<TENANT_ID>/subscription -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' -d '{"status":"past_due"}'
curl -s -X PATCH http://localhost:8000/v1/admin/tenants/<TENANT_ID>/subscription -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' -d '{"status":"suspended"}'
curl -s -X PATCH http://localhost:8000/v1/admin/tenants/<TENANT_ID>/subscription -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' -d '{"status":"active"}'
curl -s -X PATCH http://localhost:8000/v1/admin/tenants/<TENANT_ID>/subscription -H "Authorization: Bearer <TOKEN>" -H 'Content-Type: application/json' -d '{"status":"canceled"}'
```

### 2) Provisioning wizard + forced failures step 4/5/6
```bash
# Success path
curl -s -X POST http://localhost:8000/v1/admin/tenants/wizard ...

# Step-4 failure simulation (DB create): temporarily break DB superpass in api env and retry wizard
# Step-5 failure simulation (migration): temporarily set invalid alembic target/env in api container and retry
# Step-6 failure simulation (control DB write): temporary DB constraint/permission fault then retry
# Verify rollback:
#  - tenant row absent in control db
#  - tenant db absent in postgres
```

### 3) Ingestion SQL connector
```bash
# Set SQL mapping
curl -s -X PUT http://localhost:8000/v1/admin/tenants/<TENANT_ID>/sqlserver/mapping ...
# Trigger sync
curl -s -X POST http://localhost:8000/v1/admin/tenants/<TENANT_ID>/sync -H "Authorization: Bearer <TOKEN>"
# Validate sync_state + idempotency + DLQ
```

### 4) External ingest API
```bash
# Build payload and HMAC sha256 signature
# POST /v1/ingest/sales with X-API-Key, X-Tenant, X-Signature
# Repeat same event_id to verify idempotency
# Send invalid signature to verify 401
```

### 5) KPI correctness
```bash
# Query KPI endpoints
curl -s "http://localhost:8000/v1/kpi/sales/summary?from=2026-01-01&to=2026-01-31" -H "Authorization: Bearer <TENANT_TOKEN>"
curl -s "http://localhost:8000/v1/kpi/sales/by-branch?from=2026-01-01&to=2026-01-31" -H "Authorization: Bearer <TENANT_TOKEN>"
curl -s "http://localhost:8000/v1/kpi/sales/compare?A_from=2026-01-01&A_to=2026-01-15&B_from=2026-01-16&B_to=2026-01-31" -H "Authorization: Bearer <TENANT_TOKEN>"
# Compare against raw SQL totals in tenant DB
```

### 6) Aggregations verification
```bash
# Trigger refresh task and compare agg vs facts
# Ensure KPI queries hit agg tables (explain/analyze)
```

### 7) Security checks
```bash
# Rate limit burst
# RBAC negative/positive checks
# CSRF invalid/missing token checks on UI POST
# Refresh token rotation via /v1/auth/refresh
# API key rotation via /v1/admin/tenants/<id>/api-keys/rotate
```

### 8) Observability checks
```bash
curl -s http://localhost:8000/metrics | head -n 100
curl -s http://localhost:8000/health
curl -s http://localhost:8000/ready
# Verify sync metrics/log lines after ingestion jobs
# Enable SENTRY_DSN and trigger controlled exception
```

### 9) Backup + restore
```bash
make backup-nightly
make restore-db DB=bi_control FILE=/opt/cloudon-bi/backups/<ts>/control/bi_control.dump DROP_CREATE=true
make restore-db DB=bi_tenant_<slug> FILE=/opt/cloudon-bi/backups/<ts>/tenants/bi_tenant_<slug>.dump DROP_CREATE=true
```

### 10) Load smoke
```bash
# k6/locust scenario: concurrent reads + ingests
# Capture p95, error rate, queue lag
```

---

## Bugfix PRs (Minimal Diffs Applied)

1) Dockerfile operational hardening for mirror instability
- `/opt/cloudon-bi/backend/Dockerfile`
- `/opt/cloudon-bi/worker/Dockerfile`

Change:
- Added apt retry/timeout options in update command.

No feature behavior changed.

---

## Next Required Action

Before re-running this milestone end-to-end:
- Stabilize outbound network access from staging host to Debian mirrors or set an internal apt mirror.
- Re-run bootstrap command and confirm services healthy:
```bash
cd /opt/cloudon-bi
docker compose up -d --build
docker compose ps
```

After staging is healthy, execute the commands in this report and update this file from `BLOCKED` to real pass/fail results.
