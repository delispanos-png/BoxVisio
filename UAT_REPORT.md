# UAT Report - Milestone 4

Date: 2026-02-28
Environment: STAGING-like local run (`api`+`worker` in venv, `postgres`+`redis` in Docker)

## UAT Plan

### Tenant A (PharmacyOne SQL source)
- Provision tenant with source=`pharmacyone`, plan=`pro`, active subscription.
- Configure SQL mapping and trigger sync queue.
- Validate failure/retry/DLQ behavior for SQL connector.
- Validate branch coverage (3 branches), correction upsert behavior, KPI and exports using real-like ingested data.

### Tenant B (External API source)
- Provision tenant with source=`external`, plan=`pro`, active subscription.
- Send Sales & Purchases payloads.
- Include duplicates and late-arriving data.
- Validate idempotency, KPI correctness, filters, comparison A/B and exports.

## Execution Summary

### Provisioning
- Tenant A (`uat-a`) provisioned successfully with 7/7 wizard steps.
- Tenant B (`uat-b`) provisioned successfully with 7/7 wizard steps.

Evidence snippet:
- `/tmp/uat_tenant_a.json` => `"status": "ok"`, `"steps"` includes step 1..7 all `ok`
- `/tmp/uat_tenant_b.json` => `"status": "ok"`, `"steps"` includes step 1..7 all `ok`

### Ingestion Scenarios
- Tenant A:
  - Sales batch queued (3 records), correction batch queued (1 record), purchases queued (1 record).
  - Correction validated by upsert: `A-INV-1002` final values became `net=90`, `gross=110`, `cost=62`.
- Tenant B:
  - Sales queued (3) + duplicate queued (1).
  - Purchases queued (2) + duplicate queued (1).
  - Late-arriving records included in January range and reflected in compare B window.

Evidence snippet (`/tmp/uat_exec_results.json`):
- `ingest_B_sales` => `200 queued`
- `ingest_B_sales_dup` => `200 queued`
- `ingest_B_purchases` => `200 queued`
- `ingest_B_purchases_dup` => `200 queued`

Idempotency evidence (DB):
- `bi_tenant_uat-b.fact_sales`: `COUNT(*)=3`, `COUNT(DISTINCT external_id)=3`
- `bi_tenant_uat-b.fact_purchases`: `COUNT(*)=2`, `COUNT(DISTINCT external_id)=2`

### KPI Correctness (vs raw facts)
Raw SQL totals:
- Tenant A sales (2026-01-01..2026-02-28): `qty=11`, `net=240`, `gross=295`
- Tenant B sales: `qty=9`, `net=115`, `gross=145`
- Tenant B purchases: `qty=9`, `net=88`, `cost=74`

KPI endpoint totals matched:
- `kpi_A_sales_summary` => `qty 11`, `net 240`, `gross 295`
- `kpi_B_sales_summary` => `qty 9`, `net 115`, `gross 145`
- `kpi_B_purchases_summary` => `qty 9`, `net 88`, `cost 74`

### Filters Correctness
Validated examples (Tenant B):
- `branches=B2` => `qty=5`, `net=60`, `gross=75`
- `categories=BC1` => `qty=4`, `net=55`, `gross=70`
- `warehouses=BW2` in compare => A window zero, B window contains only late record (`qty=1`, `net=15`, `gross=20`)

### Comparison A/B
Validated with `A_from=2026-02-20 A_to=2026-02-21 B_from=2026-01-01 B_to=2026-01-31`:
- Tenant B compare:
  - A: `qty=8`, `net=100`, `gross=125`
  - B: `qty=1`, `net=15`, `gross=20`

### Exports
- Sales CSV export: HTTP 200, non-empty (`len=68`).
- Purchases CSV export: HTTP 200, non-empty (`len=65`).

### UI Experience & Speed
Measured page response timings:
- `/tenant/dashboard` => 200, ~14.19 ms
- `/tenant/compare` => 200, ~12.22 ms
- `/admin/tenants` => 200, ~16.3 ms

### Subscription Enforcement & Propagation
- Suspend tenant B => KPI blocked with 403 (`Subscription blocked`).
- Unsuspend tenant B => KPI restored with 200.
- Timed propagation check:
  - seconds to block: ~0.038s
  - seconds to restore: ~0.049s
- Requirement "within 1 minute" => PASS.

### SQL Connector Failure Path (Tenant A)
- Triggered `/v1/admin/tenants/{id}/sync` with intentionally invalid SQL Server mapping.
- DLQ depth for tenant A increased (`cloudon_ingest_dlq_depth{tenant="uat-a"} 4.0`).
- Queue drained (`cloudon_ingest_queue_depth{tenant="uat-a"} 0.0`).

## Pass/Fail Matrix

- Tenant A provisioning wizard: PASS
- Tenant B provisioning wizard: PASS
- Tenant B external ingest (duplicates + late): PASS
- KPI correctness vs raw facts: PASS
- Filters correctness: PASS
- Comparison A/B correctness: PASS
- CSV exports: PASS
- Subscription enforcement + suspend/unsuspend propagation < 1 min: PASS
- Tenant A real SQL Server successful incremental sync from live source: PARTIAL (failure path PASS, live source success NOT validated here)

## Issues Found and Fixes Applied

1. Alembic env runtime bug (`run_migrations(version_table_schema=...)`) causing migration failure.
- Fix: removed unsupported arg from `context.run_migrations()` calls.
- File: `backend/alembic/env.py`

2. Control DB enum migration collisions (`planname`) across revisions.
- Fixes:
  - use `postgresql.ENUM(..., create_type=False)` and explicit `create(..., checkfirst=True)` where needed.
- Files:
  - `backend/alembic/versions/20260228_0001_control_init.py`
  - `backend/alembic/versions/20260228_0005_plan_features.py`
  - `backend/alembic/versions/20260228_0007_subscription_lifecycle.py`

3. Provisioning wizard role creation failed (`CREATE ROLE ... PASSWORD $1` syntax error).
- Fix: use SQL literal composition for password in DDL.
- File: `backend/app/services/provisioning_wizard.py`

4. Provisioning wizard tenant migrations failed due ambiguous Alembic heads.
- Fix: explicit tenant migration head (`20260228_0009_tenant`) in wizard and script.
- Files:
  - `backend/app/services/provisioning_wizard.py`
  - `scripts/run_tenant_migrations.py`

5. Provisioning wizard couldn't find `alembic` binary in local run mode.
- Fix: invoke migrations via `sys.executable -m alembic`.
- File: `backend/app/services/provisioning_wizard.py`

6. Seed/login issues in UAT bootstrap:
- `passlib+bcrypt` incompatibility with bcrypt 5.x.
  - Fix: pin `bcrypt==4.0.1`.
- Missing `email-validator` dependency for `EmailStr` models.
  - Fix: add `email-validator==2.3.0`.
- Files:
  - `backend/requirements.txt`

## Repro Commands (high level)

1. Start data services:
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml up -d postgres redis
```

2. Run control migrations + seed admin (with localhost overrides for local-run mode).

3. Start API and worker (local venv, localhost DB/Redis overrides).

4. Provision tenants:
- `POST /v1/admin/tenants/wizard` for `uat-a`, `uat-b`

5. Send ingest payloads:
- `POST /v1/ingest/sales`
- `POST /v1/ingest/purchases`
with `X-API-Key`, `X-Tenant`, `X-Signature(HMAC-SHA256)`.

6. Validate KPIs, filters, compare and exports:
- `/v1/kpi/sales/summary`
- `/v1/kpi/purchases/summary`
- `/v1/kpi/sales/compare`
- `/v1/kpi/*/export.csv`

7. Validate subscription enforcement:
- `PATCH /v1/admin/tenants/{id}/subscription` -> `suspended` / `active`
- confirm KPI 403/200 transitions.

## Known Limitations
See: `KNOWN_LIMITATIONS.md`
