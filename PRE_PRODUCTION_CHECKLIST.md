# Pre-Production Checklist (CloudOn BI)

Use this checklist before production cutover.

## 1. Security Audit

- [ ] Secrets are not hardcoded:
  - [ ] `grep -R "CHANGE_ME\|password=.*@\|secret_key=.*" -n backend worker scripts` reviewed.
  - [ ] `.env` exists only on server, not committed.
- [ ] TLS/HTTPS is enabled at reverse proxy.
- [ ] Security headers present:
  - [ ] `curl -I https://bi.boxvisio.com | grep -E "X-Frame-Options|X-Content-Type-Options|Content-Security-Policy|Strict-Transport-Security"`
- [ ] CSRF protection validated for UI `POST/PUT/PATCH/DELETE` forms.
- [ ] JWT lifecycle validated:
  - [ ] access token expiry enforced
  - [ ] refresh rotation works
  - [ ] logout revokes refresh token
- [ ] API key rotation validated:
  - [ ] `POST /v1/admin/tenants/{tenant_id}/api-keys/rotate`
  - [ ] old key rejected, new key accepted
- [ ] Role-based access validated:
  - [ ] `cloudon_admin` can access admin endpoints
  - [ ] `tenant_admin`/`tenant_user` blocked from `/v1/admin/*`
- [ ] Rate limiting returns `429` when exceeding configured limits.
- [ ] Audit logs created for admin write actions (`/v1/admin*`, `/admin*`).

Pass criteria:
- No critical/high security finding open.

## 2. Load Testing

Target:
- API p95 latency, worker throughput, Redis/Postgres stability under expected + 2x load.

Recommended scenarios:
- [ ] KPI read load (`/v1/kpi/sales/summary`, `/v1/kpi/sales/by-branch`) with filters.
- [ ] External ingest burst (`/v1/ingest/sales`, `/v1/ingest/purchases`) per tenant.
- [ ] Multi-tenant mixed load (at least 10 tenants concurrently).

Example (k6):
- [ ] Create/load script and run for 10-15 min steady state.
- [ ] Capture p50/p95/p99, error rate, 429 rate, queue lag.

Pass criteria:
- Error rate < 1%
- p95 within agreed SLO
- No sustained queue growth after test stop

## 3. DB Index Verification

Control DB:
- [ ] Verify critical indexes exist (`tenants.slug`, `users(tenant_id,email)`, `subscriptions.tenant_id`, `audit_logs.created_at`, `refresh_tokens.*`).

Tenant DB (sample tenants):
- [ ] Verify fact indexes:
  - `fact_sales(doc_date, branch_id)`, `fact_sales(updated_at)`, `fact_sales(external_id unique)`
  - `fact_purchases(doc_date, branch_id)`, `fact_purchases(updated_at)`, `fact_purchases(external_id unique)`
- [ ] Verify aggregate indexes:
  - `agg_sales_daily(doc_date)`, `agg_sales_monthly(month_start)`
  - `agg_purchases_daily(doc_date)`, `agg_purchases_monthly(month_start)`

SQL checks:
- [ ] `EXPLAIN ANALYZE` for key KPI queries confirms index usage and acceptable plan cost.

Pass criteria:
- No seq-scan hot path on large tables for core KPI endpoints.

## 4. Aggregation Verification

- [ ] Ingest sample sales/purchases dataset for at least 2 months.
- [ ] Trigger aggregate refresh job (`worker.tasks.refresh_aggregates_for_entity`) and verify completion.
- [ ] Compare aggregates vs facts for sampled ranges:
  - [ ] sales net/qty daily totals match
  - [ ] purchases net/qty/cost daily totals match
- [ ] KPI endpoints confirmed reading aggregate tables only for sales/purchases.

Pass criteria:
- Differences = 0 for sampled checks (or documented rounding tolerance only).

## 5. Backup Test Restore

Backup:
- [ ] Run nightly backup script manually:
  - `make backup-nightly`
- [ ] Verify output folder contains:
  - control dump
  - globals sql
  - tenant dumps
  - manifest

Restore test (staging):
- [ ] Restore control DB dump:
  - `make restore-db DB=bi_control FILE=<control_dump> DROP_CREATE=true`
- [ ] Restore one tenant DB dump:
  - `make restore-db DB=bi_tenant_<slug> FILE=<tenant_dump> DROP_CREATE=true`
- [ ] Start app and verify login + KPI read.

Pass criteria:
- Restored environment is functional and data is consistent.

## 6. Trial -> Active Transition Test

- [ ] Create tenant in `trial`.
- [ ] Verify `trial_ends_at` set correctly.
- [ ] Force transition to `active` (admin API/UI).
- [ ] Validate access and ingestion behavior remains enabled.
- [ ] Verify subscription/audit events created.

Pass criteria:
- State transition correct in DB + expected feature access preserved.

## 7. Suspend / Unsuspend Test

- [ ] Move tenant to `suspended`.
- [ ] Verify:
  - KPI endpoints blocked
  - ingestion endpoints blocked
  - UI access restricted as designed
- [ ] Move tenant back to `active`.
- [ ] Re-verify KPI + ingestion restored.
- [ ] Confirm audit/subscription events logged for both transitions.

Pass criteria:
- Enforcement immediate and reversible with no manual DB patching.

## Final Go/No-Go Gate

- [ ] All sections passed
- [ ] Open issues triaged and accepted by owner
- [ ] Rollback plan documented
- [ ] On-call + runbook ready

Decision:
- [ ] GO
- [ ] NO-GO
