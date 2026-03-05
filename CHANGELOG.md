# Changelog

## 2026-02-28 - Milestone 6 Stabilization (Post Go-Live)

### Bugfixes
- Fixed KPI summary payloads to return real aggregate row counts instead of hardcoded `records: 0`.
  - `sales_summary` now includes `COUNT(agg_sales_daily.id)`.
  - `purchases_summary` now includes `COUNT(agg_purchases_daily.id)`.
- Updated tenant migration runner script to use `python -m alembic` for more reliable execution in containerized environments.

### Performance Tuning
- Added tenant migration `20260228_0011_tenant` with composite indexes for aggregate filtering patterns:
  - `agg_sales_daily(doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id)`
  - `agg_purchases_daily(doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id)`
  - `agg_sales_monthly(month_start, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id)`
  - `agg_purchases_monthly(month_start, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id)`
- Updated tenant migration head references to `20260228_0011_tenant` in:
  - provisioning wizard runtime migration execution
  - `scripts/run_tenant_migrations.py`

### Operational Improvements
- Added configurable ingest retry and queue tuning knobs:
  - `INGEST_RETRY_BACKOFF_SECONDS`
  - `INGEST_DRAIN_MAX_JOBS`
  - `SQLSERVER_RETRY_SLEEP_SECONDS`
- Replaced SQL Server connector hardcoded retry/sleep values with config-driven values.
- Improved worker retry behavior:
  - delayed re-drain scheduling with backoff (`countdown`) after retryable failures
  - avoids immediate tight retry loops and queue churn under failure conditions

### UX Polish (Onboarding)
- Improved admin tenant provisioning UX (`/admin/tenants`):
  - shows success details immediately after create (tenant/plan/subscription)
  - shows generated invite token + API key credentials once
  - displays step-by-step provisioning status table for quick troubleshooting
  - shows rollback visibility when provisioning fails

### Environment Defaults
- Updated default cloudon admin email:
  - `DEFAULT_ADMIN_EMAIL=admin@boxvisio.com`

### Runbook Updates
- Updated runbooks with retry/queue tuning actions:
  - `infra/runbooks/incident_response.md`
  - `infra/runbooks/how_to_drain_queues.md`
  - `infra/runbooks/how_to_replay_dlq.md`
  - `infra/runbooks/how_to_restore_tenant.md`
