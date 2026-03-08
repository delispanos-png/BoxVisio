# Changelog

## 2026-03-08 - Stabilization and Supplier Targets Refinement

### Stabilization / QA
- Performed route-level sanity audit for admin and tenant portals against nginx host routing:
  - validated sidebar-linked routes return expected auth redirect (`302 -> /login`) instead of `404/500`.
- Re-validated consolidated KPI summary endpoint paths:
  - `/v1/dashboard/executive-summary`
  - `/v1/dashboard/finance-summary`
  - `/v1/streams/*/summary`
- Project-wide Python compile pass executed for `backend/app`, Alembic revisions, worker, and scripts.
- Applied pending control and tenant migrations required by new user/profile and supplier target schema:
  - control upgraded to `20260308_0007_control`
  - tenants (`uat-a`, `uat-b`) upgraded to `20260308_0009_tenant`
- Verified authenticated smoke tests after migration:
  - tenant pages: `/tenant/dashboard`, `/tenant/finance-dashboard`, `/tenant/suppliers`, `/tenant/customers`, cash categories, supplier targets -> `200`
  - admin pages: business rules + data sources pages -> `200`
- Performance benchmark (container run):
  - consolidated executive summary: ~`47ms` miss / ~`12ms` cached hit
  - consolidated finance summary: ~`25ms`
  - inventory stream summary: ~`312ms`

### Supplier Targets Module
- Fixed supplier item loading logic in `supplier_target_filter_options`:
  - switched to resilient supplier-product sourcing from purchases with outer joins.
  - added history fallback from previous supplier agreements.
  - normalized supplier matching logic.
- Enriched item payload for agreement setup with:
  - `product_id`, `product_name`, `category`, `brand`
  - `sales_last_30_days`, `purchases_last_30_days`
- Added richer UX in supplier target editor:
  - loading state
  - explicit empty states
  - searchable/paginated table
  - quick filters (`all`, `top selling`, `slow movers`)
  - checkbox multi-selection helpers (select/clear visible page)

### Agreement Notes
- Added new supplier agreement field:
  - `agreement_notes` (multiline, optional)
- Wired through:
  - tenant schema model
  - create/update API payloads
  - service layer persistence
  - create/edit/details UI
- Added tenant migration:
  - `20260308_0009_tenant_supplier_target_agreement_notes.py`
  - backfills `agreement_notes` from legacy `notes` when present.

### Business Rules UI
- Confirmed new form-based Document Type Rules pages are in place and render:
  - main editor
  - setup wizard
  - help/documentation page
- Added operator-facing documentation:
  - `BUSINESS_RULES_DOCUMENT_EDITOR.md`
  - `BUG_DIAGNOSIS_SUPPLIER_TARGET_ITEMS.md`

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
