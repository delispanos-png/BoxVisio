# Multi-Branch Architecture

Date: 2026-03-07

## Scope
Enable full support for tenants with single or multiple branches/stores, without changing the six-stream operational model.

## 1) Branch Dimension (`dim_branches`)
Canonical branch dimension is now aligned to operational requirements:

- `id` (UUID) as `branch_id`
- `external_id` (legacy-compatible branch key)
- `branch_code`
- `name` (legacy-compatible display field)
- `branch_name`
- `company_id`
- `location_metadata` (JSONB)
- `created_at`, `updated_at`

Notes:
- `branch_code` and `branch_name` are auto-backfilled from existing `external_id` / `name`.
- `company_id` remains optional for now (tenant can be single-company or multi-company).

## 2) Fact Table Branch Coverage (Schema Review)
All canonical fact streams include `branch_id`:

1. `fact_sales` -> `branch_id` present
2. `fact_purchases` -> `branch_id` present
3. `fact_inventory` -> `branch_id` present
4. `fact_cashflows` -> `branch_id` present
5. `fact_supplier_balances` -> `branch_id` present
6. `fact_customer_balances` -> `branch_id` present

Ingestion alignment:
- SQL/API/file ingestion now resolves and writes `branch_id` for sales and purchases (in addition to already-supported streams).
- XLSX sales import now writes both `branch_id` and `warehouse_id`.

## 3) Aggregate Layer: Company + Branch Levels
Added company-level and branch-level daily aggregates:

Sales:
- `agg_sales_daily_company`
- `agg_sales_daily_branch`

Purchases:
- `agg_purchases_daily_company`
- `agg_purchases_daily_branch`

These tables are refreshed from canonical facts and store branch-comparison-ready metrics:
- sales: `contribution_pct`, `margin_pct`
- purchases: `contribution_pct`

## 4) KPI Branch Filtering and Comparison
Branch filtering is supported across KPI endpoints via `branches` filter.

Branch comparison capabilities are now explicit in sales branch KPI responses:
- `contribution_pct` (branch contribution to total turnover)
- `performance_index_pct` (branch turnover vs average branch turnover)
- `margin_pct` (branch margin comparison where cost is available)

## 5) Migration and Rollout
Implemented migration:
- `backend/alembic/versions/20260307_0005_tenant_multibranch_alignment.py`
- `backend/alembic/versions/20260307_0006_tenant_branch_id_backfill.py`

Migration actions:
- Extends `dim_branches` with multi-branch fields.
- Creates new company/branch aggregate tables.
- Backfills `branch_id` on facts that carry `branch_ext_id`.
- Rebuilds company/branch aggregates from canonical facts.

## 6) Implementation Files
- `backend/app/models/tenant.py`
- `backend/alembic/versions/20260307_0005_tenant_multibranch_alignment.py`
- `backend/alembic/versions/20260307_0006_tenant_branch_id_backfill.py`
- `backend/app/services/ingestion/engine.py`
- `backend/app/services/kpi_queries.py`
- `scripts/import_sales_xlsx.py`
- `scripts/import_purchases_xlsx.py`
- `scripts/import_items_xlsx.py`
- `scripts/seed_demo_data.py`
- `backend/scripts/seed_demo_data.py`
- `scripts/run_tenant_migrations.py`
