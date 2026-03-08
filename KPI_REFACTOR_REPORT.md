# KPI Refactor Report

Date: 2026-03-07

## Objective
Ensure all KPIs are derived from the four operational modules:
- Sales Documents
- Purchase Documents
- Inventory Items / Warehouse Documents
- Cash Transactions

## Current Status
Most KPI services already source from module-aligned facts and aggregates:
- Sales KPIs use `agg_sales_daily` / `fact_sales`
- Purchases KPIs use `agg_purchases_daily` / `fact_purchases`
- Inventory KPIs use `agg_inventory_snapshot_daily` / `fact_inventory`
- Cashflow KPIs use `fact_cashflows` (no aggregates yet)

## Required Changes
1) **Cashflow KPIs**
   - Add aggregate tables and refactor queries to use `agg_cash_*` for speed and consistency.
   - Normalize `entry_type` to the 5 required categories.

2) **Inventory KPIs**
   - Add `agg_inventory_daily` and `agg_stock_aging` to reduce heavy joins and repeated logic.
   - Standardize movement types to support warehouse-document KPIs.

3) **Cross-module KPIs**
   - Inventory item detail and price-control KPIs currently combine sales + inventory (acceptable but should be explicit in docs and UI).

## Changed Services / Endpoints
No code changes applied yet in this pass. Planned refactors:
- `backend/app/services/kpi_queries.py`:
  - `cashflow_summary`, `cashflow_by_type`, `cashflow_trend_monthly` -> use `agg_cash_*`.
  - `inventory_stock_aging` -> use `agg_stock_aging` when available.

- `worker/tasks.py`:
  - Add `_refresh_cash_aggregates`.
  - Extend `_refresh_inventory_aggregates` to produce new inventory aggregates.

## Notes
Once aggregates are added, dashboards will remain stable but gain consistency and performance.


## UI / Navigation Alignment (Current)
Route structure already maps to the four modules, but KPI cards and dashboards should be standardized per module:
- Sales Documents: `/tenant/sales`, `/tenant/sales-documents`
- Purchase Documents: `/tenant/purchases`, `/tenant/purchase-documents`
- Inventory Items / Warehouse Docs: `/tenant/inventory`, `/tenant/warehouse-documents`, `/tenant/items`
- Cash Transactions: `/tenant/cashflow`, `/tenant/cashflow/*`, `/tenant/cashflow-documents`

Required UX consistency updates:
- Each module page should have: summary KPI cards, trend chart, breakdown chart(s), data table, insights panel.
- Home dashboard should show module highlights (sales, purchases, inventory, cash) instead of mixed KPIs.

