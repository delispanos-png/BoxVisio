# AGGREGATE_READ_POLICY.md

## Policy
Dashboard and stream summary endpoints must read from aggregate tables only.
Raw fact-table reads are allowed only for document drill-down/detail/list pages.

## Aggregate-backed Endpoints (Implemented)

### Dashboard
- `GET /v1/dashboard/executive-summary`
- `GET /api/dashboard/executive-summary`
  - Sources: `agg_sales_daily`, `agg_sales_daily_branch`, `agg_sales_monthly`, `agg_purchases_daily`
- `GET /v1/dashboard/finance-summary`
- `GET /api/dashboard/finance-summary`
  - Sources: `agg_customer_balances_daily`, `agg_supplier_balances_daily`, `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`, `agg_purchases_daily`

### Operational Streams
- `GET /v1/streams/sales/summary`
- `GET /api/streams/sales/summary`
  - Sources: `agg_sales_daily`, `agg_sales_daily_branch`, `agg_sales_monthly`
- `GET /v1/streams/purchases/summary`
- `GET /api/streams/purchases/summary`
  - Sources: `agg_purchases_daily`, `agg_purchases_monthly`
- `GET /v1/streams/inventory/summary`
- `GET /api/streams/inventory/summary`
  - Sources: `agg_stock_aging` (plus dimensions for labels)
- `GET /v1/streams/cash/summary`
- `GET /api/streams/cash/summary`
  - Sources: `agg_cash_daily`, `agg_cash_by_type`
- `GET /v1/streams/balances/summary`
- `GET /api/streams/balances/summary`
  - Sources: `agg_customer_balances_daily`, `agg_supplier_balances_daily`, `agg_purchases_daily`

## Legacy Endpoints Status
- Existing `v1/kpi/*` endpoints remain for backward compatibility.
- KPI card loading in updated UI now uses consolidated aggregate endpoints above.
- Fact-based endpoints are retained for:
  - document tables
  - document detail pages
  - transaction detail pages

## Refactored Endpoints List
- Added:
  - `/v1/dashboard/executive-summary`
  - `/v1/dashboard/finance-summary`
  - `/v1/streams/sales/summary`
  - `/v1/streams/purchases/summary`
  - `/v1/streams/inventory/summary`
  - `/v1/streams/cash/summary`
  - `/v1/streams/balances/summary`
  - `/api/dashboard/executive-summary`
  - `/api/dashboard/finance-summary`
  - `/api/streams/sales/summary`
  - `/api/streams/purchases/summary`
  - `/api/streams/inventory/summary`
  - `/api/streams/cash/summary`
  - `/api/streams/balances/summary`
