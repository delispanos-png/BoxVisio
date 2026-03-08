# Aggregation Model (Module-Aligned)

Date: 2026-03-07

## 1) Sales Documents
Aggregates:
- `agg_sales_daily`
- `agg_sales_monthly`
- `agg_sales_item_daily`

## 2) Purchase Documents
Aggregates:
- `agg_purchases_daily`
- `agg_purchases_monthly`

## 3) Inventory Documents
Aggregates:
- `agg_inventory_snapshot_daily`

Planned deep-ops additions:
- `agg_inventory_daily`
- `agg_stock_aging`

## 4) Cash Transactions
Aggregates:
- `agg_cash_daily`
- `agg_cash_by_type`
- `agg_cash_accounts`

## 5) Supplier Open Balances
Aggregates:
- `agg_supplier_balances_daily`

## 6) Customer Balances / Receivables
Aggregates:
- `agg_customer_balances_daily`

## Refresh orchestration
Implemented in `worker/tasks.py`:
- `_refresh_sales_aggregates`
- `_refresh_purchases_aggregates`
- `_refresh_inventory_aggregates`
- `_refresh_cash_aggregates`
- `_refresh_supplier_balances_aggregates`
- `_refresh_customer_balances_aggregates`

Rule:
- Every aggregate refresh reads only canonical Postgres facts from its stream.
- No dashboard aggregate is built directly from legacy ERP SQL.
