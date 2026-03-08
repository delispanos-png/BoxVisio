# INDEX_PERFORMANCE_NOTES

Date: 2026-03-07

## Test Context
- Tenant: `bi_tenant_uat-a`
- Row volumes:
  - `fact_sales`: 1,484,434
  - `fact_purchases`: 38,212
  - `fact_inventory`: 185,300
  - `fact_cashflows`: 0
  - `fact_supplier_balances`: 0
  - `fact_customer_balances`: 0

## Before/After Query Plan Notes

### 1) Purchases: date + supplier
Query:
`SUM(net_value) WHERE doc_date between ... AND supplier_id = ...`

- Before:
  - Used `ix_fact_purchases_doc_date`
  - Extra filter on `supplier_id`
  - Execution: `0.904 ms`
- After:
  - Uses `ix_fact_purchases_doc_date_supplier_id`
  - Execution: `0.510 ms`

### 2) Purchases: branch + supplier + date
Query:
`SUM(net_value) WHERE branch_id = ... AND supplier_id = ... AND doc_date between ...`

- Before:
  - Used `ix_fact_purchases_doc_date` with post-filter
  - Execution: `0.536 ms`
- After:
  - Uses `ix_fact_purchases_branch_supplier_doc_date`
  - Execution: `0.200 ms`

### 3) Inventory: product + date
Query:
`SUM(value_amount) WHERE item_id = ... AND doc_date between ...`

- Before:
  - Parallel Seq Scan on `fact_inventory`
  - Execution: `13.105 ms`
- After:
  - Uses `ix_fact_inventory_item_id_doc_date`
  - Execution: `0.054 ms`

### 4) Inventory: warehouse + broad date range
Query:
`SUM(value_amount) WHERE warehouse_id = ... AND doc_date between ...`

- Before:
  - Parallel Seq Scan
  - Execution: `25.729 ms`
- After:
  - Still Parallel Seq Scan (`27.367 ms`)
  - Reason: very low selectivity (planner estimates scan is cheaper because most rows match).

## Coverage Validation
- Required index coverage check result:
  - `missing_required_indexes = 0`

## Redundant/Excessive Index Review

Removed (redundant):
- `ix_fact_supplier_balances_external_id` (covered by unique)
- `ix_fact_customer_balances_external_id` (covered by unique)

Watchlist (possible overlap, keep for now):
- `fact_sales`: `ix_fact_sales_doc_date` vs `ix_fact_sales_doc_date_branch_id`
- `fact_purchases`: `ix_fact_purchases_doc_date` vs multi-column date indexes
- `fact_cashflows`: multiple transaction-date composites may increase write cost as data volume grows

Recommendation:
- Re-run `pg_stat_user_indexes` after 2-4 weeks production load and drop unused overlaps.
