# KPI Dependency By Stream

Date: 2026-03-07

## 1) Sales Documents
Operational fact:
- `fact_sales` (logical `fact_sales_documents`)

Aggregates:
- `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`

KPI endpoints:
- `/v1/kpi/sales/*`

## 2) Purchase Documents
Operational fact:
- `fact_purchases` (logical `fact_purchase_documents`)

Aggregates:
- `agg_purchases_daily`, `agg_purchases_monthly`

KPI endpoints:
- `/v1/kpi/purchases/*`
- `/v1/kpi/suppliers` (purchase totals side)

## 3) Warehouse / Inventory Documents
Operational fact:
- `fact_inventory` (logical `fact_inventory_documents`)

Aggregates:
- `agg_inventory_snapshot_daily`

KPI endpoints:
- `/v1/kpi/inventory/*`

## 4) Cash Transactions
Operational fact:
- `fact_cashflows` (logical `fact_cash_transactions`)

Aggregates:
- `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`

KPI endpoints:
- `/v1/kpi/cashflow/*`

Mandatory subcategories:
- `customer_collections`, `customer_transfers`, `supplier_payments`, `supplier_transfers`, `financial_accounts`

## 5) Supplier Open Balances
Operational fact:
- `fact_supplier_balances`

Aggregates:
- `agg_supplier_balances_daily`

KPI endpoints:
- `/v1/kpi/suppliers` (open/overdue/aging exposure side)

## 6) Customer Balances / Receivables
Operational fact:
- `fact_customer_balances`

Aggregates:
- `agg_customer_balances_daily`

KPI endpoints:
- `/v1/kpi/receivables/summary`
- `/v1/kpi/receivables/aging`
- `/v1/kpi/receivables/top-customers`
- `/v1/kpi/receivables/collection-trend`
- `/v1/kpi/customers` and `/v1/kpi/customers/{customer_id}/detail` (enriched with receivables snapshot fields)

## Intelligence dependencies
- Sales: `SLS_*`, `PRF_*`, `MRG_*`, `BR_*`, `CAT_*`, `BRAND_DROP`, `TOP_DEPENDENCY`, `SLS_VOLATILITY`, `WEEKEND_SHIFT`
- Purchases: `PUR_SPIKE_PERIOD`, `PUR_DROP_PERIOD`, `SUP_DEPENDENCY`, `SUP_COST_UP`, `SUP_VOLATILITY`, `PUR_MARGIN_PRESSURE`, `SUP_OVERDUE_EXPOSURE`
- Inventory: `INV_DEAD_STOCK`, `INV_AGING_SPIKE`, `INV_LOW_COVERAGE`, `INV_OVERSTOCK_SLOW`
- Cash: `CASH_NET_NEG`, `CASH_COLL_DROP`, `CASH_OUT_IMBALANCE`
- Receivables: `CUSTOMER_OVERDUE_SPIKE`, `RECEIVABLES_GROWTH_ALERT`, `TOP_CUSTOMER_EXPOSURE`, `COLLECTION_DELAY_ALERT`
