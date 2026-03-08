# Architecture Review

Date: 2026-03-07  
Scope: BoxVisio BI schema, ingestion, KPI and intelligence dependencies.

## Executive summary
The platform canonical BI foundation is now aligned to six operational streams and uses legacy SoftOne SQL only as extraction logic.

Implemented first-class streams:
1. `fact_sales` (Sales Documents)
2. `fact_purchases` (Purchase Documents)
3. `fact_inventory` (Warehouse / Inventory Documents)
4. `fact_cashflows` (Cash Transactions)
5. `fact_supplier_balances` (Supplier Open Balances)
6. `fact_customer_balances` (Customer Balances / Receivables)

## Extraction and ingestion alignment
Query-pack extraction sources:
- `backend/querypacks/pharmacyone/facts/sales_facts.sql`
- `backend/querypacks/pharmacyone/facts/purchases_facts.sql`
- `backend/querypacks/pharmacyone/facts/inventory_facts.sql`
- `backend/querypacks/pharmacyone/facts/cashflow_facts.sql`
- `backend/querypacks/pharmacyone/facts/supplier_balances_facts.sql`
- `backend/querypacks/pharmacyone/facts/customer_balances_facts.sql`

Ingestion entities supported:
- `sales`, `purchases`, `inventory`, `cashflows`, `supplier_balances`, `customer_balances`

Incremental strategy:
- `updated_at` + `external_id` (stream-consistent)

## Canonical fact/aggregate inventory
Facts:
- `fact_sales`, `fact_purchases`, `fact_inventory`, `fact_cashflows`, `fact_supplier_balances`, `fact_customer_balances`

Aggregates:
- Sales: `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`, `agg_sales_daily_company`, `agg_sales_daily_branch`
- Purchases: `agg_purchases_daily`, `agg_purchases_monthly`, `agg_purchases_daily_company`, `agg_purchases_daily_branch`
- Inventory: `agg_inventory_snapshot_daily`
- Cash: `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`
- Supplier balances: `agg_supplier_balances_daily`
- Customer balances: `agg_customer_balances_daily`

Multi-branch foundation:
- `dim_branches` extended with `branch_code`, `branch_name`, `company_id`, `location_metadata`.
- All six canonical fact streams persist `branch_id` and support branch-level filtering.
- Sales branch KPI responses now include `contribution_pct`, `margin_pct`, and `performance_index_pct`.

## KPI dependency alignment
- Sales KPIs -> sales stream
- Purchases KPIs -> purchases stream
- Inventory KPIs -> inventory stream
- Cash KPIs -> cash stream (5 mandatory subcategories)
- Supplier exposure KPIs -> supplier balances stream
- Receivables KPIs -> customer balances stream

Receivables KPI coverage:
- total receivables
- overdue receivables
- receivables aging
- top customers by outstanding balance
- collection trend
- outstanding growth vs previous period

## Intelligence alignment
Categories now mapped to streams:
- `sales`, `purchases`, `inventory`, `cashflow`, `receivables`

Receivables rules:
- `CUSTOMER_OVERDUE_SPIKE`
- `RECEIVABLES_GROWTH_ALERT`
- `TOP_CUSTOMER_EXPOSURE`
- `COLLECTION_DELAY_ALERT`

## Governance conclusion
- Runtime dashboards/KPIs/insights are stream-driven from PostgreSQL facts/aggregates.
- Legacy ERP SQL remains extraction-only and is not used directly for runtime dashboard queries.
