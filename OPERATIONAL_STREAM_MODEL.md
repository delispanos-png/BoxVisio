# Operational Stream Model

Date: 2026-03-07  
Goal: Finalize BoxVisio BI on six canonical operational streams. SoftOne SQL is extraction-only and feeds PostgreSQL facts.

## Core streams (authoritative)
1. Sales Documents (`fact_sales_documents` -> physical `fact_sales`)
2. Purchase Documents (`fact_purchase_documents` -> physical `fact_purchases`)
3. Warehouse / Inventory Documents (`fact_inventory_documents` -> physical `fact_inventory`)
4. Cash Transactions (`fact_cash_transactions` -> physical `fact_cashflows`)
5. Supplier Open Balances (`fact_supplier_balances`)
6. Customer Balances / Receivables (`fact_customer_balances`)

## 1) Sales Documents
Source query pack:
- `backend/querypacks/pharmacyone/facts/sales_facts.sql`

Canonical columns:
- `external_id`, `doc_date`, `document_id`, `document_no`, `document_series`, `document_type`, `document_status`
- `branch_ext_id`, `warehouse_ext_id`, `item_code`
- `qty`, `net_value`, `gross_value`, `cost_amount`, `vat_amount`
- `customer_code`, `customer_name`, `updated_at`

Dimensions:
- `dim_branches`, `dim_warehouses`, `dim_items`, `dim_brands`, `dim_categories`, `dim_groups`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`

KPIs / Insights:
- Sales KPIs and `SLS_* / PRF_* / MRG_* / BR_* / CAT_* / BRAND_* / TOP_* / WEEKEND_SHIFT`

## 2) Purchase Documents
Source query pack:
- `backend/querypacks/pharmacyone/facts/purchases_facts.sql`

Canonical columns:
- `external_id`, `event_id`, `doc_date`, `branch_ext_id`, `warehouse_ext_id`, `supplier_ext_id`
- `item_code`, `qty`, `net_value`, `cost_amount`, `brand_ext_id`, `category_ext_id`, `group_ext_id`, `updated_at`

Dimensions:
- `dim_suppliers`, `dim_items`, `dim_branches`, `dim_warehouses`, `dim_brands`, `dim_categories`, `dim_groups`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_purchases_daily`, `agg_purchases_monthly`

KPIs / Insights:
- Purchases KPIs and `PUR_* / SUP_*`

## 3) Inventory / Warehouse Documents
Source query pack:
- `backend/querypacks/pharmacyone/facts/inventory_facts.sql`

Canonical columns:
- `external_id`, `doc_date`, `branch_id`, `warehouse_id`, `item_id`
- `qty_on_hand`, `qty_reserved`, `cost_amount`, `value_amount`, `updated_at`

Dimensions:
- `dim_items`, `dim_branches`, `dim_warehouses`, `dim_brands`, `dim_categories`, `dim_groups`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_inventory_snapshot_daily`
- Planned/required for deeper intelligence: `agg_inventory_daily`, `agg_stock_aging`

KPIs / Insights:
- Inventory KPIs and `INV_*`

## 4) Cash Transactions
Source query pack:
- `backend/querypacks/pharmacyone/facts/cashflow_facts.sql`

Mandatory subcategories:
1. `customer_collections`
2. `customer_transfers`
3. `supplier_payments`
4. `supplier_transfers`
5. `financial_accounts`

Mandatory fields:
- `transaction_id`, `transaction_date`, `transaction_type`, `subcategory`
- `branch_id`, `account_id`
- `counterparty_type`, `counterparty_id`
- `amount`, `external_id`, `updated_at`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`

KPIs / Insights:
- Cash KPIs and `CASH_*`

## 5) Supplier Open Balances
Source query pack:
- `backend/querypacks/pharmacyone/facts/supplier_balances_facts.sql`

Canonical columns:
- `supplier_id`, `balance_date`, `open_balance`, `overdue_balance`
- `aging_bucket_0_30`, `aging_bucket_31_60`, `aging_bucket_61_90`, `aging_bucket_90_plus`
- `last_payment_date`, `trend_vs_previous`, `external_id`, `updated_at`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_supplier_balances_daily`

KPIs / Insights:
- Supplier exposure KPIs and `SUP_OVERDUE_EXPOSURE`

## 6) Customer Balances / Receivables
Source query pack:
- `backend/querypacks/pharmacyone/facts/customer_balances_facts.sql`

Canonical columns:
- `customer_id`, `balance_date`, `open_balance`, `overdue_balance`
- `aging_bucket_0_30`, `aging_bucket_31_60`, `aging_bucket_61_90`, `aging_bucket_90_plus`
- `last_collection_date`, `trend_vs_previous`, `external_id`, `updated_at`

Incremental:
- `updated_at` + `external_id`

Aggregates:
- `agg_customer_balances_daily`

KPIs:
- Total receivables
- Overdue receivables
- Receivables aging
- Top customers by outstanding balance
- Collection trend
- Outstanding growth vs previous period

Insights:
- `CUSTOMER_OVERDUE_SPIKE`
- `RECEIVABLES_GROWTH_ALERT`
- `TOP_CUSTOMER_EXPOSURE`
- `COLLECTION_DELAY_ALERT`

## Governance rules
- Legacy SoftOne SQL is extraction logic only.
- Dashboard/KPI/insights must read Postgres facts/aggregates only.
- Cross-stream joins are allowed only when explicit (for example supplier and customer operational views combining documents + balances + cash).
