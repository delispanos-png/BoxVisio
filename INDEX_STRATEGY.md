# INDEX_STRATEGY

Date: 2026-03-07

## Scope
This strategy is implemented via:
- `backend/alembic/versions/20260307_0008_tenant_index_strategy.py`

Validated on:
- `bi_tenant_uat-a`
- `bi_tenant_uat-b`

## 1) Fact Table Mandatory Index Coverage

All fact tables now have coverage for:
- date column (`doc_date` / `transaction_date` / `balance_date`)
- `branch_id`
- `source_connector_id`
- `updated_at`
- unique `external_id` (where applicable)

### Sales (`fact_sales`)
- `ix_fact_sales_doc_date_branch_id (doc_date, branch_id)`
- `ix_fact_sales_doc_date_item_id (doc_date, item_id)`
- `ix_fact_sales_doc_date_customer_id (doc_date, customer_id)`
- `ix_fact_sales_branch_item_doc_date (branch_id, item_id, doc_date)`
- `ix_fact_sales_source_connector_id (source_connector_id)`

### Purchases (`fact_purchases`)
- `ix_fact_purchases_doc_date_branch_id (doc_date, branch_id)`
- `ix_fact_purchases_doc_date_supplier_id (doc_date, supplier_id)`
- `ix_fact_purchases_branch_supplier_doc_date (branch_id, supplier_id, doc_date)`
- `ix_fact_purchases_source_connector_id (source_connector_id)`

### Inventory (`fact_inventory`)
- `ix_fact_inventory_doc_date_branch_id (doc_date, branch_id)`
- `ix_fact_inventory_item_id_doc_date (item_id, doc_date)`
- `ix_fact_inventory_warehouse_id_doc_date (warehouse_id, doc_date)`
- `ix_fact_inventory_source_connector_id (source_connector_id)`

### Cash (`fact_cashflows`)
- `ix_fact_cashflows_transaction_date_branch_id (transaction_date, branch_id)`
- `ix_fact_cashflows_transaction_date_account_id (transaction_date, account_id)`
- `ix_fact_cashflows_subcategory_transaction_date (subcategory, transaction_date)`
- `ix_fact_cashflows_source_connector_id (source_connector_id)`

### Supplier Balances (`fact_supplier_balances`)
- `ix_fact_supplier_balances_balance_date_supplier_id (balance_date, supplier_id)`
- `ix_fact_supplier_balances_balance_date_branch_id (balance_date, branch_id)`
- `ix_fact_supplier_balances_source_connector_id (source_connector_id)`

### Customer Balances (`fact_customer_balances`)
- `ix_fact_customer_balances_balance_date_customer_id (balance_date, customer_id)`
- `ix_fact_customer_balances_balance_date_branch_id (balance_date, branch_id)`
- `ix_fact_customer_balances_source_connector_id (source_connector_id)`

## 2) Aggregate Read Optimization

Added/validated indexes for dashboard-heavy filters:
- Sales:
  - `ix_agg_sales_daily_doc_date_branch_brand`
  - `ix_agg_sales_monthly_month_start_branch_brand`
- Purchases:
  - `ix_agg_purchases_daily_doc_date_branch_brand_supplier`
  - `ix_agg_purchases_monthly_month_start_branch_brand_supplier`
- Cash:
  - `ix_agg_cash_daily_doc_date_branch_account`
  - `ix_agg_cash_by_type_subcategory_doc_date`
  - `ix_agg_cash_accounts_account_id_doc_date`
- Balances:
  - `ix_agg_supplier_balances_daily_date_branch`
  - `ix_agg_customer_balances_daily_date_branch`

## 3) Dimension Indexing

External IDs remain unique-indexed, plus search-friendly name/code indexes:
- `dim_branches.name`
- `dim_warehouses.name`
- `dim_brands.name`
- `dim_categories.name`
- `dim_items.sku`, `dim_items.name`
- `dim_customers.name`
- `dim_suppliers.name`
- `dim_accounts.name`
- `dim_document_types.name`
- `dim_payment_methods.name`

## 4) Sync/Support Indexing

- `sync_state (stream_code, source_connector_id)` -> `ix_sync_state_stream_source_connector`
- `ingest_batches (started_at, status)` -> `ix_ingest_batches_started_status`
- `ingest_dead_letter (status, created_at)` -> `ix_ingest_dead_letter_status_created_at`

Also added schema support fields:
- `sync_state.stream_code`
- `sync_state.source_connector_id`
- `ingest_dead_letter.status`

## 5) Redundant Index Cleanup

Removed redundant non-unique indexes (already covered by unique constraints):
- `ix_fact_supplier_balances_external_id`
- `ix_fact_customer_balances_external_id`

## 6) Dashboard/Insight Guardrails

- Dashboard KPIs/charts query `agg_*` tables only.
- Fact tables are for ingestion, drilldown, reconciliation, and rule traceability.
- Index choices prioritize:
  - date-range filters
  - branch-level slicing
  - supplier/customer/account filters
  - connector-scoped incremental processing
