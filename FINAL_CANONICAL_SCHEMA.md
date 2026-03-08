# FINAL_CANONICAL_SCHEMA

Date: 2026-03-07

## 1) Architecture Layers

1. Control DB
2. Tenant DB
3. Staging Layer
4. Canonical Fact Layer
5. Aggregate Layer
6. Intelligence Layer

The tenant model is ERP-agnostic and aligned to six operational streams:
- Sales Documents
- Purchase Documents
- Inventory Documents
- Cash Transactions
- Supplier Balances
- Customer Balances

## 2) Control DB
Control DB stores orchestration/configuration metadata only:
- tenants, users, subscriptions
- connectors and connection templates
- stream/query mappings
- global business rules and tenant overrides

No dashboard KPI data is read from Control DB.

## 3) Tenant DB Canonical Groups

### A) Dimensions
- `dim_branches` (includes `branch_code`, `branch_name`, `company_id`, `location_metadata`)
- `dim_products` (canonical compatibility view over `dim_items`)
- `dim_categories`
- `dim_brands`
- `dim_customers`
- `dim_suppliers`
- `dim_accounts`
- `dim_document_types`
- `dim_payment_methods`
- `dim_warehouses`
- `dim_calendar`

### B) Staging
- `stg_sales_documents`
- `stg_purchase_documents`
- `stg_inventory_documents`
- `stg_cash_transactions`
- `stg_supplier_balances`
- `stg_customer_balances`

### C) Facts
- `fact_sales_documents` (compatibility view over `fact_sales`)
- `fact_purchase_documents` (compatibility view over `fact_purchases`)
- `fact_inventory_documents` (compatibility view over `fact_inventory`)
- `fact_cash_transactions` (compatibility view over `fact_cashflows`)
- `fact_supplier_balances`
- `fact_customer_balances`

Branch support validated on all fact tables:
- `fact_sales.branch_id`
- `fact_purchases.branch_id`
- `fact_inventory.branch_id`
- `fact_cashflows.branch_id`
- `fact_supplier_balances.branch_id`
- `fact_customer_balances.branch_id`

### D) Aggregates
- `agg_sales_daily`
- `agg_sales_monthly`
- `agg_purchases_daily`
- `agg_purchases_monthly`
- `agg_inventory_snapshot` (compatibility view over `agg_inventory_snapshot_daily`)
- `agg_stock_aging`
- `agg_cash_daily`
- `agg_cash_by_type`
- `agg_cash_accounts`
- `agg_supplier_balances_daily`
- `agg_customer_balances_daily`

### E) Intelligence
- `insight_rules`
- `insights`
- `insight_runs`

### F) Sync Support
- `sync_state`
- `ingest_batches`
- `dead_letter_queue` (compatibility view over `ingest_dead_letter`)

## 4) Migration Set

Primary migration for final canonical alignment:
- `backend/alembic/versions/20260307_0007_tenant_final_canonical_schema.py`

Dependencies:
- `20260307_0005_tenant_multibranch_alignment.py`
- `20260307_0006_tenant_branch_id_backfill.py`

Also updated:
- `scripts/run_tenant_migrations.py` -> tenant head `20260307_0007_tenant`

## 5) Runtime Alignment

Ingestion upserts now populate new dimensions:
- `dim_customers`
- `dim_accounts`
- `dim_document_types`
- `dim_payment_methods`

## 6) Dashboard Query Contract

Dashboard KPI/summary/trend endpoints must read from aggregate tables only:
- sales -> `agg_sales_daily`, `agg_sales_monthly`
- purchases -> `agg_purchases_daily`, `agg_purchases_monthly`
- cash -> `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`
- receivables/payables -> `agg_customer_balances_daily`, `agg_supplier_balances_daily`
- inventory KPI snapshot/aging -> `agg_inventory_snapshot`, `agg_stock_aging`

Raw fact tables are allowed only for drilldown/detail views and traceability.

## 7) Validation Status

Applied and validated on:
- `bi_tenant_uat-a`
- `bi_tenant_uat-b`

Validated checks:
- table groups present
- canonical compatibility views present
- all fact tables include `branch_id`
- new canonical migration upgraded successfully
