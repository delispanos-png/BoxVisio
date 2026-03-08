# KPI Dependency Audit

Date: 2026-03-07

## Summary
Current KPI endpoints are already largely driven by four operational streams via `fact_sales`, `fact_purchases`, `fact_inventory`, `fact_cashflows` and their aggregates. The mapping below identifies each KPI endpoint, its current source tables, and the UI page(s) consuming it. This is the baseline for the module refactor.

## KPI Endpoints and Dependencies

### Sales (Παραστατικά Πωλήσεων)
- `GET /v1/kpi/sales/summary`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_summary` in `backend/app/services/kpi_queries.py`
  - Dashboard: `tenant/sales_dashboard.html`, `tenant/dashboard.html`

- `GET /v1/kpi/sales/by-branch`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_by_branch`
  - Dashboard: `tenant/sales_dashboard.html`, `tenant/dashboard.html`

- `GET /v1/kpi/sales/by-brand`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_by_brand`
  - Dashboard: `tenant/sales_dashboard.html`

- `GET /v1/kpi/sales/by-category`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_by_category`
  - Dashboard: `tenant/sales_dashboard.html`

- `GET /v1/kpi/sales/compare`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_compare`
  - Dashboard: `tenant/compare.html`

- `GET /v1/kpi/sales/decision-pack`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_decision_pack`
  - Dashboard: `tenant/sales_dashboard.html`, `tenant/dashboard.html`, `tenant/insights.html`

- `GET /v1/kpi/sales/ranking`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: `agg_sales_item_daily`
  - Service: `sales_ranking`
  - Dashboard: `tenant/sales_dashboard.html`

- `GET /v1/kpi/sales/filter-options`
  - Source module: Sales Documents
  - Fact table: `fact_sales` (fallback)
  - Aggregate table: `agg_sales_daily`
  - Service: `sales_filter_options`
  - Dashboard: `tenant/sales_dashboard.html`, `tenant/dashboard.html`, `tenant/compare.html`

- `GET /v1/kpi/sales/documents`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: none (document-level aggregation in query)
  - Service: `sales_documents_overview`
  - Dashboard: `tenant/sales_dashboard.html`

- `GET /v1/kpi/sales/documents/{document_id}/detail`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: none
  - Service: `sales_document_detail`
  - Dashboard: `tenant/sales_dashboard.html`

### Purchases (Παραστατικά Αγορών)
- `GET /v1/kpi/purchases/summary`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: `agg_purchases_daily`
  - Service: `purchases_summary`
  - Dashboard: `tenant/purchases_dashboard.html`, `tenant/dashboard.html`

- `GET /v1/kpi/purchases/by-supplier`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: `agg_purchases_daily`
  - Service: `purchases_by_supplier`
  - Dashboard: `tenant/purchases_dashboard.html`

- `GET /v1/kpi/purchases/compare`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: `agg_purchases_daily`
  - Service: `purchases_compare`
  - Dashboard: `tenant/compare.html`

- `GET /v1/kpi/purchases/decision-pack`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: `agg_purchases_daily`
  - Service: `purchases_decision_pack`
  - Dashboard: `tenant/purchases_dashboard.html`

- `GET /v1/kpi/purchases/filter-options`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases` (fallback)
  - Aggregate table: `agg_purchases_daily`
  - Service: `purchases_filter_options`
  - Dashboard: `tenant/purchases_dashboard.html`

- `GET /v1/kpi/purchases/documents`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: none (document-level aggregation in query)
  - Service: `purchases_documents_overview`
  - Dashboard: `tenant/purchase_documents_dashboard.html`

- `GET /v1/kpi/purchases/documents/{document_id}/detail`
  - Source module: Purchase Documents
  - Fact table: `fact_purchases`
  - Aggregate table: none
  - Service: `purchase_document_detail`
  - Dashboard: `tenant/purchase_documents_dashboard.html`

### Inventory (Είδη Αποθήκης / Παραστατικά Αποθήκης)
- `GET /v1/kpi/inventory/snapshot`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_snapshot`
  - Dashboard: `tenant/inventory_dashboard.html`

- `GET /v1/kpi/inventory/stock-aging`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`, `agg_sales_item_daily`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_stock_aging`
  - Dashboard: `tenant/inventory_dashboard.html`

- `GET /v1/kpi/inventory/by-brand`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_by_brand`
  - Dashboard: `tenant/inventory_dashboard.html`

- `GET /v1/kpi/inventory/by-commercial-category`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_by_commercial_category`
  - Dashboard: `tenant/inventory_dashboard.html`

- `GET /v1/kpi/inventory/by-manufacturer`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_by_manufacturer`
  - Dashboard: `tenant/inventory_dashboard.html`

- `GET /v1/kpi/inventory/filter-options`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: `agg_inventory_snapshot_daily` (fallback)
  - Service: `inventory_filter_options`
  - Dashboard: `tenant/inventory_dashboard.html`, `tenant/items_dashboard.html`

- `GET /v1/kpi/inventory/items`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory` + `dim_items`
  - Aggregate table: `agg_inventory_snapshot_daily`
  - Service: `inventory_items_overview`
  - Dashboard: `tenant/items_dashboard.html`

- `GET /v1/kpi/inventory/items/{item_code}/detail`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`, `fact_sales`, `fact_purchases`
  - Aggregate table: `agg_inventory_snapshot_daily`, `agg_sales_item_daily`
  - Service: `inventory_item_detail`
  - Dashboard: `tenant/items_dashboard.html`

- `GET /v1/kpi/inventory/documents`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: none (document-level aggregation in query)
  - Service: `inventory_documents_overview`
  - Dashboard: `tenant/warehouse_documents_dashboard.html`

- `GET /v1/kpi/inventory/documents/{document_id}/detail`
  - Source module: Inventory Items / Warehouse Documents
  - Fact table: `fact_inventory`
  - Aggregate table: none
  - Service: `inventory_document_detail`
  - Dashboard: `tenant/warehouse_documents_dashboard.html`

### Cash Transactions (Ταμειακές Συναλλαγές)
- `GET /v1/kpi/cashflow/summary`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none (query aggregation)
  - Service: `cashflow_summary`
  - Dashboard: `tenant/cashflow_dashboard.html`

- `GET /v1/kpi/cashflow/trend-monthly`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none (query aggregation)
  - Service: `cashflow_trend_monthly`
  - Dashboard: `tenant/cashflow_dashboard.html`

- `GET /v1/kpi/cashflow/by-type`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none
  - Service: `cashflow_by_type`
  - Dashboard: `tenant/cashflow_dashboard.html`

- `GET /v1/kpi/cashflow/documents`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none (document-level aggregation in query)
  - Service: `cashflow_documents_overview`
  - Dashboard: `tenant/cashflow_documents_dashboard.html`

- `GET /v1/kpi/cashflow/documents/{document_id}/detail`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none
  - Service: `cashflow_document_detail`
  - Dashboard: `tenant/cashflow_documents_dashboard.html`

- `GET /v1/kpi/cashflow/accounts`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none
  - Service: `cashflow_accounts_overview`
  - Dashboard: `tenant/cashflow_accounts_dashboard.html`

- `GET /v1/kpi/cashflow/accounts/{account_id}/detail`
  - Source module: Cash Transactions
  - Fact table: `fact_cashflows`
  - Aggregate table: none
  - Service: `cashflow_account_detail`
  - Dashboard: `tenant/cashflow_accounts_dashboard.html`

### Other / Supporting
- `GET /v1/kpi/customers`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: none
  - Service: `customers_overview`
  - Dashboard: `tenant/customers_dashboard.html`

- `GET /v1/kpi/customers/{customer_id}/detail`
  - Source module: Sales Documents
  - Fact table: `fact_sales`
  - Aggregate table: none
  - Service: `customer_detail`
  - Dashboard: `tenant/customers_dashboard.html`

- `GET /v1/kpi/supplier-targets*`
  - Source module: Purchase Documents (targets vs actual)
  - Fact table: `fact_purchases`
  - Aggregate table: `agg_purchases_daily`
  - Service: `supplier_targets_*`
  - Dashboard: `tenant/supplier_targets.html`

- `GET /v1/kpi/price-control/items`
  - Source module: Inventory + Sales (pricing)
  - Fact table: `fact_sales`, `fact_inventory`
  - Aggregate table: `agg_sales_item_daily`, `agg_inventory_snapshot_daily`
  - Dashboard: `tenant/price_control.html`

## Dependency Gaps / Mixed Sources
- Inventory item detail uses both sales and purchases facts to show item behavior; this is acceptable but should be explicitly documented as cross-module enrichment.
- Cashflow dashboards currently reuse sales filter options in UI (`tenant/cashflow_dashboard.html`), which should be replaced by cashflow-specific filters.

