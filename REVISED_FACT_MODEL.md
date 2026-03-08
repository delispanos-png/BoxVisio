# Revised Fact Model (Module-Aligned)

Date: 2026-03-07

## Goal
Align the canonical fact model to four operational modules:
1. Παραστατικά Πωλήσεων (Sales Documents)
2. Παραστατικά Αγορών (Purchase Documents)
3. Είδη Αποθήκης (Inventory Items / Warehouse Documents)
4. Ταμειακές Συναλλαγές (Cash Transactions)

## Current Tables vs Target Canonical Areas

### 1) Sales Documents
- **Target canonical**: `fact_sales_documents`
- **Current**: `fact_sales`
- **Status**: Already contains document-level and line-level fields (document id/series/type, customer, branch, item, qty, net/gross, VAT, status).
- **Action**: Keep `fact_sales` as canonical for Sales Documents. Optionally add a SQL view `fact_sales_documents` for semantic clarity.

### 2) Purchase Documents
- **Target canonical**: `fact_purchase_documents`
- **Current**: `fact_purchases`
- **Status**: Already contains purchase doc data (supplier, branch, item, qty, net/cost, status).
- **Action**: Keep `fact_purchases` as canonical for Purchase Documents. Optionally add a SQL view `fact_purchase_documents`.

### 3) Inventory Items / Warehouse Documents
- **Target canonical**: `fact_inventory_documents` + inventory item state
- **Current**: `fact_inventory`
- **Status**: Captures stock state (qty_on_hand, qty_reserved, cost/value, doc_date, item/warehouse/branch).
- **Action**: Keep `fact_inventory` as canonical. Consider extending with movement_type / doc_type for explicit warehouse documents if not already present in source payload.

### 4) Cash Transactions
- **Target canonical**: `fact_cash_transactions`
- **Current**: `fact_cashflows`
- **Status**: Holds entry_type, amount, date, branch. Needs strict categorization into 5 subtypes.
- **Action**: Keep `fact_cashflows` as canonical. Enforce/normalize `entry_type` into the 5 categories:
  - customer_collections
  - customer_transfers
  - supplier_payments
  - supplier_transfers
  - financial_accounts

## Proposed Optional Schema Enhancements
No mandatory schema changes to establish the four-module model. Recommended enhancements if we want stronger consistency:
- Create SQL views (no data migration) for semantic clarity:
  - `fact_sales_documents` -> `fact_sales`
  - `fact_purchase_documents` -> `fact_purchases`
  - `fact_inventory_documents` -> `fact_inventory`
  - `fact_cash_transactions` -> `fact_cashflows`
- Add `movement_type` / `document_type` fields to `fact_inventory` if warehouse events need explicit typing.
- Add `account_id` / `account_name` to `fact_cashflows` if cash KPIs need account-level breakdowns.

## Current Aggregates (Aligned)
- Sales: `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`
- Purchases: `agg_purchases_daily`, `agg_purchases_monthly`
- Inventory: `agg_inventory_snapshot_daily`
- Cash: none (only raw fact queries today)

