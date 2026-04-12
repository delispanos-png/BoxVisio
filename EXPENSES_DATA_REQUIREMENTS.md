# Expenses Data Requirements

Date: 2026-03-08
Scope: Required source data and mapping guidance for `Operating Expenses` ingestion.

## Required Source Data (minimum viable)
To support core expense KPIs:
- `external_id`
- `expense_date`
- `branch_code`
- `expense_category_code`
- `amount_net`
- `amount_gross`
- `document_type`
- `updated_at`

If `amount_tax` is unavailable, derive:
- `amount_tax = amount_gross - amount_net`

## Recommended Enrichment Fields
- `supplier_code`
- `account_code`
- `document_no`
- `cost_center`
- `payment_status`
- `due_date`
- `currency_code`
- `posting_date`
- `notes` (for audit/debug only, not KPI)

## Source Coverage by Connector Type

## 1) SQL Connector
Expected ERP source entities:
- Accounts payable expense documents
- General ledger expense entries (P&L operating accounts)
- Petty cash and expense vouchers
- Bank fees/charges entries

Minimum extraction behavior:
- incremental by `updated_at` or source sequence
- stable `external_id`
- explicit classification field or mapping to operating expense category

## 2) API Connector
Required endpoint capabilities:
- list/filter by date and last update
- pagination
- unique source identifier
- expense category metadata or account code for mapping

## 3) File Import (CSV/Excel/SFTP)
Required columns:
- `external_id,expense_date,branch_code,expense_category_code,amount_net,amount_gross,document_type,updated_at`

Recommended additional columns:
- `supplier_code,account_code,document_no,cost_center,payment_status,due_date,currency_code`

## Mapping to Canonical Model

Canonical mapping targets:
- `branch_code -> dim_branches.branch_code -> fact_expenses.branch_id`
- `expense_category_code -> dim_expense_categories.category_code -> fact_expenses.category_id`
- `supplier_code -> dim_suppliers.supplier_code -> fact_expenses.supplier_id` (optional)
- `account_code -> dim_accounts.account_code -> fact_expenses.account_id` (optional)

## Validation Rules
- `external_id` must be unique per tenant + source connector.
- `expense_date` is mandatory and must be valid date.
- `amount_net` and `amount_gross` must be numeric.
- `amount_gross >= amount_net` unless explicit credit/reversal rule applies.
- `expense_category_code` must map to active category.
- Non-operating categories must be excluded from `fact_expenses`.

## Sign Handling Rules
- Default: expenses stored as positive amounts.
- Credit/reversal documents: negative amounts only when document rule explicitly marks reversal.
- Mixed-sign source feeds must be normalized before fact load.

## Required Business Rules Configuration
- Document type classification (`expense_invoice`, `expense_credit_note`, `voucher`, etc.)
- Include/exclude from Operating Expenses stream
- Sign policy by document type
- Tenant-level category mapping override

## Incremental Sync Strategy
- Primary watermark: `updated_at`
- Fallback: source sequence / ingestion batch timestamp
- Upsert key: `external_id` (+ optional `source_connector_id`)

## Data Quality Checks (daily)
- unmapped categories count
- duplicate `external_id` count
- null `branch_id` count
- negative/positive sign anomalies
- expense outlier count by z-score or threshold

## Example JSON Payload
```json
{
  "external_id": "EXP-2026-000145",
  "expense_date": "2026-03-05",
  "branch_code": "ATH01",
  "expense_category_code": "UTILITIES",
  "document_type": "expense_invoice",
  "document_no": "INV-5534",
  "amount_net": 420.00,
  "amount_tax": 100.80,
  "amount_gross": 520.80,
  "supplier_code": "SUP-0012",
  "account_code": "ACC-BANK-01",
  "cost_center": "STORE_OPERATIONS",
  "payment_status": "unpaid",
  "due_date": "2026-03-20",
  "currency_code": "EUR",
  "updated_at": "2026-03-06T09:44:22Z"
}
```

## Example CSV Header
```csv
external_id,expense_date,branch_code,expense_category_code,document_type,document_no,amount_net,amount_tax,amount_gross,supplier_code,account_code,cost_center,payment_status,due_date,currency_code,updated_at
```
