# Missing Data Requirements

Date: 2026-03-07
Goal: Full pharmacy operational intelligence on top of 6 canonical streams.

## Implemented stream foundation
- `fact_sales`
- `fact_purchases`
- `fact_inventory`
- `fact_cashflows`
- `fact_supplier_balances`
- `fact_customer_balances`

Supporting aggregates:
- `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`
- `agg_purchases_daily`, `agg_purchases_monthly`
- `agg_inventory_snapshot_daily`
- `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`
- `agg_supplier_balances_daily`
- `agg_customer_balances_daily`

## Remaining gaps (priority)

## P0: Counterparty and account master governance
Still needed for clean cross-stream intelligence:
- `dim_customers` as stable master dimension (not only transaction-level customer text/code)
- `dim_accounts` for cash accounts and bank accounts
- strict mapping keys between:
  - sales -> customer balances -> customer collections
  - purchases -> supplier balances -> supplier payments

## P1: Cash enrichment gaps
- enforce hard whitelist on cash subcategories end-to-end
- ensure `transaction_type` and `transaction_date` come from source consistently
- add payment channel and instrument metadata (bank, POS, cheque, transfer method)

## P1: Inventory operational depth
Current inventory model is snapshot-centric. Missing for full intelligence:
- movement-level ledger (`entry`, `exit`, `transfer`, `adjustment`)
- movement reason codes
- lot/batch, expiry date and serial tracking
- reserved vs committed by order linkage

## P1: Receivables and payables maturity
Needed for stronger working-capital analytics:
- due-date level open-item facts (invoice-level, not only snapshot)
- collection/payment promise dates and follow-up statuses
- dispute/credit-hold flags
- write-off and settlement event tracking

## P2: Pharmacy-specific operational entities
- prescription/reimbursement lifecycle stream
- promotions/campaign economics stream
- returns/cancellations document lineage stream

## Final answer
Additional entities/fields still required for full pharmacy operational intelligence:
1. Stable master dimensions for customers, suppliers, and financial accounts.
2. Open-item level AR/AP ledgers with due dates and settlement lifecycle.
3. Inventory movement + lot/expiry granularity.
4. Payment/collection operational metadata (channel, instrument, promise, status).
5. Pharmacy-specific streams: reimbursement, promo economics, and returns lineage.
