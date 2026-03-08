# Data Structure - Cash Transactions

## 1) Description
Cash Transactions represent inflows/outflows and account movements for liquidity, collections/payments, and cash behavior insights.

## 2) Required Fields
- `transaction_id`
- `transaction_date`
- `transaction_type`
- `subcategory`
- `branch_id`
- `account_id`
- `counterparty_type`
- `counterparty_id`
- `amount`
- `external_id`
- `updated_at`

## 3) Optional Fields
- `reference_no`
- `payment_method_id`
- `currency`
- `description`
- `document_id`
- `due_date`
- `status`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `transaction_id`: source transaction identifier.
- `transaction_date`: posting date.
- `transaction_type`: `inflow`, `outflow`, or `transfer`.
- `subcategory`: one of five mandatory categories:
  1. `customer_collections`
  2. `customer_transfers`
  3. `supplier_payments`
  4. `supplier_transfers`
  5. `financial_accounts`
- `branch_id`: branch/location.
- `account_id`: cash/bank account.
- `counterparty_type`: `customer`, `supplier`, `internal`.
- `counterparty_id`: source counterparty id.
- `amount`: transaction value.
- `external_id`: idempotency key.
- `updated_at`: source update timestamp.

## 5) Data Types
- string: `transaction_id`, `transaction_type`, `subcategory`, `branch_id`, `account_id`, `counterparty_type`, `counterparty_id`, `external_id`
- date: `transaction_date`
- decimal: `amount`
- datetime: `updated_at`

## 6) Example JSON Payload
```json
{
  "stream_code": "cash_transactions",
  "records": [
    {
      "transaction_id": "CASH-20260308-00129",
      "transaction_date": "2026-03-08",
      "transaction_type": "outflow",
      "subcategory": "supplier_payments",
      "branch_id": "BR-ATH-01",
      "account_id": "ACC-BANK-01",
      "counterparty_type": "supplier",
      "counterparty_id": "SUP-3009",
      "amount": 1250.00,
      "external_id": "CASH-ERP1-20260308-00129",
      "reference_no": "PAY-10998",
      "updated_at": "2026-03-08T15:35:00Z"
    }
  ]
}
```

## 7) Example CSV Format
```csv
transaction_id,transaction_date,transaction_type,subcategory,branch_id,account_id,counterparty_type,counterparty_id,amount,external_id,reference_no,updated_at
CASH-20260308-00129,2026-03-08,outflow,supplier_payments,BR-ATH-01,ACC-BANK-01,supplier,SUP-3009,1250.00,CASH-ERP1-20260308-00129,PAY-10998,2026-03-08T15:35:00Z
```

## 8) Business Rules
- `subcategory` must be one of the five official values.
- `counterparty_type` must match subcategory semantics.
- `amount` must be positive numeric; direction is derived from `transaction_type`.
- `transaction_date` is mandatory.
- `external_id` must be unique per connector.

## 9) Mapping Notes
Typical ERP mapping:
- ERP `PAYMENT_ID` / `ENTRY_ID` -> `transaction_id`
- ERP `TRN_DATE` -> `transaction_date`
- ERP `TRN_KIND` -> `transaction_type`
- ERP `TRN_CATEGORY` -> `subcategory`
- ERP `ACCOUNT_CODE` -> `account_id`
- ERP `PARTY_TYPE` -> `counterparty_type`
- ERP `PARTY_CODE` -> `counterparty_id`
- ERP `AMOUNT` -> `amount`
- ERP `LAST_UPDATE` -> `updated_at`
