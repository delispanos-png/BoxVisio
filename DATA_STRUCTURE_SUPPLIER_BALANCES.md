# Data Structure - Supplier Balances

## 1) Description
Supplier Balances represent open liabilities toward suppliers, aging exposure, and payable risk monitoring.

## 2) Required Fields
- `supplier_id`
- `balance_date`
- `open_balance`
- `overdue_balance`
- `aging_bucket_0_30`
- `aging_bucket_31_60`
- `aging_bucket_61_90`
- `aging_bucket_90_plus`
- `last_payment_date`
- `trend_vs_previous`

## 3) Optional Fields
- `branch_id`
- `external_id`
- `updated_at`
- `currency`
- `status`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `supplier_id`: supplier identifier.
- `balance_date`: snapshot date.
- `open_balance`: total open payable amount.
- `overdue_balance`: overdue payable amount.
- `aging_bucket_0_30`: amount due in 0-30 days.
- `aging_bucket_31_60`: amount due in 31-60 days.
- `aging_bucket_61_90`: amount due in 61-90 days.
- `aging_bucket_90_plus`: amount due in 90+ days.
- `last_payment_date`: last payment date to supplier.
- `trend_vs_previous`: delta vs previous balance snapshot.

## 5) Data Types
- string: `supplier_id`
- date: `balance_date`, `last_payment_date`
- decimal: `open_balance`, `overdue_balance`, `aging_bucket_0_30`, `aging_bucket_31_60`, `aging_bucket_61_90`, `aging_bucket_90_plus`, `trend_vs_previous`

## 6) Example JSON Payload
```json
{
  "stream_code": "supplier_balances",
  "records": [
    {
      "supplier_id": "SUP-3009",
      "branch_id": "BR-ATH-01",
      "balance_date": "2026-03-08",
      "open_balance": 18450.25,
      "overdue_balance": 5420.10,
      "aging_bucket_0_30": 6200.00,
      "aging_bucket_31_60": 3900.00,
      "aging_bucket_61_90": 2450.00,
      "aging_bucket_90_plus": 5900.25,
      "last_payment_date": "2026-03-05",
      "trend_vs_previous": 325.15,
      "external_id": "SUPBAL-ERP1-20260308-SUP3009",
      "updated_at": "2026-03-08T19:00:00Z"
    }
  ]
}
```

## 7) Example CSV Format
```csv
supplier_id,branch_id,balance_date,open_balance,overdue_balance,aging_bucket_0_30,aging_bucket_31_60,aging_bucket_61_90,aging_bucket_90_plus,last_payment_date,trend_vs_previous,external_id,updated_at
SUP-3009,BR-ATH-01,2026-03-08,18450.25,5420.10,6200.00,3900.00,2450.00,5900.25,2026-03-05,325.15,SUPBAL-ERP1-20260308-SUP3009,2026-03-08T19:00:00Z
```

## 8) Business Rules
- One balance snapshot per `(supplier_id, balance_date, branch_id)` is recommended.
- `overdue_balance` cannot exceed `open_balance`.
- Sum of aging buckets should match `open_balance` (allow configurable tolerance).
- `trend_vs_previous` should compare against previous available snapshot date.

## 9) Mapping Notes
Typical ERP mapping:
- ERP supplier code -> `supplier_id`
- ERP aging report date -> `balance_date`
- ERP open payable -> `open_balance`
- ERP overdue payable -> `overdue_balance`
- ERP aging columns -> corresponding aging buckets
- ERP last supplier payment date -> `last_payment_date`
