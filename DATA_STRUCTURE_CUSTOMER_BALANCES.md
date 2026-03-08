# Data Structure - Customer Balances

## 1) Description
Customer Balances represent open receivables, overdue exposure, and collection risk monitoring.

## 2) Required Fields
- `customer_id`
- `balance_date`
- `open_balance`
- `overdue_balance`
- `aging_bucket_0_30`
- `aging_bucket_31_60`
- `aging_bucket_61_90`
- `aging_bucket_90_plus`
- `last_collection_date`
- `trend_vs_previous`
- `external_id`
- `updated_at`

## 3) Optional Fields
- `branch_id`
- `currency`
- `status`
- `source_connector_id`
- `metadata_json`

## 4) Field Definitions
- `customer_id`: customer identifier.
- `balance_date`: receivables snapshot date.
- `open_balance`: total receivable amount.
- `overdue_balance`: overdue receivable amount.
- `aging_bucket_0_30`: amount in 0-30 bucket.
- `aging_bucket_31_60`: amount in 31-60 bucket.
- `aging_bucket_61_90`: amount in 61-90 bucket.
- `aging_bucket_90_plus`: amount in 90+ bucket.
- `last_collection_date`: last collection/payment date from customer.
- `trend_vs_previous`: delta vs previous snapshot.
- `external_id`: stable source key.
- `updated_at`: source update datetime.

## 5) Data Types
- string: `customer_id`, `external_id`
- date: `balance_date`, `last_collection_date`
- decimal: `open_balance`, `overdue_balance`, `aging_bucket_0_30`, `aging_bucket_31_60`, `aging_bucket_61_90`, `aging_bucket_90_plus`, `trend_vs_previous`
- datetime: `updated_at`

## 6) Example JSON Payload
```json
{
  "stream_code": "customer_balances",
  "records": [
    {
      "customer_id": "CUS-1122",
      "branch_id": "BR-ATH-01",
      "balance_date": "2026-03-08",
      "open_balance": 9320.80,
      "overdue_balance": 2800.00,
      "aging_bucket_0_30": 3000.00,
      "aging_bucket_31_60": 1500.00,
      "aging_bucket_61_90": 1200.00,
      "aging_bucket_90_plus": 3620.80,
      "last_collection_date": "2026-03-04",
      "trend_vs_previous": 450.25,
      "external_id": "CUSBAL-ERP1-20260308-CUS1122",
      "updated_at": "2026-03-08T19:00:00Z"
    }
  ]
}
```

## 7) Example CSV Format
```csv
customer_id,branch_id,balance_date,open_balance,overdue_balance,aging_bucket_0_30,aging_bucket_31_60,aging_bucket_61_90,aging_bucket_90_plus,last_collection_date,trend_vs_previous,external_id,updated_at
CUS-1122,BR-ATH-01,2026-03-08,9320.80,2800.00,3000.00,1500.00,1200.00,3620.80,2026-03-04,450.25,CUSBAL-ERP1-20260308-CUS1122,2026-03-08T19:00:00Z
```

## 8) Business Rules
- `external_id` must be unique per connector.
- `overdue_balance` cannot exceed `open_balance`.
- Aging buckets should reconcile to `open_balance` (within tolerance).
- `balance_date` mandatory for timeseries and trend KPIs.
- `trend_vs_previous` should be derived from previous balance snapshot per customer.

## 9) Mapping Notes
Typical ERP mapping:
- ERP customer code -> `customer_id`
- ERP receivables report date -> `balance_date`
- ERP open receivable -> `open_balance`
- ERP overdue receivable -> `overdue_balance`
- ERP aging columns -> aging bucket fields
- ERP last collection/payment date -> `last_collection_date`
- ERP change vs previous period -> `trend_vs_previous`
