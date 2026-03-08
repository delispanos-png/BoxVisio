# Customer Balances Schema

Date: 2026-03-07

## Canonical stream
- Stream: `customer_balances`
- Canonical fact name: `fact_customer_balances`
- Business meaning: customer receivables/open exposure snapshot

## Required canonical fields
| Field | Type | Required | Description |
|---|---|---:|---|
| `customer_id` | string | yes | Customer external identifier |
| `balance_date` | date | yes | Snapshot date |
| `open_balance` | numeric(14,2) | yes | Total open receivable |
| `overdue_balance` | numeric(14,2) | yes | Overdue receivable |
| `aging_bucket_0_30` | numeric(14,2) | yes | Aging 0-30 days |
| `aging_bucket_31_60` | numeric(14,2) | yes | Aging 31-60 days |
| `aging_bucket_61_90` | numeric(14,2) | yes | Aging 61-90 days |
| `aging_bucket_90_plus` | numeric(14,2) | yes | Aging 90+ days |
| `last_collection_date` | date | no | Last collection date |
| `trend_vs_previous` | numeric(14,2) | no | Net change vs previous snapshot |
| `external_id` | string | yes | Idempotency key for upsert |
| `updated_at` | timestamp | yes | Source update timestamp for incremental sync |

## Physical implementation (PostgreSQL)
Table: `fact_customer_balances`

Columns:
- `id` UUID PK
- `external_id` VARCHAR(128) UNIQUE
- `customer_ext_id` VARCHAR(128)
- `customer_name` VARCHAR(255)
- `balance_date` DATE
- `branch_id` UUID (optional FK)
- `branch_ext_id` VARCHAR(64)
- `open_balance` NUMERIC(14,2)
- `overdue_balance` NUMERIC(14,2)
- `aging_bucket_0_30` NUMERIC(14,2)
- `aging_bucket_31_60` NUMERIC(14,2)
- `aging_bucket_61_90` NUMERIC(14,2)
- `aging_bucket_90_plus` NUMERIC(14,2)
- `last_collection_date` DATE NULL
- `trend_vs_previous` NUMERIC(14,2) NULL
- `currency` VARCHAR(3) default `EUR`
- `updated_at` TIMESTAMP
- `created_at` TIMESTAMP

Indexes:
- `ix_fact_customer_balances_balance_date`
- `ix_fact_customer_balances_customer_ext_id`
- `ix_fact_customer_balances_branch_ext_id`
- `ix_fact_customer_balances_updated_at`

Aggregate table:
- `agg_customer_balances_daily`

## Incremental sync contract
- Watermark: `updated_at`
- Tie-breaker: `external_id`
- Upsert key: `external_id`
- Reprocessing: date-window backfill supported

## Extraction source
- Query pack SQL: `backend/querypacks/pharmacyone/facts/customer_balances_facts.sql`
- Default fallback query: `SELECT TOP 5000 * FROM dbo.CustomerBalances`

## Data quality checks
- `open_balance >= 0`
- `overdue_balance >= 0`
- `overdue_balance <= open_balance` (warning if violated)
- Aging bucket sum should approximate open balance (warning tolerance)
- `updated_at` monotonicity per `external_id`
