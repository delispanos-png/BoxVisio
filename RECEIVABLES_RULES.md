# Receivables Rules

Date: 2026-03-07

Category: `receivables`  
Source stream: `fact_customer_balances`

## Implemented deterministic rules

## 1) `CUSTOMER_OVERDUE_SPIKE`
Purpose:
- Detect sharp increase in overdue receivables vs previous period.

Logic:
- Compare tenant-level overdue total (latest customer snapshot as of period end) vs previous period snapshot.
- Trigger when `% change >= overdue_spike_pct` and baseline overdue >= `min_overdue_baseline`.

Default params:
- `overdue_spike_pct=20`
- `min_overdue_baseline=500`
- `window_days=30`

Severity:
- `warning` default
- `critical` for large spikes

## 2) `RECEIVABLES_GROWTH_ALERT`
Purpose:
- Detect growth in open customer exposure.

Logic:
- Compare total open receivables vs previous period.
- Trigger when `% change >= growth_pct` and previous open >= `min_open_baseline`.

Default params:
- `growth_pct=15`
- `min_open_baseline=1000`
- `window_days=30`

Severity:
- `warning` default
- `critical` for strong growth

## 3) `TOP_CUSTOMER_EXPOSURE`
Purpose:
- Detect concentration risk on one customer.

Logic:
- Compute top customer share of total open receivables.
- Trigger when `top_share >= share_pct_threshold` and total open >= `min_total_open`.

Default params:
- `share_pct_threshold=25`
- `min_total_open=1000`
- `window_days=30`

Severity:
- `warning` default
- `critical` for very high concentration

## 4) `COLLECTION_DELAY_ALERT`
Purpose:
- Detect delayed collections for customers with overdue exposure.

Logic:
- For latest customer snapshot rows:
  - overdue balance >= `min_overdue_balance`
  - days since `last_collection_date` >= `days_since_last_collection`
- Emit top `N` highest-risk customers.

Default params:
- `days_since_last_collection=45`
- `min_overdue_balance=300`
- `top_n=10`
- `window_days=30`

Severity:
- `warning` default
- `critical` for long delay windows

## Rule ownership
- Module owner: Customer Balances / Receivables stream
- Runtime category: `receivables`
- Drilldown target: `/tenant/customers`
