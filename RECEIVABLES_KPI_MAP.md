# Receivables KPI Map

Date: 2026-03-07

Scope: KPI dependencies for stream `fact_customer_balances` only.

## KPI dependency matrix
| KPI | Source stream | Fact table | Aggregate table | API endpoint |
|---|---|---|---|---|
| Total receivables | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/summary` |
| Overdue receivables | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/summary` |
| Receivables aging | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/aging` |
| Top customers by outstanding | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/top-customers` |
| Collection trend | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/collection-trend` |
| Outstanding growth vs previous period | Customer Balances | `fact_customer_balances` | `agg_customer_balances_daily` | `GET /v1/kpi/receivables/summary` |

## KPI definitions (deterministic)
- `total_receivables`: sum of `open_balance` from latest snapshot per customer (as of `to` date)
- `overdue_receivables`: sum of `overdue_balance` from latest snapshot per customer (as of `to` date)
- `receivables_aging`: sum of aging buckets from latest snapshot per customer
- `top_customers_outstanding`: rank latest customer snapshots by `open_balance`
- `collection_trend`: daily trend from `trend_vs_previous` (`estimated_collections` from negative deltas, `new_outstanding` from positive deltas)
- `outstanding_growth_vs_previous`: latest period total open balance minus previous period total open balance

## Filter support
- `from`, `to`
- `branches`
- `q` (top customers search by code/name)
- pagination (`limit`, `offset`) for top-customer ranking

## Governance
- Receivables KPIs must not read ad-hoc SoftOne SQL directly.
- Runtime reads only canonical facts/aggregates in tenant PostgreSQL.
