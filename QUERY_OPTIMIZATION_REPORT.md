# QUERY_OPTIMIZATION_REPORT.md

## Date
- 2026-03-08

## Method
- Collected runtime query telemetry from `app.kpi_perf` logs.
- Ran `EXPLAIN (ANALYZE, BUFFERS)` on representative queries in tenant DB `bi_tenant_uat-a`.

## Top Slow Query Patterns
1. Repeated sales aggregate scans from legacy fan-out requests:
   - `SELECT count(...), sum(qty), sum(net_value), sum(gross_value) FROM agg_sales_daily ...`
2. Repeated monthly trend scans:
   - `SELECT month_start, sum(...) FROM agg_sales_monthly ... GROUP BY month_start`
3. Inventory stream category/brand aggregations:
   - `FROM agg_stock_aging LEFT JOIN dim_items ... GROUP BY ... ORDER BY sum(stock_value)`

## EXPLAIN Highlights

### Sales summary
- Plan: index scan on `ix_agg_sales_daily_doc_date`
- Runtime in DB: ~0.275 ms
- Conclusion: query itself is efficient; latency inflation came from endpoint fan-out/concurrency.

### Inventory by commercial category (agg_stock_aging)
- Runtime in DB: ~163 ms
- Uses parallel seq scan for the selected snapshot.
- Cost driver: high cardinality snapshot + joins to dimensions + grouped sort.

## Changes Implemented
- Consolidated dashboard endpoints reduce repeated query execution from many requests to one request per view.
- Introduced stream summary endpoints to avoid per-widget network fan-out.
- Added equivalent `/api/*` consolidated routes to keep one query plan regardless of path namespace.
- Added short-lived Redis-backed cache (with in-process fallback).
- Added cache invalidation after aggregate refresh.
- Enabled performance headers and query counters for `/v1/dashboard/*`, `/v1/streams/*`, `/api/dashboard/*`, `/api/streams/*`.
- Refactored executive summary to use window-batched aggregate queries:
  - sales windows in one SQL
  - purchases windows in one SQL
  - branch breakdown windows in one SQL
  - monthly trend fetched once and split by year in app layer
- Reduced duplicate stock snapshot date lookups in inventory summary (single lookup per request).
- Skipped unnecessary dimension joins for stock snapshot/aging when brand/category/group filters are not provided.
- Locked stream balances receivables summary to aggregate-only mode (no fact fallback on summary pipeline).
- Refactored `/v1/kpi/customers` list path to aggregate-based balance mode (`_latest_customer_balances_map(..., aggregate_only=True)`) to avoid multi-year fact scans on page load.
- Reduced default customer/supplier list date windows from 10 years to 12 months (UI + API fallback), improving initial stream switch latency.

## Before / After (Observed)
- Legacy executive fan-out: 16 calls, ~1332–1506 ms total (test runs)
- Consolidated executive summary: ~52–56 ms (MISS), ~12–13 ms (HIT)
- Legacy finance fan-out: 7 calls, ~166–179 ms total (test runs)
- Consolidated finance summary: ~25 ms (MISS)
- Customer list endpoint:
  - before optimization run: ~10.47 s (worst-case malformed range fallback)
  - after optimization: ~184 ms (`limit=250`)

## Remaining Hotspot
- `/v1/streams/inventory/summary` cold-cache runtime ~306–341 ms (DB ~283–316 ms)
- Dominant source: aggregate scans over `agg_stock_aging` with joins/grouping.

## Temporary Tradeoff
- Customer list endpoint is now balance-driven for speed.
- The list-level `turnover` field is currently returned as `0.0` until a dedicated customer-sales aggregate is introduced (e.g. `agg_sales_customer_daily`).
- Customer detail page still computes full sales history from facts on demand.

## Next Query-level Steps (optional)
- Add optional pre-aggregates for inventory distributions by snapshot date (brand/group), e.g.:
  - `agg_inventory_snapshot_brand`
  - `agg_inventory_snapshot_group`
- Use those for stream inventory charts to cut cold-start further.
