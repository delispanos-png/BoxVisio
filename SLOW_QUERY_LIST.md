# SLOW_QUERY_LIST.md

## Source
`app.kpi_perf` logs (2026-03-08)

## Slow Query Entries

1. Inventory stream by commercial category
- Endpoint: `/v1/streams/inventory/summary`
- SQL pattern:
  - `SELECT coalesce(dim_groups.name, ...), sum(agg_stock_aging.qty_on_hand), sum(agg_stock_aging.stock_value) ... GROUP BY ...`
- Observed duration in request log: ~100-206 ms per query (depending on run)

2. Inventory stream by brand
- Endpoint: `/v1/streams/inventory/summary`
- SQL pattern:
  - `SELECT coalesce(dim_brands.name, ...), sum(agg_stock_aging.qty_on_hand), sum(agg_stock_aging.stock_value) ... GROUP BY ...`
- Observed duration: ~100 ms

3. Inventory snapshot date lookup
- Endpoint: `/v1/streams/inventory/summary`
- SQL pattern:
  - `SELECT max(agg_stock_aging.snapshot_date) ...`
- Observed duration in one run: ~86 ms (cold path)

4. Legacy sales summary fan-out query (repeated many times in old UI)
- Endpoint group: `/v1/kpi/sales/summary`
- SQL pattern:
  - `SELECT count(agg_sales_daily.id), sum(qty), sum(net_value), sum(gross_value) ...`
- Individual query duration: typically 150-400 ms under fan-out/concurrency pressure
