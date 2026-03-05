# Performance Benchmarks (Milestone 6)

Date: 2026-02-28
Environment: STAGING (PostgreSQL container, tenant DB `bi_tenant_uat-a`)

## Scope
- Validate impact of new composite aggregate indexes used by KPI filters.
- Query shape mirrors `/kpi/sales/summary` and `/kpi/purchases/summary` filtered date range + branch/warehouse/brand/category/group (+ supplier for purchases).

## Benchmark Method
1. Created synthetic benchmark tables in tenant DB (`bench_agg_sales_daily`, `bench_agg_purchases_daily`).
2. Inserted 657,000 rows per table (365 days x 1,800 rows/day).
3. Ran `EXPLAIN (ANALYZE, BUFFERS)` for representative KPI aggregate queries.
4. Added composite indexes matching migration `20260228_0011_tenant` design.
5. Re-ran same `EXPLAIN (ANALYZE, BUFFERS)` queries.
6. Dropped benchmark tables.

## Exact Commands
```bash
cd /opt/cloudon-bi

docker compose exec -T -e PGPASSWORD=postgres postgres psql -U postgres -d bi_tenant_uat-a <<'SQL'
DROP TABLE IF EXISTS bench_agg_sales_daily;
DROP TABLE IF EXISTS bench_agg_purchases_daily;
CREATE UNLOGGED TABLE bench_agg_sales_daily (
  id bigserial primary key,
  doc_date date not null,
  branch_ext_id varchar(64),
  warehouse_ext_id varchar(64),
  brand_ext_id varchar(64),
  category_ext_id varchar(64),
  group_ext_id varchar(64),
  qty numeric(18,4) not null default 0,
  net_value numeric(14,2) not null default 0,
  gross_value numeric(14,2) not null default 0
);
CREATE UNLOGGED TABLE bench_agg_purchases_daily (
  id bigserial primary key,
  doc_date date not null,
  branch_ext_id varchar(64),
  warehouse_ext_id varchar(64),
  supplier_ext_id varchar(64),
  brand_ext_id varchar(64),
  category_ext_id varchar(64),
  group_ext_id varchar(64),
  qty numeric(18,4) not null default 0,
  net_value numeric(14,2) not null default 0,
  cost_amount numeric(14,2) not null default 0
);
INSERT INTO bench_agg_sales_daily (doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id, qty, net_value, gross_value)
SELECT d::date,
       'b' || (g % 50),
       'w' || (g % 12),
       'br' || (g % 120),
       'c' || (g % 90),
       'gr' || (g % 24),
       (random()*10)::numeric(18,4),
       (random()*200)::numeric(14,2),
       (random()*250)::numeric(14,2)
FROM generate_series(date '2025-01-01', date '2025-12-31', interval '1 day') d
CROSS JOIN generate_series(1, 1800) g;
INSERT INTO bench_agg_purchases_daily (doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id, qty, net_value, cost_amount)
SELECT d::date,
       'b' || (g % 50),
       'w' || (g % 12),
       's' || (g % 70),
       'br' || (g % 120),
       'c' || (g % 90),
       'gr' || (g % 24),
       (random()*9)::numeric(18,4),
       (random()*150)::numeric(14,2),
       (random()*120)::numeric(14,2)
FROM generate_series(date '2025-01-01', date '2025-12-31', interval '1 day') d
CROSS JOIN generate_series(1, 1800) g;
ANALYZE bench_agg_sales_daily;
ANALYZE bench_agg_purchases_daily;

EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(SUM(net_value),0)
FROM bench_agg_sales_daily
WHERE doc_date BETWEEN date '2025-09-01' AND date '2025-09-30'
  AND branch_ext_id IN ('b1','b2','b3','b4')
  AND warehouse_ext_id IN ('w1','w2')
  AND brand_ext_id IN ('br1','br2','br3','br4','br5')
  AND category_ext_id IN ('c1','c2','c3')
  AND group_ext_id IN ('gr1','gr2');

EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(SUM(net_value),0), COALESCE(SUM(cost_amount),0)
FROM bench_agg_purchases_daily
WHERE doc_date BETWEEN date '2025-09-01' AND date '2025-09-30'
  AND branch_ext_id IN ('b1','b2','b3','b4')
  AND warehouse_ext_id IN ('w1','w2')
  AND supplier_ext_id IN ('s1','s2','s3')
  AND brand_ext_id IN ('br1','br2','br3','br4','br5')
  AND category_ext_id IN ('c1','c2','c3')
  AND group_ext_id IN ('gr1','gr2');

CREATE INDEX ix_bench_agg_sales_daily_filters ON bench_agg_sales_daily (doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id);
CREATE INDEX ix_bench_agg_purchases_daily_filters ON bench_agg_purchases_daily (doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id);
ANALYZE bench_agg_sales_daily;
ANALYZE bench_agg_purchases_daily;

EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(SUM(net_value),0)
FROM bench_agg_sales_daily
WHERE doc_date BETWEEN date '2025-09-01' AND date '2025-09-30'
  AND branch_ext_id IN ('b1','b2','b3','b4')
  AND warehouse_ext_id IN ('w1','w2')
  AND brand_ext_id IN ('br1','br2','br3','br4','br5')
  AND category_ext_id IN ('c1','c2','c3')
  AND group_ext_id IN ('gr1','gr2');

EXPLAIN (ANALYZE, BUFFERS)
SELECT COALESCE(SUM(net_value),0), COALESCE(SUM(cost_amount),0)
FROM bench_agg_purchases_daily
WHERE doc_date BETWEEN date '2025-09-01' AND date '2025-09-30'
  AND branch_ext_id IN ('b1','b2','b3','b4')
  AND warehouse_ext_id IN ('w1','w2')
  AND supplier_ext_id IN ('s1','s2','s3')
  AND brand_ext_id IN ('br1','br2','br3','br4','br5')
  AND category_ext_id IN ('c1','c2','c3')
  AND group_ext_id IN ('gr1','gr2');
SQL

docker compose exec -T -e PGPASSWORD=postgres postgres psql -U postgres -d bi_tenant_uat-a -c "DROP TABLE IF EXISTS bench_agg_sales_daily; DROP TABLE IF EXISTS bench_agg_purchases_daily;"
```

## Results Summary

### Sales aggregate query
- Before index:
  - Plan: `Parallel Seq Scan`
  - Execution time: **49.809 ms**
- After composite index:
  - Plan: `Bitmap Index Scan` + `Bitmap Heap Scan`
  - Execution time: **12.868 ms**
- Improvement: **~74.2% faster** (3.87x)

### Purchases aggregate query
- Before index:
  - Plan: `Parallel Seq Scan`
  - Execution time: **42.033 ms**
- After composite index:
  - Plan: `Parallel Bitmap Heap Scan` + `Bitmap Index Scan`
  - Execution time: **25.295 ms**
- Improvement: **~39.8% faster** (1.66x)

## Notes
- API/worker rebuild on staging was temporarily blocked by apt mirror retries; SQL benchmarks were executed directly against tenant DB container and are valid for index effect measurement.
- Final production rollout should apply migration `20260228_0011_tenant` to all active tenant DBs.
