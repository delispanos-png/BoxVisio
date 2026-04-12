-- pharmacy295 turnover reconciliation
--
-- Purpose:
-- 1) Validate dashboard totals against canonical facts.
-- 2) Compare exact cutoff windows (YTD / month / week) with reference screenshots / baseline outputs.
-- 3) Isolate differences by branch, SoftOne behavior (TFPRMS), and document series.
-- 4) Certify that no duplicate rows were loaded.
--
-- Usage:
-- docker compose exec -T postgres psql -U postgres -d bi_tenant_pharmacy295 -f backend/querypacks/pharmacyone/kpi_validation/pharmacy295_turnover_reconciliation.sql
--
-- Adjust the dates below when reconciling a different cutoff.

\echo '=== FACTS VS AGGREGATES (same cutoff) ==='
WITH f AS (
  SELECT COALESCE(SUM(net_value), 0) AS net_value
  FROM fact_sales
  WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-10'
),
a AS (
  SELECT COALESCE(SUM(net_value), 0) AS net_value
  FROM agg_sales_daily
  WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-10'
)
SELECT
  ROUND(f.net_value::numeric, 2) AS fact_ytd_0410,
  ROUND(a.net_value::numeric, 2) AS agg_ytd_0410,
  ROUND((f.net_value - a.net_value)::numeric, 2) AS delta
FROM f, a;

\echo '=== KPI TOTALS BY WINDOW (facts) ==='
WITH w AS (
  SELECT
    DATE '2026-04-10' AS anchor_date,
    DATE_TRUNC('week', DATE '2026-04-10')::date AS week_from,
    DATE_TRUNC('month', DATE '2026-04-10')::date AS month_from,
    DATE_TRUNC('year', DATE '2026-04-10')::date AS year_from
)
SELECT 'day' AS window_name, ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value
FROM fact_sales, w
WHERE doc_date = anchor_date
UNION ALL
SELECT 'week', ROUND(COALESCE(SUM(net_value), 0)::numeric, 2)
FROM fact_sales, w
WHERE doc_date BETWEEN week_from AND anchor_date
UNION ALL
SELECT 'month', ROUND(COALESCE(SUM(net_value), 0)::numeric, 2)
FROM fact_sales, w
WHERE doc_date BETWEEN month_from AND anchor_date
UNION ALL
SELECT 'year', ROUND(COALESCE(SUM(net_value), 0)::numeric, 2)
FROM fact_sales, w
WHERE doc_date BETWEEN year_from AND anchor_date
ORDER BY 1;

\echo '=== MONTHLY TOTALS (dashboard layer) ==='
SELECT
  DATE_TRUNC('month', doc_date)::date AS month_start,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value
FROM agg_sales_daily
WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-10'
GROUP BY 1
ORDER BY 1;

\echo '=== APRIL MONTH BREAKDOWN BY BRANCH (dashboard layer) ==='
SELECT
  COALESCE(b.name, a.branch_ext_id, 'N/A') AS branch,
  ROUND(COALESCE(SUM(a.net_value), 0)::numeric, 2) AS net_value
FROM agg_sales_daily a
LEFT JOIN dim_branches b
  ON b.external_id = a.branch_ext_id
WHERE a.doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-10'
GROUP BY 1
ORDER BY 2 DESC;

\echo '=== APRIL BREAKDOWN BY SOFTONE BEHAVIOR (facts) ==='
SELECT
  (source_payload_json->>'source_transaction_type_id')::int AS tfprms,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value
FROM fact_sales
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-10'
GROUP BY 1
ORDER BY 1;

\echo '=== APRIL BREAKDOWN BY BRANCH + SOFTONE BEHAVIOR (facts) ==='
SELECT
  COALESCE(source_payload_json->>'branch_name', 'N/A') AS branch,
  (source_payload_json->>'source_transaction_type_id')::int AS tfprms,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value
FROM fact_sales
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-10'
GROUP BY 1, 2
ORDER BY 1, 2;

\echo '=== APRIL TFPRMS=102 BREAKDOWN BY SERIES (facts) ==='
SELECT
  document_series,
  COALESCE(source_payload_json->>'document_series_name', '') AS series_name,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  COUNT(DISTINCT document_id) AS documents
FROM fact_sales
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-10'
  AND (source_payload_json->>'source_transaction_type_id')::int = 102
GROUP BY 1, 2
ORDER BY 3 DESC;

\echo '=== DUPLICATE CHECKS ==='
SELECT
  'duplicate_external_id' AS check_name,
  COUNT(*)::bigint AS duplicate_groups
FROM (
  SELECT external_id
  FROM fact_sales
  GROUP BY external_id
  HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
  'duplicate_event_id' AS check_name,
  COUNT(*)::bigint AS duplicate_groups
FROM (
  SELECT event_id
  FROM fact_sales
  GROUP BY event_id
  HAVING COUNT(*) > 1
) d;
