-- pharmacy295 purchases reconciliation
--
-- Purpose:
-- 1) Validate purchases dashboard totals against canonical facts.
-- 2) Compare exact cutoff windows (YTD / month / week) with reference outputs.
-- 3) Isolate differences by branch, SoftOne SOSOURCE/object_id, and document series.
-- 4) Certify that no duplicate rows were loaded.
--
-- Usage:
-- docker compose exec -T postgres psql -U postgres -d bi_tenant_pharmacy295 -f backend/querypacks/pharmacyone/kpi_validation/pharmacy295_purchases_reconciliation.sql
--
-- Adjust the dates below when reconciling a different cutoff.

\echo '=== FACTS VS AGGREGATES (same cutoff) ==='
WITH f AS (
  SELECT
    COALESCE(SUM(net_value), 0) AS net_value,
    COALESCE(SUM(cost_amount), 0) AS cost_amount,
    COALESCE(SUM(qty), 0) AS qty
  FROM fact_purchases
  WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-12'
),
a AS (
  SELECT
    COALESCE(SUM(net_value), 0) AS net_value,
    COALESCE(SUM(cost_amount), 0) AS cost_amount,
    COALESCE(SUM(qty), 0) AS qty
  FROM agg_purchases_daily
  WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-12'
)
SELECT
  ROUND(f.net_value::numeric, 2) AS fact_ytd_net_value,
  ROUND(a.net_value::numeric, 2) AS agg_ytd_net_value,
  ROUND((f.net_value - a.net_value)::numeric, 2) AS delta_net_value,
  ROUND(f.cost_amount::numeric, 2) AS fact_ytd_cost_amount,
  ROUND(a.cost_amount::numeric, 2) AS agg_ytd_cost_amount,
  ROUND((f.cost_amount - a.cost_amount)::numeric, 2) AS delta_cost_amount,
  ROUND(f.qty::numeric, 2) AS fact_ytd_qty,
  ROUND(a.qty::numeric, 2) AS agg_ytd_qty,
  ROUND((f.qty - a.qty)::numeric, 2) AS delta_qty
FROM f, a;

\echo '=== KPI TOTALS BY WINDOW (facts) ==='
WITH w AS (
  SELECT
    DATE '2026-04-12' AS anchor_date,
    DATE_TRUNC('week', DATE '2026-04-12')::date AS week_from,
    DATE_TRUNC('month', DATE '2026-04-12')::date AS month_from,
    DATE_TRUNC('year', DATE '2026-04-12')::date AS year_from
)
SELECT
  'day' AS window_name,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2) AS qty
FROM fact_purchases, w
WHERE doc_date = anchor_date
UNION ALL
SELECT
  'week',
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2),
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2),
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2)
FROM fact_purchases, w
WHERE doc_date BETWEEN week_from AND anchor_date
UNION ALL
SELECT
  'month',
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2),
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2),
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2)
FROM fact_purchases, w
WHERE doc_date BETWEEN month_from AND anchor_date
UNION ALL
SELECT
  'year',
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2),
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2),
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2)
FROM fact_purchases, w
WHERE doc_date BETWEEN year_from AND anchor_date
ORDER BY 1;

\echo '=== MONTHLY TOTALS (dashboard layer) ==='
SELECT
  DATE_TRUNC('month', doc_date)::date AS month_start,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2) AS qty
FROM agg_purchases_daily
WHERE doc_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-12'
GROUP BY 1
ORDER BY 1;

\echo '=== APRIL MONTH BREAKDOWN BY BRANCH (dashboard layer) ==='
SELECT
  COALESCE(b.name, a.branch_ext_id, 'N/A') AS branch,
  ROUND(COALESCE(SUM(a.net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(a.cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(a.qty), 0)::numeric, 2) AS qty
FROM agg_purchases_daily a
LEFT JOIN dim_branches b
  ON b.external_id = a.branch_ext_id
WHERE a.doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-12'
GROUP BY 1
ORDER BY 2 DESC;

\echo '=== APRIL MONTH BREAKDOWN BY BRANCH (branch aggregate layer) ==='
SELECT
  COALESCE(b.name, a.branch_ext_id, 'N/A') AS branch,
  ROUND(COALESCE(SUM(a.net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(a.cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(a.qty), 0)::numeric, 2) AS qty
FROM agg_purchases_daily_branch a
LEFT JOIN dim_branches b
  ON b.external_id = a.branch_ext_id
WHERE a.doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-12'
GROUP BY 1
ORDER BY 2 DESC;

\echo '=== APRIL BREAKDOWN BY SOFTONE SOSOURCE / OBJECT_ID (facts) ==='
SELECT
  source_module_id AS sosource,
  object_id,
  document_type,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2) AS qty,
  COUNT(DISTINCT document_id) AS documents
FROM fact_purchases
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-12'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 4 DESC;

\echo '=== APRIL BREAKDOWN BY BRANCH + SOFTONE SOSOURCE (facts) ==='
SELECT
  COALESCE(source_payload_json->>'branch_name', branch_ext_id, 'N/A') AS branch,
  source_module_id AS sosource,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2) AS qty
FROM fact_purchases
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-12'
GROUP BY 1, 2
ORDER BY 1, 2;

\echo '=== APRIL BREAKDOWN BY SERIES (facts) ==='
SELECT
  document_series,
  COALESCE(source_payload_json->>'document_series_name', '') AS series_name,
  source_module_id AS sosource,
  ROUND(COALESCE(SUM(net_value), 0)::numeric, 2) AS net_value,
  ROUND(COALESCE(SUM(cost_amount), 0)::numeric, 2) AS cost_amount,
  ROUND(COALESCE(SUM(qty), 0)::numeric, 2) AS qty,
  COUNT(DISTINCT document_id) AS documents
FROM fact_purchases
WHERE doc_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-12'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

\echo '=== DUPLICATE CHECKS ==='
SELECT
  'duplicate_external_id' AS check_name,
  COUNT(*)::bigint AS duplicate_groups
FROM (
  SELECT external_id
  FROM fact_purchases
  GROUP BY external_id
  HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
  'duplicate_event_id' AS check_name,
  COUNT(*)::bigint AS duplicate_groups
FROM (
  SELECT event_id
  FROM fact_purchases
  GROUP BY event_id
  HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
  'duplicate_document_item_qty_value' AS check_name,
  COUNT(*)::bigint AS duplicate_groups
FROM (
  SELECT
    document_id,
    item_code,
    doc_date,
    qty,
    net_value
  FROM fact_purchases
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(*) > 1
) d;
