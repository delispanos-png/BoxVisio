-- pharmacy295 cashflow supplier reconciliation
-- Purpose:
-- 1) Detect missing supplier payment documents in fact_cashflows
-- 2) Compare staging vs fact classification for supplier-related cash docs
--
-- Usage:
-- docker compose exec -T postgres psql -U postgres -d bi_tenant_pharmacy295 -v from='2026-01-01' -v to='2026-04-24' -f backend/querypacks/pharmacyone/kpi_validation/pharmacy295_cashflow_supplier_reconciliation.sql

\echo '== FACT subcategory summary =='
SELECT
  COALESCE(subcategory, '(null)') AS subcategory,
  COUNT(*) AS rows_count,
  ROUND(SUM(amount)::numeric, 2) AS amount_sum
FROM fact_cashflows
WHERE doc_date BETWEEN :'from'::date AND :'to'::date
GROUP BY 1
ORDER BY rows_count DESC, subcategory;

\echo '== STAGING supplier-like docs by source_module_id and mapped subcategory =='
WITH stg AS (
  SELECT
    COALESCE((source_payload_json->>'doc_date')::date, doc_date) AS doc_date,
    source_payload_json->>'external_id' AS external_id,
    COALESCE(NULLIF(LOWER(source_payload_json->>'subcategory'), ''), '(null)') AS subcategory,
    COALESCE((source_payload_json->>'source_module_id')::int, -1) AS source_module_id,
    COALESCE((source_payload_json->>'source_entity_id')::int, -1) AS source_entity_id,
    COALESCE((source_payload_json->>'amount')::numeric, 0) AS amount
  FROM stg_cash_transactions
  WHERE transform_status = 'loaded'
)
SELECT
  source_module_id,
  source_entity_id,
  subcategory,
  COUNT(*) AS rows_count,
  ROUND(SUM(amount)::numeric, 2) AS amount_sum
FROM stg
WHERE doc_date BETWEEN :'from'::date AND :'to'::date
  AND (
    source_module_id IN (1261, 1281, 1412, 1415, 1416)
    OR subcategory IN ('supplier_payments', 'supplier_transfers')
  )
GROUP BY source_module_id, source_entity_id, subcategory
ORDER BY source_module_id, source_entity_id, subcategory;

\echo '== STAGING supplier docs missing in FACT by external_id =='
WITH stg_supplier AS (
  SELECT DISTINCT
    source_payload_json->>'external_id' AS external_id,
    COALESCE((source_payload_json->>'doc_date')::date, doc_date) AS doc_date,
    COALESCE((source_payload_json->>'source_module_id')::int, -1) AS source_module_id,
    COALESCE(NULLIF(source_payload_json->>'reference_no', ''), source_payload_json->>'transaction_id', '') AS reference_no,
    COALESCE((source_payload_json->>'amount')::numeric, 0) AS amount
  FROM stg_cash_transactions
  WHERE transform_status = 'loaded'
    AND source_payload_json->>'external_id' IS NOT NULL
    AND (
      COALESCE((source_payload_json->>'source_module_id')::int, -1) IN (1261, 1281, 1412, 1415, 1416)
      OR LOWER(COALESCE(source_payload_json->>'subcategory', '')) IN ('supplier_payments', 'supplier_transfers')
    )
    AND COALESCE((source_payload_json->>'doc_date')::date, doc_date) BETWEEN :'from'::date AND :'to'::date
),
fact_ids AS (
  SELECT DISTINCT external_id
  FROM fact_cashflows
  WHERE doc_date BETWEEN :'from'::date AND :'to'::date
)
SELECT
  s.doc_date,
  s.source_module_id,
  s.reference_no,
  s.external_id,
  ROUND(s.amount::numeric, 2) AS amount
FROM stg_supplier s
LEFT JOIN fact_ids f ON f.external_id = s.external_id
WHERE f.external_id IS NULL
ORDER BY s.doc_date DESC, s.source_module_id, s.reference_no
LIMIT 300;
