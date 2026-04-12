SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(
    CASE
      WHEN F.SOSOURCE = 1381 THEN 'customer_collections'
      WHEN F.SOSOURCE = 1413 THEN 'customer_transfers'
      WHEN F.SOSOURCE = 1281 THEN 'supplier_payments'
      WHEN F.SOSOURCE = 1412 THEN 'supplier_transfers'
      WHEN F.SOSOURCE IN (1414, 1481) THEN 'financial_accounts'
      WHEN F.SOSOURCE = 1261 THEN 'supplier_payments'
      WHEN ISNULL(F.SODTYPE, 0) = 13 THEN 'customer_collections'
      WHEN ISNULL(F.SODTYPE, 0) = 12 THEN 'supplier_payments'
      ELSE 'financial_accounts'
    END AS nvarchar(32)
  ) AS entry_type,
  CAST(ABS(ISNULL(F.SUMAMNT, 0)) AS decimal(18,4)) AS amount,
  CAST('EUR' AS nvarchar(3)) AS currency,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(64)) AS reference_no,
  CAST(ISNULL(F.COMMENTS, '') AS nvarchar(255)) AS notes,
  CAST('C|' + CAST(F.FINDOC AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,

  CAST(F.FINDOC AS nvarchar(40)) AS event_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS transaction_id,
  CAST(
    CASE
      WHEN F.SOSOURCE IN (1381, 1413) THEN 'inflow'
      WHEN F.SOSOURCE IN (1281, 1412, 1261) THEN 'outflow'
      WHEN F.SOSOURCE IN (1414, 1481) THEN 'transfer'
      WHEN ISNULL(F.SODTYPE, 0) = 13 THEN 'inflow'
      WHEN ISNULL(F.SODTYPE, 0) = 12 THEN 'outflow'
      WHEN ISNULL(F.SODTYPE, 0) = 14 THEN 'transfer'
      WHEN ISNULL(F.SUMAMNT, 0) < 0 THEN 'outflow'
      ELSE 'inflow'
    END AS nvarchar(64)
  ) AS transaction_type,
  CAST(
    CASE
      WHEN F.SOSOURCE = 1381 THEN 'customer_collections'
      WHEN F.SOSOURCE = 1413 THEN 'customer_transfers'
      WHEN F.SOSOURCE = 1281 THEN 'supplier_payments'
      WHEN F.SOSOURCE = 1412 THEN 'supplier_transfers'
      WHEN F.SOSOURCE IN (1414, 1481) THEN 'financial_accounts'
      WHEN F.SOSOURCE = 1261 THEN 'supplier_payments'
      WHEN ISNULL(F.SODTYPE, 0) = 13 THEN 'customer_collections'
      WHEN ISNULL(F.SODTYPE, 0) = 12 THEN 'supplier_payments'
      ELSE 'financial_accounts'
    END AS nvarchar(32)
  ) AS subcategory,
  CAST(NULL AS nvarchar(128)) AS account_id,
  CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(128)) AS counterparty_id,
  CAST(ISNULL(T.NAME, '') AS nvarchar(255)) AS counterparty_name,
  CAST(
    CASE
      WHEN ISNULL(F.SODTYPE, 0) = 13 THEN 'customer'
      WHEN ISNULL(F.SODTYPE, 0) = 12 THEN 'supplier'
      WHEN ISNULL(F.SODTYPE, 0) = 14 THEN 'internal'
      ELSE 'internal'
    END AS nvarchar(32)
  ) AS counterparty_type,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id,
  CAST(ISNULL(F.SUMAMNT, 0) AS decimal(18,4)) AS amount_raw
FROM FINDOC F
LEFT JOIN TRDR T ON T.TRDR = F.TRDR AND T.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
WHERE
  F.SOSOURCE IN (1381, 1281, 1412, 1413, 1414, 1481)
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('C|' + CAST(F.FINDOC AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
