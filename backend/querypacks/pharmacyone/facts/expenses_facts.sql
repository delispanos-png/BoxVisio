SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(F.TRNDATE AS date) AS expense_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(S.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(S.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(NULL AS nvarchar(128)) AS account_id,
  CAST('operational' AS nvarchar(128)) AS expense_category_code,
  CAST(ISNULL(SR.NAME, 'expense_' + CAST(ISNULL(F.SOSOURCE, 0) AS nvarchar(16))) AS nvarchar(128)) AS document_type,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(ABS(ISNULL(F.SUMAMNT, 0)) AS decimal(18,4)) AS amount_net,
  CAST(ABS(ISNULL(F.VATAMNT, 0)) AS decimal(18,4)) AS amount_tax,
  CAST(ABS(ISNULL(F.SUMAMNT, 0) + ISNULL(F.VATAMNT, 0)) AS decimal(18,4)) AS amount_gross,
  CAST('EXP|' + CAST(F.FINDOC AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,

  CAST(F.FINDOC AS nvarchar(40)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST('EUR' AS nvarchar(3)) AS currency
FROM FINDOC F
LEFT JOIN TRDR S ON S.TRDR = F.TRDR AND S.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
WHERE
  F.SOSOURCE IN (1261)
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('EXP|' + CAST(F.FINDOC AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
