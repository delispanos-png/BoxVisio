SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(F.TRNDATE AS date) AS expense_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(S.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(S.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(NULL AS nvarchar(128)) AS account_id,
  CAST('operational' AS nvarchar(128)) AS expense_category_code,
  CAST(ISNULL(SR.NAME, N'Λειτουργικά Έξοδα') AS nvarchar(255)) AS expense_category_name,
  CAST(CAST(ISNULL(F.TFPRMS, 0) AS nvarchar(16)) + N' ' + ISNULL(SR.NAME, 'expense_' + CAST(ISNULL(F.SOSOURCE, 0) AS nvarchar(16))) AS nvarchar(128)) AS document_type,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST((CASE WHEN ISNULL(F.TFPRMS, 0) = 102 THEN -1 ELSE 1 END) * ABS(ISNULL(F.SUMAMNT, 0)) AS decimal(18,4)) AS amount_net,
  CAST((CASE WHEN ISNULL(F.TFPRMS, 0) = 102 THEN -1 ELSE 1 END) * ABS(ISNULL(F.VATAMNT, 0)) AS decimal(18,4)) AS amount_tax,
  CAST((CASE WHEN ISNULL(F.TFPRMS, 0) = 102 THEN -1 ELSE 1 END) * ABS(ISNULL(F.SUMAMNT, 0) + ISNULL(F.VATAMNT, 0)) AS decimal(18,4)) AS amount_gross,
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
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  F.SOSOURCE IN (1261, 1653)
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

UNION ALL

SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(F.TRNDATE AS date) AS expense_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(S.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(S.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(NULL AS nvarchar(128)) AS account_id,
  CAST('softone_series_' + CAST(ISNULL(F.SERIES, 0) AS nvarchar(32)) AS nvarchar(128)) AS expense_category_code,
  CAST(ISNULL(SR.NAME, N'Λοιπά Έξοδα / Δαπάνες') AS nvarchar(255)) AS expense_category_name,
  CAST(CAST(ISNULL(F.TFPRMS, 0) AS nvarchar(16)) + N' purchase_expense_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_type,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(18,4)), 0)
    AS decimal(18,4)
  ) AS amount_net,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.VATAMNT, 0)) AS decimal(18,4)), 0)
    AS decimal(18,4)
  ) AS amount_tax,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152) THEN -1 ELSE 1 END)
    * (
      COALESCE(TRY_CAST(ABS(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(18,4)), 0)
      + COALESCE(TRY_CAST(ABS(ISNULL(L.VATAMNT, 0)) AS decimal(18,4)), 0)
    )
    AS decimal(18,4)
  ) AS amount_gross,
  CAST('EXP|P|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
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
INNER JOIN MTRLINES L ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
LEFT JOIN MTRL I ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN TRDR S ON S.TRDR = F.TRDR AND S.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 12
  AND F.SOSOURCE IN (1251, 1253)
  AND (
    (ISNULL(F.SOSOURCE, 0) = 1253 AND ISNULL(F.TFPRMS, 0) = 101)
    OR ISNULL(F.SERIES, 0) IN (1001, 1002, 1003, 1006, 1007, 1009, 1102, 3201)
    OR ISNULL(SR.NAME, N'') LIKE N'%Δαπαν%'
    OR ISNULL(SR.NAME, N'') LIKE N'%παροχής υπηρεσι%'
    OR ISNULL(SR.NAME, N'') LIKE N'%Αγοράς Παγίων%'
    OR ISNULL(SR.NAME, N'') LIKE N'%Λογαριασμ%'
    OR ISNULL(I.NAME, N'') LIKE N'%Δαπάν%'
    OR ISNULL(I.NAME, N'') LIKE N'%Έξοδ%'
    OR ISNULL(I.NAME, N'') LIKE N'%Πάγι%'
    OR ISNULL(I.NAME, N'') LIKE N'%Υπηρεσ%'
  )
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('EXP|P|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
