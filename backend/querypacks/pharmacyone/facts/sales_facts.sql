SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(NULL AS nvarchar(64)) AS supplier_external_id,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS qty,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS net_amount,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ISNULL(L.SALESCVAL, ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS cost_amount,
  CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,
  CAST(NULL AS nvarchar(64)) AS brand_external_id,
  CAST(NULL AS nvarchar(64)) AS category_external_id,
  CAST(NULL AS nvarchar(64)) AS group_external_id,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST('sales_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_type,
  CAST(ISNULL(C.CODE, F.TRDR) AS nvarchar(128)) AS customer_ext_id,
  CAST(ISNULL(C.NAME, '') AS nvarchar(255)) AS customer_name,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(0 AS decimal(18,6)) AS discount_pct,
  CAST(0 AS decimal(28,8)) AS discount_amount,

  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ISNULL(L.VATAMNT, 0) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS vat_amount,
  CAST(
    (CASE WHEN ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1 ELSE 1 END)
    *
    COALESCE(TRY_CAST(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS decimal(28,8)), 0)
    + COALESCE(TRY_CAST(ISNULL(L.VATAMNT, 0) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS gross_value,

  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.TFPRMS, 0) AS int) AS source_transaction_type_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id
FROM FINDOC F
INNER JOIN MTRLINES L ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
LEFT JOIN MTRDOC MD ON MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
LEFT JOIN TRDR C ON C.TRDR = F.TRDR AND C.COMPANY = F.COMPANY
LEFT JOIN MTRL I ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
WHERE
  ISNULL(F.SODTYPE, 0) = 13
  AND ISNULL(F.SOSOURCE, 0) = 1351
  AND ISNULL(F.SOREDIR, 0) IN (0, 10000)
  -- Sales/revenue behaviors only (SoftOne):
  -- 102 invoice, 131 retail receipt, 151/152/181 credits (negative sign above).
  -- Excludes orders/movements (e.g. 201/101/104) that inflate turnover KPIs.
  AND ISNULL(F.TFPRMS, 0) IN (102, 131, 151, 152, 181)
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
