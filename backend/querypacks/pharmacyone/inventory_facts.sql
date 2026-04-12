SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)) AS qty,
  CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)) AS qty_on_hand,
  CAST(0 AS decimal(18,4)) AS qty_reserved,
  CAST(ISNULL(L.COSTVAL, ISNULL(L.COSTVALUE, ISNULL(L.LCOST, ISNULL(L.COST, ISNULL(L.LINEVAL, ISNULL(L.NETLINEVAL, ISNULL(L.NETVAL, ISNULL(L.NETAMNT, 0)))))))) AS decimal(18,4)) AS cost_amount,
  CAST(ISNULL(L.LINEVAL, ISNULL(L.NETLINEVAL, ISNULL(L.NETVAL, ISNULL(L.NETAMNT, 0)))) AS decimal(18,4)) AS value_amount,
  CAST('I|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST(ISNULL(SR.NAME, 'inventory_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16))) AS nvarchar(255)) AS document_type,
  CAST(CASE WHEN ISNULL(L.QTY1, ISNULL(L.QTY, 0)) >= 0 THEN 'entry' ELSE 'exit' END AS nvarchar(32)) AS movement_type,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,

  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id,
  CAST(NULL AS bit) AS is_financial_doc,
  CAST(NULL AS nvarchar(32)) AS financial_impact_type,
  CAST(NULL AS decimal(18,4)) AS financial_value_amount
FROM FINDOC F
INNER JOIN MTRLINES L ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
LEFT JOIN MTRDOC MD ON MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
LEFT JOIN MTRL I ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
WHERE
  F.SOSOURCE IN (1151)
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('I|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
