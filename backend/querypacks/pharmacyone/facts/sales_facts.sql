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
  CAST(ISNULL(F.SOTIME, F.INSDATE) AS datetime2) AS source_created_at,
  CAST(NULL AS nvarchar(64)) AS brand_external_id,
  CAST(NULL AS nvarchar(64)) AS category_external_id,
  CAST(NULLIF(CAST(ISNULL(I.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_external_id,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST('sales_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_type,
  CAST(
    CASE
      WHEN ISNULL(F.ISCANCEL, 0) = 1 THEN N'Cancelled'
      WHEN ISNULL(F.FINSTATES, 0) IN (10) THEN N'Completed'
      WHEN ISNULL(F.FINSTATES, 0) IN (3, 4, 6) THEN N'Closed'
      WHEN ISNULL(F.FINSTATES, 0) IN (1, 2) THEN N'Open'
      ELSE N'Open'
    END AS nvarchar(128)
  ) AS document_status,
  CAST(ISNULL(C.CODE, F.TRDR) AS nvarchar(128)) AS customer_ext_id,
  CAST(ISNULL(C.NAME, '') AS nvarchar(255)) AS customer_name,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
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

  CAST(NULLIF(CAST(ISNULL(F.CCC88ECHANNEL, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS channel_ext_id,
  CAST(ISNULL(EC.NAME, '') AS nvarchar(255)) AS channel_name,
  CAST(
    COALESCE(
      NULLIF(PM.NAME, ''),
      NULLIF(PM.CODE, ''),
      NULLIF(CAST(F.PAYMENT AS nvarchar(128)), '0'),
      ''
    ) AS nvarchar(128)
  ) AS payment_method,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.TFPRMS, 0) AS int) AS source_transaction_type_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(ISNULL(WH.NAME, CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id
FROM FINDOC F WITH (NOLOCK)
INNER JOIN MTRLINES L WITH (NOLOCK) ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
OUTER APPLY (
  SELECT TOP 1 MD.WHOUSE
  FROM MTRDOC MD WITH (NOLOCK)
  WHERE MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
) MD
LEFT JOIN WHOUSE WH WITH (NOLOCK) ON WH.WHOUSE = MD.WHOUSE AND WH.COMPANY = F.COMPANY
LEFT JOIN TRDR C WITH (NOLOCK) ON C.TRDR = F.TRDR AND C.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK) ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN MTRGROUP MG WITH (NOLOCK) ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK) ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR WITH (NOLOCK) ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
OUTER APPLY (
  SELECT TOP 1 E.CCC88ECHANNEL, E.NAME
  FROM CCC88ECHANNEL E WITH (NOLOCK)
  WHERE E.CCC88ECHANNEL = TRY_CAST(F.CCC88ECHANNEL AS int)
    AND (E.COMPANY = F.COMPANY OR E.COMPANY = 1001)
  ORDER BY CASE WHEN E.COMPANY = F.COMPANY THEN 0 ELSE 1 END
) EC
OUTER APPLY (
  SELECT TOP 1 P.CODE, P.NAME
  FROM PAYMENT P WITH (NOLOCK)
  WHERE P.PAYMENT = F.PAYMENT
    AND P.SODTYPE = F.SODTYPE
    AND (P.COMPANY = F.COMPANY OR P.COMPANY = 1000)
  ORDER BY CASE WHEN P.COMPANY = F.COMPANY THEN 0 ELSE 1 END
) PM
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 13
  AND ISNULL(F.SOSOURCE, 0) = 1351
  -- SOSOURCE 1351 is the SoftOne POS/retail sales module. The JS bridge (boxvisio_bi_bridge.js)
  -- references "1351,11351" in its metadata but its WHERE clause also hardcodes =1351.
  -- SOSOURCE 11351 appears in SoftOne for certain multi-company configurations but has not been
  -- confirmed in pharmacy tenant data. If a tenant requires it, update the query template via
  -- the tenant's stream_query configuration rather than changing this global querypack.
  AND ISNULL(F.SOREDIR, 0) IN (0, 10000)
  -- Sales/revenue behaviors only (SoftOne):
  -- 102 invoice, 103 delivery note, 131 retail receipt, 151/152/181 credits (negative sign above).
  -- Excludes orders/movements (e.g. 201/101/104) and TFPRMS=100 (prescription co-pay), which is
  -- excluded by the SoftOne bridge and does NOT appear in the pharmacy's retail sales stream.
  AND ISNULL(F.TFPRMS, 0) IN (102, 103, 131, 151, 152, 181)
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
