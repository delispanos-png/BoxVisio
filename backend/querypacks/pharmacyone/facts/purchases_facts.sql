SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(ISNULL(S.CODE, F.TRDR) AS nvarchar(64)) AS supplier_external_id,
  CAST(
    (CASE WHEN ISNULL(F.SOSOURCE, 0) IN (1253) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.QTY1, ISNULL(L.QTY, 0))) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS qty,
  CAST(
    (CASE WHEN ISNULL(F.SOSOURCE, 0) IN (1253) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS net_amount,
  CAST(
    (CASE WHEN ISNULL(F.SOSOURCE, 0) IN (1253) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.SALESCVAL, ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)))) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS cost_amount,
  CAST('P|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,
  CAST(NULLIF(CAST(ISNULL(I.MTRMARK, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS brand_external_id,
  CAST(
    NULLIF(
      STUFF(
        (CASE WHEN ISNULL(I.CCC88POCAT1, 0) <> 0 THEN '|' + CAST(I.CCC88POCAT1 AS nvarchar(20)) ELSE '' END)
        + (CASE WHEN ISNULL(I.CCC88POCAT2, 0) <> 0 THEN '|' + CAST(I.CCC88POCAT2 AS nvarchar(20)) ELSE '' END)
        + (CASE WHEN ISNULL(I.CCC88POCAT3, 0) <> 0 THEN '|' + CAST(I.CCC88POCAT3 AS nvarchar(20)) ELSE '' END),
        1,
        1,
        ''
      ),
      ''
    ) AS nvarchar(64)
  ) AS category_external_id,
  CAST(NULLIF(CAST(ISNULL(I.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_external_id,
  CAST(ISNULL(MK.NAME, '') AS nvarchar(255)) AS brand_name,
  CAST(
    NULLIF(
      STUFF(
        (CASE WHEN ISNULL(PC1.NAME, N'') <> N'' THEN N' > ' + PC1.NAME ELSE N'' END)
        + (CASE WHEN ISNULL(PC2.NAME, N'') <> N'' THEN N' > ' + PC2.NAME ELSE N'' END)
        + (CASE WHEN ISNULL(PC3.NAME, N'') <> N'' THEN N' > ' + PC3.NAME ELSE N'' END),
        1,
        3,
        N''
      ),
      N''
    ) AS nvarchar(255)
  ) AS category_name,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST('purchase_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_type,
  CAST(ISNULL(S.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(S.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC1PRC, 0)) AS decimal(18,6)),
      0
    ) AS decimal(18,6)
  ) AS discount1_pct,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC2PRC, 0)) AS decimal(18,6)),
      0
    ) AS decimal(18,6)
  ) AS discount2_pct,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC3PRC, 0)) AS decimal(18,6)),
      0
    ) AS decimal(18,6)
  ) AS discount3_pct,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC1VAL, 0)) AS decimal(28,8)),
      0
    ) AS decimal(28,8)
  ) AS discount1_amount,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC2VAL, 0)) AS decimal(28,8)),
      0
    ) AS decimal(28,8)
  ) AS discount2_amount,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC3VAL, 0)) AS decimal(28,8)),
      0
    ) AS decimal(28,8)
  ) AS discount3_amount,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC1PRC, 0) + ISNULL(L.DISC2PRC, 0) + ISNULL(L.DISC3PRC, 0)) AS decimal(18,6)),
      0
    ) AS decimal(18,6)
  ) AS discount_pct,
  CAST(
    COALESCE(
      TRY_CAST(ABS(ISNULL(L.DISC1VAL, 0) + ISNULL(L.DISC2VAL, 0) + ISNULL(L.DISC3VAL, 0)) AS decimal(28,8)),
      0
    ) AS decimal(28,8)
  ) AS discount_amount,

  CAST(
    (CASE WHEN ISNULL(F.SOSOURCE, 0) IN (1253) THEN -1 ELSE 1 END)
    * COALESCE(TRY_CAST(ABS(ISNULL(L.VATAMNT, 0)) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS vat_amount,
  CAST(
    (CASE WHEN ISNULL(F.SOSOURCE, 0) IN (1253) THEN -1 ELSE 1 END)
    * (
      COALESCE(TRY_CAST(ABS(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(28,8)), 0)
      + COALESCE(TRY_CAST(ABS(ISNULL(L.VATAMNT, 0)) AS decimal(28,8)), 0)
    )
    AS decimal(28,8)
  ) AS gross_value,

  CAST(NULLIF(CAST(ISNULL(I.CCC88ECHANNEL, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS channel_ext_id,
  CAST(ISNULL(EC.NAME, '') AS nvarchar(255)) AS channel_name,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
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
LEFT JOIN TRDR S WITH (NOLOCK) ON S.TRDR = F.TRDR AND S.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK) ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN MTRMARK MK WITH (NOLOCK) ON MK.MTRMARK = I.MTRMARK AND MK.COMPANY = I.COMPANY
LEFT JOIN MTRGROUP MG WITH (NOLOCK) ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY
LEFT JOIN CCC88POCAT1 PC1 WITH (NOLOCK) ON PC1.CCC88POCAT1 = I.CCC88POCAT1
LEFT JOIN CCC88POCAT2 PC2 WITH (NOLOCK) ON PC2.CCC88POCAT2 = I.CCC88POCAT2
LEFT JOIN CCC88POCAT3 PC3 WITH (NOLOCK) ON PC3.CCC88POCAT3 = I.CCC88POCAT3
LEFT JOIN CCC88ECHANNEL EC WITH (NOLOCK) ON EC.CCC88ECHANNEL = TRY_CAST(I.CCC88ECHANNEL AS int)
LEFT JOIN BRANCH BR WITH (NOLOCK) ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN SERIES SR WITH (NOLOCK) ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
LEFT JOIN (
  SELECT DISTINCT SERIES, COMPANY
  FROM SERIES WITH (NOLOCK)
  WHERE ISNULL(NAME, N'') LIKE N'%Δαπαν%'
     OR ISNULL(NAME, N'') LIKE N'%Παραγγελ%'
     OR ISNULL(NAME, '') LIKE '%Order%'
     OR ISNULL(NAME, N'') LIKE N'%παροχής υπηρεσι%'
     OR ISNULL(NAME, N'') LIKE N'%Αγοράς Παγίων%'
     OR ISNULL(NAME, N'') LIKE N'%Λογαριασμ%'
) AS _excl_sr ON _excl_sr.SERIES = F.SERIES AND _excl_sr.COMPANY = F.COMPANY
LEFT JOIN (
  SELECT DISTINCT MTRL, COMPANY
  FROM MTRL WITH (NOLOCK)
  WHERE ISNULL(NAME, N'') LIKE N'%Δαπάν%'
     OR ISNULL(NAME, N'') LIKE N'%Έξοδ%'
     OR ISNULL(NAME, N'') LIKE N'%Πάγι%'
     OR ISNULL(NAME, N'') LIKE N'%Υπηρεσ%'
) AS _excl_it ON _excl_it.MTRL = L.MTRL AND _excl_it.COMPANY = F.COMPANY
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 12
  -- Purchase flows only (SoftOne):
  -- 1251 purchase invoice / receipt, 1253 purchase credit (negative sign above).
  -- Excludes supplier payments/transfers/expenses handled by other circuits.
  AND F.SOSOURCE IN (1251, 1253)
  AND ISNULL(F.TFPRMS, 0) NOT IN (100, 101, 154, 201, 202, 301, 500, 501)
  AND NOT (
    ISNULL(F.SERIES, 0) IN (1001, 1002, 1003, 1006, 1007, 1009, 1102, 3201)
    OR ISNULL(F.FINCODE, N'') LIKE N'ΠΑΡ%'
    OR ISNULL(F.FINCODE, '') LIKE 'PAR%'
    OR _excl_sr.SERIES IS NOT NULL
    OR _excl_it.MTRL IS NOT NULL
  )
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('P|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
