SELECT
  CAST(COALESCE(@to_date, GETDATE()) AS date) AS doc_date,
  CAST(
    CAST(ISNULL(S.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))) AS nvarchar(32))
    AS nvarchar(64)
  ) AS branch_external_id,
  CAST(ISNULL(I.CODE, S.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(S.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0) AS decimal(18,4)) AS qty,
  CAST(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0) AS decimal(18,4)) AS qty_on_hand,
  CAST(0 AS decimal(18,4)) AS qty_reserved,
  CAST(ISNULL(S.IMPVAL, 0) - ISNULL(S.EXPVAL, 0) AS decimal(18,4)) AS cost_amount,
  CAST(
    (ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) * ISNULL(I.PRICEW, 0)
    AS decimal(18,4)
  ) AS value_amount,
  CAST(
    (ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) * ISNULL(I.PRICER, 0)
    AS decimal(18,4)
  ) AS retail_value_amount,
  CAST(
    'IS|' + CAST(ISNULL(S.COMPANY, 0) AS nvarchar(32)) + '|' + CAST(ISNULL(S.FISCPRD, 0) AS nvarchar(16)) + '|'
    + CAST(ISNULL(S.WHOUSE, 0) AS nvarchar(32)) + '|' + CAST(ISNULL(S.MTRL, 0) AS nvarchar(40))
    AS nvarchar(128)
  ) AS external_id,
  CAST(COALESCE(@to_date, GETDATE()) AS datetime2) AS updated_at,

  CAST(
    CAST(ISNULL(S.COMPANY, 0) AS nvarchar(32)) + '-' + CAST(ISNULL(S.FISCPRD, 0) AS nvarchar(16)) + '-'
    + CAST(ISNULL(S.WHOUSE, 0) AS nvarchar(32)) + '-' + CAST(ISNULL(S.MTRL, 0) AS nvarchar(40))
    AS nvarchar(128)
  ) AS event_id,
  CAST('INVSNAP-' + CAST(ISNULL(S.FISCPRD, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_id,
  CAST('Inventory Snapshot ' + CAST(ISNULL(S.FISCPRD, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_no,
  CAST('SNAPSHOT' AS nvarchar(128)) AS document_series,
  CAST('Υπόλοιπο Αποθέματος' AS nvarchar(255)) AS document_series_name,
  CAST('Υπόλοιπο Αποθέματος' AS nvarchar(255)) AS document_type,
  CAST('snapshot' AS nvarchar(32)) AS movement_type,
  CAST(ISNULL(I.CODE, S.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.SODTYPE, 0) AS int) AS softone_sotype,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(ISNULL(W.NAME, CAST(S.WHOUSE AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name,
  CAST(ISNULL(BR.NAME, CAST(ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0)) AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(NULLIF(ISNULL(I.CODE1, ''), '') AS nvarchar(128)) AS barcode,
  CAST(NULL AS nvarchar(1024)) AS alternate_barcodes,
  CAST(NULLIF(CAST(ISNULL(I.MTRMARK, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS brand_external_id,
  CAST(ISNULL(MK.NAME, '') AS nvarchar(255)) AS brand_name,
  CAST(I.MTRMANFCTR AS nvarchar(128)) AS manufacturer_code,
  CAST(ISNULL(MF.NAME, '') AS nvarchar(255)) AS manufacturer_name,
  TRY_CAST(I.VAT AS decimal(18,4)) AS vat_rate,
  CAST(ISNULL(VT.NAME, '') AS nvarchar(255)) AS vat_label,
  CAST(ISNULL(CG.NAME, '') AS nvarchar(255)) AS commercial_category,
  CAST(NULLIF(CAST(ISNULL(I.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_external_id,
  CAST(ISNULL(PC1.NAME, '') AS nvarchar(255)) AS category_1,
  CAST(ISNULL(PC2.NAME, '') AS nvarchar(255)) AS category_2,
  CAST(ISNULL(PC3.NAME, '') AS nvarchar(255)) AS category_3,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
  CAST(ISNULL(I.ISACTIVE, 1) AS bit) AS is_active_source,
  CAST(ISNULL(I.COMPANY, S.COMPANY) AS nvarchar(64)) AS company_id,
  CAST(1 AS bit) AS is_financial_doc,
  CAST('stock_balance' AS nvarchar(32)) AS financial_impact_type,
  CAST(ISNULL(S.IMPVAL, 0) - ISNULL(S.EXPVAL, 0) AS decimal(18,4)) AS financial_value_amount,
  CAST(S.FISCPRD AS int) AS source_module_id,
  CAST(0 AS int) AS redirect_module_id,
  CAST(12 AS int) AS source_entity_id,
  CAST(S.FISCPRD AS int) AS object_id
FROM MTRBALSHEET S WITH (NOLOCK)
INNER JOIN (
  SELECT COMPANY, MAX(FISCPRD) AS FISCPRD
  FROM MTRBALSHEET WITH (NOLOCK)
  WHERE (@company_id IS NULL OR COMPANY = @company_id)
    AND PERIOD = 0
  GROUP BY COMPANY
) _sp ON _sp.COMPANY = S.COMPANY AND _sp.FISCPRD = S.FISCPRD AND S.PERIOD = 0
INNER JOIN (
  SELECT
    COMPANY,
    FISCPRD,
    MTRL
  FROM MTRBALSHEET WITH (NOLOCK)
  WHERE (@company_id IS NULL OR COMPANY = @company_id)
    AND PERIOD = 0
  GROUP BY COMPANY, FISCPRD, MTRL
  HAVING ABS(SUM(ISNULL(IMPQTY1, 0) - ISNULL(EXPQTY1, 0))) > 0.0001
) _item_stock
  ON _item_stock.COMPANY = S.COMPANY
 AND _item_stock.FISCPRD = S.FISCPRD
 AND _item_stock.MTRL = S.MTRL
LEFT JOIN WHOUSE W WITH (NOLOCK)
  ON W.WHOUSE = S.WHOUSE
 AND W.COMPANY = S.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK)
  ON BR.BRANCH = ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))
 AND BR.COMPANY = S.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK)
  ON I.MTRL = S.MTRL
 AND I.COMPANY = S.COMPANY
LEFT JOIN MTRMANFCTR MF WITH (NOLOCK)
  ON MF.MTRMANFCTR = I.MTRMANFCTR
 AND MF.COMPANY = I.COMPANY
LEFT JOIN MTRMARK MK WITH (NOLOCK)
  ON MK.MTRMARK = I.MTRMARK
 AND MK.COMPANY = I.COMPANY
LEFT JOIN MTRGROUP MG WITH (NOLOCK)
  ON MG.MTRGROUP = I.MTRGROUP
 AND MG.COMPANY = I.COMPANY
LEFT JOIN VAT VT WITH (NOLOCK)
  ON VT.VAT = I.VAT
LEFT JOIN CCC88POCAT1 PC1 WITH (NOLOCK)
  ON PC1.CCC88POCAT1 = I.CCC88POCAT1
LEFT JOIN CCC88POCAT2 PC2 WITH (NOLOCK)
  ON PC2.CCC88POCAT2 = I.CCC88POCAT2
LEFT JOIN CCC88POCAT3 PC3 WITH (NOLOCK)
  ON PC3.CCC88POCAT3 = I.CCC88POCAT3
LEFT JOIN MTRPCATEGORY CG WITH (NOLOCK)
  ON CG.MTRPCATEGORY = I.MTRPCATEGORY
 AND CG.COMPANY = I.COMPANY
WHERE
  (@company_id IS NULL OR S.COMPANY = @company_id)
  AND ISNULL(W.ISACTIVE, 1) = 1
  AND ISNULL(I.SODTYPE, 0) = 51
  AND ABS(ISNULL(S.IMPQTY1, 0) - ISNULL(S.EXPQTY1, 0)) > 0.0001
  -- Snapshot reflects current stock. Only run on first-ever sync (no prior sync state)
  -- or explicit backfill to today. Skip on incremental syncs where prior state exists,
  -- because movements already capture changes; snapshot re-runs only via full backfill.
  AND (@to_date IS NULL OR CAST(@to_date AS date) >= CAST(GETDATE() AS date))
  AND @last_sync_ts IS NULL

UNION ALL

SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(
    CASE
      WHEN COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0) >= 0 THEN COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0)
      ELSE COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0)
    END
    AS decimal(18,4)
  ) AS qty,
  CAST(
    CASE
      WHEN COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0) >= 0 THEN COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0)
      ELSE COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0)
    END
    AS decimal(18,4)
  ) AS qty_on_hand,
  CAST(0 AS decimal(18,4)) AS qty_reserved,
  CAST(COALESCE(TRY_CAST(ISNULL(L.SALESCVAL, ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(18,4)), 0) AS decimal(18,4)) AS cost_amount,
  CAST(COALESCE(TRY_CAST(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS decimal(18,4)), 0) AS decimal(18,4)) AS value_amount,
  CAST(COALESCE(TRY_CAST(ISNULL(L.PRICE, ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(18,4)), 0) * COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0) AS decimal(18,4)) AS retail_value_amount,
  CAST('IW|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST(ISNULL(SR.NAME, N'Κίνηση Αποθήκης') AS nvarchar(255)) AS document_type,
  CAST(
    CASE
      WHEN COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)), 0) >= 0 THEN 'entry'
      ELSE 'exit'
    END
    AS nvarchar(32)
  ) AS movement_type,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.SODTYPE, 0) AS int) AS softone_sotype,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(ISNULL(WH.NAME, CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(NULLIF(ISNULL(I.CODE1, ''), '') AS nvarchar(128)) AS barcode,
  CAST(NULL AS nvarchar(1024)) AS alternate_barcodes,
  CAST(NULLIF(CAST(ISNULL(I.MTRMARK, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS brand_external_id,
  CAST(ISNULL(MK.NAME, '') AS nvarchar(255)) AS brand_name,
  CAST(I.MTRMANFCTR AS nvarchar(128)) AS manufacturer_code,
  CAST(ISNULL(MF.NAME, '') AS nvarchar(255)) AS manufacturer_name,
  TRY_CAST(I.VAT AS decimal(18,4)) AS vat_rate,
  CAST(ISNULL(VT.NAME, '') AS nvarchar(255)) AS vat_label,
  CAST(ISNULL(CG.NAME, '') AS nvarchar(255)) AS commercial_category,
  CAST(NULLIF(CAST(ISNULL(I.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_external_id,
  CAST(ISNULL(PC1.NAME, '') AS nvarchar(255)) AS category_1,
  CAST(ISNULL(PC2.NAME, '') AS nvarchar(255)) AS category_2,
  CAST(ISNULL(PC3.NAME, '') AS nvarchar(255)) AS category_3,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
  CAST(ISNULL(I.ISACTIVE, 1) AS bit) AS is_active_source,
  CAST(ISNULL(I.COMPANY, F.COMPANY) AS nvarchar(64)) AS company_id,
  CAST(0 AS bit) AS is_financial_doc,
  CAST('inventory_adjustment' AS nvarchar(32)) AS financial_impact_type,
  CAST(0 AS decimal(18,4)) AS financial_value_amount,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id
FROM FINDOC F WITH (NOLOCK)
INNER JOIN MTRLINES L WITH (NOLOCK) ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
OUTER APPLY (
  SELECT TOP 1 MD.WHOUSE
  FROM MTRDOC MD WITH (NOLOCK)
  WHERE MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
) MD
LEFT JOIN WHOUSE WH WITH (NOLOCK) ON WH.WHOUSE = MD.WHOUSE AND WH.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK) ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK) ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN MTRMANFCTR MF WITH (NOLOCK) ON MF.MTRMANFCTR = I.MTRMANFCTR AND MF.COMPANY = I.COMPANY
LEFT JOIN MTRMARK MK WITH (NOLOCK) ON MK.MTRMARK = I.MTRMARK AND MK.COMPANY = I.COMPANY
LEFT JOIN MTRGROUP MG WITH (NOLOCK) ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY
LEFT JOIN VAT VT WITH (NOLOCK) ON VT.VAT = I.VAT
LEFT JOIN CCC88POCAT1 PC1 WITH (NOLOCK) ON PC1.CCC88POCAT1 = I.CCC88POCAT1
LEFT JOIN CCC88POCAT2 PC2 WITH (NOLOCK) ON PC2.CCC88POCAT2 = I.CCC88POCAT2
LEFT JOIN CCC88POCAT3 PC3 WITH (NOLOCK) ON PC3.CCC88POCAT3 = I.CCC88POCAT3
LEFT JOIN MTRPCATEGORY CG WITH (NOLOCK) ON CG.MTRPCATEGORY = I.MTRPCATEGORY AND CG.COMPANY = I.COMPANY
LEFT JOIN SERIES SR WITH (NOLOCK) ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 12
  -- Keep adjustment movement aligned with legacy BI logic:
  -- inventory-related warehouse movements are extracted from purchase sources.
  AND ISNULL(F.SOSOURCE, 0) IN (1251, 1253)
  AND (
    ISNULL(F.SERIES, 0) = 1006
    OR ISNULL(F.TFPRMS, 0) IN (101, 154, 301)
  )
  AND ISNULL(I.SODTYPE, 0) = 51
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('IW|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
