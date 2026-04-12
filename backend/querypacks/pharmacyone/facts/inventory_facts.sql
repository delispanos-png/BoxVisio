SELECT
  CAST(COALESCE(@to_date, GETDATE()) AS date) AS doc_date,
  CAST(
    CAST(ISNULL(S.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))) AS nvarchar(32))
    AS nvarchar(64)
  ) AS branch_external_id,
  CAST(ISNULL(I.CODE, S.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(S.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(ISNULL(S.QTY1, 0) AS decimal(18,4)) AS qty,
  CAST(ISNULL(S.QTY1, 0) AS decimal(18,4)) AS qty_on_hand,
  CAST(0 AS decimal(18,4)) AS qty_reserved,
  CAST(ISNULL(S.IMPVAL, 0) - ISNULL(S.EXPVAL, 0) AS decimal(18,4)) AS cost_amount,
  CAST(ISNULL(S.IMPVAL, 0) - ISNULL(S.EXPVAL, 0) AS decimal(18,4)) AS value_amount,
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
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(ISNULL(W.NAME, CAST(S.WHOUSE AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name,
  CAST(ISNULL(BR.NAME, CAST(ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0)) AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(NULL AS nvarchar(128)) AS barcode,
  CAST(I.MTRMANFCTR AS nvarchar(128)) AS manufacturer_code,
  CAST(ISNULL(MF.NAME, '') AS nvarchar(255)) AS manufacturer_name,
  CAST(ISNULL(CG.NAME, '') AS nvarchar(255)) AS commercial_category,
  CAST(ISNULL(C1.NAME, '') AS nvarchar(255)) AS category_1,
  CAST(NULL AS nvarchar(255)) AS category_2,
  CAST(NULL AS nvarchar(255)) AS category_3,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
  CAST(ISNULL(I.COMPANY, S.COMPANY) AS nvarchar(64)) AS company_id,
  CAST(1 AS bit) AS is_financial_doc,
  CAST('stock_balance' AS nvarchar(32)) AS financial_impact_type,
  CAST(ISNULL(S.IMPVAL, 0) - ISNULL(S.EXPVAL, 0) AS decimal(18,4)) AS financial_value_amount,
  CAST(S.FISCPRD AS int) AS source_module_id,
  CAST(0 AS int) AS redirect_module_id,
  CAST(12 AS int) AS source_entity_id,
  CAST(S.FISCPRD AS int) AS object_id
FROM MTRFINDATA S
LEFT JOIN WHOUSE W
  ON W.WHOUSE = S.WHOUSE
 AND W.COMPANY = S.COMPANY
LEFT JOIN BRANCH BR
  ON BR.BRANCH = ISNULL(NULLIF(W.WHOUSEG, 0), ISNULL(S.WHOUSE, 0))
 AND BR.COMPANY = S.COMPANY
LEFT JOIN MTRL I
  ON I.MTRL = S.MTRL
 AND I.COMPANY = S.COMPANY
LEFT JOIN MTRMANFCTR MF
  ON MF.MTRMANFCTR = I.MTRMANFCTR
 AND MF.COMPANY = I.COMPANY
LEFT JOIN MTRGROUP MG
  ON MG.MTRGROUP = I.MTRGROUP
 AND MG.COMPANY = I.COMPANY
LEFT JOIN MTRCATEGORY C1
  ON C1.MTRCATEGORY = I.MTRCATEGORY
 AND C1.COMPANY = I.COMPANY
LEFT JOIN MTRPCATEGORY CG
  ON CG.MTRPCATEGORY = I.MTRPCATEGORY
 AND CG.COMPANY = I.COMPANY
WHERE
  S.FISCPRD = (SELECT MAX(S2.FISCPRD) FROM MTRFINDATA S2 WHERE S2.COMPANY = S.COMPANY)
  AND ISNULL(W.ISACTIVE, 1) = 1
  AND ABS(ISNULL(S.QTY1, 0)) > 0.0001
