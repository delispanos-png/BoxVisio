SELECT
  CAST('SO|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST('SO|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,
  CAST(F.FINDOC AS nvarchar(128)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(ISNULL(F.SERIES, 0) AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(ISNULL(F.SERIES, 0) AS nvarchar(128))) AS nvarchar(255)) AS document_series_name,
  CAST(ISNULL(F.TFPRMS, 0) AS int) AS document_behavior_code,
  CAST(ISNULL(F.BRANCH, 0) AS nvarchar(64)) AS branch_ext_id,
  CAST(ISNULL(BR.NAME, CAST(ISNULL(F.BRANCH, 0) AS nvarchar(64))) AS nvarchar(255)) AS branch_name,
  CAST(ISNULL(SUP.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(SUP.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(ISNULL(SUP.AFM, '') AS nvarchar(64)) AS supplier_afm,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(512)) AS item_name,
  CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(18,4)) AS order_qty,
  CAST(ISNULL(L.QTY1COV, 0) AS decimal(18,4)) AS covered_qty,
  CAST(ISNULL(L.QTY1CANC, 0) AS decimal(18,4)) AS cancelled_qty,
  CAST(ISNULL(L.LINEVAL, ISNULL(L.NETLINEVAL, 0)) AS decimal(18,4)) AS line_value,
  CAST(
    CASE
      WHEN ISNULL(F.FULLYTRANSF, 0) = 1 THEN 1
      WHEN EXISTS (
        SELECT 1
        FROM MTRLINES T WITH (NOLOCK)
        WHERE T.COMPANY = L.COMPANY
          AND T.FINDOCS = F.FINDOC
      ) THEN 1
      ELSE 0
    END
    AS bit
  ) AS has_transformation,
  CAST(
    CASE
      WHEN ISNULL(F.FULLYTRANSF, 0) = 1 THEN 'closed'
      WHEN EXISTS (
        SELECT 1
        FROM MTRLINES T WITH (NOLOCK)
        WHERE T.COMPANY = L.COMPANY
          AND T.FINDOCS = F.FINDOC
      ) THEN 'closed'
      ELSE 'open'
    END
    AS nvarchar(32)
  ) AS order_status
FROM FINDOC F WITH (NOLOCK)
INNER JOIN MTRLINES L WITH (NOLOCK)
  ON L.FINDOC = F.FINDOC
 AND L.COMPANY = F.COMPANY
LEFT JOIN SERIES SR WITH (NOLOCK)
  ON SR.SERIES = F.SERIES
 AND SR.COMPANY = F.COMPANY
 AND SR.SOSOURCE = F.SOSOURCE
LEFT JOIN BRANCH BR WITH (NOLOCK)
  ON BR.BRANCH = F.BRANCH
 AND BR.COMPANY = F.COMPANY
LEFT JOIN TRDR SUP WITH (NOLOCK)
  ON SUP.TRDR = F.TRDR
 AND SUP.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK)
  ON I.MTRL = L.MTRL
 AND I.COMPANY = F.COMPANY
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND ISNULL(F.SOSOURCE, 0) = 1251
  AND ISNULL(F.SODTYPE, 0) = 12
  AND ISNULL(F.TFPRMS, 0) = 201
  AND ISNULL(F.SERIES, 0) IN (2021, 2031)
  AND ISNULL(F.ISCANCEL, 0) = 0
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('SO|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
