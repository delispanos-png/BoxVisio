SELECT
  CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(64)) AS supplier_id,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS balance_date,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(SUM(
    CASE
      WHEN F.SOSOURCE IN (1251, 1253, 1261, 1653) THEN ABS(ISNULL(F.SUMAMNT, 0))
      WHEN F.SOSOURCE IN (1281, 1412, 1416) THEN -ABS(ISNULL(F.SUMAMNT, 0))
      ELSE ISNULL(F.SUMAMNT, 0)
    END
  ) AS decimal(18,4)) AS open_balance,
  CAST(0 AS decimal(18,4)) AS overdue_balance,
  CAST(0 AS decimal(18,4)) AS aging_bucket_0_30,
  CAST(0 AS decimal(18,4)) AS aging_bucket_31_60,
  CAST(0 AS decimal(18,4)) AS aging_bucket_61_90,
  CAST(0 AS decimal(18,4)) AS aging_bucket_90_plus,
  CAST(MAX(CASE WHEN F.SOSOURCE IN (1281, 1412, 1416) THEN F.TRNDATE ELSE NULL END) AS date) AS last_payment_date,
  CAST(0 AS decimal(18,4)) AS trend_vs_previous,
  CAST('EUR' AS nvarchar(3)) AS currency,
  CAST('SB|' + CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(64)) + '|' + CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) + '|' + CONVERT(varchar(10), COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)), 23) AS nvarchar(128)) AS external_id,
  CAST(MAX(ISNULL(F.UPDDATE, F.TRNDATE)) AS datetime2) AS updated_at,

  CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(T.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(MAX(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255)))) AS nvarchar(255)) AS branch_name,
  CAST(MAX(F.BRANCH) AS nvarchar(64)) AS branch_code,
  CAST(MAX(F.COMPANY) AS nvarchar(64)) AS company_id
FROM FINDOC F
LEFT JOIN TRDR T ON T.TRDR = F.TRDR AND T.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
WHERE
  F.SODTYPE = 12
  AND F.SOSOURCE IN (1251, 1253, 1261, 1281, 1412, 1416)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
GROUP BY
  ISNULL(T.CODE, F.TRDR),
  ISNULL(T.NAME, ''),
  ISNULL(F.COMPANY, 0),
  ISNULL(F.BRANCH, 0)
