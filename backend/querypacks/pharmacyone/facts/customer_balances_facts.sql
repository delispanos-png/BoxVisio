SELECT
  CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(128)) AS customer_id,
  CAST(ISNULL(T.NAME, '') AS nvarchar(255)) AS customer_name,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS balance_date,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(SUM(
    CASE
      WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 103, 131) THEN ABS(COALESCE(TRY_CONVERT(decimal(28,6), F.SUMAMNT), 0))
      WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -ABS(COALESCE(TRY_CONVERT(decimal(28,6), F.SUMAMNT), 0))
      WHEN F.SOSOURCE IN (1381, 1413) THEN -ABS(COALESCE(TRY_CONVERT(decimal(28,6), F.SUMAMNT), 0))
      ELSE COALESCE(TRY_CONVERT(decimal(28,6), F.SUMAMNT), 0)
    END
  ) AS decimal(28,6)) AS open_balance,
  CAST(0 AS decimal(28,6)) AS overdue_balance,
  CAST(0 AS decimal(28,6)) AS aging_bucket_0_30,
  CAST(0 AS decimal(28,6)) AS aging_bucket_31_60,
  CAST(0 AS decimal(28,6)) AS aging_bucket_61_90,
  CAST(0 AS decimal(28,6)) AS aging_bucket_90_plus,
  CAST(MAX(CASE WHEN F.SOSOURCE IN (1381, 1413) THEN F.TRNDATE ELSE NULL END) AS date) AS last_collection_date,
  CAST(0 AS decimal(28,6)) AS trend_vs_previous,
  CAST('EUR' AS nvarchar(3)) AS currency,
  CAST('CB|' + CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(128)) + '|' + CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) + '|' + CONVERT(varchar(10), COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)), 23) AS nvarchar(128)) AS external_id,
  CAST(MAX(ISNULL(F.UPDDATE, F.TRNDATE)) AS datetime2) AS updated_at,

  CAST(ISNULL(T.CODE, F.TRDR) AS nvarchar(128)) AS customer_ext_id,
  CAST(MAX(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255)))) AS nvarchar(255)) AS branch_name,
  CAST(MAX(F.BRANCH) AS nvarchar(64)) AS branch_code,
  CAST(MAX(F.COMPANY) AS nvarchar(64)) AS company_id
FROM FINDOC F
LEFT JOIN TRDR T ON T.TRDR = F.TRDR AND T.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 13
  AND (
    (ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.SOREDIR, 0) IN (0, 10000) AND ISNULL(F.TFPRMS, 0) IN (102, 103, 131, 151, 152, 181))
    OR ISNULL(F.SOSOURCE, 0) IN (1381, 1413)
  )
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
GROUP BY
  ISNULL(T.CODE, F.TRDR),
  ISNULL(T.NAME, ''),
  ISNULL(F.COMPANY, 0),
  ISNULL(F.BRANCH, 0)
