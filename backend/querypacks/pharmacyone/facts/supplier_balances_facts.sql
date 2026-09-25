-- Supplier balances, one row per supplier and company, as of the requested date (today when unset).
-- Ids carry the SB2| prefix: readers use only ledger rows once a tenant has them.
--
-- The balance comes from SoftOne's own trader ledger (TRDTRN), signed by the
-- movement type (TPRMS.FLG01 debit / FLG02 credit): credit minus debit is what
-- the pharmacy owes. This reproduces the supplier card; the old version summed
-- every FINDOC.SUMAMNT since 2016 with no opening balance (billions per supplier).
-- TRDFINDATA.LBAL is not used: it is a cached total that can go stale
-- (pharmacy295 ΦΑΡΜΑΣΕΡΒΙΣ: LBAL -56k, ledger and TRDBALSHEET -134k).
--
-- Ageing is FIFO: the open balance is made of the most recent invoices, bucketed
-- by document date (SoftOne keeps no due date on FINDOC). Rows with a corrupt
-- |value| >= 1e9 are ignored.
--
-- Incremental runs return only the suppliers with a document changed since the
-- sync cursor; a full run (no cursor) returns every supplier with a non-zero balance.
SELECT * FROM (
SELECT
  CAST(ISNULL(T.CODE, CAST(B.TRDR AS nvarchar(64))) AS nvarchar(64)) AS supplier_id,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS balance_date,
  CAST(COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) AS date) AS doc_date,
  CAST(CAST(B.COMPANY AS nvarchar(32)) + ':' + CAST(ISNULL(HB.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(BAL.open_balance, 0) AS decimal(18,4)) AS open_balance,
  CAST(0 AS decimal(18,4)) AS overdue_balance,
  CAST(ISNULL(AG.b0_30, 0) AS decimal(18,4)) AS aging_bucket_0_30,
  CAST(ISNULL(AG.b31_60, 0) AS decimal(18,4)) AS aging_bucket_31_60,
  CAST(ISNULL(AG.b61_90, 0) AS decimal(18,4)) AS aging_bucket_61_90,
  CAST(ISNULL(AG.b90_plus, 0) AS decimal(18,4)) AS aging_bucket_90_plus,
  CAST(LP.TRNDATE AS date) AS last_payment_date,
  CAST(0 AS decimal(18,4)) AS trend_vs_previous,
  CAST('EUR' AS nvarchar(3)) AS currency,
  CAST('SB2|' + ISNULL(T.CODE, CAST(B.TRDR AS nvarchar(64))) + '|' + CAST(B.COMPANY AS nvarchar(32)) + '|' + CONVERT(varchar(10), COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)), 23) AS nvarchar(128)) AS external_id,
  CAST(B.updated_at AS datetime2) AS updated_at,

  CAST(ISNULL(T.CODE, CAST(B.TRDR AS nvarchar(64))) AS nvarchar(64)) AS supplier_ext_id,
  CAST(ISNULL(T.NAME, '') AS nvarchar(255)) AS supplier_name,
  CAST(ISNULL(T.AFM, '') AS nvarchar(64)) AS supplier_afm,
  CAST(ISNULL(HB.NAME, CAST(ISNULL(HB.BRANCH, 0) AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(ISNULL(HB.BRANCH, 0) AS nvarchar(64)) AS branch_code,
  CAST(B.COMPANY AS nvarchar(64)) AS company_id
FROM (
  -- Only traders with a document changed since the cursor (all of them on a full
  -- run); the open-ended range keeps the UPDDATE index usable.
  SELECT FC.COMPANY, FC.TRDR, MAX(FC.UPDDATE) AS updated_at
  FROM FINDOC FC WITH (NOLOCK)
  -- A dormant company (no document for a year) carries no live balances: its
  -- closing balances were brought forward into the successor company, so
  -- counting both would double them (pharmacy295: 1002, last used 2016).
  INNER JOIN (
    SELECT FA.COMPANY
    FROM FINDOC FA WITH (NOLOCK)
    WHERE FA.TRNDATE >= DATEADD(year, -1, CAST(GETDATE() AS date))
    GROUP BY FA.COMPANY
  ) AC ON AC.COMPANY = FC.COMPANY
  WHERE
    (@company_id IS NULL OR FC.COMPANY = @company_id)
    AND FC.SODTYPE = 12
    AND FC.TRDR IS NOT NULL
    AND FC.UPDDATE >= ISNULL(@last_sync_ts, CAST('19000101' AS datetime))
  GROUP BY FC.COMPANY, FC.TRDR
) B
LEFT JOIN TRDR T WITH (NOLOCK) ON T.TRDR = B.TRDR AND T.COMPANY = B.COMPANY
-- A trader's balance belongs to the company, not a store: book it on the head branch.
OUTER APPLY (
  SELECT TOP 1 BR.BRANCH, BR.NAME
  FROM BRANCH BR WITH (NOLOCK)
  WHERE BR.COMPANY = B.COMPANY
  ORDER BY BR.BRANCH
) HB
OUTER APPLY (
  SELECT TOP 1 FP.TRNDATE
  FROM FINDOC FP WITH (NOLOCK)
  WHERE FP.COMPANY = B.COMPANY AND FP.TRDR = B.TRDR AND FP.SOSOURCE IN (1281, 1412, 1416)
    AND (@to_date IS NULL OR FP.TRNDATE < DATEADD(day, 1, @to_date))
  ORDER BY FP.TRNDATE DESC
) LP
OUTER APPLY (
  -- As of today the per-period table gives the same total as the ledger in a
  -- fraction of the time (the retail customer alone has ~5M movements); a past
  -- date needs the ledger, which can stop on any day.
  SELECT COALESCE(
    (
      SELECT SUM((BS.LCREDIT - BS.LDEBIT))
      FROM TRDBALSHEET BS WITH (NOLOCK)
      WHERE BS.COMPANY = B.COMPANY AND BS.TRDR = B.TRDR
        AND COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) >= CAST(GETDATE() AS date)
    ),
    (
      SELECT SUM(X.LTRNVAL * (P.FLG02 - P.FLG01))
      FROM TRDTRN X WITH (NOLOCK)
      INNER JOIN TPRMS P WITH (NOLOCK) ON P.COMPANY = X.COMPANY AND P.SODTYPE = X.SODTYPE AND P.TPRMS = X.TPRMS
      WHERE X.COMPANY = B.COMPANY
        AND X.TRDR = B.TRDR
        AND ABS(ISNULL(X.LTRNVAL, 0)) < 1000000000
        AND COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)) < CAST(GETDATE() AS date)
        AND X.TRNDATE < DATEADD(day, 1, @to_date)
    )
  ) AS open_balance
) BAL
OUTER APPLY (
  SELECT
    SUM(CASE WHEN Z.age_days <= 30 THEN Z.open_part ELSE 0 END) AS b0_30,
    SUM(CASE WHEN Z.age_days BETWEEN 31 AND 60 THEN Z.open_part ELSE 0 END) AS b31_60,
    SUM(CASE WHEN Z.age_days BETWEEN 61 AND 90 THEN Z.open_part ELSE 0 END) AS b61_90,
    ISNULL(SUM(CASE WHEN Z.age_days > 90 THEN Z.open_part ELSE 0 END), 0)
      -- Balance not covered by the last 400 days of invoices is older still.
      + CASE WHEN ISNULL(BAL.open_balance, 0) > ISNULL(SUM(Z.open_part), 0)
             THEN ISNULL(BAL.open_balance, 0) - ISNULL(SUM(Z.open_part), 0) ELSE 0 END AS b90_plus
  FROM (
    SELECT
      DATEDIFF(day, Y.TRNDATE, COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date))) AS age_days,
      CASE
        WHEN ISNULL(BAL.open_balance, 0) <= 0 THEN 0
        WHEN Y.running - Y.amount >= BAL.open_balance THEN 0
        WHEN Y.running <= BAL.open_balance THEN Y.amount
        ELSE BAL.open_balance - (Y.running - Y.amount)
      END AS open_part
    FROM (
      SELECT
        X.TRNDATE,
        X.LTRNVAL * (P.FLG02 - P.FLG01) AS amount,
        SUM(X.LTRNVAL * (P.FLG02 - P.FLG01)) OVER (ORDER BY X.TRNDATE DESC, X.FINDOC DESC, X.TRDTRN DESC ROWS UNBOUNDED PRECEDING) AS running
      FROM TRDTRN X WITH (NOLOCK)
      INNER JOIN TPRMS P WITH (NOLOCK) ON P.COMPANY = X.COMPANY AND P.SODTYPE = X.SODTYPE AND P.TPRMS = X.TPRMS
      WHERE X.COMPANY = B.COMPANY
        AND X.TRDR = B.TRDR
        AND ISNULL(BAL.open_balance, 0) > 0
        AND X.LTRNVAL * (P.FLG02 - P.FLG01) > 0
        AND ABS(ISNULL(X.LTRNVAL, 0)) < 1000000000
        AND (@to_date IS NULL OR X.TRNDATE < DATEADD(day, 1, @to_date))
        AND X.TRNDATE >= DATEADD(day, -400, COALESCE(CAST(@to_date AS date), CAST(GETDATE() AS date)))
    ) Y
  ) Z
) AG
) AS src
WHERE
  (
    @last_sync_ts IS NULL
    OR src.updated_at > @last_sync_ts
    OR (src.updated_at = @last_sync_ts AND src.external_id > CAST(@last_sync_id AS nvarchar(128)))
  )
  AND (@last_sync_ts IS NOT NULL OR src.open_balance <> 0)
