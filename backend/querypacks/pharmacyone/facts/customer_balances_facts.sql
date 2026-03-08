SELECT
  CAST(cb.CustomerCode AS nvarchar(128)) AS customer_id,
  CAST(cb.CustomerName AS nvarchar(255)) AS customer_name,
  CAST(cb.BalanceDate AS date) AS balance_date,
  CAST(cb.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(cb.OpenBalance AS decimal(14,2)) AS open_balance,
  CAST(cb.OverdueBalance AS decimal(14,2)) AS overdue_balance,
  CAST(cb.AgingBucket0_30 AS decimal(14,2)) AS aging_bucket_0_30,
  CAST(cb.AgingBucket31_60 AS decimal(14,2)) AS aging_bucket_31_60,
  CAST(cb.AgingBucket61_90 AS decimal(14,2)) AS aging_bucket_61_90,
  CAST(cb.AgingBucket90Plus AS decimal(14,2)) AS aging_bucket_90_plus,
  CAST(cb.LastCollectionDate AS date) AS last_collection_date,
  CAST(cb.TrendVsPrevious AS decimal(14,2)) AS trend_vs_previous,
  CAST(cb.Currency AS nvarchar(3)) AS currency,
  CAST(cb.SnapshotId AS nvarchar(128)) AS external_id,
  CAST(cb.UpdatedAt AS datetime2) AS updated_at
FROM dbo.CustomerBalances cb
WHERE
  (@from_date IS NULL OR cb.BalanceDate >= @from_date)
  AND (@to_date IS NULL OR cb.BalanceDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR cb.UpdatedAt > @last_sync_ts
    OR (cb.UpdatedAt = @last_sync_ts AND CAST(cb.SnapshotId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY cb.UpdatedAt ASC, CAST(cb.SnapshotId AS nvarchar(128)) ASC;
