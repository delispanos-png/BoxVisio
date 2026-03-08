SELECT
  CAST(sb.SupplierCode AS nvarchar(64)) AS supplier_id,
  CAST(sb.BalanceDate AS date) AS balance_date,
  CAST(sb.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(sb.OpenBalance AS decimal(14,2)) AS open_balance,
  CAST(sb.OverdueBalance AS decimal(14,2)) AS overdue_balance,
  CAST(sb.AgingBucket0_30 AS decimal(14,2)) AS aging_bucket_0_30,
  CAST(sb.AgingBucket31_60 AS decimal(14,2)) AS aging_bucket_31_60,
  CAST(sb.AgingBucket61_90 AS decimal(14,2)) AS aging_bucket_61_90,
  CAST(sb.AgingBucket90Plus AS decimal(14,2)) AS aging_bucket_90_plus,
  CAST(sb.LastPaymentDate AS date) AS last_payment_date,
  CAST(sb.TrendVsPrevious AS decimal(14,2)) AS trend_vs_previous,
  CAST(sb.Currency AS nvarchar(3)) AS currency,
  CAST(sb.SnapshotId AS nvarchar(128)) AS external_id,
  CAST(sb.UpdatedAt AS datetime2) AS updated_at
FROM dbo.SupplierBalances sb
WHERE
  (@from_date IS NULL OR sb.BalanceDate >= @from_date)
  AND (@to_date IS NULL OR sb.BalanceDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR sb.UpdatedAt > @last_sync_ts
    OR (sb.UpdatedAt = @last_sync_ts AND CAST(sb.SnapshotId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY sb.UpdatedAt ASC, CAST(sb.SnapshotId AS nvarchar(128)) ASC;
