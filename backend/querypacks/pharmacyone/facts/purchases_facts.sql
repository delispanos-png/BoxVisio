SELECT
  CAST(pl.DocDate AS date) AS doc_date,
  CAST(pl.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(pl.ItemCode AS nvarchar(128)) AS item_external_id,
  CAST(pl.WarehouseCode AS nvarchar(64)) AS warehouse_external_id,
  CAST(pl.SupplierCode AS nvarchar(64)) AS supplier_external_id,
  CAST(pl.Qty AS decimal(18,4)) AS qty,
  CAST(pl.NetValue AS decimal(14,2)) AS net_amount,
  CAST(pl.CostValue AS decimal(14,2)) AS cost_amount,
  CAST(pl.LineId AS nvarchar(128)) AS external_id,
  CAST(pl.UpdatedAt AS datetime2) AS updated_at,
  CAST(pl.BrandCode AS nvarchar(64)) AS brand_external_id,
  CAST(pl.CategoryCode AS nvarchar(64)) AS category_external_id,
  CAST(pl.GroupCode AS nvarchar(64)) AS group_external_id
FROM dbo.PurchaseLines pl
WHERE
  (@from_date IS NULL OR pl.DocDate >= @from_date)
  AND (@to_date IS NULL OR pl.DocDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR pl.UpdatedAt > @last_sync_ts
    OR (pl.UpdatedAt = @last_sync_ts AND CAST(pl.LineId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY pl.UpdatedAt ASC, CAST(pl.LineId AS nvarchar(128)) ASC;
