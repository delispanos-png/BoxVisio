SELECT
  CAST(sl.DocDate AS date) AS doc_date,
  CAST(sl.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(sl.ItemCode AS nvarchar(128)) AS item_external_id,
  CAST(sl.WarehouseCode AS nvarchar(64)) AS warehouse_external_id,
  CAST(NULL AS nvarchar(64)) AS supplier_external_id,
  CAST(sl.Qty AS decimal(18,4)) AS qty,
  CAST(sl.NetValue AS decimal(14,2)) AS net_amount,
  CAST(sl.CostValue AS decimal(14,2)) AS cost_amount,
  CAST(sl.LineId AS nvarchar(128)) AS external_id,
  CAST(sl.UpdatedAt AS datetime2) AS updated_at,
  CAST(sl.BrandCode AS nvarchar(64)) AS brand_external_id,
  CAST(sl.CategoryCode AS nvarchar(64)) AS category_external_id,
  CAST(sl.GroupCode AS nvarchar(64)) AS group_external_id
FROM dbo.SalesLines sl
WHERE
  (@from_date IS NULL OR sl.DocDate >= @from_date)
  AND (@to_date IS NULL OR sl.DocDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR sl.UpdatedAt > @last_sync_ts
    OR (sl.UpdatedAt = @last_sync_ts AND CAST(sl.LineId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY sl.UpdatedAt ASC, CAST(sl.LineId AS nvarchar(128)) ASC;
