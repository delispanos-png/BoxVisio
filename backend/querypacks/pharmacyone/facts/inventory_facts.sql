SELECT
  CAST(i.SnapshotDate AS date) AS doc_date,
  CAST(i.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(i.ItemCode AS nvarchar(128)) AS item_external_id,
  CAST(i.WarehouseCode AS nvarchar(64)) AS warehouse_external_id,
  CAST(i.QtyOnHand AS decimal(18,4)) AS qty_on_hand,
  CAST(i.QtyReserved AS decimal(18,4)) AS qty_reserved,
  CAST(i.CostValue AS decimal(14,2)) AS cost_amount,
  CAST(i.ValueAmount AS decimal(14,2)) AS value_amount,
  CAST(i.InventoryId AS nvarchar(128)) AS external_id,
  CAST(i.UpdatedAt AS datetime2) AS updated_at
FROM dbo.InventorySnapshots i
WHERE
  (@from_date IS NULL OR i.SnapshotDate >= @from_date)
  AND (@to_date IS NULL OR i.SnapshotDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR i.UpdatedAt > @last_sync_ts
    OR (i.UpdatedAt = @last_sync_ts AND CAST(i.InventoryId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY i.UpdatedAt ASC, CAST(i.InventoryId AS nvarchar(128)) ASC;
