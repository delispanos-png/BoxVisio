SELECT
  CAST(c.DocDate AS date) AS doc_date,
  CAST(c.BranchCode AS nvarchar(64)) AS branch_external_id,
  CAST(c.EntryType AS nvarchar(32)) AS entry_type,
  CAST(c.Amount AS decimal(14,2)) AS amount,
  CAST(c.Currency AS nvarchar(3)) AS currency,
  CAST(c.ReferenceNo AS nvarchar(64)) AS reference_no,
  CAST(c.Notes AS nvarchar(255)) AS notes,
  CAST(c.EntryId AS nvarchar(128)) AS external_id,
  CAST(c.UpdatedAt AS datetime2) AS updated_at
FROM dbo.CashflowEntries c
WHERE
  (@from_date IS NULL OR c.DocDate >= @from_date)
  AND (@to_date IS NULL OR c.DocDate <= @to_date)
  AND (
    @last_sync_ts IS NULL
    OR c.UpdatedAt > @last_sync_ts
    OR (c.UpdatedAt = @last_sync_ts AND CAST(c.EntryId AS nvarchar(128)) > @last_sync_id)
  )
ORDER BY c.UpdatedAt ASC, CAST(c.EntryId AS nvarchar(128)) ASC;
