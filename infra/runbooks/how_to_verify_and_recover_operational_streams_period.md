# Verify And Recover Operational Streams Period (SQL Connector)

This runbook checks completeness for all core operational circuits and auto-recovers missing periods when needed.

Default streams:
- `sales_documents`
- `purchase_documents`
- `inventory_documents`
- `cash_transactions`
- `operating_expenses`

## 1) Verify period completeness (source vs target)

```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python ../scripts/reconcile_and_recover_streams_period.py \
  --tenant pharmacy295 \
  --from-date 2026-01-01 \
  --to-date 2026-01-31 \
  --streams sales_documents,purchase_documents,inventory_documents,cash_transactions,operating_expenses \
  --sample-size 500 \
  --limit 10000
```

Expected status: `ok`.  
If status is `mismatch`, the output includes per-stream:
- `row_delta`
- `amount_delta`
- `missing_in_target_sample`
- `day_mismatches`

## 2) Auto-recover mismatch windows

```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python ../scripts/reconcile_and_recover_streams_period.py \
  --tenant pharmacy295 \
  --from-date 2026-01-01 \
  --to-date 2026-01-31 \
  --streams sales_documents,purchase_documents,inventory_documents,cash_transactions,operating_expenses \
  --recover \
  --max-passes 3 \
  --chunk-days 1 \
  --sample-size 500 \
  --limit 10000 \
  --wait-timeout 7200
```

Behavior:
- Finds mismatch days per stream.
- Enqueues targeted backfill only for affected streams/windows.
- Waits for ingestion completion.
- Re-verifies until complete or max passes.
