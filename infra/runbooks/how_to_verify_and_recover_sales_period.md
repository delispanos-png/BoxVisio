# Verify And Recover Sales Period (SQL Connector)

This runbook ensures period sync completeness for sales documents.

## 1) Reconcile source vs target for a period

```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python ../scripts/verify_sales_ingest_period.py \
  --tenant pharmacy295 \
  --from-date 2026-01-01 \
  --to-date 2026-01-31 \
  --sample-size 5000
```

What to check:
- `reconciliation.row_delta = 0`
- `reconciliation.net_delta = 0`
- `missing_in_target_count = 0`
- `extra_in_target_count = 0`

## 2) Auto-recover missing rows for the same period

```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python ../scripts/reconcile_and_recover_sales_period.py \
  --tenant pharmacy295 \
  --from-date 2026-01-01 \
  --to-date 2026-01-31 \
  --max-passes 3 \
  --sample-size 5000 \
  --chunk-days 1 \
  --limit 10000
```

Behavior:
- detects mismatch days from source-vs-target reconciliation
- enqueues targeted `enqueue_sql_backfill` only for mismatched windows
- waits for ingest completion
- re-runs reconciliation until complete (or max passes reached)

## 3) Refresh sales aggregates after recovery

```bash
docker compose exec -T api sh -lc 'cd /opt/cloudon-bi && PYTHONPATH=/opt/cloudon-bi:/opt/cloudon-bi/backend /opt/cloudon-bi/.venv/bin/python -c "from worker.tasks import refresh_sales_aggregates; print(refresh_sales_aggregates(\"pharmacy295\", \"2026-01-01\", \"2026-01-31\"))"'
```

## 4) Final certification query (tenant DB)

```bash
docker compose exec -T postgres psql -U postgres -d bi_tenant_pharmacy295 -c "
select round(sum(net_value)::numeric,2) fact_net, count(*) rows, count(distinct external_id) distinct_ext
from fact_sales
where doc_date between '2026-01-01' and '2026-01-31';
"
```

