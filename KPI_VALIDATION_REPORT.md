# KPI Validation Report

Date: 2026-02-28 (UTC)
Tenant: `uat-a` (`bi_tenant_uat-a`)
Primary range: `2026-02-20` to `2026-02-21`
Legacy baseline range used where available: `2026-01-01` to `2026-02-28`

## 1) Legacy KPI totals vs new KPI endpoints

### Sales summary (turnover/cost/profit/qty)
Status: `PARTIAL PASS`

Evidence:
- New endpoint (`/v1/kpi/sales/summary?from=2026-01-01&to=2026-02-28`) returned:
  - `qty=11.0`
  - `net_value=240.0`
  - `gross_value=295.0`
- Legacy baseline from `UAT_REPORT.md` for Tenant A sales (`2026-01-01..2026-02-28`):
  - `qty=11`, `net=240`, `gross=295`

Result:
- Matched on qty/net/gross.
- Endpoint does not expose explicit `cost`/`profit` fields; those are derivable from facts (`sum(cost_amount)`, `gross-net` or `gross-cost` depending business definition).

### Purchases summary
Status: `PARTIAL`

Evidence:
- New endpoint (`/v1/kpi/purchases/summary?from=2026-01-01&to=2026-02-28`) returned:
  - `qty=10.0`
  - `net_value=130.0`
  - `cost_amount=110.0`
- No explicit legacy baseline for Tenant A purchases found in available docs.

Result:
- New KPI endpoint returns consistent internal values, but legacy-vs-new comparison for purchases could not be fully completed due missing legacy reference numbers.

## 2) Facts vs aggregates + duplicates

### `sum(fact_sales.net_amount)` vs `sum(agg_sales_daily.net_value)`
Status: `PASS`

Executed SQL:
```sql
with f as (
  select coalesce(sum(net_value),0) s
  from fact_sales
  where doc_date between '2026-02-20' and '2026-02-21'
), a as (
  select coalesce(sum(net_value),0) s
  from agg_sales_daily
  where doc_date between '2026-02-20' and '2026-02-21'
)
select f.s as fact_sales_net, a.s as agg_sales_daily_net, (f.s-a.s) as delta from f,a;
```

Observed:
- `fact_sales_net=240.00`
- `agg_sales_daily_net=240.00`
- `delta=0.00`

### Duplicates by `external_id`
Status: `PASS`

Observed:
- `fact_sales` duplicate `external_id` groups: `0`
- `fact_purchases` duplicate `external_id` groups: `0`

## 3) Incremental validation (sync twice with no changes)
Status: `PASS (with limitation)`

Method:
- Ran two isolated ingestion runs for `sales` and `purchases` with same range and payload.
- `processed` returned `0` on all repeated runs.
- Fact row counts unchanged before/after.

Observed counts:
- Before: `fact_sales=3`, `fact_purchases=1`
- After: `fact_sales=3`, `fact_purchases=1`

Limitation:
- `tenant_connections.enc_payload_len=0` for tenant `uat-a`, so source SQL credentials are not currently stored; connector effectively produced zero source rows.
- This validates non-growth behavior, but not a live remote delta scenario.

## 4) Idempotency validation (re-run same batch)
Status: `PASS (with limitation)`

Method:
- Re-ran same batch payload multiple times.
- Verified no count growth and no duplicate `external_id`.

Observed:
- No new rows inserted.
- No duplicates introduced.

Limitation:
- Because source fetch produced zero rows (no encrypted source credentials for this tenant), idempotency under non-empty replays was not exercised against live source changes.

## Commands used (reproducible)

```bash
# Tenant selection + connector status
cd /opt/cloudon-bi
docker compose exec -T postgres psql -U postgres -d bi_control -c "select t.id,t.slug,t.name,t.plan,t.source,t.db_name,s.plan as sub_plan,s.status as sub_status from tenants t left join subscriptions s on s.tenant_id=t.id order by t.id;"
docker compose exec -T postgres psql -U postgres -d bi_control -c "select tenant_id,connector_type,sync_status,last_sync_at,last_test_ok_at,last_test_error from tenant_connections order by tenant_id;"

# Range discovery
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a -c "select min(doc_date) as from_date,max(doc_date) as to_date from fact_sales;"

# Get tenant token
curl -k -s -H 'Content-Type: application/json' -d '{"email":"admin+uata@boxvisio.com","password":"Tenant123!"}' https://bi.boxvisio.com/v1/auth/login > /tmp/tenant_login_uata.json

# KPI endpoint checks
TOKEN=$(python3 - <<'PY'
import json
print(json.load(open('/tmp/tenant_login_uata.json'))['access_token'])
PY
)
curl -k -s -H "Authorization: Bearer $TOKEN" "https://bi.boxvisio.com/v1/kpi/sales/summary?from=2026-01-01&to=2026-02-28"
curl -k -s -H "Authorization: Bearer $TOKEN" "https://bi.boxvisio.com/v1/kpi/purchases/summary?from=2026-01-01&to=2026-02-28"

# Facts vs agg consistency
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a -c "with f as (select coalesce(sum(net_value),0) s from fact_sales where doc_date between '2026-02-20' and '2026-02-21'), a as (select coalesce(sum(net_value),0) s from agg_sales_daily where doc_date between '2026-02-20' and '2026-02-21') select f.s as fact_sales_net, a.s as agg_sales_daily_net, (f.s-a.s) as delta from f,a;"

# Duplicate checks
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a -c "select count(*) as dup_sales_external_ids from (select external_id,count(*) c from fact_sales group by external_id having count(*)>1) d;"
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a -c "select count(*) as dup_purchases_external_ids from (select external_id,count(*) c from fact_purchases group by external_id having count(*)>1) d;"

# Incremental/idempotency run (isolated single-job invocations)
docker compose exec -T worker sh -lc 'cd /opt/cloudon-bi && PYTHONPATH=/opt/cloudon-bi/backend:/opt/cloudon-bi /opt/cloudon-bi/.venv/bin/python - <<"PY"
import asyncio
from app.services.ingestion.engine import process_job
job={"connector":"pharmacyone_sql","entity":"sales","tenant_slug":"uat-a","payload":{"from_date":"2026-02-20","to_date":"2026-02-21","ignore_sync_state":True},"attempt":0,"max_retries":3}
print(asyncio.run(process_job(job)))
PY'

docker compose exec -T worker sh -lc 'cd /opt/cloudon-bi && PYTHONPATH=/opt/cloudon-bi/backend:/opt/cloudon-bi /opt/cloudon-bi/.venv/bin/python - <<"PY"
import asyncio
from app.services.ingestion.engine import process_job
job={"connector":"pharmacyone_sql","entity":"purchases","tenant_slug":"uat-a","payload":{"from_date":"2026-02-20","to_date":"2026-02-21","ignore_sync_state":True},"attempt":0,"max_retries":3}
print(asyncio.run(process_job(job)))
PY'

# Count before/after
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a -c "select count(*) as sales_after from fact_sales; select count(*) as purchases_after from fact_purchases;"

# Connector payload state
docker compose exec -T postgres psql -U postgres -d bi_control -c "select tenant_id, length(enc_payload) as enc_payload_len, sync_status, last_sync_at from tenant_connections where tenant_id=1;"
```

## Open issues / next action
1. Configure encrypted SQL Server credentials for tenant `uat-a` (`enc_payload` currently empty).
2. Re-run the same validation with live source rows to fully validate incremental delta and non-empty idempotent replays.
3. Add sales summary fields `cost_amount` and `profit_amount` directly in endpoint response to match requested KPI contract.
