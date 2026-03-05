# HOW TO ONBOARD PHARMACYONE SQL

## 1) Configure tenant connection
1. Open `adminpanel.boxvisio.com` -> `Connections`.
2. Fill SQL Server connection details.
3. Click `Test Connection`.
4. Click `Apply Default PharmacyOne QueryPack`.
5. Review mapping/profile fields (read-only by default). Enable editing only if needed.
6. Click `Save Mapping Profile`.

## 2) Run initial backfill
1. In `QueryPack And Initial Backfill`, choose tenant.
2. Set `From` / `To` date range.
3. Keep `Chunk Days=7` (recommended).
4. Click `Initial Backfill`.

This enqueues chunked jobs per tenant queue:
- sales chunk job
- purchases chunk job (if selected)

## 3) Trigger normal sync
Optionally trigger `/v1/admin/tenants/{tenant_id}/sync` after backfill.

## 4) Verification commands

### Control DB: connection and sync status
```bash
cd /opt/cloudon-bi
docker compose exec -T postgres psql -U postgres -d bi_control -c "
select tenant_id, connector_type, sync_status, last_sync_at, last_test_ok_at, last_test_error
from tenant_connections
order by id desc;
"
```

### Tenant facts row counts
```bash
# replace <TENANT_DB>
docker compose exec -T postgres psql -U postgres -d <TENANT_DB> -c "
select 'fact_sales' as table_name, count(*) from fact_sales
union all
select 'fact_purchases', count(*) from fact_purchases;
"
```

### Aggregates updated
```bash
# replace <TENANT_DB>
docker compose exec -T postgres psql -U postgres -d <TENANT_DB> -c "
select 'agg_sales_daily' as table_name, count(*) from agg_sales_daily
union all
select 'agg_sales_monthly', count(*) from agg_sales_monthly
union all
select 'agg_purchases_daily', count(*) from agg_purchases_daily
union all
select 'agg_purchases_monthly', count(*) from agg_purchases_monthly;
"
```

### Sync state progress
```bash
# replace <TENANT_DB>
docker compose exec -T postgres psql -U postgres -d <TENANT_DB> -c "
select connector_type, last_sync_timestamp, last_sync_id, updated_at
from sync_state
order by updated_at desc;
"
```

### KPI sample check vs legacy UI period
Use same date range in both systems and compare totals:
```bash
curl -k "https://bi.boxvisio.com/v1/kpi/sales/summary?from=2026-01-01&to=2026-01-31" -H "Authorization: Bearer <TENANT_TOKEN>"
curl -k "https://bi.boxvisio.com/v1/kpi/purchases/summary?from=2026-01-01&to=2026-01-31" -H "Authorization: Bearer <TENANT_TOKEN>"
```

## 5) Troubleshooting
- `test_failed`: verify firewall allowlist and SQL login permissions.
- Zero rows in facts: verify query templates return row-level facts and aliases expected by mapping profile.
- Aggregates empty but facts exist: check worker queue and `refresh_aggregates_for_entity` task logs.
