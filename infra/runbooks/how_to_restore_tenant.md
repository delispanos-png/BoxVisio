# How To Restore Tenant

## Inputs
- Tenant slug (e.g. `pharma-a`)
- Backup dump path for tenant DB (e.g. `.../tenants/bi_tenant_pharma_a.dump`)

## 1) Identify tenant DB name
```bash
cd /opt/cloudon-bi
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T postgres \
  psql -U "$TENANT_DB_SUPERUSER" -d "$CONTROL_DB_NAME" -At \
  -c "SELECT db_name FROM tenants WHERE slug='<tenant_slug>'"
```

## 2) Stop ingestion for tenant (optional safety)
- Suspend tenant from admin UI OR temporarily scale worker down:
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --scale worker=0 worker
```

## 3) Restore tenant DB
```bash
cd /opt/cloudon-bi
./scripts/restore_db.sh --db <tenant_db_name> --file <tenant_dump_path> --drop-create
```

## 4) Run tenant migrations (if release changed schema)
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml exec -T api \
  python /app/scripts/run_tenant_migrations.py --tenant <tenant_slug>
```

Current tenant migration head for this release: `20260228_0011_tenant`.

## 5) Resume workers and verify
```bash
docker compose -f docker-compose.yml -f infra/production/compose.prod.yml up -d --scale worker=1 worker
curl -fsS http://localhost:8000/ready
```

## 6) Data sanity checks
- Open tenant dashboard and compare KPI totals with expected period.
- Validate queue depth returns to normal.
