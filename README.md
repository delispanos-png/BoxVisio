# CloudOn BI SaaS (MVP)

Multi-tenant BI web app (DB-per-tenant) for pharmacies (Greece/Cyprus) using FastAPI + PostgreSQL + Redis + Celery.

## Monorepo Structure

- `backend/`: FastAPI app, auth, RBAC, API endpoints, templates, migrations.
- `worker/`: Celery worker for ingestion/sync jobs.
- `frontend/`: placeholder for purchased template integration assets.
- `infra/`: nginx config and deployment support.
- `scripts/`: tenant provisioning and bootstrap scripts.

## Quick Start

1. Copy env file:
   - `cp .env.example .env`
2. Start services:
   - `make up`
3. Run control DB migrations:
   - `make migrate-control`
4. Seed cloud admin:
   - `make seed-admin`

## Makefile Targets

- `make up`: build and start all containers.
- `make down`: stop and remove containers.
- `make logs [SERVICE=api]`: follow logs for all services or one service.
- `make migrate-control`: apply control DB migrations.
- `make migrate-tenant TENANT=pharma-a`: run tenant DB migrations.
- `make create-tenant NAME=\"Pharma A\" TENANT=pharma-a ADMIN_EMAIL=admin@pharma.gr PLAN=standard SOURCE=external CLOUDON_TOKEN=<jwt>`: create tenant from admin API.
- `make seed-admin`: create default cloud admin user.
- `make check-migrations`: verify Alembic heads for control and tenant targets.
- `make backup-nightly`: run full backup (control DB + all tenant DBs) and apply retention cleanup.
- `make restore-db DB=<db_name> FILE=<backup.dump> [DROP_CREATE=true]`: restore one database from backup file.

## Backup Strategy

- Nightly backup script: `scripts/nightly_backup.sh`
  - Backs up control DB (`bi_control` by default).
  - Backs up all tenant DBs found in control DB (`tenants.db_name`, excluding `terminated`).
  - Backs up global roles/privileges (`pg_dumpall --globals-only`).
  - Writes into timestamped folder: `BACKUP_ROOT/YYYYMMDDTHHMMSSZ`.
  - Creates `manifest.txt` with backed-up files.
- Restore script: `scripts/restore_db.sh`
  - Restores one DB from one `.dump` file.
  - Optional `--drop-create` to recreate target DB before restore.
- Retention policy:
  - Configured by `BACKUP_RETENTION_DAYS` (default `14`).
  - Old timestamped backup directories older than retention are deleted automatically after nightly backup.

### Backup Environment Variables

- `BACKUP_ROOT` (default: `/opt/cloudon-bi/backups`)
- `BACKUP_RETENTION_DAYS` (default: `14`)

### Nightly Schedule (cron example)

```bash
0 2 * * * cd /opt/cloudon-bi && /opt/cloudon-bi/scripts/nightly_backup.sh >> /var/log/cloudon-bi-backup.log 2>&1
```

### Restore Examples

```bash
# Restore control DB
cd /opt/cloudon-bi
./scripts/restore_db.sh --db bi_control --file /opt/cloudon-bi/backups/<timestamp>/control/bi_control.dump --drop-create

# Restore one tenant DB
./scripts/restore_db.sh --db bi_tenant_pharma_a --file /opt/cloudon-bi/backups/<timestamp>/tenants/bi_tenant_pharma_a.dump --drop-create
```

## Domain Routing

- `bi.boxvisio.com`: customer tenant portal.
- `adminpanel.boxvisio.com`: CloudOn admin portal.

Nginx vhost routing is configured in:
- Dev/default: `infra/nginx/default.conf`
- Production: `infra/production/nginx/conf.d/cloudon.conf` (TLS) or `infra/production/nginx/conf.d/cloudon.bootstrap.conf` (HTTP bootstrap)
On Ubuntu, add DNS A records (or temporary `/etc/hosts`) to point both domains to the server IP.

## Key Endpoints

- Auth:
  - `POST /v1/auth/login`
- Platform:
  - `GET /health`
  - `GET /ready`
- Ingestion (external API):
  - `POST /v1/ingest/sales`
  - `POST /v1/ingest/purchases`
- KPI:
  - `GET /v1/dashboard/executive-summary`
  - `GET /v1/dashboard/finance-summary`
  - `GET /v1/streams/sales/summary`
  - `GET /v1/streams/purchases/summary`
  - `GET /v1/streams/inventory/summary`
  - `GET /v1/streams/cash/summary`
  - `GET /v1/streams/balances/summary`
  - `GET /v1/kpi/sales/summary`
  - `GET /v1/kpi/sales/by-branch`
  - `GET /v1/kpi/sales/compare`
  - `GET /v1/kpi/purchases/summary` (Plan Pro+)
  - `GET /v1/kpi/purchases/by-supplier` (Plan Pro+)
  - `GET /v1/kpi/purchases/compare` (Plan Pro+)
  - `GET /v1/kpi/sales/by-branch/export.csv`
  - `GET /v1/kpi/purchases/by-supplier/export.csv`
  - `GET /v1/kpi/supplier-targets/filter-options`
  - `GET /v1/kpi/supplier-targets`
  - `POST /v1/kpi/supplier-targets`
  - `PATCH /v1/kpi/supplier-targets/{target_id}`
- Subscription/Admin lifecycle:
  - `POST /v1/admin/tenants/wizard`
  - `PATCH /v1/admin/tenants/{tenant_id}/subscription`
  - `GET /v1/admin/plans`
  - `PATCH /v1/admin/plans/{plan}/features`
- WHMCS (optional mirror only, no provisioning/control):
  - `POST /whmcs/provision`
  - `POST /whmcs/suspend`
  - `POST /whmcs/terminate`
  - `POST /whmcs/upgrade`
- SQL Server Discovery/Mapping (admin):
  - `POST /v1/admin/tenants/{tenant_id}/sqlserver/discovery`
  - `POST /v1/admin/tenants/{tenant_id}/sqlserver/test`
  - `GET /v1/admin/tenants/{tenant_id}/sqlserver/mapping`
  - `PUT /v1/admin/tenants/{tenant_id}/sqlserver/mapping`

## Business Rules UI (Admin)

- `GET /admin/business-rules/document-type-rules`
- `GET /admin/business-rules/document-type-rules/wizard`
- `GET /admin/business-rules/document-type-rules/help`
- `POST /admin/business-rules/document-type-rules/upsert-form`
- `POST /admin/business-rules/document-type-rules/apply-softone-template`

Reference docs:
- `ADMIN_CONFIGURABLE_RULES_ARCHITECTURE.md`
- `BUSINESS_RULES_DOCUMENT_EDITOR.md`

## Multi-tenancy

- Control DB: `bi_control` keeps tenant metadata and auth.
- Tenant DB naming: `bi_tenant_<tenant_slug>`.
- Tenant selection: `X-Tenant` header or JWT `tenant_id` claim.

## Notes

- Dashboards query only tenant PostgreSQL data (never live SQL Server).
- Remote SQL Server is accessed only by worker ingestion jobs.
- Ingestion runs through queue workers and persists canonical facts in tenant PostgreSQL databases.
- KPI/dashboard endpoints read only tenant PostgreSQL facts/dims.
- SQL Server mapping is configurable per tenant in control DB (`tenant_connections`) with:
  - encrypted credentials payload (`enc_payload`) + `sales_query`, `purchases_query`, `incremental_column`, `branch_column`, `item_column`, `amount_column`, `cost_column`, `qty_column`.
  - credentials are encrypted server-side with envelope encryption using `BI_SECRET_KEY` from env (base64 32 bytes).
- Generic default query placeholders are provided and should be customized per client source schema.
- SQL connector sync state is stored per tenant DB in `sync_state`:
  - `connector_type`, `last_sync_timestamp`, `last_sync_id` (incremental by timestamp or id).
- Sales KPIs are optimized through aggregate tables per tenant DB:
  - `agg_sales_daily`
  - `agg_sales_monthly`
- Aggregates are refreshed after sales ingestion sync and can be refreshed by worker task:
  - `worker.tasks.refresh_sales_aggregates`
- Plan enforcement uses middleware + `plan_features` table:
  - `standard`: purchases disabled
  - `non-enterprise`: inventory & cashflows disabled
  - feature flags per plan in `plan_features` (`plan`, `feature_name`, `enabled`)
- Subscription lifecycle is BI-autonomous and enforced via middleware:
  - statuses: `trial`, `active`, `past_due`, `suspended`, `canceled`
  - tenant creation/plan/subscription are managed only from BI admin panel
  - `trial` auto-suspends on expiry (default 14 days)
  - `active` moves to `past_due` when billing period expires
  - `past_due`: KPI read allowed, ingestion blocked, auto-suspend after grace window
  - `suspended`/`canceled`: KPI + ingestion blocked
  - control billing tables: `subscriptions`, `subscription_limits`, `invoices`, `payments`, `audit_logs`
- WHMCS, if connected, is mirror-only for invoicing metadata and never controls access/provisioning.
- Secrets are sourced from env vars.
- No credentials are hardcoded in application code; set all secrets in `.env`.
- Logging is structured JSON (application, request, and exception logs).
- DB pooling is enabled for control DB and tenant DB engines.
- Tenant engine cache is bounded with LRU eviction (`TENANT_ENGINE_CACHE_SIZE`).
- Workers support async ingestion per tenant queue.
- Horizontal worker scaling:
  - Scale workers with Docker Compose: `docker compose up -d --scale worker=3`
  - Celery worker tuning via env:
    - `CELERY_WORKER_CONCURRENCY`
    - `CELERY_WORKER_PREFETCH_MULTIPLIER`
    - `CELERY_WORKER_MAX_TASKS_PER_CHILD`
  - Queue fairness and safety:
    - `acks_late` and reject-on-worker-lost are enabled.
    - Redis distributed lock prevents concurrent queue drains for the same tenant.
    - Per-tenant ingestion throttling limits processing rate:
      - `INGEST_THROTTLE_JOBS_PER_WINDOW`
      - `INGEST_THROTTLE_WINDOW_SECONDS`
    - Tenant queue lock TTL:
      - `INGEST_TENANT_LOCK_TTL_SECONDS`
- TLS: nginx placeholder included; integrate Let's Encrypt at deployment.
- Monitoring baseline is available under `infra/monitoring`:
  - Prometheus config + alert rules
  - Grafana dashboard JSON
  - Alertmanager sample config
  - Incident/operations runbooks in `infra/runbooks`
- Go-live and production operation docs:
  - `GO_LIVE_PLAN.md`
  - `FIRST_PRODUCTION_TENANT_SOP.md`
  - `POST_GO_LIVE_CHECKLIST.md`
- `PRODUCTION_BACKUP_RESTORE_VERIFICATION.md`
- `PRODUCTION_SECURITY_VERIFICATION.md`

## Stabilization Docs

- `PERFORMANCE_AUDIT.md`
- `QUERY_OPTIMIZATION_REPORT.md`
- `CACHE_STRATEGY.md`
- `UI_CONSISTENCY_REFACTOR.md`
- `MENU_ARCHITECTURE.md`
- `BUG_DIAGNOSIS_SUPPLIER_TARGET_ITEMS.md`
- `KNOWN_ISSUES.md`
- `NEXT_STEPS.md`
