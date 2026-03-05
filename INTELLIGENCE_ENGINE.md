# Intelligence Engine v1 (Deterministic Rules)

## Scope
- Rule-based only (no ML).
- Multi-tenant execution per tenant DB.
- Daily scheduled run (`as_of = today - 1`) and on-demand run.
- Full rules catalog: `INTELLIGENCE_RULE_LIBRARY.md`.

## Architecture
1. Layer 1: Data & Aggregation (facts/dims/agg tables)
2. Layer 2: KPI Services
3. Layer 3: Intelligence Engine (this document)

## Performance Contract (v1)
- Rules must read from aggregate tables only (plus dims for labels).
- No `fact_*` queries inside rule execution.
- Current rule data sources:
  - `agg_sales_daily`
  - `agg_purchases_daily`
  - `agg_sales_item_daily`
  - `agg_inventory_snapshot_daily`

## Data Model (Tenant DB)
- `insight_rules`
  - `id` (uuid pk)
  - `code` (unique)
  - `name`, `description`
  - `category` (`sales|purchases|inventory|cashflow`)
  - `severity_default` (`info|warning|critical`)
  - `enabled`
  - `params_json` (jsonb)
  - `scope`
  - `schedule` (`daily`)
  - `created_at`, `updated_at`
- `insights`
  - `id` (uuid pk)
  - `rule_code`, `category`, `severity`
  - `title`, `message`
  - `entity_type`, `entity_external_id`, `entity_name`
  - `period_from`, `period_to`
  - `value`, `baseline_value`, `delta_value`, `delta_pct`
  - `metadata_json` (jsonb)
  - `status` (`open|acknowledged|resolved`)
  - `acknowledged_by`, `acknowledged_at`
  - `created_at`
- `insight_runs`
  - `id` (uuid pk)
  - `started_at`, `finished_at`
  - `status` (`success|fail`)
  - `rules_executed`, `insights_created`
  - `error`

## Deterministic De-duplication
- Unique scope-period index on insights:
  - `(rule_code, COALESCE(entity_type,''), COALESCE(entity_external_id,''), period_from, period_to)`
- Upsert logic updates existing row for same rule/scope/period instead of creating duplicates.

## Rule Framework
- Registry: `backend/app/services/intelligence/rules/__init__.py`
- Standard runner shape:
  - `runner(db_session, params_json, rule_context) -> list[InsightCreate]`
- Explainable output:
  - Each insight includes computed values, baseline, deltas, and human-readable message.

## v1 Rules
### Sales (Standard+)
1. `SALES_DROP_PERIOD`
2. `SALES_SPIKE_PERIOD`
3. `PROFIT_DROP_PERIOD`
4. `MARGIN_DROP_PERIOD`
5. `BRANCH_UNDERPERFORM`
6. `CATEGORY_DROP`

### Purchases (Pro+)
7. `PURCHASES_SPIKE`
8. `SUPPLIER_DEPENDENCY`
9. `COST_INCREASE_SUPPLIER`

### Inventory (Enterprise + source=pharmacyone)
10. `DEAD_STOCK`
11. `LOW_COVERAGE`
12. `OVERSTOCK_RISK`

## Default Thresholds (Day 1)
- Turnover drop (`SALES_DROP_PERIOD`): `drop_pct = 10`
- Turnover spike (`SALES_SPIKE_PERIOD`): `increase_pct = 10`
- Profit drop (`PROFIT_DROP_PERIOD`): `drop_pct = 12`
- Margin drop (`MARGIN_DROP_PERIOD`): `drop_points = 2`
- Branch underperform (`BRANCH_UNDERPERFORM`): `below_avg_pct = 15`
- Supplier dependency (`SUPPLIER_DEPENDENCY`): `share_pct = 40`
- Dead stock (`DEAD_STOCK`): `days = 60`, `min_stock_value = 300`
- Low coverage (`LOW_COVERAGE`): `min_coverage_days = 7`
- Overstock risk (`OVERSTOCK_RISK`): `min_overstock_value = 1000`

## Plan/Source Enforcement
- Standard: sales rules only.
- Pro: sales + purchases.
- Enterprise: sales + purchases + inventory/cashflow.
- Inventory/cashflow also require tenant source = `pharmacyone`.

## Scheduling
- Celery task per tenant:
  - `worker.tasks.generate_insights_for_tenant`
- Daily fan-out:
  - `worker.tasks.generate_daily_insights_all_tenants`
- Beat scheduler:
  - `worker_beat` service in docker-compose.

## APIs
- Tenant:
  - `GET /v1/intelligence/insights`
  - `POST /v1/intelligence/insights/{id}/acknowledge`
  - `POST /v1/intelligence/run-now`
- Admin:
  - `POST /v1/admin/tenants/{tenant_id}/insights/run`
  - `GET /v1/admin/tenants/{tenant_id}/insights`
  - `GET /v1/admin/tenants/{tenant_id}/insight-rules`
  - `PATCH /v1/admin/tenants/{tenant_id}/insight-rules/{code}`

## UI
- Tenant:
  - Dashboard shows top open insights.
  - Dedicated page: `/tenant/insights` with filters + acknowledge + run now.
- Admin:
  - `/admin/insights` overview by tenant and severity counts.
  - `/admin/insight-rules` per-tenant rule toggles and threshold params.
