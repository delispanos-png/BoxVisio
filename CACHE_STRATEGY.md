# CACHE_STRATEGY.md

## Scope
Applied to dashboard + stream summary endpoints:
- `/v1/dashboard/executive-summary`
- `/v1/dashboard/finance-summary`
- `/v1/streams/*/summary`
- `/api/dashboard/executive-summary`
- `/api/dashboard/finance-summary`
- `/api/streams/*/summary`

## Implementation
- File: `backend/app/services/kpi_cache.py`
- Primary store: Redis (`redis.asyncio`) at `settings.redis_url`
- Fallback store: in-process memory cache

## Key Format
- `kpi_cache:{namespace}:{tenant_key}:{sha256(params_json)}`

Where params include:
- date range (`from`, `to`, `as_of`)
- branch filters
- dimensions/limits where applicable

This satisfies requirement:
- `tenant_id + page(namespace) + filter params (+ branch/date range)`.

## TTL
- Default KPI summary TTL: 45 seconds
- Effective range: 30-60 seconds target window

## Invalidation
- Function: `invalidate_tenant_cache(tenant_key, namespace_prefix=None)`
- Called from worker after aggregate refresh commit:
  - file: `worker/tasks.py`
  - point: `_refresh_aggregates_task` after `tenant_db.commit()`

## Runtime Observations
- Dashboard summary MISS: ~35-45 ms
- Dashboard summary HIT: single-digit/low-teens ms
- Inventory summary MISS: ~294 ms
- Inventory summary HIT: ~8-12 ms
