# PERFORMANCE_AUDIT.md

## Audit Date
- 2026-03-08

## Scope
- Executive Dashboard (`/tenant/dashboard`)
- Finance Dashboard (`/tenant/finance-dashboard`)
- Operational stream summary pages (inventory/cash)
- KPI cards + chart data loading pipeline
- Customer/Supplier list modules (`/tenant/customers`, `/tenant/suppliers`) for validation hardening (422 avoidance)

## Request Path Trace
- UI templates
  - `backend/app/templates/tenant/dashboard.html`
  - `backend/app/templates/tenant/finance_dashboard.html`
  - `backend/app/templates/tenant/inventory_dashboard.html`
  - `backend/app/templates/tenant/cashflow_dashboard.html`
- API layer
  - `backend/app/api/kpi.py`
- Service/query layer
  - `backend/app/services/kpi_queries.py`
- DB instrumentation + timing headers
  - `backend/app/middleware/kpi_performance.py`
  - `backend/app/api/deps.py` (instrumented AsyncSession)

## Findings (Before Refactor)
- Executive dashboard used 16 independent KPI calls (parallel in browser).
- Finance dashboard used 7 independent KPI calls.
- Inventory page used 5 independent KPI calls.
- Cashflow page used 3 independent KPI calls.
- Round-trip fan-out was the main latency amplifier under load.
- Query profiling showed repeated aggregate scans for each KPI card/query.

## Slow Endpoints (Observed)
- Legacy fan-out path (same user interaction):
  - `/v1/kpi/sales/summary` (multiple concurrent calls)
  - `/v1/kpi/sales/by-branch` (multiple concurrent calls)
  - `/v1/kpi/sales/trend-monthly` (multiple concurrent calls)
  - `/v1/kpi/purchases/summary` (multiple concurrent calls)
- Operational inventory summary:
  - `/v1/streams/inventory/summary` (cold cache still heavier than other streams)

## Additional Stabilization Fixes
- Added `/api/*` aliases for consolidated summary endpoints:
  - `/api/dashboard/executive-summary`
  - `/api/dashboard/finance-summary`
  - `/api/streams/{sales|purchases|inventory|cash|balances}/summary`
- Extended KPI performance middleware profiling to include `/api/dashboard/*` and `/api/streams/*`.
- Hardened customer/supplier KPI list endpoints against invalid query params (`from/to/limit/offset`) to prevent UI-visible 422 errors.
- Enforced aggregate-only receivables path in stream balances summary (no fact fallback on dashboard summary path).

## Before / After Timings
Measurements executed against running API with tenant auth (UAT data).

### Legacy UI fan-out (before)
- Executive load (16 calls): **1332.31–1505.53 ms** total
- Finance load (7 calls): **166.03–179.21 ms** total

### Consolidated endpoints (after, cache MISS)
- `/v1/dashboard/executive-summary`: **51.94–55.60 ms**
- `/v1/dashboard/finance-summary`: **24.70–25.83 ms**
- `/v1/streams/inventory/summary`: **306.13–340.67 ms**
- `/v1/streams/cash/summary`: **16.14–17.05 ms**

### Consolidated endpoints (after, cache HIT)
- `/v1/dashboard/executive-summary`: **12.27–12.96 ms**

### Consolidated endpoints (latest authenticated benchmark, 2026-03-08)
- MISS:
  - `/v1/dashboard/executive-summary`: **69.19 ms** (API 59.32 / DB 40.55 / 5 queries)
  - `/v1/dashboard/finance-summary`: **22.93 ms** (API 19.97 / DB 9.51 / 8 queries)
  - `/v1/streams/inventory/summary`: **318.51 ms** (API 315.50 / DB 298.99 / 5 queries)
  - `/v1/streams/cash/summary`: **14.30 ms** (API 11.61 / DB 2.24 / 3 queries)
- HIT:
  - `/v1/dashboard/executive-summary`: **12.87 ms**
  - `/v1/dashboard/finance-summary`: **10.35 ms**
  - `/v1/streams/inventory/summary`: **12.42 ms**
  - `/v1/streams/cash/summary`: **11.84 ms**

### Operational stream list endpoints (after optimization)
- `/v1/kpi/customers?limit=250&offset=0`: **~183.97 ms**
- `/v1/kpi/suppliers?limit=250&offset=0`: **~406.45 ms**
- malformed query tolerance check (`from=bad-date&limit=foo`):
  - `/v1/kpi/customers`: **200** in **~17.87 ms** (no 422)

## Query Count / Payload (after)
- Executive summary: 5 DB queries, ~24.5 KB payload, single API call
- Finance summary: 8 DB queries, ~1.3 KB payload, single API call
- Inventory summary: 5 DB queries, ~2.4 KB payload, single API call
- Cash summary: 3 DB queries, ~0.3 KB payload, single API call

## DB Time vs API Time (after, MISS)
- Executive summary: API ~48.16–51.83 ms, DB ~32.68–34.64 ms
- Finance summary: API ~20.96–22.11 ms, DB ~10.64–11.34 ms
- Inventory summary: API ~302.05–336.77 ms, DB ~283.24–316.38 ms
- Cash summary: API ~12.33–13.28 ms, DB ~3.18–3.33 ms

## Key Conclusion
- Major latency issue from UI fan-out is removed for executive/finance/inventory/cash summary views.
- Cold inventory summary remains the slowest path due `agg_stock_aging` grouping joins, but is now in a few hundred ms instead of minutes.
- Customer/Supplier list pages now degrade gracefully on malformed filters instead of failing with raw 422 responses.
- Customer balances list endpoint was refactored to aggregate-only list mode for high-speed loading on stream navigation.
- Current customer list optimization prioritizes speed over period turnover in the list grid; detailed sales turnover remains available in customer drill-down.
