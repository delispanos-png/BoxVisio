# FRONTEND_LOADING_STRATEGY.md

## Objective
Minimize UI round-trips and avoid request fan-out stalls.

## Refactor Applied

### Executive Dashboard
- Old: 16 parallel requests (`/v1/kpi/sales/*` + `/v1/kpi/purchases/*` + insights)
- New: 1 request
  - `GET /v1/dashboard/executive-summary`
- Template updated:
  - `backend/app/templates/tenant/dashboard.html`

### Finance Dashboard
- Old: 7 parallel requests
- New: 1 request
  - `GET /v1/dashboard/finance-summary`
- Template updated:
  - `backend/app/templates/tenant/finance_dashboard.html`

### Inventory View
- Old: 5 parallel requests
- New: 1 request
  - `GET /v1/streams/inventory/summary`
- Template updated:
  - `backend/app/templates/tenant/inventory_dashboard.html`

### Cashflow View
- Old: 3 parallel requests
- New: 1 request
  - `GET /v1/streams/cash/summary`
- Template updated:
  - `backend/app/templates/tenant/cashflow_dashboard.html`

### Supplier / Customer List Stability
- Added robust response error parsing for:
  - `backend/app/templates/tenant/suppliers_dashboard.html`
  - `backend/app/templates/tenant/customers_dashboard.html`
- UI now shows backend error detail text when available instead of generic numeric status.
- Backend list endpoints were hardened to tolerate malformed date/pagination params and avoid 422 spikes.
- Customer balances list now uses an aggregate-first backend path, reducing initial list fetch from multi-second scans to sub-second responses.

## Loading Behavior
- Summary-first payload strategy via consolidated endpoints.
- Render from one payload per screen.
- Existing error banners/messages retained.
- No UI freeze from waiting multiple independent fetch chains.
- Equivalent `/api/*` summary aliases are supported for integration compatibility without changing frontend behavior.

## Expected UX Effect
- Faster first meaningful paint for KPI cards.
- Lower latency variance on page switches.
- Better consistency under high DB/API contention.
