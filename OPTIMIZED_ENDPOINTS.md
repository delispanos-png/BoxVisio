# OPTIMIZED_ENDPOINTS.md

## Consolidated Dashboard Endpoints

### `GET /v1/dashboard/executive-summary`
Alias: `GET /api/dashboard/executive-summary`
Proxy compatibility path: `GET /dashboard/executive-summary` (used by nginx `/api/*` rewrite)
Query params:
- `from`, `to`
- `branches[]`, `warehouses[]`, `brands[]`, `categories[]`, `groups[]`

Response shape:
- `period`
- `anchors`
- `cards`
- `branch_breakdown`
- `trend`
- `key_alerts`

### `GET /v1/dashboard/finance-summary`
Alias: `GET /api/dashboard/finance-summary`
Proxy compatibility path: `GET /dashboard/finance-summary`
Query params:
- `from`, `to`
- `branches[]`
- `supplier_limit`, `account_limit`

Response shape:
- `period`
- `receivables_summary`
- `receivables_aging`
- `receivables_trend`
- `suppliers`
- `cash_summary`
- `cash_types`
- `cash_accounts`

## Consolidated Stream Endpoints

### `GET /v1/streams/sales/summary`
Alias: `GET /api/streams/sales/summary`
Proxy compatibility path: `GET /streams/sales/summary`
Response:
- `summary`, `by_branch`, `trend`

### `GET /v1/streams/purchases/summary`
Alias: `GET /api/streams/purchases/summary`
Proxy compatibility path: `GET /streams/purchases/summary`
Response:
- `summary`, `by_supplier`, `trend`

### `GET /v1/streams/inventory/summary`
Alias: `GET /api/streams/inventory/summary`
Proxy compatibility path: `GET /streams/inventory/summary`
Query params:
- `as_of`
- `branches[]`, `brands[]`, `categories[]`, `groups[]`

Response:
- `snapshot`, `aging`, `by_brand`, `by_commercial_category`, `by_manufacturer`

### `GET /v1/streams/cash/summary`
Alias: `GET /api/streams/cash/summary`
Proxy compatibility path: `GET /streams/cash/summary`
Response:
- `summary`, `by_type`, `trend`

### `GET /v1/streams/balances/summary`
Alias: `GET /api/streams/balances/summary`
Proxy compatibility path: `GET /streams/balances/summary`
Response:
- `receivables`, `supplier_balances`, `top_suppliers`
