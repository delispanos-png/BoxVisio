# Known Issues (2026-03-08)

## 1. Runtime reload dependency after large UI/router changes
- Symptom: stale running process may still serve old routes/templates after large refactors.
- Impact: temporary false `404`/`500` on admin pages even when source code is correct.
- Current mitigation: restart `api`, `nginx`, and workers after deployment.
- Suggested improvement: add deployment hook/health gate that enforces service restart and route smoke check.

## 2. Inventory list endpoint remains heavy for large datasets
- Endpoint: `/v1/kpi/inventory/items` can still be slow on broad date/window scans.
- Observed behavior: significantly slower than summary endpoints.
- Current mitigation: keep dashboard/stream summaries on aggregates and use pagination in UI.
- Suggested improvement: dedicated pre-aggregated inventory listing materialization or stronger index/selectivity strategy.

## 3. Inventory stream summary still the slowest consolidated stream
- Endpoint: `/v1/streams/inventory/summary`.
- Observed behavior: slower than sales/purchases/cash/balances summaries due multi-breakdown joins on stock aging aggregates.
- Current mitigation: short TTL caching active.
- Suggested improvement: additional covering indexes and optional precomputed brand/group breakdown aggregate.

## 4. Supplier targets filter-options validation strictness
- Endpoint: `/v1/kpi/supplier-targets/filter-options`.
- `max_items` enforces `>= 50` (lower values return `422` by design).
- Suggested improvement: consider lowering minimum to improve API ergonomics for lightweight UIs.

## 5. Environment-specific git push authentication
- In this environment, `git push origin main` fails due missing GitHub credentials.
- Commits are created locally, but push requires configured credentials/token on runner.
