# TIMING_RESULTS.md

## Measurement Date
- 2026-03-08

## Dataset
- Tenant: `uat-a`

## Legacy Multi-call UI
- Executive dashboard (16 API calls): **1332.31–1505.53 ms** total (observed range)
- Finance dashboard (7 API calls): **166.03–179.21 ms** total (observed range)

## Consolidated Endpoints (cache MISS)
- `/v1/dashboard/executive-summary`: **51.94–55.60 ms**
- `/v1/dashboard/finance-summary`: **24.70–25.83 ms**
- `/v1/streams/inventory/summary`: **306.13–340.67 ms**
- `/v1/streams/cash/summary`: **16.14–17.05 ms**

## Consolidated Endpoints (cache HIT)
- `/v1/dashboard/executive-summary`: **12.27–12.96 ms**

## Query Counts (MISS)
- `/v1/dashboard/executive-summary`: **5**
- `/v1/dashboard/finance-summary`: **8**
- `/v1/streams/inventory/summary`: **5**
- `/v1/streams/cash/summary`: **3**
