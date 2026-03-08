# Next Steps

## 1. Intelligence rules completion
- Expand deterministic rules for all six streams with clearer severity/action mapping.
- Add rule tuning UI safeguards (threshold presets, validation hints, preview on sample data).
- Add automated rule regression tests per stream.

## 2. KPI and aggregation enhancements
- Add branch-level contribution/margin delta widgets for executive and finance dashboards.
- Materialize heavy inventory breakdown slices to reduce summary latency under large datasets.
- Extend supplier/customer balances trend KPIs with period-over-period decomposition.

## 3. UI consistency finalization
- Run visual QA pass with authenticated browser sessions on desktop/mobile breakpoints.
- Normalize any remaining legacy cards/tables to shared `bv-unified-ui` components.
- Add explicit empty/loading/error state pattern checklist in UI runbook.

## 4. Performance hardening
- Add periodic benchmark CI task using `scripts/benchmark_dashboard_perf.py`.
- Track p95 per endpoint family and trigger alerts when thresholds regress.
- Review and tune slow inventory aggregate joins identified by `kpi_performance` logs.

## 5. Connector framework maturity
- Complete connector admin UI for stream mapping + field mapping with validation previews.
- Add ingestion dry-run mode and per-stream error counters in monitoring UI.
- Add automated reconciliation reports from staging -> facts -> aggregates.

## 6. Operational reliability
- Add a post-deploy smoke command bundle (tenant/admin route checks + core API checks).
- Add startup check that verifies required control/tenant migrations are current.
- Include restart + smoke verification as mandatory release step.
