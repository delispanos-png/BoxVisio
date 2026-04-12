# PharmacyOne QueryPack Structure

This folder is intentionally split so query intent does not get mixed:

- `facts/`:
  - Row-level ingestion queries only.
  - Used by worker ingestion jobs and backfill.
  - Production source for canonical facts (`fact_sales`, `fact_purchases`, `fact_inventory`, `fact_cashflows`, `fact_supplier_balances`, `fact_customer_balances`).
- `kpi_validation/`:
  - Optional KPI verification queries for temporary reconciliation against reference screenshots / baseline outputs.
  - Never used by production dashboard endpoints.
- `admin_discovery/`:
  - Discovery/helper queries for admin mapping (tables/columns/sample rows).
  - Never used for ingestion or dashboard reads.

Rules:

1. Keep ingestion SQL in `facts/` only.
2. Keep validation SQL in `kpi_validation/` only.
3. Keep discovery SQL in `admin_discovery/` only.
4. Dashboard APIs must read only Postgres aggregates/facts, never remote SQL Server.
5. Do not keep duplicate SQL copies at querypack root; `facts/` is the only canonical location.
