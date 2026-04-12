# Expenses Stream Proposal

Date: 2026-03-08
Scope: Evaluation for adding stream `7) Operating Expenses` as a first-class operational stream.

## Decision Summary
Recommendation: **Yes, add Operating Expenses as a first-class stream**.

Reason:
- Current 6-stream architecture covers revenue, purchasing, inventory, cash, receivables, and payables.
- It does not provide a clean operational view for non-COGS operating cost behavior (rent, payroll allocations, utilities, services, marketing, admin overhead).
- Without a dedicated expense stream, expense KPIs and insights are either missing or mixed into other streams, reducing decision quality.

## Scope Boundaries (critical)
`fact_expenses` must include only **operating expenses**.

Include:
- Rent and facilities
- Utilities
- Payroll and payroll burden allocations (if available)
- Professional services
- Marketing and advertising
- Office/admin expenses
- Logistics and operating services
- Bank/processing fees

Exclude:
- Inventory procurement and merchandise purchases (already in Purchase Documents)
- Balance sheet movements (asset acquisition/capex, loan principal)
- Tax-only settlements that are not operating cost

## Canonical Tables

## 1) `dim_expense_categories`
Purpose: controlled taxonomy for consistent classification and analytics.

Core columns:
- `category_id` (PK, bigint)
- `external_id` (text, unique, nullable)
- `category_code` (text, unique)
- `category_name` (text)
- `parent_category_id` (bigint, nullable)
- `level_no` (smallint)
- `classification` (text)  
  Values: `opex`, `non_opex`, `unknown`
- `gl_account_code` (text, nullable)
- `is_active` (boolean)
- `updated_at` (timestamptz)

## 2) `fact_expenses`
Purpose: normalized operating expense facts used by aggregates, KPIs, and insights.

Core columns:
- `expense_id` (PK, bigint)
- `external_id` (text, unique per connector/tenant scope)
- `expense_date` (date)
- `posting_date` (date, nullable)
- `branch_id` (bigint, FK `dim_branches`)
- `location_id` (bigint, nullable)
- `category_id` (bigint, FK `dim_expense_categories`)
- `supplier_id` (bigint, nullable)
- `account_id` (bigint, nullable)
- `document_type` (text)
- `document_no` (text, nullable)
- `amount_net` (numeric(18,2))
- `amount_tax` (numeric(18,2), default 0)
- `amount_gross` (numeric(18,2))
- `currency_code` (text, default `EUR`)
- `cost_center` (text, nullable)
- `payment_status` (text, nullable)  
  Values example: `unpaid`, `partial`, `paid`
- `due_date` (date, nullable)
- `source_connector_id` (bigint)
- `updated_at` (timestamptz)

Normalization rule:
- Store expense amounts as positive values for expense impact.
- Reversal/credit entries must be normalized with explicit negative sign handling in ingestion rules.

## Aggregate Layer (recommended)
- `agg_expenses_daily`
- `agg_expenses_monthly`
- `agg_expenses_by_category_daily`
- `agg_expenses_by_branch_daily`

## KPI and Intelligence Integration
- Dashboards must read from aggregate tables only.
- Intelligence rules read KPI deltas and aggregate trends, not raw facts for UI paths.

## Proposed Stream Dependencies
- Source streams affecting context only:
  - Sales Documents (for expense-to-revenue ratio denominator)
  - Branch dimension (for branch cost pressure)
- Core source for expense values:
  - `fact_expenses`

## Rollout Plan
1. Add canonical tables and indexes.
2. Ingest minimal expense facts from one trusted source path (SQL connector first).
3. Build daily/monthly aggregates.
4. Enable KPI cards and trend chart.
5. Enable deterministic intelligence rules with configurable thresholds.
6. Expand ingestion to API/File connectors with field mapping UI.

## Risks and Mitigations
- Risk: double counting with Purchase Documents.  
  Mitigation: strict classification policy (`opex` only) and exclusion rules at ingestion.
- Risk: inconsistent category mapping across tenants.  
  Mitigation: tenant override mapping with global defaults.
- Risk: accrual vs cash confusion.  
  Mitigation: expose KPI basis (`accrual` default), and clearly label reports.
