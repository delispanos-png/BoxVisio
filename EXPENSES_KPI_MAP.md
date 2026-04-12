# Expenses KPI Map

Date: 2026-03-08
Scope: KPI and intelligence mapping for proposed stream `Operating Expenses`.

## KPI Dependency Matrix
| KPI | Source stream | Fact table | Aggregate table | Proposed endpoint |
|---|---|---|---|---|
| Total expenses | Operating Expenses | `fact_expenses` | `agg_expenses_daily` | `GET /v1/kpi/expenses/summary` |
| Expenses by category | Operating Expenses | `fact_expenses` | `agg_expenses_by_category_daily` | `GET /v1/kpi/expenses/by-category` |
| Expenses by branch | Operating Expenses | `fact_expenses` | `agg_expenses_by_branch_daily` | `GET /v1/kpi/expenses/by-branch` |
| Expense ratio to revenue | Operating Expenses + Sales context | `fact_expenses` + sales aggregate context | `agg_expenses_daily` + `agg_sales_daily` | `GET /v1/kpi/expenses/summary` |
| Expense trend | Operating Expenses | `fact_expenses` | `agg_expenses_daily`, `agg_expenses_monthly` | `GET /v1/kpi/expenses/trend` |

## KPI Definitions
- `total_expenses`: sum(`amount_net`) for selected period/filters.
- `expenses_by_category`: sum(`amount_net`) grouped by `category_id` or category hierarchy.
- `expenses_by_branch`: sum(`amount_net`) grouped by `branch_id`.
- `expense_ratio_to_revenue_pct`: `(total_expenses / total_sales_revenue) * 100`.
- `expense_trend`: time series of daily or monthly expense totals and period-over-period delta.

## Standard Filters
- `from`, `to`
- `branches`
- `categories`
- `suppliers` (optional)
- `cost_center` (optional)
- `q` (search by document/category/supplier)

## Intelligence Rules (deterministic)

## 1) `EXPENSE_SPIKE`
Trigger:
- Current period total expenses exceeds baseline by threshold.

Example:
- `(current_7d - previous_7d) / previous_7d >= 0.20`

Output:
- severity: warning/critical by threshold tier
- explanation: branch/category and delta amount
- suggested action: inspect top categories and recent documents

## 2) `ABNORMAL_CATEGORY_GROWTH`
Trigger:
- One category has disproportionate growth compared to total expense growth.

Example:
- `category_growth_pct >= 30%` and `category_contribution_to_delta >= 40%`

Output:
- explanation: category, growth %, contribution %
- suggested action: review contracts, one-off invoices, recurring setup

## 3) `BRANCH_COST_PRESSURE`
Trigger:
- Branch expense ratio to revenue deteriorates above threshold.

Example:
- `branch_expense_ratio_pct >= target_ratio_pct + tolerance`

Output:
- explanation: branch ratio vs company baseline
- suggested action: branch-level cost review and corrective plan

## Optional Additional Rules
- `FIXED_COST_DRIFT`: fixed-cost categories rising steadily for N periods.
- `LOW_REVENUE_HIGH_EXPENSE`: expense growth while sales trend is flat/negative.

## Governance
- KPI and insights must be aggregate-driven for UI performance.
- Thresholds must be configurable via Business Rules (global defaults + tenant overrides).
- No dashboard path should query `fact_expenses` directly.
