# User Experience Architecture

## Objective
BoxVisio BI supports two primary tenant personas while keeping one shared operational data foundation.

Personas:
1. Business Owner / Manager
2. Finance / Accounting

Both experiences are powered by the same six operational streams:
1) Sales Documents
2) Purchase Documents
3) Inventory Documents
4) Cash Transactions
5) Supplier Balances
6) Customer Balances

## Experience Layers

### 1) Manager Dashboard (`/tenant/dashboard`)
Focus:
- Revenue
- Profit
- Margin
- Cash position (high-level)
- Sales trend
- Insights panel

Primary usage:
- Operational monitoring
- Early warning on business performance

### 2) Finance Dashboard (`/tenant/finance-dashboard`)
Focus:
- Customer receivables
- Supplier payables
- Cash transactions
- Aging analysis
- Reconciliation view

Primary usage:
- Working-capital control
- Exposure and liquidity management

## Shared Data Model / Aggregates

No persona-specific fact schema is introduced.
Both dashboards read from the same canonical KPI/API layer and aggregate model.

Examples used by Finance dashboard:
- `/v1/kpi/receivables/summary`
- `/v1/kpi/receivables/aging`
- `/v1/kpi/receivables/collection-trend`
- `/v1/kpi/suppliers`
- `/v1/kpi/cashflow/summary`
- `/v1/kpi/cashflow/by-type`
- `/v1/kpi/cashflow/accounts`

## Role Mapping

Role-to-persona mapping:
- `tenant_admin` -> `manager` persona
- `tenant_user` -> `finance` persona

Runtime behavior:
- Login redirect is persona-aware.
- Tenant request state exposes `ui_persona` for template rendering.

## Updated Menu Structure

### Manager persona
- Dashboard Διοίκησης
- Analytics modules (sales, purchases, inventory, etc.)
- Operational Streams group
- Insights / Comparison

### Finance persona
- Dashboard Οικονομικών
- Customer Open Balances / Receivables
- Supplier Open Balances / Liabilities
- Cash Transactions

## Visibility Rules

UI visibility is persona-driven from role mapping:
- Manager role sees operational insights and monitoring modules.
- Finance role sees balances and financial-flow modules.

## Implementation Notes

Key implementation points:
- Request state includes: `current_user`, `user_role`, `ui_persona`.
- Sidebar menu renders persona-specific navigation.
- `/tenant/dashboard` and `/tenant/finance-dashboard` enforce default persona redirects.
