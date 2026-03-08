# Decision Engine Architecture

## 1) Two-Layer Product Model

The platform operates with two complementary layers:

1. Operational Dashboard Layer
- Purpose: real-time monitoring of operational performance.
- Output: fast KPI cards, trends, breakdowns, and aggregated tables.
- Constraint: dashboards must read from aggregates, not raw facts.

2. Decision Engine Layer
- Purpose: detect important business changes and provide deterministic recommendations.
- Output: structured insights with `severity`, `explanation`, and `suggested_action`.
- Constraint: rules run on canonical operational streams and aggregate outputs.

## 2) Runtime Flow

Canonical flow:
`stream -> staging -> facts -> aggregates -> KPI -> insights`

Read/write responsibilities:
- Connectors write to `stg_*` only.
- Transform jobs write to `fact_*` only.
- Dashboard KPIs read `agg_*` only.
- Insight rules evaluate KPI/aggregate deltas and create insight events.

## 3) Operational Dashboard (Fast + Simple)

Performance principles:
- Use pre-aggregated tables for dashboard endpoints.
- Keep KPI payloads aggregated and compact.
- Execute independent KPI requests in parallel.
- Cache dashboard KPI responses (short TTL, e.g. 30-60s).

Expected dashboard behavior:
- Stable response latency for main KPI cards.
- No raw fact scans for homepage KPI endpoints.

## 4) Rule-Based Insight Generation Model

### Rule definition model
Rules are declared centrally with:
- `code`
- `category` (sales, purchases, inventory, cashflow, receivables)
- `severity_default`
- `scope`
- `schedule`
- `params_json`
- `runner`

### Evaluation context
Each run evaluates rules with a deterministic context:
- tenant identity
- plan
- period
- previous period
- as-of date

### Insight output contract
Each generated insight must include:
- `severity`
- `title`
- `message`
- `explanation`
- `suggested_action`
- optional `suggested_actions` (list)
- period and metric deltas

This contract is exposed via:
- `GET /v1/intelligence/insights`

### Examples of supported insight families
- sales drop vs previous period
- margin erosion
- supplier dependency
- slow moving inventory / dead stock
- overdue customer balances
- abnormal cash movement

## 5) Insights UI Design

### Dashboard panel (new)
The tenant dashboard includes a dedicated “Decision Engine | Κρίσιμα Insights” panel:
- severity counters (`critical`, `warning`, `info`)
- top prioritized open insights
- each card shows:
  - severity badge
  - explanation
  - suggested action
  - deep-link to module drilldown

### Full insights workspace
The existing `/tenant/insights` page remains the detailed view for:
- filtering
- sorting
- acknowledgment
- detailed metric context

## 6) Operational Separation

Design separation is explicit:
- Dashboard layer answers: “what is happening now?”
- Decision engine answers: “what changed, why, and what should we do next?”

This keeps KPI UX lightweight while still delivering decision support without custom tenant code branches.
