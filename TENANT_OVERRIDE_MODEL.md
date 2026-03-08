# Tenant Override Model

Date: 2026-03-07

## Goal
Provide deterministic, non-hardcoded rule resolution for all tenants:
- global default behavior
- tenant-specific override behavior
- no code deployment required for business logic changes

## Core Entities
Defined in `backend/app/models/control.py`.

### 1) `global_rule_sets`
Represents a default ruleset package.

Key fields:
- `code` (unique)
- `name`
- `description`
- `is_active`
- `priority` (highest active priority wins)

### 2) `global_rule_entries`
Represents one default rule item.

Key fields:
- `ruleset_id`
- `domain`:
  - `document_type_rules`
  - `source_mapping`
  - `kpi_participation_rules`
  - `intelligence_threshold_rules`
- `stream`:
  - `sales_documents`
  - `purchase_documents`
  - `inventory_documents`
  - `cash_transactions`
  - `supplier_balances`
  - `customer_balances`
- `rule_key`
- `payload_json`
- `is_active`

### 3) `tenant_rule_overrides`
Represents tenant override for one scoped rule.

Key fields:
- `tenant_id`
- `domain`
- `stream`
- `rule_key`
- `override_mode`:
  - `merge`
  - `replace`
  - `disable`
- `payload_json`
- `is_active`

Unique scope:
- `(tenant_id, domain, stream, rule_key)`

## Resolution Semantics
Implemented in `backend/app/services/rule_config.py`.

Given `(tenant_id, domain, stream, rule_key)`:
1. Read active global rule entry from highest-priority active ruleset.
2. Read tenant override.
3. Resolve:
- no override: global payload
- override `merge`: deep merge(global, override)
- override `replace`: override payload only
- override `disable`: `{"enabled": false}`
4. If no global exists, use explicit fallback payload (compatibility mode).

## Example Payload Patterns

### A) Source Mapping / Query Mapping
`domain=source_mapping`, `rule_key=query_template`

```json
{
  "query_template": "SELECT ... FROM ... WHERE UpdatedAt >= :last_sync_ts",
  "incremental_column": "UpdatedAt",
  "id_column": "LineId"
}
```

### B) Document Type Classification
`domain=document_type_rules`, `rule_key=doc_type:ALP`

```json
{
  "document_type": "ALP",
  "classification": "sales_receipt",
  "stream_assignment": "sales_documents",
  "sign_policy": "positive",
  "balance_impact": "none"
}
```

### C) KPI Participation
`domain=kpi_participation_rules`, `rule_key=kpi:turnover`

```json
{
  "include_revenue": true,
  "include_qty": false,
  "include_cost": false,
  "excluded_document_types": ["CANCELLED"],
  "sign_mode": "doc_rule"
}
```

### D) Intelligence Thresholds
`domain=intelligence_threshold_rules`, `rule_key=CUSTOMER_OVERDUE_SPIKE`

```json
{
  "enabled": true,
  "severity": "warning",
  "window_days": 30,
  "overdue_spike_pct": 20,
  "min_overdue_baseline": 500
}
```

## Admin API Endpoints
Global:
- `GET /v1/admin/rulesets/global`
- `POST /v1/admin/rulesets/global`
- `GET /v1/admin/rules/global`
- `PUT /v1/admin/rules/global`

Tenant:
- `GET /v1/admin/tenants/{tenant_id}/rules/overrides`
- `PUT /v1/admin/tenants/{tenant_id}/rules/overrides`
- `GET /v1/admin/tenants/{tenant_id}/rules/resolve`

## Backward Compatibility
Current ingestion still accepts `TenantConnection` query templates.
Resolution now layers this as fallback:
- tenant override -> global default -> `TenantConnection`/code fallback

Target is to phase out hardcoded and connection-level semantics and rely fully on ruleset-driven behavior.

## Operational Notes
- Seeded default ruleset: `softone_default_v1`.
- Every override is independent and auditable by scope.
- This model supports tenant-by-tenant rollout without forking code paths.
