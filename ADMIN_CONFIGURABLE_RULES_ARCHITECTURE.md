# Admin Configurable Rules Architecture

Date: 2026-03-07

## Decision
All document logic, KPI participation logic, intelligence thresholds and source mappings are now modeled as admin-configurable rules with tenant-specific override capability.

Runtime resolution contract:
1. Tenant override
2. Global default
3. Local fallback (temporary compatibility only)

The target state is to remove step 3 for all business logic.

## Scope of Configurable Business Logic
The rules engine must cover:
- document type classification
- stream assignment
- revenue inclusion/exclusion
- quantity inclusion/exclusion
- cost inclusion/exclusion
- balance impact
- sign handling
- KPI participation rules
- intelligence thresholds
- source query mappings

## Implemented Schema Changes
Migration:
- `backend/alembic/versions/20260307_0003_control_configurable_rules.py`

New control enums:
- `RuleDomain`
- `OperationalStream`
- `OverrideMode`

New control tables:
1. `global_rule_sets`
- global default packs (for example `softone_default_v1`)
- priority + activation support

2. `global_rule_entries`
- one rule entry per `(ruleset_id, domain, stream, rule_key)`
- JSON payload for behavior config

3. `tenant_rule_overrides`
- one override per `(tenant_id, domain, stream, rule_key)`
- supports `merge`, `replace`, `disable`

## Runtime Resolution Flow
Implemented service:
- `backend/app/services/rule_config.py`

Resolver method:
- `resolve_rule_payload(...)`

Flow:
1. Load active global rule for `(domain, stream, rule_key)` from highest-priority active ruleset.
2. Load active tenant override for same scope.
3. If override exists:
- `disable` -> returns `{"enabled": false}`
- `replace` -> override payload
- `merge` -> deep merge(global, override)
4. If no override -> global payload.
5. If no global payload -> fallback payload (temporary compatibility).

## Current Runtime Integration
Already wired in ingestion query resolution:
- `backend/app/services/ingestion/engine.py`

`_build_context(...)` now resolves stream query templates via:
- `resolve_source_query_template(...)`

This gives immediate tenant/global override behavior for source query mapping.

## Admin API Surface (for Admin Panel)
Added endpoints in:
- `backend/app/api/admin.py`

Global defaults:
- `GET /v1/admin/rulesets/global`
- `POST /v1/admin/rulesets/global`
- `GET /v1/admin/rules/global`
- `PUT /v1/admin/rules/global`

Tenant overrides:
- `GET /v1/admin/tenants/{tenant_id}/rules/overrides`
- `PUT /v1/admin/tenants/{tenant_id}/rules/overrides`
- `GET /v1/admin/tenants/{tenant_id}/rules/resolve`

## Admin Panel Page Design Proposal
Create 4 pages under Admin > Business Rules:

1. Document Type Rules
- grid by stream + document_type
- fields: class, stream, sign policy, balance impact

2. Source Mapping / Query Mapping
- per stream SQL template
- field mapping JSON editor
- validation/test action

3. KPI Participation Rules
- per KPI + document type toggles
- include_revenue/include_qty/include_cost + sign

4. Intelligence Threshold Rules
- rule-by-rule threshold inputs
- per-tenant override with baseline display

## Implemented UI Update (2026-03-08)
Document Type Rules page was upgraded from raw JSON editing to a form-based UX for non-technical users:
- route: `/admin/business-rules/document-type-rules`
- setup wizard: `/admin/business-rules/document-type-rules/wizard`
- help page: `/admin/business-rules/document-type-rules/help`

Form fields now include:
- Document Type
- Stream
- Include in Revenue / Quantity / Cost
- Affects Customer Balance / Supplier Balance
- Amount Sign / Quantity Sign
- Scope (global or tenant override)
- Override mode (replace/merge for tenant scope)

JSON payload is still stored internally in rule tables and generated automatically from form selections.

UX requirements:
- global default badge vs tenant override badge
- “reset to global” action on each rule
- effective rule preview panel (resolved payload)
- audit trail per change

## Hardcoded Logic Audit (Must Move to Rules)
The following logic is still hardcoded and must be migrated to admin rules:

1. Cashflow subcategory normalization aliases
- `backend/app/services/ingestion/engine.py` (`_normalize_cashflow_subcategory`)
- should become Document Type Rules + Stream Assignment rules

2. Fact-field fallback and inference behavior
- `backend/app/services/ingestion/engine.py` (`_build_fact`)
- includes implicit source-field mapping, counterparty inference, default IDs
- should move to Source Mapping rules

3. Cashflow sign handling and category mapping
- `backend/app/services/kpi_queries.py` (`_cashflow_amount_sign`, category alias helpers)
- should move to KPI Participation + sign rules

4. Intelligence default rule catalog and plan gating
- `backend/app/services/intelligence/rules/__init__.py` (`RULE_SPECS`)
- `backend/app/services/intelligence/engine.py` (`_rule_allowed_for_plan`)
- should move to Intelligence Threshold + Rule Activation config

5. Generic fallback SQL templates in code constants
- `backend/app/services/sqlserver_connector.py`
- should remain only as bootstrapping fallback; operational behavior must come from global rule sets and tenant overrides

## Migration Priorities
1. Source mapping and query templates (already started)
2. Cashflow sign/category rule externalization
3. KPI participation filters externalization
4. Intelligence catalog/plan gating externalization
5. Full removal of hardcoded stream semantics from ingestion and KPI services

## Governance
- No new business rule logic should be merged as hardcoded constants.
- New behavior must be represented as rule payload and managed via admin APIs/pages.
