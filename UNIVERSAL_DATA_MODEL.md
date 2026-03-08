# Universal Data Model (ERP-Agnostic, Industry-Neutral)

## Objective
BoxVisio BI must work for any retail, wholesale, or service company.
Business logic, KPI logic, and insight rules must not depend on pharmacy-specific assumptions.

Core streams remain fixed:
1. Sales Documents
2. Purchase Documents
3. Inventory Documents
4. Cash Transactions
5. Supplier Balances
6. Customer Balances

## Canonical Neutral Entity Naming
Use neutral business entities in all new features and integrations:
- `product`
- `supplier`
- `customer`
- `category`
- `location`
- `account`

Implementation reference:
- `backend/app/services/universal_entity_naming.py`
  - central canonical naming map
  - legacy aliases documented per entity

## Canonical Stream Model

### 1) `sales_documents`
Canonical fact target: `fact_sales`
Key entities: `product`, `customer`, `category`, `location`

### 2) `purchase_documents`
Canonical fact target: `fact_purchases`
Key entities: `product`, `supplier`, `category`, `location`

### 3) `inventory_documents`
Canonical fact target: `fact_inventory`
Key entities: `product`, `location`

### 4) `cash_transactions`
Canonical fact target: `fact_cashflows`
Key entities: `account`, `customer`/`supplier`/`internal`, `location`
Required subcategories:
- `customer_collections`
- `customer_transfers`
- `supplier_payments`
- `supplier_transfers`
- `financial_accounts`

### 5) `supplier_balances`
Canonical fact target: `fact_supplier_balances`
Key entities: `supplier`, `location`

### 6) `customer_balances`
Canonical fact target: `fact_customer_balances`
Key entities: `customer`, `location`

## Runtime Data Contract
`stream -> staging -> facts -> aggregates -> KPI -> insights`

- Connectors write to `stg_*` only.
- Transform layer writes to `fact_*` only after successful staging persist.
- Dashboards/KPI use aggregate read-models (`agg_*`) for KPI metrics.
- Insight rules consume aggregate/KPI outputs.

## Existing Schema to Neutral Mapping
Current physical names are kept for backward compatibility, but semantic mapping is:

- `dim_item` => `product`
- `dim_supplier` => `supplier`
- `dim_branch` + `dim_warehouse` => `location` (logical location hierarchy)
- `dim_category` => `category`
- `fact_cashflows.account_id` => `account`

## Schema Review (Domain-Specific Assumptions)
Status across key areas:

| Area | Previous assumption | Current state |
| --- | --- | --- |
| Plan/source gating | Inventory/Cashflow tied to pharmacy-specific source | Removed; now enterprise + active source |
| Tenant source values | `pharmacyone`, `pharmacyone_sql` treated as primary | Canonical source is `sql`; legacy values normalized |
| Connector default | SQL connector defaulted to `pharmacyone_sql` | Default is now `sql_connector` |
| Ingestion stream runs | Legacy sync tasks queued `pharmacyone_sql` | Legacy task names retained, but they queue `sql_connector` |
| QueryPack provider naming | Pharmacy provider naming in runtime defaults | Runtime default provider is `erp_sql` with alias support |
| Facts/Dimensions names | Legacy physical table names (`dim_item`, etc.) | Kept for compatibility, semantically mapped to neutral entities |

## Changes Applied for Industry-Neutral Runtime

### Removed domain-specific source gating
- Enterprise inventory/cashflow features are no longer restricted to pharmacy-specific source identifiers.
- Insight engine plan gating no longer requires `tenant_source == pharmacyone`.

Files:
- `backend/app/services/plan_rules.py`
- `backend/app/services/subscriptions.py`
- `backend/app/services/intelligence/engine.py`
- `worker/tasks.py`

### Generic source normalization
- Tenant source normalization now resolves old values (`pharmacyone`, `pharmacyone_sql`) to canonical `sql`.
- Admin tenant forms now use `sql | external | files`.

Files:
- `backend/app/api/ui.py`
- `backend/app/templates/admin/tenants.html`
- `backend/app/templates/admin/tenant_edit.html`
- `backend/app/services/provisioning_wizard.py`

### Generic connector naming with compatibility aliases
- New neutral connector id: `sql_connector`.
- Legacy `pharmacyone_sql` remains supported as compatibility alias.
- QueryPack default provider is now generic (`erp_sql`) with alias resolution.

Files:
- `backend/app/services/ingestion/engine.py`
- `backend/app/services/ingestion/pharmacyone_connector.py`
- `backend/app/services/querypacks.py`
- `backend/app/api/admin.py`
- `backend/app/api/ui.py`
- `backend/app/templates/admin/connections.html`
- `worker/tasks.py`

### Generic language updates
- Removed hardcoded PharmacyOne wording from key UI text.

Files:
- `backend/app/core/i18n.py`

## Compatibility Strategy
To avoid breaking existing tenants:
- Existing connector/source values remain readable.
- Legacy task/connector identifiers remain operational.
- New defaults for new setup paths are neutral (`sql_connector`, `sql`).

## Remaining Legacy Technical Names (Intentional)
Some table/class names still include historical naming (`DimItem`, `FactCashflow`, `pharmacyone_* task names`) for runtime compatibility.
These do not affect the canonical operational model and can be migrated in a separate DB refactor phase.
