# Legacy Query Classification

Date: 2026-03-07

Scope: SoftOne ERP SQL extraction assets mapped to operational streams.

## Classification matrix
| Query asset | Operational stream | Runtime use |
|---|---|---|
| `backend/querypacks/pharmacyone/facts/sales_facts.sql` | Sales Documents | Extraction only |
| `backend/querypacks/pharmacyone/facts/purchases_facts.sql` | Purchase Documents | Extraction only |
| `backend/querypacks/pharmacyone/facts/inventory_facts.sql` | Warehouse / Inventory Documents | Extraction only |
| `backend/querypacks/pharmacyone/facts/cashflow_facts.sql` | Cash Transactions | Extraction only |
| `backend/querypacks/pharmacyone/facts/supplier_balances_facts.sql` | Supplier Open Balances | Extraction only |
| `backend/querypacks/pharmacyone/facts/customer_balances_facts.sql` | Customer Balances / Receivables | Extraction only |
| `backend/querypacks/pharmacyone/kpi_validation/README.sql` | Validation utility | Not used by runtime KPIs |
| `backend/querypacks/pharmacyone/admin_discovery/README.sql` | Discovery utility | Not used by runtime KPIs |

## Customer balances / receivables query class
Identified required query classes:
- customer balances snapshots (`open_balance`, `overdue_balance`)
- receivables/open-items aging buckets
- last collection date extraction
- payment/collection linkage support via trend and external ids

## Connector fallback defaults
Defined in `backend/app/services/sqlserver_connector.py`:
- `DEFAULT_GENERIC_SALES_QUERY`
- `DEFAULT_GENERIC_PURCHASES_QUERY`
- `DEFAULT_GENERIC_INVENTORY_QUERY`
- `DEFAULT_GENERIC_CASHFLOW_QUERY`
- `DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY`
- `DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY`

## Governance
- Legacy SQL is extraction-only.
- KPIs, dashboards and insights must consume canonical Postgres facts/aggregates.
- No ad-hoc runtime query path to SoftOne ERP is allowed for production KPI logic.
