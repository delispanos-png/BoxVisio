# Universal Connector Framework - BoxVisio BI

## Objective
Implement a universal ingestion framework so multiple ERP/data-source connectors feed a single operational BI model without changing KPI/dashboard core logic.

## Runtime Data Contract

`stream -> aggregates -> KPI -> insights`

- Connectors ingest stream rows into staging (`stg_*`) only.
- Staging rows are transformed into canonical facts (`fact_*`).
- Aggregation jobs build dashboard/read models (`agg_*`) from facts.
- KPI endpoints read aggregate models for KPI metrics.
- Insight rules consume KPI/aggregate outputs (not raw connector payloads).

## Core Operational Streams
All connectors map into exactly these 6 streams:

1. `sales_documents`
2. `purchase_documents`
3. `inventory_documents`
4. `cash_transactions`
5. `supplier_balances`
6. `customer_balances`

## A) Connector Abstraction Layer
Connector abstraction is implemented in:

- `backend/app/services/ingestion/base.py`

Key contract:

- `connector_type` (`connector_name`)
- `source_type` (`sql | api | file`)
- `supported_streams`
- `required_connection_parameters`
- `fetch_rows(...)`

Implemented connectors:

- `pharmacyone_sql` (`sql`) - supports all 6 streams
- `external_api` (`api`) - default support for `sales_documents`, `purchase_documents`
- `file_import` (`file`) - supports stream-based file ingestion payload across all streams

Connector catalog endpoint:

- `GET /v1/admin/connectors/catalog`

## B) Stream-specific Ingestion Pipelines
Implemented Celery stream jobs in `worker/tasks.py`:

- `ingest_sales_documents`
- `ingest_purchase_documents`
- `ingest_inventory_documents`
- `ingest_cash_transactions`
- `ingest_supplier_balances`
- `ingest_customer_balances`

Legacy wrappers are preserved for backward compatibility.

## C) Staging Layer
### Staging tables (tenant DB)

- `stg_sales_documents`
- `stg_purchase_documents`
- `stg_inventory_documents`
- `stg_cash_transactions`
- `stg_supplier_balances`
- `stg_customer_balances`

Schema migration:

- `backend/alembic/versions/20260307_0004_tenant_connector_staging.py`

Reference SQL:

- `STAGING_TABLE_SCHEMA.sql`

Runtime flow in `backend/app/services/ingestion/engine.py`:

1. Connector fetches source rows
2. Rows are inserted into stream staging table (`stg_*`)
3. Rows are transformed to canonical facts
4. Staging row marked `processed`

Hard rule enforced in code:

- No connector row can be transformed/upserted to facts without a persisted staging row id.
- If staging mapping/table is missing, ingestion fails fast.
- If transform/upsert fails, staging row is marked `failed` with error details.

## D) Admin Panel Configuration - Data Sources
New admin section/menu:

- **Data Sources** (`Πηγές Δεδομένων`)
- URL: `/admin/data-sources`

Configuration includes:

- connector type (`pharmacyone_sql`, `external_api`, `file_import`)
- source type (`sql`, `api`, `file`)
- enabled streams
- stream query mapping
- stream field mapping JSON (source -> canonical)

Updated UI files:

- `backend/app/templates/base_admin.html`
- `backend/app/api/ui.py`
- `backend/app/templates/admin/connections.html`

## E) Flexible Field Mapping
Per-connector/per-stream field mapping is stored in control DB:

- `tenant_connections.stream_field_mapping`

Format example:

```json
{
  "sales_documents": {
    "doc_date": "HMER",
    "item_code": "KOD",
    "net_value": "AXIA"
  }
}
```

Runtime mapping behavior:

- `_row_getter(..., field_mapping=...)` resolves mapped source fields first
- canonical transformation remains unchanged

## Control Schema Additions
`tenant_connections` now includes:

- `connection_parameters`
- `source_type`
- `supported_streams`
- `enabled_streams`
- `stream_query_mapping`
- `stream_field_mapping`
- `stream_file_mapping`
- `stream_api_endpoint`

Migrations:

- `20260307_0004_control_connector_stream_config.py`
- `20260307_0005_control_connector_field_mapping.py`

## Canonical Fact Targets
Stream -> fact target:

- `sales_documents` -> `fact_sales`
- `purchase_documents` -> `fact_purchases`
- `inventory_documents` -> `fact_inventory`
- `cash_transactions` -> `fact_cashflows`
- `supplier_balances` -> `fact_supplier_balances`
- `customer_balances` -> `fact_customer_balances`

## Backward Compatibility
- Legacy `entity` jobs still accepted.
- Stream-first processing is now canonical (`job.stream`).
- Sync state migrates to stream-based keys (`connector_stream`).
