# Connector Architecture Refactor (Operational Streams)

## Scope
Refactor ingestion so connectors are stream-aware and the ingestion runtime is aligned with 6 core operational streams:

1. `sales_documents`
2. `purchase_documents`
3. `inventory_documents`
4. `cash_transactions`
5. `supplier_balances`
6. `customer_balances`

## 1) Connector Contract (New)
Each connector now has explicit stream semantics.

- `connector_type`: connector key (example: `pharmacyone_sql`, `external_api`, `file_import`)
- `source_type`: `sql | api | file`
- `supported_streams`: declared streams that connector can feed
- `enabled_streams`: per-tenant enabled subset
- `stream_query_mapping`: stream -> SQL query template
- `stream_file_mapping`: stream -> file mapping metadata
- `stream_api_endpoint`: stream -> API endpoint mapping

This is stored in `tenant_connections`.

## 2) DB / Model Changes
`tenant_connections` now includes:

- `source_type`
- `supported_streams` (JSON array)
- `enabled_streams` (JSON array)
- `stream_query_mapping` (JSON object)
- `stream_file_mapping` (JSON object)
- `stream_api_endpoint` (JSON object)

Migration:
- `backend/alembic/versions/20260307_0004_control_connector_stream_config.py`

Backfill rules in migration:
- classify `source_type` from existing `connector_type`
- initialize supported/enabled streams
- initialize `stream_query_mapping` from legacy per-entity query template columns

## 3) Runtime Ingestion Refactor
### Stream-first job contract
Queue jobs now support:

- `stream` (primary)
- `entity` (legacy compatibility)

Entity mapping:

- `sales_documents -> sales`
- `purchase_documents -> purchases`
- `inventory_documents -> inventory`
- `cash_transactions -> cashflows`
- `supplier_balances -> supplier_balances`
- `customer_balances -> customer_balances`

### Engine behavior
`process_job` now:

1. Resolves stream from `job.stream` (or fallback from legacy `job.entity`)
2. Loads connector context with stream config from `tenant_connections`
3. Validates `enabled_streams`
4. Uses stream-aware query resolution (`stream_query_mapping` + rule-based overrides)
5. Writes canonical facts by stream
6. Stores sync state with stream key (`{connector}_{stream}`), with fallback migration from legacy entity keys

Updated files:
- `backend/app/services/ingestion/base.py`
- `backend/app/services/ingestion/engine.py`
- `backend/app/services/ingestion/pharmacyone_connector.py`
- `backend/app/services/ingestion/external_api_connector.py`

## 4) Stream-specific Pipelines (Tasks)
New explicit pipelines added:

- `worker.tasks.ingest_sales_documents`
- `worker.tasks.ingest_purchase_documents`
- `worker.tasks.ingest_inventory_documents`
- `worker.tasks.ingest_cash_transactions`
- `worker.tasks.ingest_supplier_balances`
- `worker.tasks.ingest_customer_balances`

Legacy wrappers remain for compatibility:

- `sync_pharmacyone_sales`
- `sync_pharmacyone_purchases`
- `sync_pharmacyone_inventory`
- `sync_pharmacyone_cashflows`
- `sync_pharmacyone_supplier_balances`
- `sync_pharmacyone_customer_balances`

Updated files:
- `worker/tasks.py`
- `worker/celery_app.py`

## 5) Admin Configuration (Connector -> Stream Mapping)
Admin connections UI now supports:

- connector selector
- source type selector
- enabled streams checkboxes
- query templates for all 6 streams
- visibility of source type + enabled streams in connections list

Updated files:
- `backend/app/api/ui.py`
- `backend/app/templates/admin/connections.html`

Also updated admin APIs for connection save/mapping retrieval to include new stream config fields:

- `backend/app/api/admin.py`

## 6) Ingestion API Alignment
External ingestion endpoints now enqueue stream-aware jobs:

- `/v1/ingest/sales -> sales_documents`
- `/v1/ingest/purchases -> purchase_documents`
- `/v1/ingest/inventory -> inventory_documents`
- `/v1/ingest/cashflows|cash-transactions -> cash_transactions`
- `/v1/ingest/supplier-balances -> supplier_balances`
- `/v1/ingest/customer-balances -> customer_balances`

Updated file:
- `backend/app/api/ingest.py`

## 7) Connector-to-Stream Design
### SoftOne SQL Connector (`pharmacyone_sql`)
- `source_type = sql`
- supports all 6 streams
- uses `stream_query_mapping` and fallback query templates

### External API Connector (`external_api`)
- `source_type = api`
- stream-aware payload ingestion
- defaults can be constrained per tenant via `enabled_streams`

### File Import Connector (`file_import`)
- `source_type = file`
- stream mapping persisted via `stream_file_mapping`
- runtime hook is prepared in connector model; ingestion adapters can be attached per stream

## 8) Canonical Fact Validation by Stream
Operational stream -> canonical fact area -> current physical table:

- `sales_documents -> fact_sales_documents -> fact_sales`
- `purchase_documents -> fact_purchase_documents -> fact_purchases`
- `inventory_documents -> fact_inventory_documents -> fact_inventory`
- `cash_transactions -> fact_cash_transactions -> fact_cashflows`
- `supplier_balances -> fact_supplier_balances -> fact_supplier_balances`
- `customer_balances -> fact_customer_balances -> fact_customer_balances`

This keeps existing physical tables stable while enforcing stream-level ingestion contract.

## 9) Backward Compatibility
- Legacy jobs using only `entity` continue to run.
- Legacy sync state keys migrate automatically to stream keys.
- Existing connectors/query templates continue to operate via fallback mappings.
