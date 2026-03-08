# BoxVisio BI Integration Guide

## 1) Purpose
This guide defines how external systems integrate with BoxVisio BI and feed the canonical operational streams.

All integrations must be ERP-agnostic and map source data into the same six streams.

## 2) Core Integration Principle
`connector -> staging tables -> canonical facts -> aggregates -> KPI -> insights`

No connector writes directly to fact tables.

## 3) Supported Connector Types
- SQL Connector
- API Connector
- File Import Connector (CSV / Excel / SFTP)

## 4) Operational Streams
1. Sales Documents
2. Purchase Documents
3. Inventory Documents
4. Cash Transactions
5. Supplier Balances
6. Customer Balances

Detailed field specs:
- [DATA_STRUCTURE_SALES_DOCUMENTS.md](/opt/cloudon-bi/DATA_STRUCTURE_SALES_DOCUMENTS.md)
- [DATA_STRUCTURE_PURCHASE_DOCUMENTS.md](/opt/cloudon-bi/DATA_STRUCTURE_PURCHASE_DOCUMENTS.md)
- [DATA_STRUCTURE_INVENTORY_DOCUMENTS.md](/opt/cloudon-bi/DATA_STRUCTURE_INVENTORY_DOCUMENTS.md)
- [DATA_STRUCTURE_CASH_TRANSACTIONS.md](/opt/cloudon-bi/DATA_STRUCTURE_CASH_TRANSACTIONS.md)
- [DATA_STRUCTURE_SUPPLIER_BALANCES.md](/opt/cloudon-bi/DATA_STRUCTURE_SUPPLIER_BALANCES.md)
- [DATA_STRUCTURE_CUSTOMER_BALANCES.md](/opt/cloudon-bi/DATA_STRUCTURE_CUSTOMER_BALANCES.md)
- [INTEGRATION_EXAMPLES.md](/opt/cloudon-bi/INTEGRATION_EXAMPLES.md)

## 5) Sales Minimal Required Payload
```json
{
  "external_id": "INV-2024-001",
  "document_type": "sales_invoice",
  "document_date": "2024-02-01",
  "branch_code": "ATH01",
  "customer_code": "CUST100",
  "product_code": "PRD001",
  "quantity": 2,
  "net_amount": 50.00,
  "cost_amount": 30.00
}
```

Optional enrichment fields for Sales:
- `brand`
- `category`
- `warehouse`
- `payment_method`
- `salesperson`

## 6) Required Integration Rules
- `external_id` must be stable and unique within connector scope.
- Date fields must use ISO format:
  - date: `YYYY-MM-DD`
  - datetime: `YYYY-MM-DDTHH:MM:SSZ`
- Numeric fields must be valid decimals.
- Source field names can differ, but must map to canonical names before fact load.
- Sign handling and KPI participation are controlled by Business Rules (global defaults + tenant overrides).

## 7) Idempotency and Incremental Sync
- Idempotency key: `(tenant_id, source_connector_id, external_id)`.
- Use `updated_at` (or equivalent source change timestamp) for incremental sync where available.
- Connector state is tracked via sync metadata tables (`sync_state`, `ingest_batches`).

## 8) Validation and Error Handling
- Invalid records must not be loaded into facts.
- Failed records are stored in dead-letter queue with reason.
- Integrations should support replay after correction.

## 9) Recommended Mapping Workflow
1. Identify source entity/table or API endpoint.
2. Map source fields to canonical stream fields.
3. Validate required fields and data types.
4. Load into stream staging table.
5. Transform staging rows into canonical fact table.
6. Refresh aggregates.
7. Validate KPI and insight outputs.

## 10) API and Dashboard Behavior
- KPI and dashboard endpoints must read from aggregate tables only.
- Stream pages and dashboards should never query raw facts for summary cards/charts.

## 11) Go-Live Checklist (Integration)
- Connector configured and authenticated.
- Stream mapping completed for all required streams.
- Initial backfill completed and verified.
- Incremental sync scheduled and tested.
- Dead-letter queue monitored.
- KPI totals reconciled with source system sample period.
