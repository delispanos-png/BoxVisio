# Data Structure Overview

This document defines the official ingestion structures for the six operational streams of BoxVisio BI.

Companion integration manual:
- [INTEGRATION_GUIDE.md](/opt/cloudon-bi/INTEGRATION_GUIDE.md)
- [INTEGRATION_EXAMPLES.md](/opt/cloudon-bi/INTEGRATION_EXAMPLES.md)

These specifications are used by:
- SQL connectors
- API integrations
- File imports (CSV / Excel / SFTP)
- External ERP systems

## General Ingestion Contract

1. All external payloads must be mapped to one of the six operational streams.
2. Each record must include a stable `external_id` (or equivalent source key).
3. Dates must be ISO format (`YYYY-MM-DD` for date, `YYYY-MM-DDTHH:MM:SSZ` for datetime).
4. Decimal values must use `.` as decimal separator in API payloads and CSV files.
5. Ingestion is idempotent by `(tenant_id, source_connector_id, external_id)`.
6. Canonical transformation happens after staging; connectors must not write directly to facts.

## Stream Specifications

1. [DATA_STRUCTURE_SALES_DOCUMENTS.md](DATA_STRUCTURE_SALES_DOCUMENTS.md)
2. [DATA_STRUCTURE_PURCHASE_DOCUMENTS.md](DATA_STRUCTURE_PURCHASE_DOCUMENTS.md)
3. [DATA_STRUCTURE_INVENTORY_DOCUMENTS.md](DATA_STRUCTURE_INVENTORY_DOCUMENTS.md)
4. [DATA_STRUCTURE_CASH_TRANSACTIONS.md](DATA_STRUCTURE_CASH_TRANSACTIONS.md)
5. [DATA_STRUCTURE_SUPPLIER_BALANCES.md](DATA_STRUCTURE_SUPPLIER_BALANCES.md)
6. [DATA_STRUCTURE_CUSTOMER_BALANCES.md](DATA_STRUCTURE_CUSTOMER_BALANCES.md)

## Canonical Target Streams

1. Sales Documents
2. Purchase Documents
3. Inventory Documents
4. Cash Transactions
5. Supplier Balances
6. Customer Balances

## Validation Baseline (all streams)

- Required fields must be present and non-empty.
- Business keys must be unique within connector scope.
- Numeric fields must parse as numeric.
- Date fields must parse as valid dates.
- Records with validation failures are stored in dead-letter queue with reason.

## Mapping Responsibility

Source systems can keep their native naming. Connector mapping must translate source fields to canonical fields before fact load.
