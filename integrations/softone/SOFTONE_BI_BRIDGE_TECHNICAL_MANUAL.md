# SoftOne -> BoxVisio BI Custom JavaScript Bridge

This document describes the custom SoftOne `Advanced JavaScript` bridge implemented in:

- [boxvisio_bi_bridge.js](/opt/cloudon-bi/integrations/softone/boxvisio_bi_bridge.js)

Goal:

- Extract the operational data that BoxVisio BI needs.
- Do it without depending on SoftOne browser list templates.
- Return BI-ready JSON payloads for `sales`, `purchases`, `inventory`.

## 1) What Was Implemented In BI (Current Project Context)

The current BI ingestion/runtime is stream-based:

1. `sales_documents`
2. `purchase_documents`
3. `inventory_documents`
4. (also supported globally: `cash_transactions`, `supplier_balances`, `customer_balances`)

Key technical points from the current codebase:

- Ingest endpoints: `POST /v1/ingest/sales|purchases|inventory` (plus other streams).
- Canonical row contract accepts fields like:
  - `event_id`, `external_id`, `doc_date`, `updated_at`
  - `branch_ext_id`, `warehouse_ext_id`, `item_code`
  - `customer_ext_id` (sales), `supplier_ext_id` (purchases)
  - `qty`, `net_value`, `gross_value`, `cost_amount`, `value_amount`
- Engine normalizes/matches aliases and writes to staging -> facts -> aggregates.

## 2) SoftOne BlackBook References Used

From `SoftOne BlackBook ENG ver.3.5.pdf`:

1. Form Scripts / Object functions:
   - `GETSQLDATASET(...)` (page 297)
   - `HTTPCALL(...)` (page 298)
2. Advanced JavaScript:
   - module/package approach (page 331+)
3. Web Services:
   - custom JS endpoint pattern `/s1services/JS/<package>/<function>` (case study section around pages 496-499)
   - object/table anatomy examples (`SALDOC`, `ITELINES`, `MTRDOC`, `TRDR`, `MTRL`) from web service case studies.

## 3) Bridge Design

The script exposes these public functions:

1. `GetSalesDocumentsForBI(obj)`
2. `GetPurchaseDocumentsForBI(obj)`
3. `GetInventoryDocumentsForBI(obj)`
4. `GetAllForBI(obj)`
5. `BuildBoxVisioIngestPayload(obj)`
6. `HealthCheckBIBridge(obj)`

### Input parameters (`obj`)

- `clientID` (required; request guard)
- `company` (optional, defaults to `X.SYS.COMPANY`)
- `fromDate` (optional `YYYY-MM-DD`)
- `toDate` (optional `YYYY-MM-DD`)
- `limit` (optional, default `2000`, max `10000`)
- `includeSales`, `includePurchases`, `includeInventory` (for `GetAllForBI`)
- Optional source filters:
  - `salesSourceCodes` (default `1351`)
  - `purchaseSourceCodes` (default `1251`)
  - `inventorySourceCodes` (default `1151`)
- `debug` (optional, adds SQL in response)

### "No list dependency" behavior

The bridge reads directly from SQL tables (`FINDOC`, `MTRLINES`, `MTRDOC`, `TRDR`, `MTRL`) and auto-detects available columns through `INFORMATION_SCHEMA.COLUMNS`.  
So it does not require custom browser list setup.

## 4) Field Mapping To BI Payload

### Sales -> `sales_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`: transaction date
- `document_id`, `document_no`, `document_series`, `document_type`
- `branch_ext_id`, `warehouse_ext_id`
- `customer_ext_id`, `customer_name`
- `item_code`
- `qty`, `net_value`, `gross_value`, `cost_amount`

### Purchases -> `purchase_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`
- `document_id`, `document_no`, `document_series`, `document_type`
- `branch_ext_id`, `warehouse_ext_id`
- `supplier_ext_id`, `supplier_name`
- `item_code`
- `qty`, `net_value`, `cost_amount`

### Inventory -> `inventory_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`
- `document_id`, `document_series`, `document_type`
- `movement_type` (`entry` / `exit` based on quantity sign)
- `branch_ext_id`, `warehouse_ext_id`, `item_code`
- `qty`, `value_amount`

Note:

- Current inventory extraction is document/movement-based.
- If pure on-hand snapshot is required, add a dedicated snapshot query (for your specific SoftOne schema) and map to `qty_on_hand`.

## 5) Installation In SoftOne (Advanced JavaScript)

1. Open SoftOne Customization -> Advanced JavaScript.
2. Create (or select) package, e.g. `myWS`.
3. Add a module and paste script content from:
   - [boxvisio_bi_bridge.js](/opt/cloudon-bi/integrations/softone/boxvisio_bi_bridge.js)
4. Save and publish.
5. Test from SoftOne first:
   - `HealthCheckBIBridge({clientID:'test'})`
6. Test through web endpoint:
   - `POST https://<registered>.oncloud.gr/s1services/JS/myWS/GetAllForBI`

## 6) Request Examples

## 6.1 Get all 3 streams

```json
{
  "clientID": "<softone-client-id>",
  "company": 1000,
  "fromDate": "2026-01-01",
  "toDate": "2026-03-31",
  "limit": 3000,
  "includeSales": true,
  "includePurchases": true,
  "includeInventory": true
}
```

## 6.2 Get only sales

```json
{
  "clientID": "<softone-client-id>",
  "fromDate": "2026-03-01",
  "toDate": "2026-03-31",
  "limit": 1000,
  "includeSales": true,
  "includePurchases": false,
  "includeInventory": false,
  "salesSourceCodes": "1351,1353"
}
```

## 7) How To Feed BoxVisio BI

Use `BuildBoxVisioIngestPayload(obj)` output and send each stream to:

1. `POST /v1/ingest/sales`
2. `POST /v1/ingest/purchases`
3. `POST /v1/ingest/inventory`

Required BI headers:

1. `X-API-Key`
2. `X-Tenant`
3. `X-Signature` (HMAC-SHA256 of request body)

Important:

- The SoftOne script returns BI-ready data payload.
- Signature generation should be handled by your transport layer/orchestrator.

## 8) Validation Checklist

1. `HealthCheckBIBridge` returns expected table/column metadata.
2. `GetSalesDocumentsForBI` returns records with non-empty:
   - `event_id`, `doc_date`, `item_code`.
3. `GetPurchaseDocumentsForBI` returns non-empty `supplier_ext_id`.
4. `GetInventoryDocumentsForBI` returns non-empty `qty`.
5. BI ingest responses return queued/accepted without validation errors.
6. Dashboards in BI populate cards/tables for Sales/Purchases/Warehouse flows.

## 9) Known Constraints / Next Improvements

1. Inventory is movement-oriented by default; add dedicated on-hand snapshot query if required.
2. Source-specific document sign logic remains managed in BI Business Rules (already available in project).
3. Extend bridge with:
   - `cash_transactions`
   - `supplier_balances`
   - `customer_balances`
   when your SoftOne schema mapping is finalized.
