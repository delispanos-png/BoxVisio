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
4. `GetItemMasterForBI(obj)`
5. `GetCashTransactionsForBI(obj)`
6. `GetSupplierBalancesForBI(obj)`
7. `GetCustomerBalancesForBI(obj)`
8. `GetOperatingExpensesForBI(obj)`
9. `GetAllForBI(obj)`
10. `HealthCheckBIBridge(obj)`
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
  - `purchaseSourceCodes` (default `1251,1253`)
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

Sales extraction follows SoftOne revenue-document logic:
- only customer-side sales documents (`SODTYPE = 13`)
- only `SOSOURCE = 1351`
- only revenue behaviors (`TFPRMS IN (102,103,131,151,152,181)`)
- `TFPRMS 151/152/181` count negative
- therefore sales KPI totals follow:
  - `Sales = 102 + 103 + 131 - 151 - 152 - 181`

### Purchases -> `purchase_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`
- `document_id`, `document_no`, `document_series`, `document_type`
- `branch_ext_id`, `warehouse_ext_id`
- `supplier_ext_id`, `supplier_name`
- `item_code`
- `qty`, `net_value`, `cost_amount`

Purchase extraction follows SoftOne supplier-document logic:
- only supplier-side purchase documents (`SODTYPE = 12`)
- `SOSOURCE 1251` counts positive
- `SOSOURCE 1253` counts negative
- therefore purchase KPI totals follow:
  - `Purchases = 1251 - 1253`

### Inventory -> `inventory_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`
- `document_id`, `document_series`, `document_type`
- `movement_type` (`entry` / `exit` based on quantity sign)
- `branch_ext_id`, `warehouse_ext_id`, `item_code`
- `qty`, `value_amount`
- Only stock items are included: `MTRL.SODTYPE = 51`

Note:

- Current inventory extraction is document/movement-based.
- If pure on-hand snapshot is required, add a dedicated snapshot query (for your specific SoftOne schema) and map to `qty_on_hand`.

### Item master -> `item_master`

- `item_code`
- `item_name`
- `barcode` (from `MTRL.CODE1`)
- `alternate_barcodes` (from `MTRSUBSTITUTE.CODE`)
- `softone_sotype` (from `MTRL.SODTYPE`)
- `brand_external_id` (from `MTRL.MTRMARK`)
- `brand_name` (from `MTRMARK.NAME`)
- `vat_rate` (from `MTRL.VAT`)
- `vat_label` (from `VAT.NAME`)
- `category_1` (from `CCC88POCAT1.NAME`)
- `category_2` (from `CCC88POCAT2.NAME`)
- `category_3` (from `CCC88POCAT3.NAME`)
- `commercial_category` (from `MTRPCATEGORY.NAME`)
- `group_ext_id` (from `MTRL.MTRGROUP`)
- `group_name` (from `MTRGROUP.NAME`)
- `is_active` (from `ITEM.ISACTIVE` / `MTRL.ISACTIVE`)

Use case:

- metadata refresh of existing `dim_items`
- classification of true stock items (`SODTYPE = 51`)
- item barcode / brand / category enrichment from SoftOne item master

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

## 10) Balance Sign Logic

Customer and supplier open-balance streams follow SoftOne document behavior, not a naive source-only sum.

- `customer_balances`
  - sales invoices / retail sales on receivable account: `SOSOURCE=1351`, `SOREDIR IN (0,10000)`, `TFPRMS IN (102,103,131)` => positive
  - sales credit / cancellation behavior: `SOSOURCE=1351`, `SOREDIR IN (0,10000)`, `TFPRMS IN (151,152,181)` => negative
  - collections / customer transfers: `SOSOURCE IN (1381,1413)` => negative

- `supplier_balances`
  - purchase invoices / supplier expense debt / opening supplier debt: `SOSOURCE IN (1251,1261,1653)` => positive
  - purchase credit: `SOSOURCE=1253` => negative
  - supplier payments / transfers: `SOSOURCE IN (1281,1412,1416)` => negative

Open balance snapshots are calculated `ως ημερομηνία` (`toDate`) and should not be restricted by `fromDate`.


## 13) Release Workflow For Customer JS Files

Canonical source:

- `integrations/softone/boxvisio_bi_bridge.js`

After every SQL/querypack change that affects SoftOne sales extraction, run:

```bash
python3 integrations/softone/generate_bridge_release.py
```

What the generator does:

- validates that the canonical bridge still matches the current `sales_facts.sql` mappings
- creates a dated customer snapshot like `boxvisio_bi_bridge_2026-04-22.js`
- keeps the deployable customer JS under the same `integrations/softone/` folder

Validation currently guards these synchronized fields:

- `FINDOC.CCC88ECHANNEL` -> channel
- `FINDOC.PAYMENT` -> payment method
- `FINDOC.SOTIME/INSDATE` -> `source_created_at`
- `ITEM.MTRGROUP` -> item group
