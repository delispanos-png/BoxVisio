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
9. `GetSupplierOrdersForBI(obj)`
10. `GetAllForBI(obj)`
11. `HealthCheckBIBridge(obj)`
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
- `includeSupplierOrders` (optional, default `false`) for supplier purchase-order lines
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
- `qty`, `net_value`, `line_value`, `cost_amount`
- `discount1_pct`, `discount2_pct`, `discount3_pct` from `MTRLINES.DISC1PRC`, `DISC2PRC`, `DISC3PRC`
- `nodscamnt` / `no_discount_amount` from `MTRLINES.NODSCAMNT`

Purchase extraction follows SoftOne supplier-document logic:
- only supplier-side purchase documents (`SODTYPE = 12`)
- `TFPRMS 102` and `103` count positive
- `TFPRMS 151` and `152` count negative
- purchase analysis item value uses `MTRLINES.LINEVAL`
- purchase YTD/PYTD discount per item is the average line discount percentage:

### Supplier Orders -> `supplier_orders`

Supplier order extraction is used by the BI Supplier Orders circuit and by FnR/Availability as the source of expected incoming quantities before a new supplier order proposal is created.

SoftOne filters:
- `FINDOC.SOSOURCE = 1251`
- `FINDOC.SODTYPE = 12`
- `FINDOC.TFPRMS = 201`
- `FINDOC.SERIES IN (2021, 2031)`

Fields:
- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`, `document_id`, `document_no`, `document_series`, `document_series_name`
- `supplier_ext_id`, `supplier_name`, `supplier_afm`
- `item_code`, `item_name`
- `order_qty` from `MTRLINES.QTY1`
- `covered_qty` from `MTRLINES.QTY1COV`
- `cancelled_qty` from `MTRLINES.QTY1CANC`
- `line_value` from `MTRLINES.LINEVAL` fallback chain
- `order_status`: `open` or `closed`

Open/closed rule:
- an order is open when it has no transformation through `MTRLINES.FINDOCS` and `FINDOC.FULLYTRANSF` is not set
- once any transformation exists, BI treats the order as closed, even if the delivery was partial; supplier backorders are not maintained

Connector parity:
- The SQL connector exposes the same stream through `backend/querypacks/pharmacyone/facts/supplier_orders_facts.sql`.
- The JavaScript bridge endpoint is `GetSupplierOrdersForBI(obj)` and the combined endpoint flag is `includeSupplierOrders`.
  `AVG(DISC1PRC + DISC2PRC + DISC3PRC)` for the selected period
- purchase period spend uses document clean value plus document expenses:
  `net value + FINDOC.EXPN`

### Inventory -> `inventory_documents`

- `event_id` / `external_id`: `FINDOC-MTRLINES`
- `doc_date`
- `document_id`, `document_series`, `document_type`
- `movement_type` (`entry` / `exit` based on quantity sign)
- `branch_ext_id`, `warehouse_ext_id`, `item_code`
- `qty`, `qty_reserved`, `qty_expected`, `qty_available`, `value_amount`
- Replenishment metadata repeated on stock rows for BI enrichment:
  - `replenishment_status_1`
  - `replenishment_status_2`
  - `min_stock`
  - `replenishment_moq`
  - `vendor_moq`
  - `current_purchase_price`
- Only stock items are included: `MTRL.SODTYPE = 51`

Note:

- Current inventory extraction includes a current `MTRBALSHEET` stock snapshot plus warehouse movements.
- `qty_expected` is supplier-side expected stock already on the way to the business. It is read dynamically from `MTRBALSHEET` candidates (`EXPCTQTY1`, `EXPECTEDQTY`, `ONORDERQTY`, `ORDEREDQTY1`) when available; otherwise it defaults to `0`. It is used by FnR before calculating a new proposed supplier order.
- `qty_reserved` is read dynamically when matching SoftOne columns exist; otherwise it defaults to `0`.
- `qty_available = qty_on_hand - qty_reserved` for stock snapshots.
- `min_stock` and `replenishment_moq` are store/branch-specific and are read from `MTRBRNLIMITS` using `COMPANY + MTRL + BRANCH`; while SoftOne is not populated, BI sends/stores `1`.
- `vendor_moq` comes from `MTRSUPCODE.CCC88MOQ` (`Vendor MOQ`) joined by `MTRSUPCODE.MTRL`; BI uses the minimum positive MOQ per item and falls back to `1` when SoftOne is empty.

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
- `manual_order_category` (logical SoftOne item extra `UTBL04`; on pharmacy295 SQL this is stored in `MTREXTRA.UTBL04`)
- `commercial_status` (logical SoftOne item extra `UTBL05`; on pharmacy295 SQL this is stored in `MTREXTRA.UTBL05`, with `UTBL05.NAME` / `UTBL05.CODE` fallback)
- `replenishment_status_1` / `replenishment_status_2` (dynamic candidates from `MTREXTRA`/`ITEEXTRA` or `MTRL`)
- `min_stock` (not branch-specific in the item master refresh; defaults to `1` until inventory rows provide `MTRBRNLIMITS.REMAINLIMMIN`)
- `replenishment_moq` (not branch-specific in the item master refresh; defaults to `1` until inventory rows provide `MTRBRNLIMITS.REORDERLEVEL`)
- `vendor_moq` (`MTRSUPCODE.CCC88MOQ`, minimum positive value per item; fallback `1`)
- `current_purchase_price`
- `group_ext_id` (from `MTRL.MTRGROUP`)
- `group_name` (from `MTRGROUP.NAME`)
- `is_active` (from `ITEM.ISACTIVE` / `MTRL.ISACTIVE`)

Use case:

- metadata refresh of existing `dim_items`
- classification of true stock items (`SODTYPE = 51`)
- item barcode / brand / category enrichment from SoftOne item master
- FnR/Replenishment and Availability calculations without depending on Excel-only fields

### FnR / Availability SoftOne Field Strategy

The bridge resolves replenishment fields from the confirmed SoftOne sources and leaves pending fields empty instead of inventing values.

Resolved fields:

- `manual_order_category`: item extra `UTBL04` with `UTBL04.NAME` / `UTBL04.CODE` fallback. In the SoftOne SQL schema of pharmacy295 this is `MTREXTRA.UTBL04`; the JS bridge can fall back between `ITEEXTRA` and `MTREXTRA` when available.
- `commercial_status`: item extra `UTBL05` with `UTBL05.NAME` / `UTBL05.CODE` fallback. In the SoftOne SQL schema of pharmacy295 this is `MTREXTRA.UTBL05`; the JS bridge can fall back between `ITEEXTRA` and `MTREXTRA` when available.
- `replenishment_status_1`: candidates such as `REPLSTATUS1`, `FNRSTATUS1`, `STATUS1`, `STATUS_1`, `UTBL01`.
- `replenishment_status_2`: candidates such as `REPLSTATUS2`, `FNRSTATUS2`, `STATUS2`, `STATUS_2`, `UTBL02`.
- `min_stock`: `MTRBRNLIMITS.REMAINLIMMIN` (`Ελάχιστο όριο ανά κατάστημα / ΑΧ`), joined by `MTRBRNLIMITS.MTRL` and branch; fallback `1`.
- `replenishment_moq`: `MTRBRNLIMITS.REORDERLEVEL` (`Όριο αναπαραγγελίας ανά κατάστημα / ΑΧ`), joined by `MTRBRNLIMITS.MTRL` and branch; fallback `1`.
- `vendor_moq`: `MTRSUPCODE.CCC88MOQ`, joined by `MTRSUPCODE.MTRL`; when multiple supplier records exist, BI uses the minimum positive MOQ for the item. Fallback `1`.
- `current_purchase_price`: candidates such as `PURCHASEPRICE`, `PURCHASE_PRICE`, `LASTPURPRICE`, `FINALPURCHASEPRICE`, `NUM04`, falling back to `MTRL.PRICEW`.

For a customer-specific rollout, confirm the exact SoftOne custom field names and add them to the candidate list before production sync.

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
3. Keep SQL querypacks and JavaScript bridge in parity for every SoftOne stream. Current standalone streams are sales, purchases, inventory, cash, supplier balances, customer balances, operating expenses, item master, and supplier orders.

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

After every SQL/querypack change that affects SoftOne sales or purchase extraction, run:

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

## 14) Release Candidate Bridge (2026-05-21)

Release candidate deployable file:

- `integrations/softone/boxvisio_bi_bridge_2026-05-21.js`

Canonical source:

- `integrations/softone/boxvisio_bi_bridge.js`

Bridge version:

- `2026-05-19_15-30-00`

Checksums:

- canonical SHA-256: `a14d6df91565a77535f40e2c99fd0213eb90b0a5dd67e4a1e7399a96739781b6`
- release snapshot SHA-256: `4c25f50fc070681d9119ad9662904f52aca7d0ce53224cdd6f12e4565e8b4d01`

Generator validation:

- ERP source timestamp: PASS
- sales channel from document header: PASS
- payment method from `FINDOC.PAYMENT`: PASS
- item group from `ITEM.MTRGROUP`: PASS

Release rule:

- The RC file is the customer-facing JavaScript bridge for this release.
- Any SoftOne stream change must be applied to both SQL querypacks and the JavaScript bridge before a new customer bridge file is issued.
