# BUG_DIAGNOSIS_SUPPLIER_TARGET_ITEMS

## Scope
Module: `Supplier Targets / Supplier Agreements`  
Page: `Create / Edit Supplier Target` (`/tenant/supplier-targets`)

## Observed Issue
- Supplier dropdown loaded correctly.
- Items area stayed empty after supplier selection.

## Data Loading Trace (UI -> API -> Service -> DB)
1. UI triggers `GET /v1/kpi/supplier-targets/filter-options?supplier_ext_id=...` on supplier change.
2. API endpoint: `backend/app/api/kpi.py#get_supplier_targets_filter_options`.
3. Service method: `backend/app/services/supplier_targets.py#supplier_target_filter_options`.
4. DB reads from:
   - `fact_purchases`
   - `dim_items`
   - `dim_suppliers`
   - `supplier_target_items` + `supplier_targets` (history fallback)
   - `fact_sales` (30-day sales enrichment)

## Root Cause
Primary cause:
- Old query required an **inner join** from purchases to item master:
  - `join(DimItem, DimItem.external_id == FactPurchases.item_code)`
- If `dim_items` was partially populated or item mapping was not perfect, valid supplier purchases were excluded.

Secondary issues:
- Item response had only `value/label`, without business fields required by the screen.
- UX lacked explicit loading/empty states and table-level usability controls.

## Fix Implemented
### Backend
- Replaced strict item join with resilient logic:
  - `fact_purchases` + `outer join dim_items` (by `item_id` or `item_code`)
  - supplier match by normalized `supplier_ext_id` and supplier dimension fallback.
- Added supplier history fallback from previous agreements:
  - `supplier_target_items` joined with `supplier_targets`.
- Added 30-day metrics:
  - `sales_last_30_days`
  - `purchases_last_30_days`
- Returned enriched item payload fields:
  - `product_id`
  - `product_name`
  - `category`
  - `brand`
  - `sales_last_30_days`
  - `purchases_last_30_days`
  - plus backward-compatible `value/label`.

### Frontend
- Refactored items selector to a paginated multi-select table with checkboxes.
- Added:
  - loading state
  - empty-state messages
  - search/filter across product/code/brand/category
  - quick filters (`all`, `top selling`, `slow movers`)
  - page size + pagination
  - select current page / clear current page / toggle visible rows.

## Agreement Notes Requirement
- Added new schema field: `agreement_notes`.
- Form now uses:
  - Greek label: `Σχόλια Συμφωνίας`
  - English label visible in UI: `Agreement Notes`.
- Field is multiline, optional, saved/loaded in create/edit/details.

## Verification Checklist
1. Open `/tenant/supplier-targets`.
2. Click `Νέος στόχος`.
3. Select supplier and verify items load automatically.
4. Verify loading indicator appears while request is in-flight.
5. Search/filter items, change page size, move pages.
6. Select multiple items via checkboxes, save target, edit target, confirm selected items persist.
7. Fill `Σχόλια Συμφωνίας`, save, reopen target, verify value is persisted and visible in card details.
