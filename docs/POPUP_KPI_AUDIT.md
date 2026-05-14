# Popup KPI Audit

Scope: Sales, Purchases, Warehouse/Inventory, eShop, Sell Out.

Date: 2026-05-13

## Rule

Popups should not repeat top-level dashboard KPIs unless the value is scoped to the opened entity.

Allowed inside popups:
- Document-level totals for the specific document being opened.
- Item-level or supplier-level values for the selected row.
- Breakdown rows that explain a clicked KPI.
- Operational fields that are not available on the main page.

Avoid inside popups:
- Repeating the same period-level total already shown in the page header.
- Showing generic KPI cards with no new slice, cause, rank, or drilldown.
- Repeating GMROI / sell-through / sales / stock when the row itself already shows the same values and the popup adds no context.

## Sales

File: `backend/app/templates/tenant/sales_dashboard.html`

Popup:
- `salesDocumentModal`

Current content:
- Header fields: branch, warehouse, series, type, document no, eShop code, customer, payment, shipping, reason, channel.
- Line table: item code/name, quantity, price, discount, VAT, line value.
- Document totals: net, expenses, VAT, gross, quantities.
- Delivery and notes tabs.

Assessment:
- Acceptable. The totals are document-level, not period-level duplicates.
- Cleanup applied: field label changed to "Έξοδα παραστατικού", because the value contains the document expense description/value and not a status.

## Purchases

Files:
- `backend/app/templates/tenant/purchases_dashboard.html`
- `backend/app/templates/tenant/purchase_documents_dashboard.html`

Popups:
- `purchaseDocumentModal`

Current content:
- Header fields: branch, warehouse, series, type, document no, supplier, date, payment, status, reason.
- Line table: item code/name, channel, qty, price, discounts 1/2/3, VAT, line value.
- Document totals: net, expenses, VAT, gross, cost, qty.
- Notes/audit fields.

Assessment:
- Document popup is acceptable. It shows one document, not repeated period totals.
- Main purchase documents "control center" duplicates some high-level concepts from `purchases_dashboard` but it is a documents-specific operational view. Keep it if the labels stay documents-oriented.
- Watch item: "Δαπάνη περιόδου" should be clearly "Θετικές αγορές" or "Καθαρή αξία παραστατικών" depending on the business meaning. Avoid using the same label as the executive purchases KPI if the formula differs.

## Warehouse / Inventory

Files:
- `backend/app/templates/tenant/inventory_dashboard.html`
- `backend/app/templates/tenant/warehouse_documents_dashboard.html`
- `backend/app/templates/tenant/items_dashboard.html`

Popups:
- `inventoryValueModal`
- `warehouseDocumentModal`
- `itemDetailModal`

Current content:
- `inventoryValueModal`: cost value, wholesale value, retail value, prospective profit, stock qty, turnover, coverage days, sell-through, GMROI, inactive stock, stock/sales.
- `warehouseDocumentModal`: document header, lines, document totals, movement fields, notes.
- `itemDetailModal`: item identity, stock, reserved qty, stock value, cost, sales 30d, purchases 30d, last sale, status, movement, raw fields.

Assessment:
- `warehouseDocumentModal` is acceptable because values are document-scoped.
- `itemDetailModal` is mostly acceptable because values are item-scoped.
- Cleanup applied: `inventoryValueModal` now stays focused on value analysis. The repeated smart KPI block was removed from the popup.

Kept in `inventoryValueModal`:
- Keep in `inventoryValueModal`: cost value, wholesale value, retail value, prospective wholesale/retail profit, margin explanation.
- Removed from `inventoryValueModal`: turnover, coverage days, sell-through, GMROI, inactive stock, stock/sales.
- Replaced with useful context: average cost per unit, wholesale premium over acquisition cost, retail premium over acquisition cost.

## eShop

File: `backend/app/templates/tenant/eshop_analysis_dashboard.html`

Popups:
- `eshopOrdersDetailsModal`
- `eshopShipmentsDetailsModal`
- `eshopRevenueDetailsModal`
- `eshopCityDetailsModal`
- `eshopBranchesDetailsModal`
- `eshopGenericDetailsModal`
- `eshopDocumentModal`

Current content:
- Almost every detail modal starts with mini KPI cards like Orders, Revenue, Average Order, Charges, Top payment/city/store.
- Then it shows the actual breakdown table.
- `eshopDocumentModal` is document-scoped and fine.

Assessment:
- Strong repetition risk. The mini KPI row repeats the same period-level Orders/Revenue/Charges already shown by the main eShop dashboard.
- The breakdown tables are useful. The repeated mini KPI cards are mostly not.

Cleanup applied:
- Replaced repeated mini KPI cards in Orders, Shipments, Revenue/Payments, Cities and Branches detail modals.
- Orders now shows execution model count, concentration of the top execution model and pickup/store share.
- Shipments now shows carrier count, average charge per shipment and charge share over revenue.
- Payments now shows payment method count, top payment share and average revenue per payment method.
- Cities now shows city count, top city share and average orders per city.
- Branches now shows branch count, top branch share and average orders per branch.
- Kept the breakdown tables, because they provide the drilldown.
- Kept Generic modal dynamic KPIs only where they are specific to the clicked KPI.
- Keep `eshopDocumentModal` as-is.

## Sell Out

File: `backend/app/templates/tenant/sellout_report.html`

Popups:
- `selloutDetailModal`
- `selloutActionZoomModal`

Current content:
- Main page already shows sales, qty, gross profit, margin, stock, sell-through, GMROI, stock/sales.
- Row table also shows item-level sales, qty, margin, profit, stock, sell-through, coverage, GMROI.
- Detail modal repeats many of those row values.
- Action zoom modal lists more rows for a selected action category.

Assessment:
- `selloutActionZoomModal` is acceptable: it expands a decision list.
- Cleanup applied: `selloutDetailModal` no longer repeats the sales/margin/stock/sell-through/coverage/GMROI values already visible on the row.

Kept in `selloutDetailModal`: item identity, supplier/brand/category, recommended reorder quantity, estimated lost demand and recommended action.
Added useful context: stock signal, priority diagnosis and next step.

## Priority

1. Done: eShop detail modals replace repeated period-level KPI cards with concentration/share/count context.
2. Done: Inventory value modal replaces repeated smart KPIs with valuation signals.
3. Done: Sell Out detail modal was reduced to product/action context and decision diagnosis.
4. Kept: Sales/Purchases/Warehouse document modals, because document totals are not duplicate page KPIs.

## Implementation Notes

- Do not remove document-level totals from document modals.
- Avoid changing backend payloads unless a frontend-only cleanup cannot solve the duplication.
- Prefer replacing repeated cards with contextual labels and ratios.
- Keep one source of truth for formulas in backend services.

## Table Footer Follow-Up

Cleanup applied after the popup KPI pass:
- Customers, Suppliers and Cash Accounts list footers now use the shared `bvRenderFooterSummary` pill layout instead of pipe-separated text.
- Sales, Purchase Documents and Warehouse Documents were already using the shared footer summary component.
- Kept range/help text such as "double click row" as plain text because it is instructional context, not a KPI summary.
