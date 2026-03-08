# UI Consistency Refactor

## Scope
Platform-wide UI/UX consistency refactor focused on:
- tenant operational streams
- tenant dashboards/analytics tables
- admin list pages
- shared layout/table/filter/pagination behavior

## 1) Shared Page Layout Rules

Unified page composition standard:
1. Page title + breadcrumb (base layout)
2. KPI summary cards (where applicable)
3. Filter section (top filter card)
4. Main charts
5. Main data table
6. Secondary insights/actions

Applied via:
- `backend/app/templates/base_tenant.html`
- `backend/app/templates/base_admin.html`
- shared style tokens and layout classes in `backend/app/static/css/bv-unified-ui.css`

## 2) Shared Table Component Rules

Unified table rules:
- standard class: `bv-unified-table`
- consistent header typography and spacing
- sticky header support where needed
- unified row hover/selection feel
- consistent table card footer with summary + pagination

Applied globally with:
- `backend/app/static/css/bv-unified-ui.css`
- `backend/app/static/js/bv-unified-ui.js`

Admin list pages with unified table tooling:
- client-side search
- sortable headers
- page size selector
- pagination controls
- range summary

## 3) Shared Pagination / Filter UX Rules

Unified pagination block:
- page size selector (`Ανά σελίδα`)
- page jump + `Μετάβαση`
- first/prev/next/last controls
- page info (`x / y`)
- visible range summary (`Εμφάνιση a έως b από total`)

Unified filter behavior:
- `Εφαρμογή` action
- `Καθαρισμός` action
- date fields normalized in `dd/mm/yyyy`
- search input resets pagination to first page
- standardized error handling blocks per page (`alert` with consistent placement)
- stable loading/empty states in tables/charts for failed requests

## 4) Refactored Pages (Route-by-Route)

### Tenant - Operational Streams
- `/tenant/sales-documents`
  - unified table + full pager + range summary
  - unified filter reset (`salesFiltersReset`)
- `/tenant/purchase-documents`
  - unified table + full pager + range summary
  - unified filter reset
- `/tenant/warehouse-documents`
  - unified table + full pager + range summary
  - pagination wired to API `limit/offset`
  - unified filter reset
- `/tenant/cashflow/{category_slug}` (documents mode)
  - unified table + full pager + range summary
  - pagination wired to API `limit/offset`
  - unified filter reset
- `/tenant/suppliers`
  - unified table visual style
  - unified pager footer style
  - filter reset added
  - unified backend error message rendering (status + detail)
- `/tenant/customers`
  - unified table visual style
  - unified pager footer style
  - search clear action added
  - unified backend error message rendering (status + detail) for list/detail requests
- `/tenant/cashflow/financial-accounts`
  - unified table + full pager + range summary
  - search reset + page navigation behavior
  - server-side pagination wiring (`limit/offset`)

### Tenant - Dashboards / Analytics (table visual consistency)
- `/tenant/dashboard`
- `/tenant/finance-dashboard`
- `/tenant/inventory`
- `/tenant/purchases`
- `/tenant/cashflow`
- `/tenant/cashflow/financial-accounts`
- `/tenant/insights`
- `/tenant/compare`
- `/tenant/items`
- `/tenant/messages`

Updates: standardized table styling through `bv-unified-table`.
Additional functional alignment:
- `purchases` KPI labels normalized to Greek product language
- `purchases` filter reset flow added
- `cashflow` summary page reset flow + consistent error handling added
- `price-control` unified table styling + localized error messages

### Admin Panel
Unified table behavior (search/sort/pagination) enabled on major list pages:
- `/admin/tenants`
- `/admin/users`
- `/admin/subscriptions`
- `/admin/sync-status`
- `/admin/insights`
- `/admin/connections`
- `/admin/plans`
- `/admin/messages`
- `/admin/business-rules/*` list pages
- `/admin/insight-rules`

## 5) Notes
- Stream document pages now share the same pagination/filter interaction model.
- No business KPI logic was changed in this refactor.
- Existing modal/detail behaviors were preserved and visually aligned.

## 6) Files Updated (UI Consistency Phase)

Core shared layer:
- `backend/app/static/css/bv-unified-ui.css`
- `backend/app/static/js/bv-unified-ui.js`
- `backend/app/templates/base_tenant.html`
- `backend/app/templates/base_admin.html`

Operational streams:
- `backend/app/templates/tenant/sales_dashboard.html`
- `backend/app/templates/tenant/purchase_documents_dashboard.html`
- `backend/app/templates/tenant/warehouse_documents_dashboard.html`
- `backend/app/templates/tenant/cashflow_documents_dashboard.html`
- `backend/app/templates/tenant/suppliers_dashboard.html`
- `backend/app/templates/tenant/customers_dashboard.html`

Additional tenant table consistency:
- `backend/app/templates/tenant/dashboard.html`
- `backend/app/templates/tenant/finance_dashboard.html`
- `backend/app/templates/tenant/inventory_dashboard.html`
- `backend/app/templates/tenant/purchases_dashboard.html`
- `backend/app/templates/tenant/cashflow_dashboard.html`
- `backend/app/templates/tenant/cashflow_accounts_dashboard.html`
- `backend/app/templates/tenant/insights.html`
- `backend/app/templates/tenant/compare.html`
- `backend/app/templates/tenant/items_dashboard.html`
- `backend/app/templates/tenant/messages.html`

Admin list consistency:
- `backend/app/templates/admin/tenants.html`
- `backend/app/templates/admin/users.html`
- `backend/app/templates/admin/subscriptions.html`
- `backend/app/templates/admin/sync_status.html`
- `backend/app/templates/admin/insights.html`
- `backend/app/templates/admin/connections.html`
- `backend/app/templates/admin/plans.html`
- `backend/app/templates/admin/messages.html`
- `backend/app/templates/admin/business_rules_domain.html`
- `backend/app/templates/admin/insight_rules.html`
