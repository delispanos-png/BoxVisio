# BoxVisio BI - Technical Manual

## 1. Scope
This manual documents the production-ready setup of the BoxVisio BI platform:
- Multi-tenant SaaS BI for pharmacies
- DB-per-tenant architecture (PostgreSQL)
- Admin portal and tenant portal on separate hosts
- Ingestion + KPI + deterministic insights
- Docker Compose deployment on a single Ubuntu server

## 2. High-Level Architecture
- `control DB`: `bi_control`
- `tenant DBs`: `bi_tenant_<slug>`
- `api`: FastAPI (UI + API)
- `worker`: Celery tasks (ingestion, insights, backfills)
- `redis`: queue + cache + rate-limit backend
- `postgres`: control + tenant DB host
- `nginx`: reverse proxy, host routing, TLS, headers

Host routing:
- `bi.boxvisio.com` -> tenant UI + tenant API (`/api/*`)
- `adminpanel.boxvisio.com` -> admin UI + admin API (`/api/*`)

## 3. Repository Layout
- `backend/`: FastAPI app, templates, models, services
- `worker/`: Celery tasks
- `infra/`: nginx/docker infra assets
- `scripts/`: operational scripts (seed/demo/admin/tenant ops)
- `alembic/`: migrations (control + tenant)

## 4. Multi-Tenancy Model
- No tenant facts in control DB.
- Tenant context resolved from JWT (`tenant_id`) and/or host.
- Tenant DB engine/session created dynamically and cached with LRU-like eviction.
- Pooling configured for control and tenant engines.

## 5. Security Model
- JWT auth + RBAC (`cloudon_admin`, `tenant_admin`, `tenant_user`)
- Host-based guard:
  - admin host accepts admin role only
  - tenant host blocks admin-only tenant access
- Rate limiting middleware per tenant/user
- CSRF middleware for form flows
- Secure headers middleware
- `/metrics` restricted at nginx level
- SQL credentials encrypted in control DB (`tenant_connections.enc_payload`)

## 6. Subscription/Plan Enforcement
Plans:
- `standard`: sales only
- `pro`: sales + purchases
- `enterprise`: sales + purchases + inventory + cashflow (if `source=pharmacyone`)

Enforcement points:
- middleware plan/subscription guards
- feature flag checks at UI and API levels

## 7. Data Pipelines
### 7.1 Ingestion
- SQL Server connector (PharmacyOne) via worker queue
- External ingest API with auth/signature/idempotency
- Writes canonical facts to tenant PostgreSQL
- No live remote SQL for dashboards

### 7.2 Canonical Tables
Facts:
- `fact_sales`, `fact_purchases`, `fact_inventory`, `fact_cashflows`

Dimensions:
- `dim_branches`, `dim_items`, `dim_suppliers`, `dim_brands`, `dim_categories`, `dim_groups`, `dim_warehouses`, `dim_calendar`

### 7.3 Aggregates
- `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`
- `agg_purchases_daily`, `agg_purchases_monthly`
- `agg_inventory_daily`, `agg_inventory_monthly`
- `agg_cashflow_daily`

## 8. KPI Layer
Key endpoints (examples):
- Sales summary/by-branch/by-brand/by-category/compare/decision-pack
- Purchases summary/by-supplier/compare/decision-pack
- Inventory snapshot/stock-aging/by-brand/by-commercial-category/by-manufacturer
- Cashflow summary/by-type/monthly-trend

Dashboards read from PostgreSQL only (facts/aggregates), never from remote SQL Server.

## 9. Intelligence Engine (Deterministic)
- Rule-based daily insight generation per tenant
- Stored in tenant DB tables: `insight_rules`, `insights`, `insight_runs`
- Supports status lifecycle: `open -> acknowledged -> resolved`
- Trigger methods:
  - scheduled (worker beat)
  - on-demand from admin/tenant UI

## 10. Internationalization (EL/EN)
- Translation dictionary: `backend/app/core/i18n.py`
- Template usage: `{{ tt(request, 'key') }}`
- Current implementation includes bilingual labels/messages for core dashboard and filter flows.

## 11. Deployment (Docker Compose)
Typical commands:
```bash
cd /opt/cloudon-bi
docker compose up -d
docker compose logs -f api
```

Restart services:
```bash
docker compose restart api
docker compose restart worker
```

## 12. Database Operations
Control DB migration:
```bash
make migrate-control
```

Tenant DB migration:
```bash
make migrate-tenant TENANT_SLUG=<slug>
```

Create tenant DB:
```bash
make create-tenant TENANT_SLUG=<slug>
```

Seed admin:
```bash
make seed-admin
```

## 13. Demo/UAT Data Seeding
Script:
```bash
python scripts/seed_demo_data.py --tenant-slug uat-a --years 2024,2025,2026
python scripts/seed_demo_data.py --tenant-slug uat-b --years 2024,2025,2026
```

Extended dataset option (more demo items):
```bash
python scripts/seed_demo_data.py --tenant-slug uat-a --years 2024,2025,2026 --item-count 60
python scripts/seed_demo_data.py --tenant-slug uat-b --years 2024,2025,2026 --item-count 60
```
- `--item-count` supports `6..200` and generates dynamic SKUs/items for richer KPI coverage.

## 14. Date Format Policy
- UI date display is standardized globally to `dd/mm/yyyy` (and `dd/mm/yyyy hh:mm[:ss]` for timestamps).
- Implemented in:
  - `backend/app/templates/base_tenant.html`
  - `backend/app/templates/base_admin.html`
- Note:
  - HTML `input[type=date]` still stores ISO value (`yyyy-mm-dd`) for API compatibility.
  - Presentation layer normalizes visible text to `day/month/year`.

## 15. Insights "View Details" Behavior
- From Sales dashboard insight cards, `View details` now passes current filters/date-range to `/tenant/insights`.
- `/tenant/insights` loads filters from query string automatically.
- If no persisted intelligence insights exist for the selected period, the page now shows fallback insight rows from `sales decision-pack` so the page is not empty.

## 16. Latest Operational Update (2026-03-01)
- Demo data reseeded for both UAT tenants with richer catalog:
  - `item_count=60`
  - years: `2024,2025,2026`
- Example execution:
```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python /opt/cloudon-bi/scripts/seed_demo_data.py --tenant-slug uat-a --years 2024,2025,2026 --item-count 60
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python /opt/cloudon-bi/scripts/seed_demo_data.py --tenant-slug uat-b --years 2024,2025,2026 --item-count 60
```

### UI Naming Improvements
- Sales and Purchases filter options now return value-label maps from dimensions, so UI can display:
  - `Brand Name (BRNx)` instead of only `BRNx`
  - same behavior for branches/warehouses/categories/groups
- Sales top-products endpoint now returns `item_name` along with `item_code`, and UI displays both.
- Brand-to-manufacturer mapping in demo seed:
  - `Frezyderm -> Frezyderm ABEE`
  - `La Roche-Posay -> L'Oreal Hellas`
  - `Vichy -> L'Oreal Hellas`
  - `Korres -> KORRES SA`
  - `Apivita -> APIVITA SA`
  - `Lanes -> Lanes Health`
  - `Solgar -> Solgar Hellas`
- Dropdown label policy:
  - Show only display name in UI (no `(CODE)` suffix)
  - Keep code as underlying value for filtering/API compatibility

### Date Display Improvements
- Global date rendering updated for dynamic content as well (MutationObserver-based).
- Any ISO date/timestamp text injected after page load is normalized to `dd/mm/yyyy` (or with time).

Regenerate insights:
- via UI run-now action, or
- via worker/admin endpoints

## 17. Latest Stabilization Update (2026-03-08)
- Supplier Targets module refinement completed:
  - supplier item loading now derives from purchases + supplier target history fallback.
  - item payload enriched with category/brand and 30-day sales/purchases metrics.
  - editor moved to a searchable/paginated checkbox table with loading/empty states.
- Supplier agreement notes promoted to first-class field:
  - model field: `supplier_targets.agreement_notes`
  - migration: `20260308_0009_tenant_supplier_target_agreement_notes.py`
  - API + UI support in create/edit/details flow.
- Business Rules Document Type configuration now has form-based UI:
  - `/admin/business-rules/document-type-rules`
  - `/admin/business-rules/document-type-rules/wizard`
  - `/admin/business-rules/document-type-rules/help`
- Related docs:
  - `BUSINESS_RULES_DOCUMENT_EDITOR.md`
  - `BUG_DIAGNOSIS_SUPPLIER_TARGET_ITEMS.md`
- Migration alignment completed for runtime stability:
  - control DB advanced to `20260308_0007_control` (professional profile enforcement path).
  - tenant DBs advanced to `20260308_0009_tenant` (`agreement_notes` support).
  - `scripts/run_tenant_migrations.py` pinned tenant head updated to `20260308_0009_tenant`.
  - `make migrate-control` target updated to run current control revision path with venv Alembic.

### 17.1 Performance Sanity Snapshot
- `scripts/benchmark_dashboard_perf.py` latest run:
  - executive summary: `~47ms` miss, `~12ms` cache hit
  - finance summary: `~25ms`
  - inventory stream summary: `~312ms`
  - cash stream summary: `~16ms`
- Smoke tests confirmed authenticated `200` responses for key tenant/admin pages and KPI APIs.

## 18. Observability
- Structured JSON logging
- `/health`, `/ready`
- `/metrics` (restricted)
- Pool stats metrics for control/tenant engines
- Queue/worker health via Docker + Celery

## 19. Backup & Restore Strategy
- Nightly backups for `bi_control` + each tenant DB
- Retention policy and restore drill required
- Validate restore into clean environment before production changes

## 20. Troubleshooting
### 20.1 Inventory/Cashflow page error
- check plan/source gating (`enterprise` + `pharmacyone`)
- check API logs (`docker logs cloudon_bi-api-1`)
- verify tenant DB has facts/aggregates

### 20.2 Empty charts
- verify filter date range
- verify aggregate refresh after ingestion/seed
- inspect endpoint JSON directly

### 20.3 Wrong language text
- add/update key in `i18n.py`
- replace hardcoded template text with `tt(request, 'key')`
- restart `api`

## 17. Change Control
For each change:
1. patch code
2. restart affected service
3. validate endpoint + UI

## 21. Integration References
- `INTEGRATION_GUIDE.md`
- `INTEGRATION_EXAMPLES.md`
- `DATA_STRUCTURE_OVERVIEW.md`
- `DATA_STRUCTURE_SALES_DOCUMENTS.md`
- `DATA_STRUCTURE_PURCHASE_DOCUMENTS.md`
- `DATA_STRUCTURE_INVENTORY_DOCUMENTS.md`
- `DATA_STRUCTURE_CASH_TRANSACTIONS.md`
- `DATA_STRUCTURE_SUPPLIER_BALANCES.md`
- `DATA_STRUCTURE_CUSTOMER_BALANCES.md`

## 22. User Manual + Context Help (2026-03-08)
- Added tenant user manual route and page:
  - UI route: `/tenant/manual`
  - template: `backend/app/templates/tenant/user_manual.html`
  - markdown companion: `USER_MANUAL.md`
- User manual is now training-oriented (not only informational):
  - each section includes `learning goal`, `step-by-step flow`, `practice exercise`.
  - language is simplified for non-technical users.
- Added visual training assets (stream diagrams) under:
  - `backend/app/static/docs/streams/sales-documents.svg`
  - `backend/app/static/docs/streams/purchase-documents.svg`
  - `backend/app/static/docs/streams/warehouse-documents.svg`
  - `backend/app/static/docs/streams/cash-transactions.svg`
  - `backend/app/static/docs/streams/supplier-balances.svg`
  - `backend/app/static/docs/streams/customer-balances.svg`
  - `backend/app/static/docs/streams/supplier-targets.svg`
- Added context help button (`Βοήθεια`) in tenant page header for operational circuits.
  - implemented in `backend/app/templates/base_tenant.html` via active-page -> anchor mapping.
  - help link pattern: `/tenant/manual#<section-anchor>`
- Current help mappings:
  - `sales_documents` -> `#sales-documents`
  - `purchase_documents` -> `#purchase-documents`
  - `warehouse_documents` -> `#warehouse-documents`
  - `cashflow` -> `#cash-transactions`
  - `suppliers` -> `#supplier-balances`
  - `customers` -> `#customer-balances`
  - `supplier_targets` -> `#supplier-targets`
  - `sales` -> `#sales-analytics`
  - `purchases` -> `#purchases-analytics`
  - `inventory` -> `#inventory-analytics`
  - `items` -> `#warehouse-items`

### Supplier Agreements Output Actions
- Details modal supports:
  - `Εκτύπωση` (print-friendly summary view)
  - `Αποστολή email` (prefilled `mailto` body)
- Agreement details screen remains progress-oriented.
- Print/email summary focuses on basic agreement attributes:
  - agreement amount
  - rebate percentage and fixed rebate amount
  - participating items
  - agreement notes/comments
4. confirm logs have no traceback
5. document in changelog/runbook

## 18. Current Notes
- Movers chart now uses:
  - fast movers from sales quantities
  - real purchases quantities for same items/period/filters
- Inventory and Cashflow filters use multi-select dropdown pattern with search/select-all/clear.
- Insights panel uses modern severity cards and metadata badges.

### Sales Filter Date Input (dd/mm/yyyy)
- Sales dashboard filter fields (`from`, `to`) now use explicit `dd/mm/yyyy` display/input format.
- UI converts `dd/mm/yyyy` to ISO (`yyyy-mm-dd`) before API calls.
- This avoids browser-locale reversal issues in native date pickers.

### Global Filter Date Standardization (01/03/2026)
- All filter date inputs across tenant + admin UI now use the same format:
  - display/input: `dd/mm/yyyy`
  - backend/API transport: `yyyy-mm-dd` (auto-converted at submit/request time)
- Implemented globally in `base_tenant.html` and `base_admin.html`:
  - `bvToIsoDate(raw)`
  - `bvToDmyDate(raw)`
  - `bvInitDmyDateInputs(rootNode)`
- Any input with `data-date-format="dmy"` is automatically normalized and converted.
- Updated pages:
  - Tenant: dashboard, sales, purchases, comparison, inventory, cashflow, insights
  - Admin: connections backfill (`from_date`, `to_date`)

### KPI Label Normalization (01/03/2026)
- Sales KPI responses now prioritize display names from dimensions instead of technical codes.

### Adminpanel UI/UX Refresh (02/03/2026)
- Global admin shell redesign applied through:
  - `backend/app/templates/base_admin.html`
  - `backend/app/static/css/boxvisio-ui.css`
- New UX additions:
  - Functional sidebar toggle (collapse/expand)
  - Functional admin menu search in topbar (filters sidebar entries live)
  - Enhanced page header with contextual subtitle per section
  - Improved visual hierarchy for cards/tables/forms/buttons
  - Better hover/focus states and dark-mode parity
- Insight Rules UX:
  - Rule list simplified to read-focused rows with `Edit` action
  - Rule editing moved to dedicated page:
    - `/admin/insight-rules/{tenant_id}/{code}/edit`
  - Larger JSON editor area (`min-height: 360px`, monospace font, vertical resize)
  - Tenant selector and edit header now show tenant **name** (with slug helper), not slug only.

### Translation + Admin Server Info Update (01/03/2026)
- Greek translation refinements applied for tenant/admin UI keys:
  - `stock_aging` -> `Παλαιότητα Αποθέματος`
  - `entry_type_details` -> `Λεπτομέρεια Τύπων Εγγραφής`
  - `baseline` -> `Βάση σύγκρισης`
  - `metric` -> `Μετρική`
  - added keys: `product`, `inflow`, `outflow`, `refund`
- Sales top-products/movers now strip SKU/code suffixes like `ITM55` in both:
  - backend KPI payloads (`_clean_item_name` in `kpi_queries.py`)
  - frontend rendering fallback
  (clean display name only; code remains internal for API/filtering).
- Admin Dashboard now includes **Server Info** live cards:
  - CPU usage (%)
  - RAM usage (% + used/total GB)
  - Disk usage (% + used/total GB)
- Admin Server Info is now **auto-refreshing every 5 seconds** via:
  - `GET /admin/server-info.json`
  - no page reload required.
- Server metrics source:
  - CPU: `/proc/stat` short interval sampling
  - RAM: `/proc/meminfo`
  - Disk: `shutil.disk_usage('/')`
- Fixed areas:
  - `Συνεισφορά Καταστημάτων`: now returns branch names (`DimBranch.name`) with fallback to code.
  - `Ταχυκίνητα Προϊόντα και οι Αγορές τους`: now returns/uses item names (`DimItem.name`) for chart labels.
- API payload now also includes optional raw codes for traceability:
  - `branch_code` alongside `branch`
  - `item_code` alongside `item_name`

### Inventory Filter Labels Fix (02/03/2026)
- `GET /v1/kpi/inventory/filter-options` now returns `labels` maps like sales/purchases:
  - `labels.branches`, `labels.warehouses`, `labels.brands`, `labels.categories`, `labels.groups`
- Backward-compatible alias added:
  - `GET /kpi/inventory/filter-options` (same response schema as `/v1/...`)
- Tenant inventory UI (`inventory_dashboard.html`) now uses labels in:
  - dropdown options
  - search within dropdown
  - selected chips
- Tenant items UI (`items_dashboard.html`) already consumes these labels; after API fix, brand/category filters now render readable names instead of technical codes (`BRN1`, `CAT1`, etc.).

### Cashflow Branch Filter Labels Fix (02/03/2026)
- Tenant cashflow UI (`cashflow_dashboard.html`) now renders branch labels (e.g. `Έδρα`, `Κηφισιά`, `Σπάτα`) instead of raw codes (`BR1`, `BR2`, `BR3`) in:
  - dropdown options
  - search match text
  - selected chips
- The page continues using `GET /v1/kpi/sales/filter-options` as source for branch options, but now consumes `labels.branches` correctly.

### Items Dashboard KPI Contrast + Category Seed (02/03/2026)
- UI fix:
  - Added missing KPI color classes in `boxvisio-ui.css`:
    - `.bv-kpi-red`
    - `.bv-kpi-cyan`
  - This fixes the last two KPI cards in `Είδη Αποθήκης` that appeared white/low-contrast.
- Data seed fix:
  - Inserted extra demo products and facts to both tenant DBs (`bi_tenant_uat-a`, `bi_tenant_uat-b`) with balanced distribution across all 3 categories:
    - `Παραφάρμακα`
    - `Συμπληρώματα`
    - `Δερμοκαλλυντικά`
  - Also inserted demo inventory/sales/purchases rows for immediate KPI visibility.

### Purchases UX Upgrade (01/03/2026)
- Improved readability for:
  - `Περιθώριο ανά Προμηθευτή`
  - `Εντοπισμός Μεταβολής Κόστους`
- Added:
  - currency formatting in `el-GR` with `€`
  - supplier name normalization in UI (`SUP_` prefix hidden for display)
  - margin status badges (good/medium/low)
  - card-style cost-change alerts with clear direction (`▲` increase / `▼` decrease)

### KPI + Inventory Items Expansion (01/03/2026 - Night Batch)
- Completed general consistency pass on tenant dashboards and KPI API wiring.

#### New KPI Categories (Sales + Purchases)
- Added seasonality KPI dataset in decision packs:
  - `sales_decision_pack -> seasonality`
  - `purchases_decision_pack -> seasonality`
- Added new-item-codes KPI dataset in decision packs:
  - `sales_decision_pack -> new_codes`
  - `purchases_decision_pack -> new_codes`
- New visuals:
  - Sales dashboard:
    - `Πωλήσεις ανά Εποχή` chart
    - `Νέοι Κωδικοί (Πωλήσεις)` table
  - Purchases dashboard:
    - `Αγορές ανά Εποχή` chart
    - `Νέοι Κωδικοί (Αγορές)` table

#### New BI Section: Warehouse Items
- Added tenant route/page:
  - `GET /tenant/items`
  - template: `tenant/items_dashboard.html`
- Added KPI endpoint:
  - `GET /v1/kpi/inventory/items`
- Capabilities:
  - Full items list from latest inventory snapshot (`as_of`)
  - Search by name/code
  - Filters: branches, warehouses, brands, categories
  - Status filter: `active/inactive`
  - Movement filter: `fast/normal/slow`
  - KPI cards:
    - total items
    - active items
    - inactive items
    - fast items
    - slow items
    - total stock value
  - Fast-items chart (sales 30d vs stock qty)

#### Date Format Standardization Reinforced
- Updated date filter values to explicit `dd/mm/yyyy` rendering in templates:
  - tenant dashboard
  - purchases dashboard
  - compare dashboard
  - items dashboard
  - insights dashboard
- Keeps backend/API date transport in ISO (`yyyy-mm-dd`) via JS conversion.

#### Translation Updates
- Added/updated keys:
  - `new_codes_purchases` -> `Νέοι Κωδικοί (Αγορές)`
  - `sales_30d`
  - `purchases_30d`

#### Verification
- Static compile check executed:
```bash
cd /opt/cloudon-bi
python3 -m compileall backend/app
```
- Result: success (no Python syntax errors).

### Adminpanel UI/UX Refresh v2 + Deploy (02/03/2026)
- Visual upgrade for adminpanel applied in `backend/app/static/css/boxvisio-ui.css`:
  - stronger card look (border/shadow/radius),
  - cleaner table row interaction,
  - more consistent control/button sizing,
  - improved dark-mode depth.
- Cache-busting update:
  - `backend/app/templates/base_admin.html` now loads:
    - `/static/css/boxvisio-ui.css?v=20260302d`
- Runtime rollout:
  - restarted services:
    - `docker compose -f /opt/cloudon-bi/docker-compose.yml restart api nginx`
  - status verified healthy for `api`/`nginx`.
- Verification from logs:
  - `GET /static/css/boxvisio-ui.css?v=20260302d` served with `200`.

### Items List UX + Quantity Formatting (02/03/2026)
- Tenant UI: sticky headers added for `Είδη Αποθήκης` list table.
  - Added dedicated scroll container (`.items-table-scroll`) with persistent table headers during vertical scroll.
- Quantity formatting policy reinforced (no decimals in quantities):
  - `items_dashboard.html`: `qty_on_hand`, `sales_qty_30`, `purchases_qty_30` now rendered as rounded integers.
  - Items fast-movers chart datasets now use rounded integer quantity values.
  - `inventory_dashboard.html`: quantity fields/cards/tables now use integer formatter.
  - `compare.html`: quantity values in A/B table now display as integers.
- Tenant CSS cache token updated:
  - `base_tenant.html` now loads `boxvisio-ui.css?v=20260302c`.

### Sales Insights UX + Business Suggestions (02/03/2026)
- Sales decision-pack service now returns group breakdown:
  - Added `sales_by_group(...)` in `backend/app/services/kpi_queries.py`
  - Added `by_group` in `sales_decision_pack` response payload.
- Sales dashboard insights area upgraded:
  - `insights-grid` widened (`minmax(460px, 1fr)`) for cleaner readability.
  - Insight title/message wrapping improved for multi-line readability (no hard single-line truncation).
  - Mobile fallback preserved with single-column insights grid under 768px.
- New contextual “business suggestion” insights generated client-side from real KPI data:
  - best product (`top_products[0]`)
  - best brand (`by_brand[0]`)
  - best category (`by_category[0]`)
  - best group (`by_group[0]`)
  - best branch (`by_branch[0]`).
- The insights panel now merges:
  - rule-engine insights (`insight_records`) + contextual business suggestions.

### Sales/Purchases/Cashflow Formatting + Insights Labels Fix (02/03/2026)
- Sales business insights:
  - `BEST BRAND` / `BEST CATEGORY` now use dimension names (not raw codes like `BRN1` / `CAT3`).
  - Added name resolution in `sales_by_brand` and `sales_by_category` via joins to `DimBrand` / `DimCategory`.
  - Context insight text simplified to avoid duplicate information in message + metric pills.
  - Insight type titles mapped to readable Greek labels (e.g. `Καλύτερη Μάρκα`, `Καλύτερη Κατηγορία`).
  - Info icon updated to trophy style (`fe-award`) for business insights.
  - Details default link now routes to `/tenant/insights` (no empty filtered target).
- Number formatting consistency:
  - Integer KPI quantities/cards now use grouped `el-GR` formatting (thousands separators) in:
    - sales dashboard (`cardQty`)
    - tenant home dashboard (`kpiMonthQty`, `kpiActiveBranches`)
    - cashflow dashboard (`kpiCashEntries`, by-type entries table)
    - purchases dashboard (`cardQty`, new-codes purchases qty table)
- Purchases KPI labels localized:
  - Added i18n keys:
    - `purchased_qty`
    - `units`

### Insights v2 Split by Logic (02/03/2026)
- Sales Insights now include explicit “best” split by business logic:
  - profitability (`κερδοφορία`)
  - quantity (`ποσότητα πωλήσεων`)
  - seasonality (`εποχικότητα`)
- Applied to entities:
  - product, brand, category, group, branch.
- Backend:
  - `sales_decision_pack()` now returns `best_entities`.
  - For each entity it calculates:
    - `profitability` (best by `net - cost`)
    - `quantity` (best by `qty`)
    - `seasonality` (best by strongest seasonal share of turnover).
- Frontend:
  - sales insights renderer consumes `best_entities`.
  - Added label fallback from filter labels map to reduce raw codes in UI.
  - Added titles for all generated insight types: `BEST_*_PROFITABILITY`, `BEST_*_QUANTITY`, `BEST_*_SEASONALITY`.

### Global Filters Dropdown Visibility Fix (02/03/2026)
- Fixed issue where multi-select filter dropdowns were clipped/hidden after opening.
- Root cause:
  - global card style had `overflow: hidden` and cut absolute-positioned menus.
- Changes:
  - `boxvisio-ui.css`:
    - increased `.bv-multi-menu` z-index to `3200`
    - `.bv-multi:focus-within` z-index to `3300`
    - global override for filter cards:
      - `.filter-card`, `.bv-filters-panel`, grid rows/cols set to `overflow: visible !important`
      - filter card stacking context raised (`z-index`).
  - cache bust:
    - `base_tenant.html` CSS version updated to `boxvisio-ui.css?v=20260302e`.

### Supplier Targets UX + Rebate Amount + Supplier Item Scope (04/03/2026)
- Supplier Targets editor behavior:
  - editor is hidden by default and opens only on:
    - `Νέος στόχος`
    - `Edit` existing target.
  - added close action (`Κλείσιμο`) and reset action (`Νέος στόχος`) inside editor.
- Supplier-scoped items:
  - `GET /v1/kpi/supplier-targets/filter-options` now accepts `supplier_ext_id`.
  - when supplier is selected, `Είδη που συμμετέχουν στον στόχο` is filtered to that supplier’s items (derived from purchases + item dimensions).
- Large item list UX:
  - target items multi-select enlarged (`size=16`, min-height 360px).
  - added inline search box for item code/name filtering.
- Rebate amount support:
  - new field: `Επιστροφή ποσού (€)` alongside `Επιστροφή %`.
  - API payloads now support `rebate_amount` on create/update.
  - progress cards include both components in credit calculation.
- Tenant schema:
  - new column on `supplier_targets`: `rebate_amount NUMERIC(14,2) NOT NULL DEFAULT 0`.
  - migration: `20260304_0016_tenant`.
  - migration runner updated to latest tenant head (`run_tenant_migrations.py`).

### Items Dashboard Load + Supplier Target Filtering (05/03/2026)
- Items dashboard:
  - guarded bootstrap modal instantiation to avoid JS crash when bootstrap is not present.
  - prevents silent failure that blocked `/v1/kpi/inventory/items` fetch.
- Supplier targets:
  - filter-options now resolves supplier scope by:
    - `fact_purchases.supplier_ext_id` OR
    - `fact_purchases.supplier_id -> dim_suppliers.external_id/name`.
  - ensures item list is truly scoped to selected supplier even when ext_id mapping varies.
- Filters dropdown visibility:
  - raised filter card/menu stacking context for multi-selects.
- Cache bust:
  - `boxvisio-ui.css` version bumped to `20260305a` (tenant + admin).

## 21. Data Ingestion Structure Specifications (2026-03-08)

Official external ingestion specifications are now documented and versioned for all six operational streams.

Purpose:
- define required/optional canonical fields
- standardize validation/business rules for external integrations
- provide JSON and CSV examples for connectors, APIs, and file imports

These specs are the reference for:
- SQL connectors
- API integrations
- CSV/Excel/SFTP imports
- external ERP integrations

### Master Index
- `DATA_STRUCTURE_OVERVIEW.md`
- `INTEGRATION_GUIDE.md`
- `INTEGRATION_EXAMPLES.md`

### Stream Specifications
1. `DATA_STRUCTURE_SALES_DOCUMENTS.md`
2. `DATA_STRUCTURE_PURCHASE_DOCUMENTS.md`
3. `DATA_STRUCTURE_INVENTORY_DOCUMENTS.md`
4. `DATA_STRUCTURE_CASH_TRANSACTIONS.md`
5. `DATA_STRUCTURE_SUPPLIER_BALANCES.md`
6. `DATA_STRUCTURE_CUSTOMER_BALANCES.md`

### Governance Notes
- Mapping to canonical fields happens in connector mapping/staging, not dashboard logic.
- Ingestion remains idempotent using source keys (`external_id`) and connector context.
- Validation failures must be routed to DLQ with explicit reason and source payload reference.
- CloudOn customer onboarding should start from `INTEGRATION_GUIDE.md` and then apply stream-level specs.
