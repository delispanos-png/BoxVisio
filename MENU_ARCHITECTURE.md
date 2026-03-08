# Menu Architecture

## Sidebar UX Rules
- All primary sections are rendered as collapsible groups.
- Sidebar labels are intentionally short.
- Full descriptive titles are rendered inside each page header.
- Tenant/Admin sidebars default to Greek labels (`el`) and use short menu text to avoid visual overload.

## Tenant Portal (`/tenant/*`)

### Dashboard
- Executive Dashboard: `/tenant/dashboard` (manager persona)
- Finance Dashboard: `/tenant/finance-dashboard`
- Insights: `/tenant/insights`

### Επιχειρησιακά Κυκλώματα
- Παραστατικά Πωλήσεων: `/tenant/sales-documents`
- Παραστατικά Αγορών: `/tenant/purchase-documents`
- Παραστατικά Αποθήκης: `/tenant/warehouse-documents`
- Ταμειακές Συναλλαγές: `/tenant/cashflow` (+ subcategories under `/tenant/cashflow/{category}`)
- Υπόλοιπα Προμηθευτών: `/tenant/suppliers`
- Υπόλοιπα Πελατών: `/tenant/customers`

### Αναλύσεις
- Πωλήσεις: `/tenant/sales`
- Αγορές: `/tenant/purchases`
- Αποθέματα: `/tenant/inventory`
- Ταμειακές Ροές: `/tenant/cashflow`
- Απαιτήσεις / Υποχρεώσεις: `/tenant/analytics/receivables-payables`

### Συγκρίσεις
- Period vs Period: `/tenant/comparisons/period-vs-period`
- Branch vs Branch: `/tenant/comparisons/branch-vs-branch`
- Category vs Category: `/tenant/comparisons/category-vs-category`

### Εξαγωγές
- Reports: `/tenant/exports/reports`
- CSV / Excel: `/tenant/exports/csv-excel`

### Tenant visibility rules
- `tenant_admin` (manager persona): full menu.
- `tenant_user` (finance persona): finance-focused dashboard/analytics exposure.
- `Branch vs Branch` is hidden when `tenant_has_multiple_branches = false`.
- Cash Transactions submenu includes 5 explicit categories:
  - `customer_collections`
  - `customer_transfers`
  - `supplier_payments`
  - `supplier_transfers`
  - `financial_accounts`

## Admin Panel (`/admin/*`)

### Overview
- System Dashboard: `/admin/dashboard`
- Tenant Health: `/admin/overview/tenant-health`
- Sync Status: `/admin/sync-status`

### Tenants
- Tenants: `/admin/tenants`
- Subscriptions: `/admin/subscriptions`
- Users: `/admin/users`

### Data Sources
- Connectors: `/admin/connections`
- Stream Mapping: `/admin/data-sources/stream-mapping`
- Query Mapping: `/admin/data-sources/query-mapping`
- File Imports: `/admin/data-sources/file-imports`

### Κανόνες Λειτουργίας
- Κανόνες Τύπων Παραστατικών: `/admin/business-rules/document-type-rules`
- Κανόνες Κυκλωμάτων: `/admin/business-rules/stream-mapping-rules`
- Κανόνες Συμμετοχής KPI: `/admin/business-rules/kpi-participation-rules`
- Κανόνες Insights: `/admin/business-rules/intelligence-rules`
- Tenant Overrides: `/admin/business-rules/tenant-overrides`

### Επιχειρησιακά Κυκλώματα
- Παραστατικά Πωλήσεων: `/admin/operational-streams/sales-documents`
- Παραστατικά Αγορών: `/admin/operational-streams/purchase-documents`
- Παραστατικά Αποθήκης: `/admin/operational-streams/warehouse-documents`
- Ταμειακές Συναλλαγές: `/admin/operational-streams/cash-transactions`
- Υπόλοιπα Προμηθευτών: `/admin/operational-streams/supplier-balances`
- Υπόλοιπα Πελατών: `/admin/operational-streams/customer-balances`

### Monitoring
- Jobs: `/admin/monitoring/jobs`
- Dead Letter Queue: `/admin/monitoring/dead-letter-queue`
- Metrics: `/admin/monitoring/metrics`
- Logs: `/admin/monitoring/logs`

### Settings
- Plans: `/admin/plans`
- Feature Flags: `/admin/settings/feature-flags`
- System Defaults: `/admin/settings/system-defaults`

### Admin visibility rules
- Sidebar sections are intended for `cloudon_admin` users only.
