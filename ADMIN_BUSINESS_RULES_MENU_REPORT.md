# Admin Business Rules Menu Report

Date: 2026-03-07

## Sidebar / Menu updates
Parent section added in Admin sidebar:
- `Κανόνες Λειτουργίας`

Subpages added in this order:
1. `Κανόνες Τύπων Παραστατικών`
2. `Κανόνες Κυκλωμάτων`
3. `Κανόνες Συμμετοχής KPI`
4. `Κανόνες Insights`
5. `Αντιστοίχιση Queries`

## Routes / URLs
- `/admin/business-rules`
- `/admin/business-rules/document-type-rules`
- `/admin/business-rules/stream-mapping-rules`
- `/admin/business-rules/kpi-participation-rules`
- `/admin/business-rules/intelligence-rules`
- `/admin/business-rules/query-mapping`

All routes are protected with:
- `require_roles(RoleName.cloudon_admin)`

## Files changed
- `backend/app/templates/base_admin.html`
- `backend/app/api/ui.py`
- `backend/app/core/i18n.py`
- `backend/app/templates/admin/business_rules_overview.html` (new)
- `backend/app/templates/admin/business_rules_placeholder.html` (new)

## Notes
- Pages are visible and navigable now.
- Subpages are placeholder UIs using the current admin template style.
- Backend rule APIs are already available under `/v1/admin/rules*` and `/v1/admin/tenants/{tenant_id}/rules*`.
