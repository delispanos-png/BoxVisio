# Business Rules Document Editor

## Purpose
`/admin/business-rules/document-type-rules` now uses a form-based editor for non-technical users.
No raw JSON editing is required.

## Form Fields
- `Document Type`
- `Stream` (`sales_documents`, `purchase_documents`, `inventory_documents`, `cash_transactions`)
- `Include in Revenue` (`yes/no`)
- `Include in Quantity` (`yes/no`)
- `Include in Cost` (`yes/no`)
- `Affects Customer Balance` (`yes/no`)
- `Affects Supplier Balance` (`yes/no`)
- `Amount Sign` (`positive/negative/none`)
- `Quantity Sign` (`positive/negative/none`)
- `Scope` (`global` or `tenant override`)
- `Override Mode` (`replace` or `merge`)

## Internal JSON
The UI submits form fields and backend generates payload JSON automatically.
JSON storage remains in:
- `global_rule_entries.payload_json`
- `tenant_rule_overrides.payload_json`

## Rule Resolution
Runtime precedence:
1. Tenant override
2. Global default

## SoftOne Templates
Default SoftOne document templates can be applied from:
- Main page template panel
- Setup wizard (`/admin/business-rules/document-type-rules/wizard`)

## Endpoints
- `GET /admin/business-rules/document-type-rules`
- `GET /admin/business-rules/document-type-rules/help`
- `GET /admin/business-rules/document-type-rules/wizard`
- `POST /admin/business-rules/document-type-rules/upsert-form`
- `POST /admin/business-rules/document-type-rules/apply-softone-template`
