# First Production Tenant Onboarding SOP

## Goal
Onboard first live tenant end-to-end with controlled validation and subscription activation.

## Inputs Required
- Tenant legal name and slug (e.g. `pharma-athens`).
- Tenant admin email.
- Plan (`standard` / `pro` / `enterprise`).
- Source (`pharmacyone` or `external`).
- If PharmacyOne: SQL Server connection/mapping details.

## Step 1: Create Tenant
Use admin UI wizard (`/admin/tenants`) or API.

API example:
```bash
curl -fsS -X POST https://adminpanel.boxvisio.com/v1/admin/tenants/wizard \
  -H "Authorization: Bearer <cloudon_admin_jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"First Pharmacy",
    "slug":"pharma-athens",
    "admin_email":"owner@pharmacy.gr",
    "plan":"pro",
    "source":"pharmacyone",
    "subscription_status":"trial",
    "trial_days":14
  }'
```

Expected:
- Wizard returns `status=ok`.
- Tenant DB created (`bi_tenant_<slug>`).
- Tenant admin user created with invite/reset token.
- Tenant API key generated.

## Step 2: Connect Data Source

### 2A) PharmacyOne SQL
Configure mapping:
```bash
curl -fsS -X PUT https://adminpanel.boxvisio.com/v1/admin/tenants/<tenant_id>/sqlserver/mapping \
  -H "Authorization: Bearer <cloudon_admin_jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "connection_string":"Driver={ODBC Driver 18 for SQL Server};Server=<host>;Database=<db>;UID=<user>;PWD=<pass>;Encrypt=yes;TrustServerCertificate=yes",
    "sales_query":"SELECT ...",
    "purchases_query":"SELECT ...",
    "incremental_column":"UpdatedAt",
    "branch_column":"BranchCode",
    "item_column":"ItemCode",
    "amount_column":"NetValue",
    "cost_column":"CostValue",
    "qty_column":"Qty"
  }'
```

### 2B) External API
Distribute API credentials (key id + secret) and provide HMAC request example.

## Step 3: Run First Sync

### PharmacyOne
```bash
curl -fsS -X POST https://adminpanel.boxvisio.com/v1/admin/tenants/<tenant_id>/sync \
  -H "Authorization: Bearer <cloudon_admin_jwt>"
```

### External API
Send first signed batch to:
- `POST /v1/ingest/sales`
- `POST /v1/ingest/purchases` (Pro+)

## Step 4: Validate KPIs
Run these checks as tenant admin:
1. `GET /v1/kpi/sales/summary?from=&to=`
2. `GET /v1/kpi/sales/by-branch?from=&to=`
3. `GET /v1/kpi/sales/compare?...`
4. Purchases endpoints for Pro+.
5. CSV exports from dashboard/API.

Cross-check with source totals for one known date range.

## Step 5: Set Subscription Active
After KPI sign-off:
```bash
curl -fsS -X PATCH https://adminpanel.boxvisio.com/v1/admin/tenants/<tenant_id>/subscription \
  -H "Authorization: Bearer <cloudon_admin_jwt>" \
  -H "Content-Type: application/json" \
  -d '{"status":"active","note":"first production activation"}'
```

## Step 6: Final Acceptance
Checklist:
- Tenant login works.
- Dashboards load with expected data.
- Ingestion queue healthy (`ingest:<tenant>` depth trends to 0).
- No DLQ spike for first sync.
- Subscription status active.
