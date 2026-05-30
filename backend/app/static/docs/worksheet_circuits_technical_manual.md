# Excel-Style Worksheet Circuits Technical Manual

Last updated: 2026-05-30

## Scope

This document covers the three tenant reporting circuits implemented from the source Excel workbooks:

- FNR: `/tenant/fnr`
- Availability Brief: `/tenant/availability`
- Destocking Brief: `/tenant/destocking`

All three pages are reporting views. The Excel workbooks define layout, filters, worksheet sections and business formulas; the application computes the results from BI facts instead of reading static Excel output.

## Runtime Components

- Routes and exports: `backend/app/api/ui.py`
- Calculation service: `backend/app/services/replenishment.py`
- Tenant navigation: `backend/app/templates/base_tenant.html`
- FNR template: `backend/app/templates/tenant/fnr_dashboard.html`
- Availability template: `backend/app/templates/tenant/availability_dashboard.html`
- Destocking template: `backend/app/templates/tenant/destocking_dashboard.html`
- User manual: `backend/app/templates/tenant/user_manual.html`
- Responsive QA: `tools/fnr_availability_destocking_responsive_qa.py`

## Data Flow

1. Tenant opens a worksheet route.
2. UI query parameters are normalized into worksheet parameters.
3. The route calls the matching builder in `replenishment.py`.
4. Builders read BI fact/dimension tables through the existing async SQLAlchemy session.
5. The returned payload contains summary KPIs, filter options, table rows, trend series, correlation series and recommendations.
6. The template renders an Excel-style reporting page.
7. Export routes generate formatted XLSX files from the same computed payload.

## Caching

The worksheet routes use tenant-aware cache keys with a 300 second TTL:

- `tenant:worksheet:fnr`
- `tenant:worksheet:availability`
- `tenant:worksheet:destocking`

The cache key includes tenant id, user role scope and normalized query parameters. Export routes use the same computed payload to avoid recalculating heavy worksheet data during download.

## FNR

FNR produces a supplier order worksheet.

Inputs:

- Pharmacies
- Category 1, Category 2, Category 3
- Supplier
- Target Stock
- Overstock
- Average week 1
- Average week 2
- Search

Primary KPIs:

- Worksheet rows
- Order rows
- Order quantity
- Order value
- Items with need

Export:

- XLSX only.
- Includes the order worksheet with human-readable formatting.
- Intended to be sent to suppliers after review.

## Availability Brief

Availability reproduces the workbook structure as four worksheet tabs:

- Table
- Trends
- Correlation
- Recommendations

Primary KPIs:

- SKU Count
- SKU Live Online
- Web Availability
- Sales Value
- Recommendations

Trend and correlation sections include the numeric period summary above each chart so users can verify the line values without reading only the visualization.

## Destocking Brief

Destocking reproduces the workbook structure as four worksheet tabs:

- Table
- Trends
- Correlation
- Recommendations

Primary KPIs:

- SKU Count
- Stock Value Date 1
- Total Overstock
- Margin
- Recommendations

Important implementation details:

- D3 is treated as a separate bucket from D in trends and correlation.
- Trend values are period-specific; values must not be copied from the first period into every period.
- Correlation displays overstock, D3 overstock and margin using the same computed period series.

## Responsive UI Rules

The three worksheet templates use constrained workbench widths so filter controls do not become page-wide rails. Filter inputs are intentionally taller than the previous compact version so text remains readable.

Responsive QA covers:

- Desktop: 1600 x 900
- Tablet: 1024 x 768
- Mobile: 430 x 932

The QA script checks route access, required worksheet selectors, tabs, horizontal overflow and screenshots.

Run:

```bash
QA_ACCESS_TOKEN='<tenant_token>' QA_BASE_URL='https://bi.boxvisio.com' /opt/cloudon-bi/.venv/bin/python tools/fnr_availability_destocking_responsive_qa.py
```

For local nginx testing, the script maps `bi.boxvisio.com` to `127.0.0.1` in Chromium by default.
