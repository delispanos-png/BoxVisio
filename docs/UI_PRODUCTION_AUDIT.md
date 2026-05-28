# UI Production Audit

Date: 2026-05-13

## Summary

The UI now loads through the BoxVisio unified CSS entrypoint, but visual drift can still happen when page-local overrides touch shared components:

- BoxVisio unified entrypoint (`backend/app/static/css/boxvisio-unified.css`)
- BoxVisio shell overrides (`backend/app/static/css/boxvisio-ui.css`)
- Tenant inline shell overrides (`backend/app/templates/base_tenant.html`)
- Unified component overrides (`backend/app/static/css/bv-unified-ui.css`, `bv-modal-system.css`)

This is why small visual bugs can persist: generic selectors or page-local overrides may still target newer shared markup.

## Immediate Sidebar Finding

Problem:
- Active submenu links previously inherited legacy theme pseudo-elements.

Impact:
- These pseudo-elements render a small marker next to the active submenu icon.
- In the current custom sidebar this marker visually collides with the icon, looking like a stray arrow.

Fix applied:
- Tenant sidebar now suppresses active `::before` and `::after` markers for nav links and submenu links.
- Sidebar group markup was also normalized to `icon -> label -> chevron`.

## High-Risk UI Areas

1. Sidebar/menu
- Multiple owners: boxvisio-ui.css, tenant-shell.css, base_tenant inline CSS, custom sidebar JS.
- Risk: active/hover/collapsed/mobile rules override each other.
- Recommendation: keep tenant sidebar rules inside the unified CSS stack and reduce page-local shell styling.

2. Modals
- Multiple owners: Bootstrap, `bv-modal-system.css`, `bv-unified-ui.css`, page-local modal CSS.
- Risk: z-index, scroll height, close buttons, responsive widths can drift.
- Recommendation: keep all modal shell behavior in `bv-modal-system.css`; page templates should style only modal content.

3. Tables and active rows
- Multiple owners: boxvisio-ui.css, bv-unified-ui.css, page-local table CSS.
- Risk: row selection colors and hover states become inconsistent across sales/purchases/warehouse/customer modals.
- Recommendation: use one `.bv-unified-table` contract for selectable rows and remove page-level active row color overrides gradually.

4. Multi-select filters
- Multiple owners: `boxvisio-ui.css`, `bv-unified-ui.css`, and repeated page-local JS.
- Risk: dropdown positioning, search rows, and z-index can differ by page.
- Recommendation: extract shared multi-select JS/CSS into one reusable component.

5. Pseudo-elements
- Multiple `::before` / `::after` rules exist in shared and page-local components.
- Risk: decorative markers appear on new markup unexpectedly.
- Recommendation: new tenant components should use `body.tenant-shell` scoped selectors and avoid generic theme class names like `.active.open .active a`.

## Production Recommendation

Before production, run a visual QA pass over these pages at desktop, tablet, and mobile:

- Executive dashboard
- Finance dashboard
- Sales documents
- Purchase documents
- Warehouse documents
- Sales analytics
- Purchases analytics
- Inventory
- eShop analysis
- Sell Out

Focus checks:
- Sidebar active state and collapsed state.
- Modal open/close and scroll behavior.
- Table active row state.
- Multi-select dropdown search and z-index.
- Dark theme if enabled.

## Next Cleanup Step

Create a single tenant shell stylesheet and move the inline sidebar CSS from `base_tenant.html` into it. This gives one owner for layout/active/collapsed/mobile sidebar behavior and removes the current “theme plus inline overrides” ambiguity.

## Progress

- Created `backend/app/static/css/tenant-shell.css` as the new owner for tenant shell polish and collision overrides.
- Loaded `tenant-shell.css` after the legacy inline shell rules so it can safely take precedence.
- First pass covers sidebar active states, pseudo-element suppression, icon/label/chevron ordering, topbar polish, card radius/shadow and layout spacing.
- Second pass extends the same stylesheet with unified premium rules for cards, metric cards, tables, summary pills and modal shells.
- Third pass adds unified styling for filters, form controls, buttons, multi-select dropdowns, selected options and disabled/readonly fields.
- Fourth pass started page-specific cleanup by removing duplicated modal shell styling from `inventory_dashboard.html`; the modal now uses the central tenant/modal shell for border, radius and shadow while keeping its content-specific layout.
- Removed inline modal shell styles from the executive dashboard weekly/YTD/monthly/intraday modals so they now rely on `.bv-modal` and `tenant-shell.css`.
- Cleaned POS/eShop page-local CSS by removing duplicated table/modal typography that is now owned by `tenant-shell.css`; retained chart, recent-document table width, document-field and detail-layout rules that are page-specific.
- Unified table pager navigation controls on Customers, Items, Suppliers and Cash Accounts by switching their first/previous/next/last buttons to the shared `bv-page-icon` component used by the document dashboards.
- Started multi-select consolidation: added shared `window.BvMultiSelect` helper in `bv-unified-ui.js` for bind/sync/populate/selection handling, bumped tenant asset cache version, and migrated Finance, Items, Sales, Purchases, Sell Out and Inventory filters to the shared component while preserving category hierarchy behavior.
- Continued popup/table polish by moving Customers, Suppliers and Cash Accounts table footer summaries from pipe-separated text into the shared `bvRenderFooterSummary` pill component, matching Sales/Purchases/Warehouse document footers.
- Extended multi-select consolidation through Executive, Cashflow, Compare, POS and Expense Documents filters. Purchase Documents and Warehouse Documents were rechecked and already use the shared helper.
- Static QA no longer finds old page-local `.bv-multi-opt` renderers or direct dropdown click handlers in tenant templates.
- Moved the first production shell batch out of `base_tenant.html` into `tenant-shell.css`: collapsed sidebar, tablet/mobile sidebar, mobile backdrop, fullscreen sidebar hiding and modal z-index ownership now live in the central stylesheet. Removed the old viewport-based root font-size scaling and bumped the tenant shell CSS cache version.
- Moved additional global shell rules into `tenant-shell.css`: page title weight and KPI fallback color classes now live with the rest of the tenant shell styling, while the table row-header rule was confirmed already centralized.
- Continued shell extraction by moving topbar, bottom metadata, KPI help, insight rows, metric card and dark-theme ownership into `tenant-shell.css`; bumped the tenant shell CSS cache version to `20260513c`.
- Completed the base tenant shell extraction: the sidebar/menu layer and global date picker styles now live in `tenant-shell.css`, `base_tenant.html` no longer contains inline `<style>` blocks, and the tenant shell CSS cache version is `20260513e`.
- Added `tools/visual_qa.py` as a repeatable Playwright QA runner for desktop/tablet/mobile screenshots and shell checks.
- Browser visual QA completed on 2026-05-13 against `https://bi.boxvisio.com` with authenticated `pharmacy295` tenant access: 12 pages x 3 viewports plus the dashboard date modal, `issues: []`. Report and screenshots are in `artifacts/visual-qa/20260513-181940/`.
- Fixed Sell Out popup regression caused by the shared multi-select helper loading after page inline scripts. `bv-unified-ui.js` now loads before tenant page scripts, and `tools/visual_qa.py` includes a Sell Out action popup click check.
- Browser visual QA re-run on 2026-05-13 with the Sell Out popup check included: `issues: []`. Report and screenshots are in `artifacts/visual-qa/20260513-183456/`.
- Remaining cleanup: keep any future shell styling in `tenant-shell.css`; repeat `tools/visual_qa.py` before production deploys.

## Page-Specific Cleanup Queue

Low-risk next targets:
- No blocking UI shell QA items remain from this audit.

Keep page-specific for now:
- Chart containers and canvas sizing.
- Domain-specific cards such as inventory valuation tiles, eShop carrier breakdowns and Sell Out action cards.
- Print-only CSS inside generated print windows.
