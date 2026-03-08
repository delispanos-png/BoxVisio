# Professional Profile Model

## Scope
Added a professional profile layer so tenant UI behavior is resolved by:

`tenant + role + professional_profile`

Policy:
- one user -> one profile (single persona)
- no multi-persona assignments

This affects:
- default dashboard redirect
- sidebar/menu visibility
- persona-style UI adaptation
- insight ordering priority by profile
- admin user management

## Control Schema Changes

### New Dimension
Table: `dim_professional_profiles`

Fields:
- `id` (PK)
- `profile_code` (unique)
- `profile_name`
- `description`
- `created_at`
- `updated_at`

Default seeded profiles:
- `OWNER`
- `MANAGER`
- `FINANCE`
- `INVENTORY`
- `SALES`

### Users Extension
Table: `users`

Added field:
- `professional_profile_id` (FK -> `dim_professional_profiles.id`, indexed, **NOT NULL**)

Migration:
- `backend/alembic/versions/20260308_0006_control_professional_profiles.py`
- `backend/alembic/versions/20260308_0007_control_user_profile_required.py`

Backfill policy:
- `tenant_user` -> `FINANCE`
- `tenant_admin` -> `MANAGER`
- `cloudon_admin` -> `OWNER`

## Runtime Resolution

In auth dependency resolution (`get_current_user`):
1. Load user + professional profile (if assigned).
2. If no profile exists, fallback by role:
   - `cloudon_admin` -> `OWNER`
   - `tenant_admin` -> `MANAGER`
   - `tenant_user` -> `FINANCE`
3. Set request state:
   - `request.state.professional_profile_code`
   - `request.state.professional_profile_name`
   - `request.state.ui_persona`
   - `request.state.menu_visibility`

KPI summary endpoints also include profile-based KPI emphasis payload:
- `kpi_emphasis.profile_code`
- `kpi_emphasis.surface`
- `kpi_emphasis.priorities`

## Default Profile Mapping to UI

### OWNER / MANAGER
- full menu visibility (dashboards, streams, analytics, comparisons, exports)
- executive dashboard default
- KPI emphasis: turnover/profit/margin + branch contribution

### FINANCE
- focus on:
  - cash transactions
  - supplier balances
  - customer balances
  - finance dashboard
  - receivables/payables analytics
- executive dashboard hidden from menu
- default redirect to finance dashboard
- insight priority: receivables -> cashflow -> purchases
- KPI emphasis: receivables/payables/cash movement

### INVENTORY
- focus on:
  - inventory documents
  - inventory analytics
- finance-heavy and sales-heavy sections hidden
- executive dashboard default
- insight priority: inventory
- KPI emphasis: stock on hand/value/aging

### SALES
- focus on:
  - sales documents
  - sales analytics
  - comparisons
- finance and inventory-heavy sections hidden
- executive dashboard default
- insight priority: sales
- KPI emphasis: turnover/qty/margin/sales growth

## Admin Panel Configuration

Admin users page now supports profile assignment:
- create user form: `professional_profile_code`
- edit user form: `professional_profile_code`
- users list: profile column visible per user

Only `cloudon_admin` can manage this, same as existing users admin routes.

## Files Updated
- `backend/app/models/control.py`
- `backend/alembic/versions/20260308_0006_control_professional_profiles.py`
- `backend/app/api/deps.py`
- `backend/app/api/ui.py`
- `backend/app/templates/base_tenant.html`
- `backend/app/templates/admin/users.html`
- `backend/app/templates/admin/user_edit.html`
