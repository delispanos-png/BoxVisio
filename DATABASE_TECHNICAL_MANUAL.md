# BoxVisio BI - Database Technical Manual

Date: 2026-03-10  
Environment: `/opt/cloudon-bi` (Docker Compose, PostgreSQL 16)

## 1. Scope

This manual documents the PostgreSQL data layer used by BoxVisio BI:
- Database topology and tenancy model
- Full table catalog for `bi_control` and tenant DBs
- Purpose of each table family (`dim_*`, `fact_*`, `agg_*`, `stg_*`, ops, rules, billing)
- Data flow from ingestion to KPI and insights
- Daily operations (migrations, backup/restore, diagnostics)

Verified against:
- `bi_control`
- `bi_tenant_uat-a`
- `bi_tenant_uat-b`

## 2. Database Topology

PostgreSQL instance hosts multiple logical databases:
- `bi_control`: control plane (auth, tenant registry, subscriptions, billing, rules)
- `bi_tenant_<slug>`: tenant data plane (dimensions, facts, aggregates, ingestion ops)
- `postgres`: PostgreSQL system database

Tenancy routing is DB-per-tenant:
- Tenant metadata in `bi_control.tenants`
- Application resolves tenant context from JWT/host
- Tenant DB credentials are read from `tenants.db_name`, `tenants.db_user`, `tenants.db_password`

## 3. Naming Conventions

- `dim_*`: master/reference entities
- `stg_*` and `staging_*`: ingestion staging/raw payload tracking
- `fact_*`: canonical event-level records
- `agg_*`: pre-aggregated KPI tables
- `insight_*`/`insights`: deterministic intelligence outputs
- `sync_*`, `ingest_*`, `dead_letter_*`: operational ingestion state

## 4. Schema Inventory Snapshot

As of this snapshot:
- `bi_control`: 59 tables
- `bi_tenant_uat-a`: 57 tables
- Common tables between both: 39
- Control-only tables: 20
- Tenant-only tables: 18

### 4.1 Control-only tables (20)

- `alembic_version_control`
- `audit_logs`
- `dim_professional_profiles`
- `global_rule_entries`
- `global_rule_sets`
- `invoices`
- `payments`
- `plan_features`
- `plans`
- `refresh_tokens`
- `subscription_events`
- `subscription_limits`
- `subscriptions`
- `tenant_api_keys`
- `tenant_connections`
- `tenant_rule_overrides`
- `tenants`
- `users`
- `whmcs_events`
- `whmcs_services`

### 4.2 Tenant-only tables (18)

- `agg_expenses_by_branch_daily`
- `agg_expenses_by_category_daily`
- `agg_expenses_daily`
- `agg_expenses_monthly`
- `agg_purchases_daily_branch`
- `agg_purchases_daily_company`
- `agg_sales_daily_branch`
- `agg_sales_daily_company`
- `agg_stock_aging`
- `alembic_version_tenant`
- `dim_accounts`
- `dim_customers`
- `dim_document_types`
- `dim_expense_categories`
- `dim_payment_methods`
- `fact_expenses`
- `ingest_batches`
- `stg_expense_documents`

## 5. `bi_control` Technical Catalog

`bi_control` is the orchestration and governance database.

### 5.1 Identity, tenancy, and access

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `tenants` | Master tenant registry and DB routing | `slug`, `status`, `db_name`, `db_user`, `db_password`, `subscription_status` | Source of truth for tenant lifecycle and DB binding |
| `users` | Platform users and RBAC principals | `tenant_id`, `email`, `role`, `is_active`, `professional_profile_id` | Roles include `cloudon_admin`, `tenant_admin`, `tenant_user` |
| `dim_professional_profiles` | User profile taxonomy | `profile_code`, `profile_name` | FK from `users.professional_profile_id` |
| `tenant_api_keys` | Ingest API credentials per tenant | `tenant_id`, `key_id`, `key_secret`, `is_active` | Used by `/v1/ingest/*` APIs |
| `refresh_tokens` | JWT refresh-token lifecycle | `user_id`, `token_jti`, `expires_at`, `revoked_at` | Supports revocation and rotation |

### 5.2 Connector and integration configuration

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `tenant_connections` | Connector credentials and query mappings | `tenant_id`, `connector_type`, `enc_payload`, `stream_query_mapping`, `stream_field_mapping`, `enabled_streams` | SQL credentials and stream config storage |
| `whmcs_events` | Raw WHMCS webhook events | `service_id`, `event_type`, `payload` | Event intake/audit |
| `whmcs_services` | WHMCS service linkage to tenants | `service_id`, `tenant_id`, `product_id`, `status` | Cross-system mapping |

### 5.3 Subscription, plan, and billing

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `plans` | Plan definitions and feature bundle defaults | `code`, `display_name`, `feature_*`, `max_users`, `max_branches` | Product catalog |
| `plan_features` | Per-plan feature flags | `plan`, `feature_name`, `enabled` | Fine-grained capability flags |
| `subscriptions` | Active subscription contract per tenant | `tenant_id`, `plan`, `status`, `trial_*`, `current_period_*`, `feature_flags` | One subscription per tenant |
| `subscription_limits` | Quotas and usage counters | `subscription_id`, `limit_key`, `limit_value`, `used_value` | Unique on `(subscription_id, limit_key)` |
| `subscription_events` | Subscription status transitions audit | `tenant_id`, `from_status`, `to_status`, `note` | Timeline/audit stream |
| `invoices` | Tenant invoice ledger | `tenant_id`, `subscription_id`, `invoice_no`, `amount_due`, `status` | Billing records |
| `payments` | Payment transactions | `tenant_id`, `invoice_id`, `amount`, `provider`, `status`, `paid_at` | Linked to invoices when available |

### 5.4 Governance and policy

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `global_rule_sets` | Global business-rule bundles | `code`, `is_active`, `priority` | Rule set orchestration |
| `global_rule_entries` | Global rules by domain/stream/key | `ruleset_id`, `domain`, `stream`, `rule_key`, `payload_json`, `is_active` | Unique scope per ruleset |
| `tenant_rule_overrides` | Tenant-specific rule overrides | `tenant_id`, `domain`, `stream`, `rule_key`, `override_mode`, `payload_json`, `is_active` | Effective rules = global + override |
| `audit_logs` | Audit trail for privileged actions | `tenant_id`, `actor_user_id`, `action`, `entity_type`, `entity_id`, `payload` | Admin and operational auditing |

### 5.5 Migration state

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `alembic_version_control` | Current control migration revision | `version_num` | Used by Alembic control upgrades |

### 5.6 Shared analytics tables currently present in `bi_control`

The following analytics tables also exist in `bi_control` (shared with tenant schema):
- Dimensions: `dim_branches`, `dim_brands`, `dim_calendar`, `dim_categories`, `dim_groups`, `dim_items`, `dim_suppliers`, `dim_warehouses`
- Facts: `fact_sales`, `fact_purchases`, `fact_inventory`, `fact_cashflows`, `fact_supplier_balances`, `fact_customer_balances`
- Aggregates: `agg_sales_daily`, `agg_sales_monthly`, `agg_sales_item_daily`, `agg_purchases_daily`, `agg_purchases_monthly`, `agg_inventory_snapshot_daily`, `agg_cash_daily`, `agg_cash_by_type`, `agg_cash_accounts`, `agg_supplier_balances_daily`, `agg_customer_balances_daily`
- Staging/Ops: `staging_ingest_events`, `stg_sales_documents`, `stg_purchase_documents`, `stg_inventory_documents`, `stg_cash_transactions`, `stg_supplier_balances`, `stg_customer_balances`, `ingest_dead_letter`, `sync_state`
- Intelligence/Targets: `insight_rules`, `insights`, `insight_runs`, `supplier_targets`, `supplier_target_items`

Operational guidance:
- Treat tenant DBs as primary source for tenant KPI workloads.
- Use shared analytics tables in `bi_control` only if explicitly required by a migration/ops workflow.

## 6. Tenant DB Technical Catalog (`bi_tenant_*`)

Tenant DBs hold business data and KPI-ready aggregates for each tenant.

### 6.1 Dimensions (`dim_*`)

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `dim_branches` | Branch/store master | `external_id`, `branch_code`, `branch_name`, `company_id` | Branch reference across all facts |
| `dim_warehouses` | Warehouse master | `external_id`, `name` | Inventory and document linkage |
| `dim_brands` | Brand master | `external_id`, `name` | Product classification |
| `dim_categories` | Category hierarchy | `external_id`, `name`, `parent_id`, `level` | Tree-like product categorization |
| `dim_groups` | Group master | `external_id`, `name` | Additional item grouping |
| `dim_items` | Product/item master | `external_id`, `sku`, `barcode`, `name`, `brand_id`, `category_id`, `group_id`, `is_active_source` | Primary table for item lookups |
| `dim_customers` | Customer master | `external_id`, `customer_code`, `name` | Used in sales and receivables |
| `dim_suppliers` | Supplier master | `external_id`, `name` | Used in purchases/payables/targets |
| `dim_accounts` | Account master | `external_id`, `name`, `account_type`, `currency` | Cashflow accounting dimension |
| `dim_document_types` | Document type mapping by stream | `external_id`, `name`, `stream` | Stream-aware document taxonomy |
| `dim_payment_methods` | Payment method taxonomy | `external_id`, `name` | Sales/payment enrichment |
| `dim_expense_categories` | Expense category hierarchy | `external_id`, `category_code`, `classification`, `parent_category_id` | Expense analytics model |
| `dim_calendar` | Date dimension | `full_date`, `year`, `month`, `week_of_year`, `is_weekend` | Time intelligence |
| `dim_products` | Legacy compatibility item table | similar to `dim_items` | Kept for compatibility paths |

### 6.2 Staging and raw ingestion (`stg_*`, `staging_*`)

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `staging_ingest_events` | Raw event landing table | `entity`, `event_id`, `payload_json`, `received_at` | Earliest normalized intake |
| `stg_sales_documents` | Sales stream staging | `event_id`, `external_id`, `doc_date`, `transform_status`, `source_payload_json` | Pre-canonical sales |
| `stg_purchase_documents` | Purchases stream staging | same pattern | Pre-canonical purchases |
| `stg_inventory_documents` | Inventory stream staging | same pattern | Pre-canonical inventory |
| `stg_cash_transactions` | Cashflow stream staging | same pattern | Pre-canonical cashflow |
| `stg_supplier_balances` | Supplier balance staging | same pattern | Pre-canonical payables |
| `stg_customer_balances` | Customer balance staging | same pattern | Pre-canonical receivables |
| `stg_expense_documents` | Expense stream staging | same pattern | Pre-canonical expenses |

### 6.3 Canonical facts (`fact_*`)

| Table | Purpose | Key Columns | Notes |
|---|---|---|---|
| `fact_sales` | Sales line-level canonical fact | `event_id`, `doc_date`, `branch_id`, `item_id`, `customer_id`, `qty`, `net_value`, `cost_amount`, `profit_amount` | Core sales truth for drilldowns and aggregates |
| `fact_purchases` | Purchases line-level canonical fact | `event_id`, `doc_date`, `branch_id`, `item_id`, `supplier_id`, `qty`, `net_value`, `cost_amount` | Core purchases truth |
| `fact_inventory` | Inventory snapshots/events | `external_id`, `doc_date`, `branch_id`, `item_id`, `warehouse_id`, `qty_on_hand`, `value_amount` | Stock state inputs |
| `fact_cashflows` | Cashflow fact | `external_id`, `doc_date`, `branch_id`, `entry_type`, `amount`, `transaction_type`, `account_id` | Inflows/outflows |
| `fact_supplier_balances` | Supplier balances fact | `supplier_id`, `balance_date`, `branch_id`, `open_balance`, `overdue_balance`, aging buckets | Payables analytics |
| `fact_customer_balances` | Customer balances fact | `customer_id`, `balance_date`, `branch_id`, `open_balance`, `overdue_balance`, aging buckets | Receivables analytics |
| `fact_expenses` | Expenses canonical fact | `expense_date`, `branch_id`, `category_id`, `supplier_id`, `account_id`, `amount_net`, `amount_tax`, `amount_gross` | Operating expenses stream |

Legacy/compatibility fact tables:
- `fact_sales_documents` (legacy compatibility shape for sales)
- `fact_purchase_documents` (legacy compatibility shape for purchases)
- `fact_inventory_documents` (legacy compatibility shape for inventory)
- `fact_cash_transactions` (legacy compatibility shape for cashflow)

### 6.4 Aggregates (`agg_*`)

#### Sales aggregates

| Table | Purpose |
|---|---|
| `agg_sales_daily` | Daily sales by branch/warehouse/brand/category/group |
| `agg_sales_daily_company` | Daily company-level sales summary |
| `agg_sales_daily_branch` | Daily branch-level sales summary and contribution |
| `agg_sales_monthly` | Monthly sales by major dimensions |
| `agg_sales_item_daily` | Daily item-level sales summary |

#### Purchases aggregates

| Table | Purpose |
|---|---|
| `agg_purchases_daily` | Daily purchases by branch/warehouse/supplier/brand/category/group |
| `agg_purchases_daily_company` | Daily company-level purchases summary |
| `agg_purchases_daily_branch` | Daily branch-level purchases summary and contribution |
| `agg_purchases_monthly` | Monthly purchases by major dimensions |

#### Inventory aggregates

| Table | Purpose |
|---|---|
| `agg_inventory_snapshot_daily` | Daily item inventory snapshot |
| `agg_inventory_snapshot` | Legacy compatibility snapshot table |
| `agg_stock_aging` | Stock aging by item/branch and days since last sale |

#### Cashflow aggregates

| Table | Purpose |
|---|---|
| `agg_cash_daily` | Daily cash by branch/subcategory/type/account |
| `agg_cash_by_type` | Daily cash grouped by subcategory/type |
| `agg_cash_accounts` | Daily cash grouped by account |

#### Balance aggregates

| Table | Purpose |
|---|---|
| `agg_supplier_balances_daily` | Daily payables summary and aging |
| `agg_customer_balances_daily` | Daily receivables summary and aging |

#### Expense aggregates

| Table | Purpose |
|---|---|
| `agg_expenses_daily` | Daily expense totals by branch/category/supplier/account |
| `agg_expenses_monthly` | Monthly expense totals |
| `agg_expenses_by_category_daily` | Daily expense totals by category |
| `agg_expenses_by_branch_daily` | Daily expense totals by branch |

### 6.5 Insights and targets

| Table | Purpose | Key Columns |
|---|---|---|
| `insight_rules` | Deterministic insight rule definitions | `code`, `enabled`, `params_json`, `scope` |
| `insights` | Generated insight records | `rule_code`, `severity`, `entity_type`, `period_from`, `period_to`, `status` |
| `insight_runs` | Insight engine execution log | `started_at`, `finished_at`, `status`, `rules_executed`, `insights_created` |
| `supplier_targets` | Supplier annual target contracts/rebates | `supplier_ext_id`, `target_year`, `target_amount`, `rebate_percent`, `is_active` |
| `supplier_target_items` | Items included in each supplier target | `supplier_target_id`, `item_external_id`, `item_name` |

### 6.6 Ingestion operations and migration state

| Table | Purpose | Key Columns |
|---|---|---|
| `sync_state` | Incremental cursor/watermark per stream/connector | `connector_type`, `stream_code`, `last_sync_timestamp`, `last_sync_id`, `source_connector_id` |
| `ingest_batches` | Batch processing telemetry | `stream`, `batch_id`, `status`, `rows_read`, `rows_loaded`, `rows_failed` |
| `ingest_dead_letter` | Failed payload store for retries/manual replay | `entity`, `event_id`, `error_message`, `retry_count`, `status` |
| `dead_letter_queue` | Legacy dead-letter compatibility table | similar to `ingest_dead_letter` |
| `alembic_version_tenant` | Tenant migration revision state | `version_num` |

## 7. Key Relationships

### 7.1 Control DB relationships

- `users.tenant_id -> tenants.id`
- `users.professional_profile_id -> dim_professional_profiles.id`
- `tenant_api_keys.tenant_id -> tenants.id`
- `tenant_connections.tenant_id -> tenants.id`
- `subscriptions.tenant_id -> tenants.id`
- `subscription_limits.subscription_id -> subscriptions.id`
- `invoices.tenant_id -> tenants.id`
- `invoices.subscription_id -> subscriptions.id`
- `payments.tenant_id -> tenants.id`
- `payments.invoice_id -> invoices.id`
- `refresh_tokens.user_id -> users.id`
- `global_rule_entries.ruleset_id -> global_rule_sets.id`
- `tenant_rule_overrides.tenant_id -> tenants.id`
- `audit_logs.tenant_id -> tenants.id`
- `audit_logs.actor_user_id -> users.id`
- `whmcs_services.tenant_id -> tenants.id`

### 7.2 Tenant DB relationships (selected)

- `dim_categories.parent_id -> dim_categories.id`
- `dim_items.brand_id -> dim_brands.id`
- `dim_items.category_id -> dim_categories.id`
- `dim_items.group_id -> dim_groups.id`
- `fact_sales.branch_id -> dim_branches.id`
- `fact_sales.item_id -> dim_items.id`
- `fact_sales.customer_id -> dim_customers.id`
- `fact_sales.warehouse_id -> dim_warehouses.id`
- `fact_purchases.supplier_id -> dim_suppliers.id`
- `fact_inventory.warehouse_id -> dim_warehouses.id`
- `fact_supplier_balances.supplier_id -> dim_suppliers.id`
- `fact_customer_balances.customer_id -> dim_customers.id`
- `fact_expenses.category_id -> dim_expense_categories.id`
- `fact_expenses.supplier_id -> dim_suppliers.id`
- `fact_expenses.account_id -> dim_accounts.id`
- `supplier_target_items.supplier_target_id -> supplier_targets.id` (`ON DELETE CASCADE`)

## 8. End-to-End Data Flow

1. Connector/API receives payload.
2. Event lands in `staging_ingest_events` and stream-specific `stg_*`.
3. Transform/upsert populates `dim_*` and `fact_*`.
4. Aggregate jobs build/refresh `agg_*`.
5. KPI API/UI reads from `agg_*` for dashboards and from `fact_*` for drilldowns.
6. Insight engine evaluates rules (`insight_rules`) and writes `insights`, tracked in `insight_runs`.
7. Failures are captured in `ingest_dead_letter` (or legacy `dead_letter_queue`).
8. Sync cursors advance in `sync_state`.

## 9. Operational Runbook

### 9.1 Connect quickly (inside Docker host)

```bash
# Control DB
docker compose exec -T postgres psql -U postgres -d bi_control

# Tenant DB
docker compose exec -T postgres psql -U postgres -d bi_tenant_uat-a
```

### 9.2 Inspect schema

```sql
\dt
\d public.dim_items
\d public.fact_sales
\d public.agg_sales_daily
```

### 9.3 Useful diagnostics SQL

```sql
-- Active tenants
SELECT id, slug, db_name, status, subscription_status
FROM tenants
ORDER BY id;
```

```sql
-- Item count in a tenant
SELECT count(*) FROM public.dim_items;
```

```sql
-- Latest sales rows
SELECT doc_date, item_code, qty, net_value
FROM public.fact_sales
ORDER BY doc_date DESC
LIMIT 20;
```

```sql
-- Validate aggregate freshness
SELECT max(doc_date) AS latest_sales_day
FROM public.agg_sales_daily;
```

### 9.4 Migrations

```bash
# Control DB migration
make migrate-control

# Tenant DB migration (script-managed)
make migrate-tenant TENANT=<slug>
```

### 9.5 Backup and restore

```bash
# Nightly backup script (control + active tenants)
./scripts/nightly_backup.sh

# Restore one DB
./scripts/restore_db.sh --db bi_control --file /path/to/bi_control.dump --drop-create
```

## 10. Security and Access Model

- External PostgreSQL access must be restricted by firewall (`ufw`) and cloud firewall rules.
- Tenant DB credentials are stored per tenant in `bi_control.tenants`.
- Connector secrets and SQL payloads are stored encrypted in `tenant_connections.enc_payload`.
- API ingest access is controlled by `tenant_api_keys` plus request signatures.
- User access is RBAC-driven via `users.role` and active flags.

## 11. Known Compatibility/Legacy Objects

The following tables exist for compatibility with older flows and should be treated carefully:
- `dim_products`
- `fact_sales_documents`
- `fact_purchase_documents`
- `fact_inventory_documents`
- `fact_cash_transactions`
- `agg_inventory_snapshot`
- `dead_letter_queue`

Guideline:
- Prefer canonical tables (`dim_items`, `fact_*`, `agg_*` current set).
- Use legacy tables only for backward compatibility where explicitly needed.

## 12. Troubleshooting Checklist

### 12.1 Cannot connect from pgAdmin

1. Confirm container is healthy:
```bash
docker compose ps postgres
```
2. Confirm port publish:
```bash
docker compose ps postgres
# expect 0.0.0.0:5432->5432/tcp
```
3. Confirm firewall rule:
```bash
sudo ufw status numbered
```
4. If pgAdmin runs on same host, use `127.0.0.1` as host.

### 12.2 Tenant appears inactive

Check:
```sql
SELECT id, slug, status, subscription_status
FROM tenants
ORDER BY id;
```

`status != active` blocks tenant access paths; suspended/canceled subscription also blocks guarded paths.

### 12.3 Missing table/column after deploy

Check migration head:
```sql
SELECT * FROM alembic_version_control;
SELECT * FROM alembic_version_tenant;
```

Then run pending migrations and re-check schema.

## 13. Change Management Recommendations

- Keep control-plane schema changes in control migrations only.
- Keep KPI data model changes in tenant migrations only.
- Avoid introducing tenant facts into `bi_control` for new features.
- Version all schema changes through Alembic and document in changelog/manual.
- For any new stream, update:
  - `stg_*` schema
  - canonical `fact_*`
  - relevant `agg_*`
  - ingestion retry/dead-letter handling

