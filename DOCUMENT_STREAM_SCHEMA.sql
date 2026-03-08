BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
    CREATE TYPE sales_document_type AS ENUM ('invoice', 'receipt', 'credit_note', 'pos_sale');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE purchase_document_type AS ENUM ('supplier_invoice', 'purchase_receipt', 'purchase_credit_note');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE inventory_document_type AS ENUM ('stock_transfer', 'stock_adjustment', 'stock_entry', 'stock_exit', 'inventory_correction');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cash_subcategory_type AS ENUM (
        'customer_collections',
        'customer_transfers',
        'supplier_payments',
        'supplier_transfers',
        'financial_accounts'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE counterparty_type AS ENUM ('customer', 'supplier', 'internal', 'none');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE document_status_type AS ENUM ('posted', 'canceled', 'reversed', 'draft');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE inventory_movement_direction_type AS ENUM ('in', 'out', 'transfer', 'adjustment');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE cash_direction_type AS ENUM ('inflow', 'outflow', 'internal');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- -----------------------------------------------------------------------------
-- Dimensions (required)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_branches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    parent_id UUID NULL REFERENCES dim_categories(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_brands (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(128) NOT NULL UNIQUE,
    sku VARCHAR(128) NULL,
    name VARCHAR(255) NOT NULL,
    brand_id UUID NULL REFERENCES dim_brands(id),
    category_id UUID NULL REFERENCES dim_categories(id),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(64) NULL,
    payment_terms_days INTEGER NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(64) NULL,
    customer_group VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    account_code VARCHAR(64) NULL,
    name VARCHAR(255) NOT NULL,
    account_type VARCHAR(32) NOT NULL,
    currency_code CHAR(3) NOT NULL DEFAULT 'EUR',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Operationally required for warehouse stream
CREATE TABLE IF NOT EXISTS dim_warehouses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL UNIQUE,
    branch_id UUID NOT NULL REFERENCES dim_branches(id),
    name VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Fact streams
-- All 4 tables contain mandatory canonical fields:
-- document_id, document_type, document_date, branch_id, entity_id, item_id,
-- quantity, net_amount, cost_amount, account_id, external_id, updated_at
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_sales_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id VARCHAR(128) NOT NULL,
    document_line_no INTEGER NOT NULL DEFAULT 1,
    document_type sales_document_type NOT NULL,
    document_status document_status_type NOT NULL DEFAULT 'posted',
    document_date DATE NOT NULL,
    branch_id UUID NOT NULL REFERENCES dim_branches(id),
    entity_type counterparty_type NOT NULL DEFAULT 'customer',
    entity_id UUID NULL,
    item_id UUID NULL REFERENCES dim_products(id),
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    cost_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    account_id UUID NULL REFERENCES dim_accounts(id),
    external_id VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Extra fields required for KPI fidelity and reconciliation
    gross_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency_code CHAR(3) NOT NULL DEFAULT 'EUR',
    sign_multiplier SMALLINT NOT NULL DEFAULT 1 CHECK (sign_multiplier IN (-1, 1)),
    source_system VARCHAR(64) NOT NULL DEFAULT 'unknown',
    source_doc_no VARCHAR(128) NULL,
    source_updated_at TIMESTAMPTZ NULL,
    event_id VARCHAR(128) NULL,
    is_void BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (document_line_no >= 1),
    UNIQUE (source_system, external_id),
    UNIQUE (source_system, document_id, document_line_no)
);

CREATE TABLE IF NOT EXISTS fact_purchase_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id VARCHAR(128) NOT NULL,
    document_line_no INTEGER NOT NULL DEFAULT 1,
    document_type purchase_document_type NOT NULL,
    document_status document_status_type NOT NULL DEFAULT 'posted',
    document_date DATE NOT NULL,
    branch_id UUID NOT NULL REFERENCES dim_branches(id),
    entity_type counterparty_type NOT NULL DEFAULT 'supplier',
    entity_id UUID NULL,
    item_id UUID NULL REFERENCES dim_products(id),
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    cost_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    account_id UUID NULL REFERENCES dim_accounts(id),
    external_id VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Extra fields required for KPI fidelity and reconciliation
    gross_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency_code CHAR(3) NOT NULL DEFAULT 'EUR',
    sign_multiplier SMALLINT NOT NULL DEFAULT 1 CHECK (sign_multiplier IN (-1, 1)),
    source_system VARCHAR(64) NOT NULL DEFAULT 'unknown',
    source_doc_no VARCHAR(128) NULL,
    source_updated_at TIMESTAMPTZ NULL,
    event_id VARCHAR(128) NULL,
    is_void BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (document_line_no >= 1),
    UNIQUE (source_system, external_id),
    UNIQUE (source_system, document_id, document_line_no)
);

CREATE TABLE IF NOT EXISTS fact_inventory_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id VARCHAR(128) NOT NULL,
    document_line_no INTEGER NOT NULL DEFAULT 1,
    document_type inventory_document_type NOT NULL,
    document_status document_status_type NOT NULL DEFAULT 'posted',
    document_date DATE NOT NULL,
    branch_id UUID NOT NULL REFERENCES dim_branches(id),
    entity_type counterparty_type NOT NULL DEFAULT 'internal',
    entity_id UUID NULL,
    item_id UUID NULL REFERENCES dim_products(id),
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    cost_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    account_id UUID NULL REFERENCES dim_accounts(id),
    external_id VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Warehouse stream specific fields
    movement_direction inventory_movement_direction_type NOT NULL,
    warehouse_id UUID NULL REFERENCES dim_warehouses(id),
    from_warehouse_id UUID NULL REFERENCES dim_warehouses(id),
    to_warehouse_id UUID NULL REFERENCES dim_warehouses(id),
    lot_no VARCHAR(64) NULL,
    expiry_date DATE NULL,
    source_system VARCHAR(64) NOT NULL DEFAULT 'unknown',
    source_doc_no VARCHAR(128) NULL,
    source_updated_at TIMESTAMPTZ NULL,
    event_id VARCHAR(128) NULL,
    is_void BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (document_line_no >= 1),
    CHECK (
        (document_type <> 'stock_transfer')
        OR (from_warehouse_id IS NOT NULL AND to_warehouse_id IS NOT NULL AND from_warehouse_id <> to_warehouse_id)
    ),
    CHECK (
        (document_type = 'stock_transfer')
        OR (warehouse_id IS NOT NULL)
    ),
    UNIQUE (source_system, external_id),
    UNIQUE (source_system, document_id, document_line_no)
);

CREATE TABLE IF NOT EXISTS fact_cash_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id VARCHAR(128) NOT NULL,
    document_line_no INTEGER NOT NULL DEFAULT 1,
    document_type VARCHAR(64) NOT NULL,
    document_status document_status_type NOT NULL DEFAULT 'posted',
    document_date DATE NOT NULL,
    branch_id UUID NOT NULL REFERENCES dim_branches(id),
    entity_type counterparty_type NOT NULL DEFAULT 'none',
    entity_id UUID NULL,
    item_id UUID NULL REFERENCES dim_products(id),
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    cost_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    account_id UUID NOT NULL REFERENCES dim_accounts(id),
    external_id VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Cash stream mandatory business split
    cash_subcategory cash_subcategory_type NOT NULL,
    cash_direction cash_direction_type NOT NULL,
    counterparty_account_id UUID NULL REFERENCES dim_accounts(id),
    payment_method VARCHAR(32) NULL,
    currency_code CHAR(3) NOT NULL DEFAULT 'EUR',
    source_system VARCHAR(64) NOT NULL DEFAULT 'unknown',
    source_doc_no VARCHAR(128) NULL,
    source_updated_at TIMESTAMPTZ NULL,
    event_id VARCHAR(128) NULL,
    is_void BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (document_line_no >= 1),
    CHECK (
        (cash_subcategory <> 'financial_accounts')
        OR (counterparty_account_id IS NOT NULL)
    ),
    UNIQUE (source_system, external_id),
    UNIQUE (source_system, document_id, document_line_no)
);

-- -----------------------------------------------------------------------------
-- Core performance indexes
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_sales_docs_date_branch ON fact_sales_documents (document_date, branch_id);
CREATE INDEX IF NOT EXISTS ix_sales_docs_entity ON fact_sales_documents (entity_id);
CREATE INDEX IF NOT EXISTS ix_sales_docs_item ON fact_sales_documents (item_id);
CREATE INDEX IF NOT EXISTS ix_sales_docs_updated_at ON fact_sales_documents (updated_at);
CREATE INDEX IF NOT EXISTS ix_sales_docs_type ON fact_sales_documents (document_type);

CREATE INDEX IF NOT EXISTS ix_purchase_docs_date_branch ON fact_purchase_documents (document_date, branch_id);
CREATE INDEX IF NOT EXISTS ix_purchase_docs_entity ON fact_purchase_documents (entity_id);
CREATE INDEX IF NOT EXISTS ix_purchase_docs_item ON fact_purchase_documents (item_id);
CREATE INDEX IF NOT EXISTS ix_purchase_docs_updated_at ON fact_purchase_documents (updated_at);
CREATE INDEX IF NOT EXISTS ix_purchase_docs_type ON fact_purchase_documents (document_type);

CREATE INDEX IF NOT EXISTS ix_inventory_docs_date_branch ON fact_inventory_documents (document_date, branch_id);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_item ON fact_inventory_documents (item_id);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_wh ON fact_inventory_documents (warehouse_id);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_from_wh ON fact_inventory_documents (from_warehouse_id);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_to_wh ON fact_inventory_documents (to_warehouse_id);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_type ON fact_inventory_documents (document_type);
CREATE INDEX IF NOT EXISTS ix_inventory_docs_updated_at ON fact_inventory_documents (updated_at);

CREATE INDEX IF NOT EXISTS ix_cash_tx_date_branch ON fact_cash_transactions (document_date, branch_id);
CREATE INDEX IF NOT EXISTS ix_cash_tx_entity ON fact_cash_transactions (entity_id);
CREATE INDEX IF NOT EXISTS ix_cash_tx_account ON fact_cash_transactions (account_id);
CREATE INDEX IF NOT EXISTS ix_cash_tx_subcategory ON fact_cash_transactions (cash_subcategory);
CREATE INDEX IF NOT EXISTS ix_cash_tx_updated_at ON fact_cash_transactions (updated_at);

COMMIT;
