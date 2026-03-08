-- Universal Connector Framework staging layer (tenant database)

CREATE TABLE IF NOT EXISTS stg_sales_documents (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'sales_documents',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_purchase_documents (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'purchase_documents',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_inventory_documents (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'inventory_documents',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_cash_transactions (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'cash_transactions',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_supplier_balances (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'supplier_balances',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_customer_balances (
  id BIGSERIAL PRIMARY KEY,
  connector_type VARCHAR(64) NOT NULL,
  stream VARCHAR(64) NOT NULL DEFAULT 'customer_balances',
  event_id VARCHAR(128),
  external_id VARCHAR(128),
  doc_date DATE,
  transform_status VARCHAR(16) NOT NULL DEFAULT 'loaded',
  error_message TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_stg_sales_documents_ingested_at ON stg_sales_documents (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_sales_documents_connector_external ON stg_sales_documents (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_sales_documents_status ON stg_sales_documents (transform_status);

CREATE INDEX IF NOT EXISTS ix_stg_purchase_documents_ingested_at ON stg_purchase_documents (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_purchase_documents_connector_external ON stg_purchase_documents (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_purchase_documents_status ON stg_purchase_documents (transform_status);

CREATE INDEX IF NOT EXISTS ix_stg_inventory_documents_ingested_at ON stg_inventory_documents (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_inventory_documents_connector_external ON stg_inventory_documents (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_inventory_documents_status ON stg_inventory_documents (transform_status);

CREATE INDEX IF NOT EXISTS ix_stg_cash_transactions_ingested_at ON stg_cash_transactions (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_cash_transactions_connector_external ON stg_cash_transactions (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_cash_transactions_status ON stg_cash_transactions (transform_status);

CREATE INDEX IF NOT EXISTS ix_stg_supplier_balances_ingested_at ON stg_supplier_balances (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_supplier_balances_connector_external ON stg_supplier_balances (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_supplier_balances_status ON stg_supplier_balances (transform_status);

CREATE INDEX IF NOT EXISTS ix_stg_customer_balances_ingested_at ON stg_customer_balances (ingested_at);
CREATE INDEX IF NOT EXISTS ix_stg_customer_balances_connector_external ON stg_customer_balances (connector_type, external_id);
CREATE INDEX IF NOT EXISTS ix_stg_customer_balances_status ON stg_customer_balances (transform_status);
