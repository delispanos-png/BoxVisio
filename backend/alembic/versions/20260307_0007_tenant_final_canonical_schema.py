"""tenant final canonical schema alignment

Revision ID: 20260307_0007_tenant
Revises: 20260307_0006_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0007_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0006_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _view_exists(bind, view_name: str) -> bool:
    return view_name in set(inspect(bind).get_view_names())


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _create_dim_customers(bind) -> None:
    if _table_exists(bind, 'dim_customers'):
        return
    op.create_table(
        'dim_customers',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('customer_code', sa.String(length=128), nullable=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_customers_external_id'),
    )
    op.create_index('ix_dim_customers_external_id', 'dim_customers', ['external_id'])
    op.create_index('ix_dim_customers_customer_code', 'dim_customers', ['customer_code'])
    op.create_index('ix_dim_customers_updated_at', 'dim_customers', ['updated_at'])


def _create_dim_accounts(bind) -> None:
    if _table_exists(bind, 'dim_accounts'):
        return
    op.create_table(
        'dim_accounts',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('account_type', sa.String(length=64), nullable=True),
        sa.Column('currency', sa.String(length=3), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_accounts_external_id'),
    )
    op.create_index('ix_dim_accounts_external_id', 'dim_accounts', ['external_id'])
    op.create_index('ix_dim_accounts_account_type', 'dim_accounts', ['account_type'])
    op.create_index('ix_dim_accounts_updated_at', 'dim_accounts', ['updated_at'])


def _create_dim_document_types(bind) -> None:
    if _table_exists(bind, 'dim_document_types'):
        return
    op.create_table(
        'dim_document_types',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('stream', sa.String(length=64), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_document_types_external_id'),
    )
    op.create_index('ix_dim_document_types_external_id', 'dim_document_types', ['external_id'])
    op.create_index('ix_dim_document_types_stream', 'dim_document_types', ['stream'])
    op.create_index('ix_dim_document_types_updated_at', 'dim_document_types', ['updated_at'])


def _create_dim_payment_methods(bind) -> None:
    if _table_exists(bind, 'dim_payment_methods'):
        return
    op.create_table(
        'dim_payment_methods',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_payment_methods_external_id'),
    )
    op.create_index('ix_dim_payment_methods_external_id', 'dim_payment_methods', ['external_id'])
    op.create_index('ix_dim_payment_methods_updated_at', 'dim_payment_methods', ['updated_at'])


def _create_agg_stock_aging(bind) -> None:
    if _table_exists(bind, 'agg_stock_aging'):
        return
    op.create_table(
        'agg_stock_aging',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('item_external_id', sa.String(length=128), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty_on_hand', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('stock_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('last_sale_date', sa.Date(), nullable=True),
        sa.Column('days_since_last_sale', sa.Integer(), nullable=True),
        sa.Column('aging_bucket', sa.String(length=16), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('snapshot_date', 'item_external_id', 'branch_ext_id', name='uq_agg_stock_aging_dims'),
    )
    op.alter_column('agg_stock_aging', 'qty_on_hand', server_default=None)
    op.alter_column('agg_stock_aging', 'stock_value', server_default=None)
    op.alter_column('agg_stock_aging', 'updated_at', server_default=None)
    op.alter_column('agg_stock_aging', 'created_at', server_default=None)
    op.create_index('ix_agg_stock_aging_snapshot_date', 'agg_stock_aging', ['snapshot_date'])
    op.create_index('ix_agg_stock_aging_item_external_id', 'agg_stock_aging', ['item_external_id'])
    op.create_index('ix_agg_stock_aging_branch_ext_id', 'agg_stock_aging', ['branch_ext_id'])
    op.create_index('ix_agg_stock_aging_snapshot_branch', 'agg_stock_aging', ['snapshot_date', 'branch_ext_id'])
    op.create_index('ix_agg_stock_aging_days', 'agg_stock_aging', ['days_since_last_sale'])
    op.create_index('ix_agg_stock_aging_bucket', 'agg_stock_aging', ['aging_bucket'])


def _seed_agg_stock_aging(bind) -> None:
    if not _table_exists(bind, 'agg_stock_aging'):
        return
    if not _table_exists(bind, 'fact_inventory') or not _table_exists(bind, 'fact_sales'):
        return
    op.execute(
        """
        WITH latest_snapshot AS (
            SELECT max(doc_date) AS snapshot_date FROM fact_inventory
        ),
        base AS (
            SELECT
                fi.doc_date AS snapshot_date,
                di.external_id AS item_external_id,
                db.external_id AS branch_ext_id,
                COALESCE(SUM(fi.qty_on_hand), 0) AS qty_on_hand,
                COALESCE(SUM(fi.value_amount), 0) AS stock_value,
                max(fs.doc_date) AS last_sale_date
            FROM fact_inventory fi
            JOIN latest_snapshot ls ON ls.snapshot_date = fi.doc_date
            LEFT JOIN dim_items di ON di.id = fi.item_id
            LEFT JOIN dim_branches db ON db.id = fi.branch_id
            LEFT JOIN fact_sales fs
              ON fs.item_code = di.external_id
             AND (fs.branch_ext_id IS NOT DISTINCT FROM db.external_id)
             AND fs.doc_date <= fi.doc_date
            GROUP BY fi.doc_date, di.external_id, db.external_id
        )
        INSERT INTO agg_stock_aging (
            snapshot_date,
            item_external_id,
            branch_ext_id,
            qty_on_hand,
            stock_value,
            last_sale_date,
            days_since_last_sale,
            aging_bucket,
            created_at,
            updated_at
        )
        SELECT
            b.snapshot_date,
            b.item_external_id,
            b.branch_ext_id,
            b.qty_on_hand,
            b.stock_value,
            b.last_sale_date,
            CASE WHEN b.last_sale_date IS NULL THEN NULL ELSE (b.snapshot_date - b.last_sale_date)::int END AS days_since_last_sale,
            CASE
                WHEN b.last_sale_date IS NULL THEN 'NO_SALES'
                WHEN (b.snapshot_date - b.last_sale_date) <= 30 THEN '0_30'
                WHEN (b.snapshot_date - b.last_sale_date) <= 60 THEN '31_60'
                WHEN (b.snapshot_date - b.last_sale_date) <= 90 THEN '61_90'
                ELSE '90_PLUS'
            END AS aging_bucket,
            NOW(),
            NOW()
        FROM base b
        WHERE b.item_external_id IS NOT NULL
        ON CONFLICT (snapshot_date, item_external_id, branch_ext_id) DO UPDATE SET
            qty_on_hand = EXCLUDED.qty_on_hand,
            stock_value = EXCLUDED.stock_value,
            last_sale_date = EXCLUDED.last_sale_date,
            days_since_last_sale = EXCLUDED.days_since_last_sale,
            aging_bucket = EXCLUDED.aging_bucket,
            updated_at = NOW()
        """
    )


def _create_ingest_batches(bind) -> None:
    if _table_exists(bind, 'ingest_batches'):
        return
    op.create_table(
        'ingest_batches',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_type', sa.String(length=64), nullable=False),
        sa.Column('stream', sa.String(length=64), nullable=False),
        sa.Column('batch_id', sa.String(length=128), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='running'),
        sa.Column('rows_read', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('rows_loaded', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('rows_failed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('connector_type', 'stream', 'batch_id', name='uq_ingest_batches_connector_stream_batch'),
    )
    op.alter_column('ingest_batches', 'status', server_default=None)
    op.alter_column('ingest_batches', 'rows_read', server_default=None)
    op.alter_column('ingest_batches', 'rows_loaded', server_default=None)
    op.alter_column('ingest_batches', 'rows_failed', server_default=None)
    op.alter_column('ingest_batches', 'created_at', server_default=None)
    op.alter_column('ingest_batches', 'updated_at', server_default=None)
    op.create_index('ix_ingest_batches_started_at', 'ingest_batches', ['started_at'])
    op.create_index('ix_ingest_batches_stream_started_at', 'ingest_batches', ['stream', 'started_at'])
    op.create_index('ix_ingest_batches_status', 'ingest_batches', ['status'])


def _create_or_replace_view(bind, view_name: str, select_sql: str) -> None:
    if _table_exists(bind, view_name):
        return
    if _view_exists(bind, view_name):
        op.execute(f'CREATE OR REPLACE VIEW {view_name} AS {select_sql}')
    else:
        op.execute(f'CREATE VIEW {view_name} AS {select_sql}')


def upgrade() -> None:
    bind = op.get_bind()

    _create_dim_customers(bind)
    _create_dim_accounts(bind)
    _create_dim_document_types(bind)
    _create_dim_payment_methods(bind)
    _create_agg_stock_aging(bind)
    _create_ingest_batches(bind)

    _create_or_replace_view(
        bind,
        'dim_products',
        """
        SELECT
            id,
            external_id,
            sku,
            barcode,
            name,
            main_unit,
            vat_rate,
            vat_label,
            use_batch,
            commercial_category,
            category_1,
            category_2,
            category_3,
            model_name,
            business_unit_name,
            unit2,
            purchase_unit,
            sales_unit,
            rel_2_to_1,
            rel_purchase_to_1,
            rel_sale_to_1,
            strict_rel_2_to_1,
            strict_purchase_rel,
            strict_sale_rel,
            abc_category,
            image_url,
            discount_pct,
            is_active_source,
            brand_id,
            category_id,
            group_id,
            updated_at,
            created_at
        FROM dim_items
        """,
    )
    _create_or_replace_view(bind, 'fact_sales_documents', 'SELECT * FROM fact_sales')
    _create_or_replace_view(bind, 'fact_purchase_documents', 'SELECT * FROM fact_purchases')
    _create_or_replace_view(bind, 'fact_inventory_documents', 'SELECT * FROM fact_inventory')
    _create_or_replace_view(bind, 'fact_cash_transactions', 'SELECT * FROM fact_cashflows')
    _create_or_replace_view(bind, 'agg_inventory_snapshot', 'SELECT * FROM agg_inventory_snapshot_daily')
    _create_or_replace_view(bind, 'dead_letter_queue', 'SELECT * FROM ingest_dead_letter')

    _seed_agg_stock_aging(bind)


def downgrade() -> None:
    bind = op.get_bind()

    for view_name in (
        'dead_letter_queue',
        'agg_inventory_snapshot',
        'fact_cash_transactions',
        'fact_inventory_documents',
        'fact_purchase_documents',
        'fact_sales_documents',
        'dim_products',
    ):
        if _view_exists(bind, view_name):
            op.execute(f'DROP VIEW {view_name}')

    if _table_exists(bind, 'ingest_batches'):
        for idx_name in ('ix_ingest_batches_status', 'ix_ingest_batches_stream_started_at', 'ix_ingest_batches_started_at'):
            if idx_name in _index_names(bind, 'ingest_batches'):
                op.drop_index(idx_name, table_name='ingest_batches')
        op.drop_table('ingest_batches')

    if _table_exists(bind, 'agg_stock_aging'):
        for idx_name in (
            'ix_agg_stock_aging_bucket',
            'ix_agg_stock_aging_days',
            'ix_agg_stock_aging_snapshot_branch',
            'ix_agg_stock_aging_branch_ext_id',
            'ix_agg_stock_aging_item_external_id',
            'ix_agg_stock_aging_snapshot_date',
        ):
            if idx_name in _index_names(bind, 'agg_stock_aging'):
                op.drop_index(idx_name, table_name='agg_stock_aging')
        op.drop_table('agg_stock_aging')

    for table_name, idx_names in (
        ('dim_payment_methods', ('ix_dim_payment_methods_updated_at', 'ix_dim_payment_methods_external_id')),
        ('dim_document_types', ('ix_dim_document_types_updated_at', 'ix_dim_document_types_stream', 'ix_dim_document_types_external_id')),
        ('dim_accounts', ('ix_dim_accounts_updated_at', 'ix_dim_accounts_account_type', 'ix_dim_accounts_external_id')),
        ('dim_customers', ('ix_dim_customers_updated_at', 'ix_dim_customers_customer_code', 'ix_dim_customers_external_id')),
    ):
        if not _table_exists(bind, table_name):
            continue
        current_idx = _index_names(bind, table_name)
        for idx_name in idx_names:
            if idx_name in current_idx:
                op.drop_index(idx_name, table_name=table_name)
        op.drop_table(table_name)
