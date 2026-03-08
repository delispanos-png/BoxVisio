"""tenant index strategy hardening

Revision ID: 20260307_0008_tenant
Revises: 20260307_0007_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0008_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0007_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _view_exists(bind, view_name: str) -> bool:
    return view_name in set(inspect(bind).get_view_names())


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    if not _table_exists(bind, table_name):
        return False
    cols = inspect(bind).get_columns(table_name)
    return any(str(col.get('name')) == column_name for col in cols)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _add_column_if_missing(bind, table_name: str, column: sa.Column) -> None:
    if not _table_exists(bind, table_name):
        return
    if _column_exists(bind, table_name, str(column.name)):
        return
    op.add_column(table_name, column)


def _create_index_if_missing(
    bind,
    table_name: str,
    index_name: str,
    columns: list[str],
    *,
    unique: bool = False,
) -> None:
    if not _table_exists(bind, table_name):
        return
    if index_name in _index_names(bind, table_name):
        return
    for col in columns:
        if not _column_exists(bind, table_name, col):
            return
    op.create_index(index_name, table_name, columns, unique=unique)


def _drop_index_if_exists(bind, table_name: str, index_name: str) -> None:
    if not _table_exists(bind, table_name):
        return
    if index_name in _index_names(bind, table_name):
        op.drop_index(index_name, table_name=table_name)


def _add_missing_columns(bind) -> None:
    _add_column_if_missing(bind, 'fact_sales', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_sales', sa.Column('customer_id', postgresql.UUID(as_uuid=True), nullable=True))

    _add_column_if_missing(bind, 'fact_purchases', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_cashflows', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_supplier_balances', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_customer_balances', sa.Column('source_connector_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_customer_balances', sa.Column('customer_id', postgresql.UUID(as_uuid=True), nullable=True))

    _add_column_if_missing(bind, 'sync_state', sa.Column('stream_code', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'sync_state', sa.Column('source_connector_id', sa.String(length=64), nullable=True))

    if not _column_exists(bind, 'ingest_dead_letter', 'status'):
        op.add_column(
            'ingest_dead_letter',
            sa.Column('status', sa.String(length=16), nullable=False, server_default=sa.text("'new'")),
        )

    if _column_exists(bind, 'ingest_dead_letter', 'status'):
        op.execute("UPDATE ingest_dead_letter SET status='new' WHERE status IS NULL")

    if _column_exists(bind, 'sync_state', 'source_connector_id'):
        op.execute(
            """
            UPDATE sync_state
            SET source_connector_id = connector_type
            WHERE source_connector_id IS NULL
            """
        )
    if _column_exists(bind, 'sync_state', 'stream_code'):
        op.execute(
            """
            UPDATE sync_state
            SET stream_code = 'all_streams'
            WHERE stream_code IS NULL
            """
        )


def _create_fact_indexes(bind) -> None:
    # Fact table mandatory coverage
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_source_connector_id', ['source_connector_id'])
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_customer_id', ['customer_id'])
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_doc_date_item_id', ['doc_date', 'item_id'])
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_doc_date_customer_id', ['doc_date', 'customer_id'])
    _create_index_if_missing(bind, 'fact_sales', 'ix_fact_sales_branch_item_doc_date', ['branch_id', 'item_id', 'doc_date'])

    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_supplier_id', ['supplier_id'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_source_connector_id', ['source_connector_id'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_doc_date_supplier_id', ['doc_date', 'supplier_id'])
    _create_index_if_missing(
        bind,
        'fact_purchases',
        'ix_fact_purchases_branch_supplier_doc_date',
        ['branch_id', 'supplier_id', 'doc_date'],
    )

    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_source_connector_id', ['source_connector_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_item_id_doc_date', ['item_id', 'doc_date'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_warehouse_id_doc_date', ['warehouse_id', 'doc_date'])

    _create_index_if_missing(bind, 'fact_cashflows', 'ix_fact_cashflows_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_cashflows', 'ix_fact_cashflows_source_connector_id', ['source_connector_id'])
    _create_index_if_missing(bind, 'fact_cashflows', 'ix_fact_cashflows_transaction_date', ['transaction_date'])
    _create_index_if_missing(bind, 'fact_cashflows', 'ix_fact_cashflows_transaction_date_branch_id', ['transaction_date', 'branch_id'])
    _create_index_if_missing(
        bind,
        'fact_cashflows',
        'ix_fact_cashflows_transaction_date_account_id',
        ['transaction_date', 'account_id'],
    )
    _create_index_if_missing(
        bind,
        'fact_cashflows',
        'ix_fact_cashflows_subcategory_transaction_date',
        ['subcategory', 'transaction_date'],
    )

    _create_index_if_missing(bind, 'fact_supplier_balances', 'ix_fact_supplier_balances_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_supplier_balances', 'ix_fact_supplier_balances_supplier_id', ['supplier_id'])
    _create_index_if_missing(
        bind,
        'fact_supplier_balances',
        'ix_fact_supplier_balances_source_connector_id',
        ['source_connector_id'],
    )
    _create_index_if_missing(
        bind,
        'fact_supplier_balances',
        'ix_fact_supplier_balances_balance_date_supplier_id',
        ['balance_date', 'supplier_id'],
    )
    _create_index_if_missing(
        bind,
        'fact_supplier_balances',
        'ix_fact_supplier_balances_balance_date_branch_id',
        ['balance_date', 'branch_id'],
    )

    _create_index_if_missing(bind, 'fact_customer_balances', 'ix_fact_customer_balances_branch_id', ['branch_id'])
    _create_index_if_missing(bind, 'fact_customer_balances', 'ix_fact_customer_balances_customer_id', ['customer_id'])
    _create_index_if_missing(
        bind,
        'fact_customer_balances',
        'ix_fact_customer_balances_source_connector_id',
        ['source_connector_id'],
    )
    _create_index_if_missing(
        bind,
        'fact_customer_balances',
        'ix_fact_customer_balances_balance_date_customer_id',
        ['balance_date', 'customer_id'],
    )
    _create_index_if_missing(
        bind,
        'fact_customer_balances',
        'ix_fact_customer_balances_balance_date_branch_id',
        ['balance_date', 'branch_id'],
    )


def _create_aggregate_indexes(bind) -> None:
    _create_index_if_missing(
        bind,
        'agg_sales_daily',
        'ix_agg_sales_daily_doc_date_branch_brand',
        ['doc_date', 'branch_ext_id', 'brand_ext_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_sales_monthly',
        'ix_agg_sales_monthly_month_start_branch_brand',
        ['month_start', 'branch_ext_id', 'brand_ext_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_purchases_daily',
        'ix_agg_purchases_daily_doc_date_branch_brand_supplier',
        ['doc_date', 'branch_ext_id', 'brand_ext_id', 'supplier_ext_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_purchases_monthly',
        'ix_agg_purchases_monthly_month_start_branch_brand_supplier',
        ['month_start', 'branch_ext_id', 'brand_ext_id', 'supplier_ext_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_cash_daily',
        'ix_agg_cash_daily_doc_date_branch_account',
        ['doc_date', 'branch_ext_id', 'account_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_cash_by_type',
        'ix_agg_cash_by_type_subcategory_doc_date',
        ['subcategory', 'doc_date'],
    )
    _create_index_if_missing(
        bind,
        'agg_cash_accounts',
        'ix_agg_cash_accounts_account_id_doc_date',
        ['account_id', 'doc_date'],
    )
    _create_index_if_missing(
        bind,
        'agg_supplier_balances_daily',
        'ix_agg_supplier_balances_daily_date_branch',
        ['balance_date', 'branch_ext_id'],
    )
    _create_index_if_missing(
        bind,
        'agg_customer_balances_daily',
        'ix_agg_customer_balances_daily_date_branch',
        ['balance_date', 'branch_ext_id'],
    )


def _create_dimension_indexes(bind) -> None:
    _create_index_if_missing(bind, 'dim_branches', 'ix_dim_branches_name', ['name'])
    _create_index_if_missing(bind, 'dim_warehouses', 'ix_dim_warehouses_name', ['name'])
    _create_index_if_missing(bind, 'dim_brands', 'ix_dim_brands_name', ['name'])
    _create_index_if_missing(bind, 'dim_categories', 'ix_dim_categories_name', ['name'])
    _create_index_if_missing(bind, 'dim_items', 'ix_dim_items_sku', ['sku'])
    _create_index_if_missing(bind, 'dim_items', 'ix_dim_items_name', ['name'])
    _create_index_if_missing(bind, 'dim_customers', 'ix_dim_customers_name', ['name'])
    _create_index_if_missing(bind, 'dim_suppliers', 'ix_dim_suppliers_name', ['name'])
    _create_index_if_missing(bind, 'dim_accounts', 'ix_dim_accounts_name', ['name'])
    _create_index_if_missing(bind, 'dim_document_types', 'ix_dim_document_types_name', ['name'])
    _create_index_if_missing(bind, 'dim_payment_methods', 'ix_dim_payment_methods_name', ['name'])


def _create_sync_support_indexes(bind) -> None:
    _create_index_if_missing(
        bind,
        'sync_state',
        'ix_sync_state_stream_source_connector',
        ['stream_code', 'source_connector_id'],
    )
    _create_index_if_missing(
        bind,
        'ingest_batches',
        'ix_ingest_batches_started_status',
        ['started_at', 'status'],
    )
    _create_index_if_missing(
        bind,
        'ingest_dead_letter',
        'ix_ingest_dead_letter_status_created_at',
        ['status', 'created_at'],
    )


def _drop_redundant_indexes(bind) -> None:
    # Redundant with existing unique indexes on external_id.
    _drop_index_if_exists(bind, 'fact_supplier_balances', 'ix_fact_supplier_balances_external_id')
    _drop_index_if_exists(bind, 'fact_customer_balances', 'ix_fact_customer_balances_external_id')


def upgrade() -> None:
    bind = op.get_bind()

    _add_missing_columns(bind)
    _create_fact_indexes(bind)
    _create_aggregate_indexes(bind)
    _create_dimension_indexes(bind)
    _create_sync_support_indexes(bind)
    _drop_redundant_indexes(bind)

    if _view_exists(bind, 'dead_letter_queue') and _table_exists(bind, 'ingest_dead_letter'):
        op.execute('CREATE OR REPLACE VIEW dead_letter_queue AS SELECT * FROM ingest_dead_letter')


def downgrade() -> None:
    bind = op.get_bind()

    if _view_exists(bind, 'dead_letter_queue'):
        op.execute('DROP VIEW dead_letter_queue')

    for table_name, indexes in (
        (
            'fact_sales',
            (
                'ix_fact_sales_branch_id',
                'ix_fact_sales_source_connector_id',
                'ix_fact_sales_customer_id',
                'ix_fact_sales_doc_date_item_id',
                'ix_fact_sales_doc_date_customer_id',
                'ix_fact_sales_branch_item_doc_date',
            ),
        ),
        (
            'fact_purchases',
            (
                'ix_fact_purchases_branch_id',
                'ix_fact_purchases_supplier_id',
                'ix_fact_purchases_source_connector_id',
                'ix_fact_purchases_doc_date_supplier_id',
                'ix_fact_purchases_branch_supplier_doc_date',
            ),
        ),
        (
            'fact_inventory',
            (
                'ix_fact_inventory_branch_id',
                'ix_fact_inventory_source_connector_id',
                'ix_fact_inventory_item_id_doc_date',
                'ix_fact_inventory_warehouse_id_doc_date',
            ),
        ),
        (
            'fact_cashflows',
            (
                'ix_fact_cashflows_branch_id',
                'ix_fact_cashflows_source_connector_id',
                'ix_fact_cashflows_transaction_date',
                'ix_fact_cashflows_transaction_date_branch_id',
                'ix_fact_cashflows_transaction_date_account_id',
                'ix_fact_cashflows_subcategory_transaction_date',
            ),
        ),
        (
            'fact_supplier_balances',
            (
                'ix_fact_supplier_balances_branch_id',
                'ix_fact_supplier_balances_supplier_id',
                'ix_fact_supplier_balances_source_connector_id',
                'ix_fact_supplier_balances_balance_date_supplier_id',
                'ix_fact_supplier_balances_balance_date_branch_id',
            ),
        ),
        (
            'fact_customer_balances',
            (
                'ix_fact_customer_balances_branch_id',
                'ix_fact_customer_balances_customer_id',
                'ix_fact_customer_balances_source_connector_id',
                'ix_fact_customer_balances_balance_date_customer_id',
                'ix_fact_customer_balances_balance_date_branch_id',
            ),
        ),
        (
            'agg_sales_daily',
            ('ix_agg_sales_daily_doc_date_branch_brand',),
        ),
        (
            'agg_sales_monthly',
            ('ix_agg_sales_monthly_month_start_branch_brand',),
        ),
        (
            'agg_purchases_daily',
            ('ix_agg_purchases_daily_doc_date_branch_brand_supplier',),
        ),
        (
            'agg_purchases_monthly',
            ('ix_agg_purchases_monthly_month_start_branch_brand_supplier',),
        ),
        (
            'agg_cash_daily',
            ('ix_agg_cash_daily_doc_date_branch_account',),
        ),
        (
            'agg_cash_by_type',
            ('ix_agg_cash_by_type_subcategory_doc_date',),
        ),
        (
            'agg_cash_accounts',
            ('ix_agg_cash_accounts_account_id_doc_date',),
        ),
        (
            'agg_supplier_balances_daily',
            ('ix_agg_supplier_balances_daily_date_branch',),
        ),
        (
            'agg_customer_balances_daily',
            ('ix_agg_customer_balances_daily_date_branch',),
        ),
        (
            'dim_branches',
            ('ix_dim_branches_name',),
        ),
        (
            'dim_warehouses',
            ('ix_dim_warehouses_name',),
        ),
        (
            'dim_brands',
            ('ix_dim_brands_name',),
        ),
        (
            'dim_categories',
            ('ix_dim_categories_name',),
        ),
        (
            'dim_items',
            ('ix_dim_items_sku', 'ix_dim_items_name'),
        ),
        (
            'dim_customers',
            ('ix_dim_customers_name',),
        ),
        (
            'dim_suppliers',
            ('ix_dim_suppliers_name',),
        ),
        (
            'dim_accounts',
            ('ix_dim_accounts_name',),
        ),
        (
            'dim_document_types',
            ('ix_dim_document_types_name',),
        ),
        (
            'dim_payment_methods',
            ('ix_dim_payment_methods_name',),
        ),
        (
            'sync_state',
            ('ix_sync_state_stream_source_connector',),
        ),
        (
            'ingest_batches',
            ('ix_ingest_batches_started_status',),
        ),
        (
            'ingest_dead_letter',
            ('ix_ingest_dead_letter_status_created_at',),
        ),
    ):
        for idx in indexes:
            _drop_index_if_exists(bind, table_name, idx)

    for table_name, col_name in (
        ('fact_sales', 'source_connector_id'),
        ('fact_sales', 'customer_id'),
        ('fact_purchases', 'source_connector_id'),
        ('fact_inventory', 'source_connector_id'),
        ('fact_cashflows', 'source_connector_id'),
        ('fact_supplier_balances', 'source_connector_id'),
        ('fact_customer_balances', 'source_connector_id'),
        ('fact_customer_balances', 'customer_id'),
        ('sync_state', 'stream_code'),
        ('sync_state', 'source_connector_id'),
    ):
        if _table_exists(bind, table_name) and _column_exists(bind, table_name, col_name):
            op.drop_column(table_name, col_name)

    if _table_exists(bind, 'ingest_dead_letter') and _column_exists(bind, 'ingest_dead_letter', 'status'):
        op.drop_column('ingest_dead_letter', 'status')

    # Restore non-unique external_id indexes dropped in upgrade.
    _create_index_if_missing(bind, 'fact_supplier_balances', 'ix_fact_supplier_balances_external_id', ['external_id'])
    _create_index_if_missing(bind, 'fact_customer_balances', 'ix_fact_customer_balances_external_id', ['external_id'])

    if _table_exists(bind, 'ingest_dead_letter'):
        op.execute(
            """
            CREATE VIEW dead_letter_queue AS
            SELECT
                id,
                connector_type,
                entity,
                event_id,
                payload_json,
                error_message,
                retry_count,
                created_at
            FROM ingest_dead_letter
            """
        )
