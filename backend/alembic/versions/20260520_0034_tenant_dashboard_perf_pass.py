"""tenant dashboard performance pass indexes

Revision ID: 20260520_0034_tenant
Revises: 20260519_0033_tenant
Create Date: 2026-05-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260520_0034_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0033_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {column['name'] for column in inspect(bind).get_columns(table_name)}


def _create_index_if_missing(
    bind,
    name: str,
    table_name: str,
    columns: list[str],
    *,
    postgresql_where=None,
) -> None:
    if not _table_exists(bind, table_name):
        return
    existing_columns = _column_names(bind, table_name)
    if any(column not in existing_columns for column in columns):
        return
    if name in _index_names(bind, table_name):
        return
    op.create_index(name, table_name, columns, postgresql_where=postgresql_where)


def _drop_index_if_exists(bind, name: str, table_name: str) -> None:
    if not _table_exists(bind, table_name):
        return
    if name in _index_names(bind, table_name):
        op.drop_index(name, table_name=table_name)


def upgrade() -> None:
    bind = op.get_bind()

    # Product-centric dashboards and FnR/Availability aggregate by item_code
    # and recent dates. Existing indexes mostly cover item_id; SQL connector
    # facts frequently arrive with item_code, so these avoid wide date scans.
    _create_index_if_missing(bind, 'ix_fact_sales_item_code_doc_date', 'fact_sales', ['item_code', 'doc_date'])
    _create_index_if_missing(bind, 'ix_fact_sales_doc_date_item_code', 'fact_sales', ['doc_date', 'item_code'])
    _create_index_if_missing(bind, 'ix_fact_purchases_item_code_doc_date', 'fact_purchases', ['item_code', 'doc_date'])
    _create_index_if_missing(bind, 'ix_fact_purchases_doc_date_item_code', 'fact_purchases', ['doc_date', 'item_code'])

    # Inventory dashboards repeatedly read the latest snapshot and then group
    # by item/branch/warehouse. Keep the partial index small by only indexing
    # snapshot rows.
    snapshot_filter = sa.text("movement_type = 'snapshot'")
    _create_index_if_missing(
        bind,
        'ix_fact_inventory_snapshot_date_item_branch_wh',
        'fact_inventory',
        ['doc_date', 'item_code', 'branch_ext_id', 'warehouse_ext_id'],
        postgresql_where=snapshot_filter,
    )
    _create_index_if_missing(
        bind,
        'ix_fact_inventory_snapshot_item_date',
        'fact_inventory',
        ['item_code', 'doc_date'],
        postgresql_where=snapshot_filter,
    )
    _create_index_if_missing(
        bind,
        'ix_fact_inventory_snapshot_item_id_date',
        'fact_inventory',
        ['item_id', 'doc_date'],
        postgresql_where=snapshot_filter,
    )

    # Cashflow and balance screens often filter by date plus partner/account.
    _create_index_if_missing(bind, 'ix_fact_cashflows_doc_date_subcategory', 'fact_cashflows', ['doc_date', 'subcategory'])
    _create_index_if_missing(bind, 'ix_fact_cashflows_doc_date_account', 'fact_cashflows', ['doc_date', 'account_id'])
    _create_index_if_missing(bind, 'ix_fact_cashflows_doc_date_branch', 'fact_cashflows', ['doc_date', 'branch_ext_id'])

    # Supplier order dashboard and replenishment expected quantities need fast
    # open-order lookups per item and supplier.
    _create_index_if_missing(
        bind,
        'ix_fact_supplier_orders_status_item_date',
        'fact_supplier_orders',
        ['order_status', 'item_code', 'doc_date'],
    )
    _create_index_if_missing(
        bind,
        'ix_fact_supplier_orders_status_supplier_date',
        'fact_supplier_orders',
        ['order_status', 'supplier_ext_id', 'doc_date'],
    )


def downgrade() -> None:
    bind = op.get_bind()
    _drop_index_if_exists(bind, 'ix_fact_supplier_orders_status_supplier_date', 'fact_supplier_orders')
    _drop_index_if_exists(bind, 'ix_fact_supplier_orders_status_item_date', 'fact_supplier_orders')
    _drop_index_if_exists(bind, 'ix_fact_cashflows_doc_date_branch', 'fact_cashflows')
    _drop_index_if_exists(bind, 'ix_fact_cashflows_doc_date_account', 'fact_cashflows')
    _drop_index_if_exists(bind, 'ix_fact_cashflows_doc_date_subcategory', 'fact_cashflows')
    _drop_index_if_exists(bind, 'ix_fact_inventory_snapshot_item_id_date', 'fact_inventory')
    _drop_index_if_exists(bind, 'ix_fact_inventory_snapshot_item_date', 'fact_inventory')
    _drop_index_if_exists(bind, 'ix_fact_inventory_snapshot_date_item_branch_wh', 'fact_inventory')
    _drop_index_if_exists(bind, 'ix_fact_purchases_doc_date_item_code', 'fact_purchases')
    _drop_index_if_exists(bind, 'ix_fact_purchases_item_code_doc_date', 'fact_purchases')
    _drop_index_if_exists(bind, 'ix_fact_sales_doc_date_item_code', 'fact_sales')
    _drop_index_if_exists(bind, 'ix_fact_sales_item_code_doc_date', 'fact_sales')
