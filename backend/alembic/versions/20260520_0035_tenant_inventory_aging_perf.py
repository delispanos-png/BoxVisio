"""tenant inventory aging performance indexes

Revision ID: 20260520_0035_tenant
Revises: 20260520_0034_tenant
Create Date: 2026-05-20
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = '20260520_0035_tenant'
down_revision: Union[str, Sequence[str], None] = '20260520_0034_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {column['name'] for column in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _create_index_if_missing(bind, name: str, table_name: str, columns: list[str]) -> None:
    if not _table_exists(bind, table_name):
        return
    existing_columns = _column_names(bind, table_name)
    if any(column not in existing_columns for column in columns):
        return
    if name in _index_names(bind, table_name):
        return
    op.create_index(name, table_name, columns)


def _drop_index_if_exists(bind, name: str, table_name: str) -> None:
    if not _table_exists(bind, table_name):
        return
    if name in _index_names(bind, table_name):
        op.drop_index(name, table_name=table_name)


def upgrade() -> None:
    bind = op.get_bind()
    # Stock aging asks "last sale before snapshot" for item + branch. SQL
    # connector inventory rows are usually keyed by item_code and branch_ext_id.
    _create_index_if_missing(
        bind,
        'ix_fact_sales_branch_ext_item_code_doc_date',
        'fact_sales',
        ['branch_ext_id', 'item_code', 'doc_date'],
    )
    _create_index_if_missing(
        bind,
        'ix_fact_inventory_doc_branch_wh_item_updated',
        'fact_inventory',
        ['doc_date', 'branch_ext_id', 'warehouse_ext_id', 'item_code', 'updated_at'],
    )


def downgrade() -> None:
    bind = op.get_bind()
    _drop_index_if_exists(bind, 'ix_fact_inventory_doc_branch_wh_item_updated', 'fact_inventory')
    _drop_index_if_exists(bind, 'ix_fact_sales_branch_ext_item_code_doc_date', 'fact_sales')
