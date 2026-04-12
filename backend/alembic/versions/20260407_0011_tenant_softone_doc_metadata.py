"""tenant softone document metadata alignment

Revision ID: 20260407_0011_tenant
Revises: 20260308_0010_tenant
Create Date: 2026-04-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260407_0011_tenant'
down_revision: Union[str, Sequence[str], None] = '20260308_0010_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _add_column_if_missing(bind, table_name: str, column: sa.Column) -> None:
    if column.name in _column_names(bind, table_name):
        return
    op.add_column(table_name, column)


def _drop_column_if_exists(bind, table_name: str, column_name: str) -> None:
    if column_name not in _column_names(bind, table_name):
        return
    op.drop_column(table_name, column_name)


def _create_index_if_missing(bind, table_name: str, index_name: str, columns: list[str]) -> None:
    if index_name in _index_names(bind, table_name):
        return
    op.create_index(index_name, table_name, columns)


def _drop_index_if_exists(bind, table_name: str, index_name: str) -> None:
    if index_name not in _index_names(bind, table_name):
        return
    op.drop_index(index_name, table_name=table_name)


def _upgrade_fact_purchases(bind) -> None:
    if not _table_exists(bind, 'fact_purchases'):
        return

    _add_column_if_missing(bind, 'fact_purchases', sa.Column('document_id', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('document_no', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('document_series', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('document_type', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('source_module_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('redirect_module_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('source_entity_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('object_id', sa.Integer(), nullable=True))
    _add_column_if_missing(
        bind,
        'fact_purchases',
        sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_document_id', ['document_id'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_document_no', ['document_no'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_document_id_doc_date', ['document_id', 'doc_date'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_source_module_id', ['source_module_id'])
    _create_index_if_missing(bind, 'fact_purchases', 'ix_fact_purchases_object_id', ['object_id'])


def _upgrade_fact_inventory(bind) -> None:
    if not _table_exists(bind, 'fact_inventory'):
        return

    _add_column_if_missing(bind, 'fact_inventory', sa.Column('event_id', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('branch_ext_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('item_code', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('document_id', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('document_no', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('document_series', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('document_type', sa.String(length=128), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('movement_type', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('source_module_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('redirect_module_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('source_entity_id', sa.Integer(), nullable=True))
    _add_column_if_missing(bind, 'fact_inventory', sa.Column('object_id', sa.Integer(), nullable=True))
    _add_column_if_missing(
        bind,
        'fact_inventory',
        sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_event_id', ['event_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_branch_ext_id', ['branch_ext_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_warehouse_ext_id', ['warehouse_ext_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_item_code', ['item_code'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_document_id', ['document_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_document_id_doc_date', ['document_id', 'doc_date'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_source_module_id', ['source_module_id'])
    _create_index_if_missing(bind, 'fact_inventory', 'ix_fact_inventory_object_id', ['object_id'])


def upgrade() -> None:
    bind = op.get_bind()
    _upgrade_fact_purchases(bind)
    _upgrade_fact_inventory(bind)


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'fact_inventory'):
        for idx_name in (
            'ix_fact_inventory_object_id',
            'ix_fact_inventory_source_module_id',
            'ix_fact_inventory_document_id_doc_date',
            'ix_fact_inventory_document_id',
            'ix_fact_inventory_item_code',
            'ix_fact_inventory_warehouse_ext_id',
            'ix_fact_inventory_branch_ext_id',
            'ix_fact_inventory_event_id',
        ):
            _drop_index_if_exists(bind, 'fact_inventory', idx_name)
        for col_name in (
            'source_payload_json',
            'object_id',
            'source_entity_id',
            'redirect_module_id',
            'source_module_id',
            'movement_type',
            'document_type',
            'document_series',
            'document_no',
            'document_id',
            'item_code',
            'warehouse_ext_id',
            'branch_ext_id',
            'event_id',
        ):
            _drop_column_if_exists(bind, 'fact_inventory', col_name)

    if _table_exists(bind, 'fact_purchases'):
        for idx_name in (
            'ix_fact_purchases_object_id',
            'ix_fact_purchases_source_module_id',
            'ix_fact_purchases_document_id_doc_date',
            'ix_fact_purchases_document_no',
            'ix_fact_purchases_document_id',
        ):
            _drop_index_if_exists(bind, 'fact_purchases', idx_name)
        for col_name in (
            'source_payload_json',
            'object_id',
            'source_entity_id',
            'redirect_module_id',
            'source_module_id',
            'document_type',
            'document_series',
            'document_no',
            'document_id',
        ):
            _drop_column_if_exists(bind, 'fact_purchases', col_name)
