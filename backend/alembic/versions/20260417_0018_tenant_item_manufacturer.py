"""tenant dim_items manufacturer fields

Revision ID: 20260417_0018_tenant
Revises: 20260414_0017_tenant
Create Date: 2026-04-17
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260417_0018_tenant'
down_revision: Union[str, Sequence[str], None] = '20260414_0017_tenant'
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


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    columns = _column_names(bind, 'dim_items')
    if 'manufacturer_code' not in columns:
        op.add_column('dim_items', sa.Column('manufacturer_code', sa.String(length=128), nullable=True))
    if 'manufacturer_name' not in columns:
        op.add_column('dim_items', sa.Column('manufacturer_name', sa.String(length=255), nullable=True))
    if 'ix_dim_items_manufacturer_code' not in _index_names(bind, 'dim_items'):
        op.create_index('ix_dim_items_manufacturer_code', 'dim_items', ['manufacturer_code'])


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    indexes = _index_names(bind, 'dim_items')
    columns = _column_names(bind, 'dim_items')
    if 'ix_dim_items_manufacturer_code' in indexes:
        op.drop_index('ix_dim_items_manufacturer_code', table_name='dim_items')
    if 'manufacturer_name' in columns:
        op.drop_column('dim_items', 'manufacturer_name')
    if 'manufacturer_code' in columns:
        op.drop_column('dim_items', 'manufacturer_code')
