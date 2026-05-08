"""tenant dim_items softone sotype

Revision ID: 20260414_0016_tenant
Revises: 20260409_0015_tenant
Create Date: 2026-04-14
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260414_0016_tenant'
down_revision: Union[str, Sequence[str], None] = '20260409_0015_tenant'
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
    if 'softone_sotype' not in _column_names(bind, 'dim_items'):
        op.add_column('dim_items', sa.Column('softone_sotype', sa.Integer(), nullable=True))
    if 'ix_dim_items_softone_sotype' not in _index_names(bind, 'dim_items'):
        op.create_index('ix_dim_items_softone_sotype', 'dim_items', ['softone_sotype'])


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    if 'ix_dim_items_softone_sotype' in _index_names(bind, 'dim_items'):
        op.drop_index('ix_dim_items_softone_sotype', table_name='dim_items')
    if 'softone_sotype' in _column_names(bind, 'dim_items'):
        op.drop_column('dim_items', 'softone_sotype')
