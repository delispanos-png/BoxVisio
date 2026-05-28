"""deduplicate tenant market imports

Revision ID: 20260522_0037_tenant
Revises: 20260522_0036_tenant
Create Date: 2026-05-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260522_0037_tenant'
down_revision: Union[str, Sequence[str], None] = '20260522_0036_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _columns(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _add_source_sha256(table_name: str, index_name: str) -> None:
    bind = op.get_bind()
    if not _table_exists(bind, table_name):
        return
    if 'source_sha256' not in _columns(bind, table_name):
        op.add_column(table_name, sa.Column('source_sha256', sa.String(length=64), nullable=True))
    if index_name not in _index_names(bind, table_name):
        op.create_index(index_name, table_name, ['source_sha256'])


def upgrade() -> None:
    _add_source_sha256('era_exploration_snapshots', 'ix_era_exploration_snapshots_source_sha256')
    _add_source_sha256('iqvia_snapshots', 'ix_iqvia_snapshots_source_sha256')


def downgrade() -> None:
    bind = op.get_bind()
    for table_name, index_name in (
        ('iqvia_snapshots', 'ix_iqvia_snapshots_source_sha256'),
        ('era_exploration_snapshots', 'ix_era_exploration_snapshots_source_sha256'),
    ):
        if not _table_exists(bind, table_name):
            continue
        if index_name in _index_names(bind, table_name):
            op.drop_index(index_name, table_name=table_name)
        if 'source_sha256' in _columns(bind, table_name):
            op.drop_column(table_name, 'source_sha256')
