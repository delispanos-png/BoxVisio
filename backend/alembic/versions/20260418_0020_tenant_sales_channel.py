"""tenant fact_sales channel fields (CCC88ECHANNEL)

Revision ID: 20260418_0020_tenant
Revises: 20260418_0019_tenant
Create Date: 2026-04-18
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260418_0020_tenant'
down_revision: Union[str, Sequence[str], None] = '20260418_0019_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _add_column_if_missing(bind, table_name: str, column: sa.Column) -> None:
    if column.name in _column_names(bind, table_name):
        return
    op.add_column(table_name, column)


def _drop_column_if_exists(bind, table_name: str, column_name: str) -> None:
    if column_name not in _column_names(bind, table_name):
        return
    op.drop_column(table_name, column_name)


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'fact_sales'):
        return

    _add_column_if_missing(bind, 'fact_sales', sa.Column('channel_ext_id', sa.String(length=64), nullable=True))
    _add_column_if_missing(bind, 'fact_sales', sa.Column('channel_name', sa.String(length=255), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'fact_sales'):
        return

    _drop_column_if_exists(bind, 'fact_sales', 'channel_name')
    _drop_column_if_exists(bind, 'fact_sales', 'channel_ext_id')
