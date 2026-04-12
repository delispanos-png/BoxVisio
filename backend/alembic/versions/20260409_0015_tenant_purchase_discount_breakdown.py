"""tenant purchase discount breakdown columns

Revision ID: 20260409_0015_tenant
Revises: 20260409_0014_tenant
Create Date: 2026-04-09
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260409_0015_tenant'
down_revision: Union[str, Sequence[str], None] = '20260409_0014_tenant'
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
    if not _table_exists(bind, 'fact_purchases'):
        return

    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount1_pct', sa.Numeric(10, 4), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount2_pct', sa.Numeric(10, 4), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount3_pct', sa.Numeric(10, 4), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount1_amount', sa.Numeric(14, 2), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount2_amount', sa.Numeric(14, 2), nullable=True))
    _add_column_if_missing(bind, 'fact_purchases', sa.Column('discount3_amount', sa.Numeric(14, 2), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'fact_purchases'):
        return

    _drop_column_if_exists(bind, 'fact_purchases', 'discount3_amount')
    _drop_column_if_exists(bind, 'fact_purchases', 'discount2_amount')
    _drop_column_if_exists(bind, 'fact_purchases', 'discount1_amount')
    _drop_column_if_exists(bind, 'fact_purchases', 'discount3_pct')
    _drop_column_if_exists(bind, 'fact_purchases', 'discount2_pct')
    _drop_column_if_exists(bind, 'fact_purchases', 'discount1_pct')

