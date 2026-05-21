"""tenant fnr default one

Revision ID: 20260519_0032_tenant
Revises: 20260519_0031_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = '20260519_0032_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0031_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    return inspect(op.get_bind()).has_table(table_name)


def _column_names(table_name: str) -> set[str]:
    if not _table_exists(table_name):
        return set()
    return {col['name'] for col in inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    if not _table_exists('dim_items'):
        return
    columns = _column_names('dim_items')
    for column in ('min_stock', 'replenishment_moq', 'vendor_moq'):
        if column in columns:
            op.execute(f'UPDATE dim_items SET {column} = 1 WHERE {column} IS NULL')


def downgrade() -> None:
    # Data-only migration. We intentionally do not erase operational defaults.
    pass
