"""tenant supplier balance afm

Revision ID: 20260518_0027_tenant
Revises: 20260518_0026_tenant
Create Date: 2026-05-18
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision: str = '20260518_0027_tenant'
down_revision: Union[str, Sequence[str], None] = '20260518_0026_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _columns(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_supplier_balances') and 'supplier_afm' not in _columns(bind, 'fact_supplier_balances'):
        op.add_column('fact_supplier_balances', sa.Column('supplier_afm', sa.String(length=64), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_supplier_balances') and 'supplier_afm' in _columns(bind, 'fact_supplier_balances'):
        op.drop_column('fact_supplier_balances', 'supplier_afm')
