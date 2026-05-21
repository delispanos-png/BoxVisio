"""tenant cashflow reference index

Revision ID: 20260516_0023_tenant
Revises: 20260512_0022_tenant
Create Date: 2026-05-16
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = '20260516_0023_tenant'
down_revision: Union[str, Sequence[str], None] = '20260512_0022_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_cashflows'):
        idx = _index_names(bind, 'fact_cashflows')
        if 'ix_fact_cashflows_reference_no' not in idx:
            op.create_index('ix_fact_cashflows_reference_no', 'fact_cashflows', ['reference_no'])


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_cashflows'):
        idx = _index_names(bind, 'fact_cashflows')
        if 'ix_fact_cashflows_reference_no' in idx:
            op.drop_index('ix_fact_cashflows_reference_no', table_name='fact_cashflows')
