"""control tenant connection supplier balance query template

Revision ID: 20260307_0001_control
Revises: 20260306_0016_control
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260307_0001_control'
down_revision: Union[str, Sequence[str], None] = '20260306_0016_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    cols = {col['name'] for col in inspect(bind).get_columns('tenant_connections')}
    if 'supplier_balances_query_template' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column(
                'supplier_balances_query_template',
                sa.Text(),
                nullable=False,
                server_default='SELECT TOP 5000 * FROM dbo.SupplierBalances',
            ),
        )
        op.alter_column('tenant_connections', 'supplier_balances_query_template', server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    cols = {col['name'] for col in inspect(bind).get_columns('tenant_connections')}
    if 'supplier_balances_query_template' in cols:
        op.drop_column('tenant_connections', 'supplier_balances_query_template')
