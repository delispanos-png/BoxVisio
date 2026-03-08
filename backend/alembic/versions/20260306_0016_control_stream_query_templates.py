"""control stream query templates (inventory/cashflow)

Revision ID: 20260306_0016_control
Revises: 20260301_0015_control
Create Date: 2026-03-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260306_0016_control'
down_revision: Union[str, Sequence[str], None] = '20260301_0015_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'tenant_connections',
        sa.Column(
            'inventory_query_template',
            sa.Text(),
            nullable=False,
            server_default='SELECT TOP 5000 * FROM dbo.InventorySnapshots',
        ),
    )
    op.add_column(
        'tenant_connections',
        sa.Column(
            'cashflow_query_template',
            sa.Text(),
            nullable=False,
            server_default='SELECT TOP 5000 * FROM dbo.CashflowEntries',
        ),
    )
    op.alter_column('tenant_connections', 'inventory_query_template', server_default=None)
    op.alter_column('tenant_connections', 'cashflow_query_template', server_default=None)


def downgrade() -> None:
    op.drop_column('tenant_connections', 'cashflow_query_template')
    op.drop_column('tenant_connections', 'inventory_query_template')
