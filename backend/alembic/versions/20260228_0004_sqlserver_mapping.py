"""sqlserver mapping config in tenant_connections

Revision ID: 20260228_0004_control
Revises: 20260228_0003_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0004_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0003_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tenant_connections', sa.Column('connection_string', sa.Text(), nullable=False, server_default=''))
    op.add_column('tenant_connections', sa.Column('sales_query', sa.Text(), nullable=False, server_default=''))
    op.add_column('tenant_connections', sa.Column('purchases_query', sa.Text(), nullable=False, server_default=''))
    op.add_column('tenant_connections', sa.Column('incremental_column', sa.String(length=128), nullable=False, server_default='UpdatedAt'))
    op.add_column('tenant_connections', sa.Column('branch_column', sa.String(length=128), nullable=False, server_default='BranchCode'))
    op.add_column('tenant_connections', sa.Column('item_column', sa.String(length=128), nullable=False, server_default='ItemCode'))
    op.add_column('tenant_connections', sa.Column('amount_column', sa.String(length=128), nullable=False, server_default='NetValue'))
    op.add_column('tenant_connections', sa.Column('cost_column', sa.String(length=128), nullable=False, server_default='CostValue'))
    op.add_column('tenant_connections', sa.Column('qty_column', sa.String(length=128), nullable=False, server_default='Qty'))


def downgrade() -> None:
    op.drop_column('tenant_connections', 'qty_column')
    op.drop_column('tenant_connections', 'cost_column')
    op.drop_column('tenant_connections', 'amount_column')
    op.drop_column('tenant_connections', 'item_column')
    op.drop_column('tenant_connections', 'branch_column')
    op.drop_column('tenant_connections', 'incremental_column')
    op.drop_column('tenant_connections', 'purchases_query')
    op.drop_column('tenant_connections', 'sales_query')
    op.drop_column('tenant_connections', 'connection_string')
