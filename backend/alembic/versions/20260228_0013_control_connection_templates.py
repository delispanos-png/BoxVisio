"""tenant connection query templates and incremental columns

Revision ID: 20260228_0013_control
Revises: 20260228_0012_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0013_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0012_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('tenant_connections', 'sales_query', new_column_name='sales_query_template')
    op.alter_column('tenant_connections', 'purchases_query', new_column_name='purchases_query_template')
    op.add_column(
        'tenant_connections',
        sa.Column('id_column', sa.String(length=128), nullable=False, server_default='LineId'),
    )
    op.add_column(
        'tenant_connections',
        sa.Column('date_column', sa.String(length=128), nullable=False, server_default='DocDate'),
    )
    op.alter_column('tenant_connections', 'id_column', server_default=None)
    op.alter_column('tenant_connections', 'date_column', server_default=None)


def downgrade() -> None:
    op.drop_column('tenant_connections', 'date_column')
    op.drop_column('tenant_connections', 'id_column')
    op.alter_column('tenant_connections', 'purchases_query_template', new_column_name='purchases_query')
    op.alter_column('tenant_connections', 'sales_query_template', new_column_name='sales_query')
