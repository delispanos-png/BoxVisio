"""tenant facts group filter columns

Revision ID: 20260228_0003_tenant
Revises: 20260228_0002_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0003_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0002_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('fact_sales', sa.Column('group_ext_id', sa.String(length=64), nullable=True))
    op.create_index('ix_fact_sales_group_ext_id', 'fact_sales', ['group_ext_id'])

    op.add_column('fact_purchases', sa.Column('group_ext_id', sa.String(length=64), nullable=True))
    op.create_index('ix_fact_purchases_group_ext_id', 'fact_purchases', ['group_ext_id'])


def downgrade() -> None:
    op.drop_index('ix_fact_purchases_group_ext_id', table_name='fact_purchases')
    op.drop_column('fact_purchases', 'group_ext_id')

    op.drop_index('ix_fact_sales_group_ext_id', table_name='fact_sales')
    op.drop_column('fact_sales', 'group_ext_id')
