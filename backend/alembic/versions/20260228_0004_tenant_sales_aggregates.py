"""tenant sales aggregate tables

Revision ID: 20260228_0004_tenant
Revises: 20260228_0003_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0004_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0003_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'agg_sales_daily',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Float(), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            'doc_date',
            'branch_ext_id',
            'warehouse_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_sales_daily_dims',
        ),
    )
    op.create_index('ix_agg_sales_daily_doc_date', 'agg_sales_daily', ['doc_date'])

    op.create_table(
        'agg_sales_monthly',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('month_start', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Float(), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            'month_start',
            'branch_ext_id',
            'warehouse_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_sales_monthly_dims',
        ),
    )
    op.create_index('ix_agg_sales_monthly_month_start', 'agg_sales_monthly', ['month_start'])


def downgrade() -> None:
    op.drop_index('ix_agg_sales_monthly_month_start', table_name='agg_sales_monthly')
    op.drop_table('agg_sales_monthly')

    op.drop_index('ix_agg_sales_daily_doc_date', table_name='agg_sales_daily')
    op.drop_table('agg_sales_daily')
