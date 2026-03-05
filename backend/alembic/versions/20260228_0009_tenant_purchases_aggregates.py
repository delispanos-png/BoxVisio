"""tenant purchases aggregate tables

Revision ID: 20260228_0009_tenant
Revises: 20260228_0008_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0009_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0008_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'agg_purchases_daily',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint(
            'doc_date',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_purchases_daily_dims',
        ),
    )
    op.create_index('ix_agg_purchases_daily_doc_date', 'agg_purchases_daily', ['doc_date'])

    op.create_table(
        'agg_purchases_monthly',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('month_start', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint(
            'month_start',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_purchases_monthly_dims',
        ),
    )
    op.create_index('ix_agg_purchases_monthly_month_start', 'agg_purchases_monthly', ['month_start'])


def downgrade() -> None:
    op.drop_index('ix_agg_purchases_monthly_month_start', table_name='agg_purchases_monthly')
    op.drop_table('agg_purchases_monthly')

    op.drop_index('ix_agg_purchases_daily_doc_date', table_name='agg_purchases_daily')
    op.drop_table('agg_purchases_daily')
