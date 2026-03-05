"""tenant performance indexes on aggregate tables

Revision ID: 20260228_0011_tenant
Revises: 20260228_0009_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op

revision: str = '20260228_0011_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0009_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'ix_agg_sales_daily_filters',
        'agg_sales_daily',
        ['doc_date', 'branch_ext_id', 'warehouse_ext_id', 'brand_ext_id', 'category_ext_id', 'group_ext_id'],
    )
    op.create_index(
        'ix_agg_purchases_daily_filters',
        'agg_purchases_daily',
        [
            'doc_date',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
        ],
    )
    op.create_index(
        'ix_agg_sales_monthly_filters',
        'agg_sales_monthly',
        ['month_start', 'branch_ext_id', 'warehouse_ext_id', 'brand_ext_id', 'category_ext_id', 'group_ext_id'],
    )
    op.create_index(
        'ix_agg_purchases_monthly_filters',
        'agg_purchases_monthly',
        [
            'month_start',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
        ],
    )


def downgrade() -> None:
    op.drop_index('ix_agg_purchases_monthly_filters', table_name='agg_purchases_monthly')
    op.drop_index('ix_agg_sales_monthly_filters', table_name='agg_sales_monthly')
    op.drop_index('ix_agg_purchases_daily_filters', table_name='agg_purchases_daily')
    op.drop_index('ix_agg_sales_daily_filters', table_name='agg_sales_daily')
