"""tenant dashboard performance indexes

Revision ID: 20260307_0003_tenant
Revises: 20260307_0002_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect

revision: str = '20260307_0003_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0002_tenant'
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

    if _table_exists(bind, 'agg_sales_daily'):
        idx = _index_names(bind, 'agg_sales_daily')
        if 'ix_agg_sales_daily_date_branch_category' not in idx:
            op.create_index(
                'ix_agg_sales_daily_date_branch_category',
                'agg_sales_daily',
                ['doc_date', 'branch_ext_id', 'category_ext_id'],
            )

    if _table_exists(bind, 'agg_sales_monthly'):
        idx = _index_names(bind, 'agg_sales_monthly')
        if 'ix_agg_sales_monthly_date_branch_category' not in idx:
            op.create_index(
                'ix_agg_sales_monthly_date_branch_category',
                'agg_sales_monthly',
                ['month_start', 'branch_ext_id', 'category_ext_id'],
            )

    if _table_exists(bind, 'agg_purchases_daily'):
        idx = _index_names(bind, 'agg_purchases_daily')
        if 'ix_agg_purchases_daily_date_branch_category_supplier' not in idx:
            op.create_index(
                'ix_agg_purchases_daily_date_branch_category_supplier',
                'agg_purchases_daily',
                ['doc_date', 'branch_ext_id', 'category_ext_id', 'supplier_ext_id'],
            )

    if _table_exists(bind, 'agg_purchases_monthly'):
        idx = _index_names(bind, 'agg_purchases_monthly')
        if 'ix_agg_purchases_monthly_date_branch_category_supplier' not in idx:
            op.create_index(
                'ix_agg_purchases_monthly_date_branch_category_supplier',
                'agg_purchases_monthly',
                ['month_start', 'branch_ext_id', 'category_ext_id', 'supplier_ext_id'],
            )


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'agg_purchases_monthly'):
        idx = _index_names(bind, 'agg_purchases_monthly')
        if 'ix_agg_purchases_monthly_date_branch_category_supplier' in idx:
            op.drop_index('ix_agg_purchases_monthly_date_branch_category_supplier', table_name='agg_purchases_monthly')

    if _table_exists(bind, 'agg_purchases_daily'):
        idx = _index_names(bind, 'agg_purchases_daily')
        if 'ix_agg_purchases_daily_date_branch_category_supplier' in idx:
            op.drop_index('ix_agg_purchases_daily_date_branch_category_supplier', table_name='agg_purchases_daily')

    if _table_exists(bind, 'agg_sales_monthly'):
        idx = _index_names(bind, 'agg_sales_monthly')
        if 'ix_agg_sales_monthly_date_branch_category' in idx:
            op.drop_index('ix_agg_sales_monthly_date_branch_category', table_name='agg_sales_monthly')

    if _table_exists(bind, 'agg_sales_daily'):
        idx = _index_names(bind, 'agg_sales_daily')
        if 'ix_agg_sales_daily_date_branch_category' in idx:
            op.drop_index('ix_agg_sales_daily_date_branch_category', table_name='agg_sales_daily')
