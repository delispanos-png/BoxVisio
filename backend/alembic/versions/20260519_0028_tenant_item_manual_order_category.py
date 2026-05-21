"""tenant dim_items manual order category

Revision ID: 20260519_0028_tenant
Revises: 20260518_0027_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260519_0028_tenant'
down_revision: Union[str, Sequence[str], None] = '20260518_0027_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _refresh_dim_products_view(include_manual_order_category: bool) -> None:
    manual_column = ',\n            manual_order_category' if include_manual_order_category else ''
    op.execute(
        f"""
        CREATE OR REPLACE VIEW dim_products AS
        SELECT
            id,
            external_id,
            sku,
            barcode,
            name,
            main_unit,
            vat_rate,
            vat_label,
            use_batch,
            commercial_category,
            category_1,
            category_2,
            category_3,
            model_name,
            business_unit_name,
            unit2,
            purchase_unit,
            sales_unit,
            rel_2_to_1,
            rel_purchase_to_1,
            rel_sale_to_1,
            strict_rel_2_to_1,
            strict_purchase_rel,
            strict_sale_rel,
            abc_category,
            image_url,
            discount_pct,
            is_active_source,
            brand_id,
            category_id,
            group_id,
            updated_at,
            created_at{manual_column}
        FROM dim_items
        """
    )


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    columns = _column_names(bind, 'dim_items')
    if 'manual_order_category' not in columns:
        op.add_column('dim_items', sa.Column('manual_order_category', sa.String(length=128), nullable=True))
    if 'ix_dim_items_manual_order_category' not in _index_names(bind, 'dim_items'):
        op.create_index('ix_dim_items_manual_order_category', 'dim_items', ['manual_order_category'])
    _refresh_dim_products_view(include_manual_order_category=True)


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    _refresh_dim_products_view(include_manual_order_category=False)
    if 'ix_dim_items_manual_order_category' in _index_names(bind, 'dim_items'):
        op.drop_index('ix_dim_items_manual_order_category', table_name='dim_items')
    if 'manual_order_category' in _column_names(bind, 'dim_items'):
        op.drop_column('dim_items', 'manual_order_category')
