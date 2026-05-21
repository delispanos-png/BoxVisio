"""tenant item commercial status

Revision ID: 20260519_0031_tenant
Revises: 20260519_0030_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260519_0031_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0030_tenant'
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


def _dim_products_columns(bind) -> str:
    columns = _column_names(bind, 'dim_items')
    optional = [
        'manual_order_category',
        'commercial_status',
        'replenishment_status_1',
        'replenishment_status_2',
        'min_stock',
        'replenishment_moq',
        'vendor_moq',
        'current_purchase_price',
    ]
    return ''.join(f',\n            {name}' for name in optional if name in columns)


def _refresh_dim_products_view() -> None:
    bind = op.get_bind()
    op.execute('DROP VIEW IF EXISTS dim_products')
    op.execute(
        f"""
        CREATE VIEW dim_products AS
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
            created_at{_dim_products_columns(bind)}
        FROM dim_items
        """
    )


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    if 'commercial_status' not in _column_names(bind, 'dim_items'):
        op.add_column('dim_items', sa.Column('commercial_status', sa.String(length=128), nullable=True))
    if 'ix_dim_items_commercial_status' not in _index_names(bind, 'dim_items'):
        op.create_index('ix_dim_items_commercial_status', 'dim_items', ['commercial_status'])
    _refresh_dim_products_view()


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    indexes = _index_names(bind, 'dim_items')
    if 'ix_dim_items_commercial_status' in indexes:
        op.drop_index('ix_dim_items_commercial_status', table_name='dim_items')
    if 'commercial_status' in _column_names(bind, 'dim_items'):
        op.drop_column('dim_items', 'commercial_status')
    _refresh_dim_products_view()
