"""tenant replenishment softone fields

Revision ID: 20260519_0030_tenant
Revises: 20260519_0029_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260519_0030_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0029_tenant'
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


def _add_column_if_missing(table: str, name: str, column: sa.Column) -> None:
    if name not in _column_names(op.get_bind(), table):
        op.add_column(table, column)


def _refresh_dim_products_view(include_fields: bool) -> None:
    extra = ''
    if include_fields:
        extra = """,
            manual_order_category,
            replenishment_status_1,
            replenishment_status_2,
            min_stock,
            replenishment_moq,
            vendor_moq,
            current_purchase_price"""
    else:
        extra = ',\n            manual_order_category' if 'manual_order_category' in _column_names(op.get_bind(), 'dim_items') else ''
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
            created_at{extra}
        FROM dim_items
        """
    )


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'dim_items'):
        _add_column_if_missing('dim_items', 'replenishment_status_1', sa.Column('replenishment_status_1', sa.String(length=64), nullable=True))
        _add_column_if_missing('dim_items', 'replenishment_status_2', sa.Column('replenishment_status_2', sa.String(length=128), nullable=True))
        _add_column_if_missing('dim_items', 'min_stock', sa.Column('min_stock', sa.Numeric(18, 4), nullable=True))
        _add_column_if_missing('dim_items', 'replenishment_moq', sa.Column('replenishment_moq', sa.Numeric(18, 4), nullable=True))
        _add_column_if_missing('dim_items', 'vendor_moq', sa.Column('vendor_moq', sa.Numeric(18, 4), nullable=True))
        _add_column_if_missing('dim_items', 'current_purchase_price', sa.Column('current_purchase_price', sa.Numeric(14, 4), nullable=True))
        indexes = _index_names(bind, 'dim_items')
        if 'ix_dim_items_replenishment_status_1' not in indexes:
            op.create_index('ix_dim_items_replenishment_status_1', 'dim_items', ['replenishment_status_1'])
        if 'ix_dim_items_replenishment_status_2' not in indexes:
            op.create_index('ix_dim_items_replenishment_status_2', 'dim_items', ['replenishment_status_2'])
        _refresh_dim_products_view(include_fields=True)

    if _table_exists(bind, 'fact_inventory'):
        _add_column_if_missing('fact_inventory', 'qty_expected', sa.Column('qty_expected', sa.Numeric(18, 4), nullable=False, server_default='0'))
        _add_column_if_missing('fact_inventory', 'qty_available', sa.Column('qty_available', sa.Numeric(18, 4), nullable=False, server_default='0'))


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'dim_items'):
        _refresh_dim_products_view(include_fields=False)
        indexes = _index_names(bind, 'dim_items')
        if 'ix_dim_items_replenishment_status_2' in indexes:
            op.drop_index('ix_dim_items_replenishment_status_2', table_name='dim_items')
        if 'ix_dim_items_replenishment_status_1' in indexes:
            op.drop_index('ix_dim_items_replenishment_status_1', table_name='dim_items')
        for col in (
            'current_purchase_price',
            'vendor_moq',
            'replenishment_moq',
            'min_stock',
            'replenishment_status_2',
            'replenishment_status_1',
        ):
            if col in _column_names(bind, 'dim_items'):
                op.drop_column('dim_items', col)
    if _table_exists(bind, 'fact_inventory'):
        for col in ('qty_available', 'qty_expected'):
            if col in _column_names(bind, 'fact_inventory'):
                op.drop_column('fact_inventory', col)
