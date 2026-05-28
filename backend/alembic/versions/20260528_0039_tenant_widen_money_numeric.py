"""widen tenant monetary numeric precision

Revision ID: 20260528_0039_tenant
Revises: 20260522_0037_tenant
Create Date: 2026-05-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = '20260528_0039_tenant'
down_revision: Union[str, Sequence[str], None] = '20260522_0037_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


VIEW_SQL: dict[str, str] = {
    'agg_inventory_snapshot': (
        'CREATE VIEW agg_inventory_snapshot AS '
        'SELECT id, snapshot_date, item_external_id, qty_on_hand, value_amount, updated_at, created_at '
        'FROM agg_inventory_snapshot_daily'
    ),
    'dim_products': (
        'CREATE VIEW dim_products AS '
        'SELECT id, external_id, sku, barcode, name, main_unit, vat_rate, vat_label, use_batch, '
        'commercial_category, category_1, category_2, category_3, model_name, business_unit_name, '
        'unit2, purchase_unit, sales_unit, rel_2_to_1, rel_purchase_to_1, rel_sale_to_1, '
        'strict_rel_2_to_1, strict_purchase_rel, strict_sale_rel, abc_category, image_url, '
        'discount_pct, is_active_source, brand_id, category_id, group_id, updated_at, created_at, '
        'manual_order_category, commercial_status, replenishment_status_1, replenishment_status_2, '
        'min_stock, replenishment_moq, vendor_moq, current_purchase_price FROM dim_items'
    ),
    'fact_cash_transactions': (
        'CREATE VIEW fact_cash_transactions AS '
        'SELECT id, external_id, doc_date, branch_id, entry_type, amount, currency, reference_no, '
        'notes, updated_at, created_at, transaction_id, transaction_date, transaction_type, '
        'subcategory, account_id, counterparty_type, counterparty_id FROM fact_cashflows'
    ),
    'fact_inventory_documents': (
        'CREATE VIEW fact_inventory_documents AS '
        'SELECT id, external_id, doc_date, branch_id, item_id, warehouse_id, qty_on_hand, '
        'qty_reserved, cost_amount, value_amount, updated_at, created_at FROM fact_inventory'
    ),
    'fact_purchase_documents': (
        'CREATE VIEW fact_purchase_documents AS '
        'SELECT id, external_id, event_id, doc_date, branch_id, item_id, supplier_id, warehouse_id, '
        'brand_id, category_id, group_id, branch_ext_id, warehouse_ext_id, supplier_ext_id, '
        'brand_ext_id, category_ext_id, group_ext_id, item_code, qty, net_value, cost_amount, '
        'updated_at, created_at FROM fact_purchases'
    ),
    'fact_sales_documents': (
        'CREATE VIEW fact_sales_documents AS '
        'SELECT id, external_id, event_id, doc_date, branch_id, item_id, warehouse_id, brand_id, '
        'category_id, group_id, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, '
        'group_ext_id, item_code, qty, net_value, gross_value, cost_amount, profit_amount, '
        'updated_at, created_at, document_id, document_no, document_series, document_type, '
        'document_status, eshop_code, customer_code, customer_name, payment_method, shipping_method, '
        'reason, origin_ref, destination_ref, delivery_address, delivery_zip, delivery_city, '
        'delivery_area, movement_type, carrier_name, transport_medium, transport_no, route_name, '
        'loading_date, delivery_date, notes, notes_2, source_created_at, source_created_by, '
        'source_updated_at, source_updated_by, line_no, qty_executed, unit_price, discount_pct, '
        'discount_amount, vat_amount, source_payload_json FROM fact_sales'
    ),
}


def _existing_views(bind) -> list[str]:
    inspector = inspect(bind)
    existing = set(inspector.get_view_names(schema='public'))
    return [name for name in VIEW_SQL if name in existing]


def _drop_views(bind, views: list[str]) -> None:
    for view_name in reversed(views):
        bind.execute(sa.text(f'DROP VIEW IF EXISTS {view_name}'))


def _create_views(bind, views: list[str]) -> None:
    for view_name in views:
        bind.execute(sa.text(VIEW_SQL[view_name]))


def upgrade() -> None:
    bind = op.get_bind()
    views = _existing_views(bind)
    _drop_views(bind, views)
    rows = bind.execute(
        sa.text(
            """
            SELECT table_name, column_name, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND data_type = 'numeric'
              AND numeric_precision = 14
              AND numeric_scale IN (2, 4)
            ORDER BY table_name, column_name
            """
        )
    ).all()
    for table_name, column_name, scale in rows:
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.Numeric(14, int(scale)),
            type_=sa.Numeric(18, int(scale)),
            existing_nullable=True,
        )
    _create_views(bind, views)


def downgrade() -> None:
    bind = op.get_bind()
    views = _existing_views(bind)
    _drop_views(bind, views)
    rows = bind.execute(
        sa.text(
            """
            SELECT table_name, column_name, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND data_type = 'numeric'
              AND numeric_precision = 18
              AND numeric_scale IN (2, 4)
            ORDER BY table_name, column_name
            """
        )
    ).all()
    for table_name, column_name, scale in rows:
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.Numeric(18, int(scale)),
            type_=sa.Numeric(14, int(scale)),
            existing_nullable=True,
        )
    _create_views(bind, views)
