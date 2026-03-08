"""enrich fact_sales with ERP document metadata and primary payload

Revision ID: 20260306_0020_tenant
Revises: 20260306_0019_tenant
Create Date: 2026-03-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260306_0020_tenant'
down_revision: Union[str, Sequence[str], None] = '20260306_0019_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('fact_sales', sa.Column('document_id', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('document_no', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('document_series', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('document_type', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('document_status', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('eshop_code', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('customer_code', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('customer_name', sa.String(length=255), nullable=True))
    op.add_column('fact_sales', sa.Column('payment_method', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('shipping_method', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('reason', sa.String(length=255), nullable=True))
    op.add_column('fact_sales', sa.Column('origin_ref', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('destination_ref', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('delivery_address', sa.Text(), nullable=True))
    op.add_column('fact_sales', sa.Column('delivery_zip', sa.String(length=32), nullable=True))
    op.add_column('fact_sales', sa.Column('delivery_city', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('delivery_area', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('movement_type', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('carrier_name', sa.String(length=255), nullable=True))
    op.add_column('fact_sales', sa.Column('transport_medium', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('transport_no', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('route_name', sa.String(length=255), nullable=True))
    op.add_column('fact_sales', sa.Column('loading_date', sa.Date(), nullable=True))
    op.add_column('fact_sales', sa.Column('delivery_date', sa.Date(), nullable=True))
    op.add_column('fact_sales', sa.Column('notes', sa.Text(), nullable=True))
    op.add_column('fact_sales', sa.Column('notes_2', sa.Text(), nullable=True))
    op.add_column('fact_sales', sa.Column('source_created_at', sa.DateTime(), nullable=True))
    op.add_column('fact_sales', sa.Column('source_created_by', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('source_updated_at', sa.DateTime(), nullable=True))
    op.add_column('fact_sales', sa.Column('source_updated_by', sa.String(length=128), nullable=True))
    op.add_column('fact_sales', sa.Column('line_no', sa.Integer(), nullable=True))
    op.add_column('fact_sales', sa.Column('qty_executed', sa.Numeric(precision=18, scale=4), nullable=True))
    op.add_column('fact_sales', sa.Column('unit_price', sa.Numeric(precision=14, scale=4), nullable=True))
    op.add_column('fact_sales', sa.Column('discount_pct', sa.Numeric(precision=10, scale=4), nullable=True))
    op.add_column('fact_sales', sa.Column('discount_amount', sa.Numeric(precision=14, scale=2), nullable=True))
    op.add_column('fact_sales', sa.Column('vat_amount', sa.Numeric(precision=14, scale=2), nullable=True))
    op.add_column('fact_sales', sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    op.create_index('ix_fact_sales_document_id_doc_date', 'fact_sales', ['document_id', 'doc_date'])
    op.create_index('ix_fact_sales_document_no', 'fact_sales', ['document_no'])


def downgrade() -> None:
    op.drop_index('ix_fact_sales_document_no', table_name='fact_sales')
    op.drop_index('ix_fact_sales_document_id_doc_date', table_name='fact_sales')

    op.drop_column('fact_sales', 'source_payload_json')
    op.drop_column('fact_sales', 'vat_amount')
    op.drop_column('fact_sales', 'discount_amount')
    op.drop_column('fact_sales', 'discount_pct')
    op.drop_column('fact_sales', 'unit_price')
    op.drop_column('fact_sales', 'qty_executed')
    op.drop_column('fact_sales', 'line_no')
    op.drop_column('fact_sales', 'source_updated_by')
    op.drop_column('fact_sales', 'source_updated_at')
    op.drop_column('fact_sales', 'source_created_by')
    op.drop_column('fact_sales', 'source_created_at')
    op.drop_column('fact_sales', 'notes_2')
    op.drop_column('fact_sales', 'notes')
    op.drop_column('fact_sales', 'delivery_date')
    op.drop_column('fact_sales', 'loading_date')
    op.drop_column('fact_sales', 'route_name')
    op.drop_column('fact_sales', 'transport_no')
    op.drop_column('fact_sales', 'transport_medium')
    op.drop_column('fact_sales', 'carrier_name')
    op.drop_column('fact_sales', 'movement_type')
    op.drop_column('fact_sales', 'delivery_area')
    op.drop_column('fact_sales', 'delivery_city')
    op.drop_column('fact_sales', 'delivery_zip')
    op.drop_column('fact_sales', 'delivery_address')
    op.drop_column('fact_sales', 'destination_ref')
    op.drop_column('fact_sales', 'origin_ref')
    op.drop_column('fact_sales', 'reason')
    op.drop_column('fact_sales', 'shipping_method')
    op.drop_column('fact_sales', 'payment_method')
    op.drop_column('fact_sales', 'customer_name')
    op.drop_column('fact_sales', 'customer_code')
    op.drop_column('fact_sales', 'eshop_code')
    op.drop_column('fact_sales', 'document_status')
    op.drop_column('fact_sales', 'document_type')
    op.drop_column('fact_sales', 'document_series')
    op.drop_column('fact_sales', 'document_no')
    op.drop_column('fact_sales', 'document_id')
