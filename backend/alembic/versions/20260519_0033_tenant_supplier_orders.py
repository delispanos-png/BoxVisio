"""tenant supplier orders fact

Revision ID: 20260519_0033_tenant
Revises: 20260519_0032_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision: str = '20260519_0033_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0032_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    return inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if _table_exists('fact_supplier_orders'):
        return
    op.create_table(
        'fact_supplier_orders',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_name', sa.String(length=255), nullable=True),
        sa.Column('supplier_afm', sa.String(length=64), nullable=True),
        sa.Column('item_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('item_name', sa.String(length=512), nullable=True),
        sa.Column('document_id', sa.String(length=128), nullable=True),
        sa.Column('document_no', sa.String(length=128), nullable=True),
        sa.Column('document_series', sa.String(length=128), nullable=True),
        sa.Column('document_series_name', sa.String(length=255), nullable=True),
        sa.Column('document_behavior_code', sa.Integer(), nullable=True),
        sa.Column('order_qty', sa.Numeric(18, 4), nullable=True),
        sa.Column('covered_qty', sa.Numeric(18, 4), nullable=True),
        sa.Column('cancelled_qty', sa.Numeric(18, 4), nullable=True),
        sa.Column('line_value', sa.Numeric(14, 2), nullable=True),
        sa.Column('has_transformation', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('order_status', sa.String(length=32), server_default='open', nullable=False),
        sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('source_connector_id', sa.String(length=64), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['branch_id'], ['dim_branches.id']),
        sa.ForeignKeyConstraint(['item_id'], ['dim_items.id']),
        sa.ForeignKeyConstraint(['supplier_id'], ['dim_suppliers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('external_id', name='uq_fact_supplier_orders_external_id'),
    )
    op.create_index('ix_fact_supplier_orders_doc_date', 'fact_supplier_orders', ['doc_date'])
    op.create_index('ix_fact_supplier_orders_doc_date_supplier_id', 'fact_supplier_orders', ['doc_date', 'supplier_id'])
    op.create_index('ix_fact_supplier_orders_doc_date_branch_id', 'fact_supplier_orders', ['doc_date', 'branch_id'])
    op.create_index('ix_fact_supplier_orders_document_id', 'fact_supplier_orders', ['document_id'])
    op.create_index('ix_fact_supplier_orders_supplier_ext_id', 'fact_supplier_orders', ['supplier_ext_id'])
    op.create_index('ix_fact_supplier_orders_item_code', 'fact_supplier_orders', ['item_code'])
    op.create_index('ix_fact_supplier_orders_order_status', 'fact_supplier_orders', ['order_status'])
    op.create_index('ix_fact_supplier_orders_updated_at', 'fact_supplier_orders', ['updated_at'])


def downgrade() -> None:
    if not _table_exists('fact_supplier_orders'):
        return
    op.drop_index('ix_fact_supplier_orders_updated_at', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_order_status', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_item_code', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_supplier_ext_id', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_document_id', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_doc_date_branch_id', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_doc_date_supplier_id', table_name='fact_supplier_orders')
    op.drop_index('ix_fact_supplier_orders_doc_date', table_name='fact_supplier_orders')
    op.drop_table('fact_supplier_orders')
