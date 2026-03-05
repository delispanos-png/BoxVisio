"""tenant init

Revision ID: 20260228_0001_tenant
Revises:
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0001_tenant'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'dim_branches',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_warehouses',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_brands',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_categories',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('level', sa.Integer(), nullable=False, server_default='1'),
    )
    op.create_table(
        'fact_sales',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Float(), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.UniqueConstraint('event_id', name='uq_sales_event_id'),
    )
    op.create_index('ix_fact_sales_doc_date', 'fact_sales', ['doc_date'])

    op.create_table(
        'fact_purchases',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Float(), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.UniqueConstraint('event_id', name='uq_purchases_event_id'),
    )
    op.create_index('ix_fact_purchases_doc_date', 'fact_purchases', ['doc_date'])

    op.create_table(
        'staging_ingest_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('entity', sa.String(length=64), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('received_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('event_id', 'entity', name='uq_staging_event_entity'),
    )


def downgrade() -> None:
    op.drop_table('staging_ingest_events')
    op.drop_index('ix_fact_purchases_doc_date', table_name='fact_purchases')
    op.drop_table('fact_purchases')
    op.drop_index('ix_fact_sales_doc_date', table_name='fact_sales')
    op.drop_table('fact_sales')
    op.drop_table('dim_categories')
    op.drop_table('dim_brands')
    op.drop_table('dim_warehouses')
    op.drop_table('dim_branches')
