"""extend dim_items with item master fields used by item card

Revision ID: 20260306_0017_tenant
Revises: 20260304_0016_tenant
Create Date: 2026-03-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260306_0017_tenant'
down_revision: Union[str, Sequence[str], None] = '20260304_0016_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('dim_items', sa.Column('barcode', sa.String(length=128), nullable=True))
    op.add_column('dim_items', sa.Column('main_unit', sa.String(length=64), nullable=True))
    op.add_column('dim_items', sa.Column('vat_rate', sa.Numeric(6, 2), nullable=True))
    op.add_column('dim_items', sa.Column('vat_label', sa.String(length=64), nullable=True))
    op.add_column('dim_items', sa.Column('use_batch', sa.Boolean(), nullable=True))
    op.add_column('dim_items', sa.Column('commercial_category', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('category_1', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('category_2', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('category_3', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('model_name', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('business_unit_name', sa.String(length=255), nullable=True))
    op.add_column('dim_items', sa.Column('unit2', sa.String(length=64), nullable=True))
    op.add_column('dim_items', sa.Column('purchase_unit', sa.String(length=64), nullable=True))
    op.add_column('dim_items', sa.Column('sales_unit', sa.String(length=64), nullable=True))
    op.add_column('dim_items', sa.Column('rel_2_to_1', sa.Numeric(18, 6), nullable=True))
    op.add_column('dim_items', sa.Column('rel_purchase_to_1', sa.Numeric(18, 6), nullable=True))
    op.add_column('dim_items', sa.Column('rel_sale_to_1', sa.Numeric(18, 6), nullable=True))
    op.add_column('dim_items', sa.Column('strict_rel_2_to_1', sa.Boolean(), nullable=True))
    op.add_column('dim_items', sa.Column('strict_purchase_rel', sa.Boolean(), nullable=True))
    op.add_column('dim_items', sa.Column('strict_sale_rel', sa.Boolean(), nullable=True))
    op.add_column('dim_items', sa.Column('discount_pct', sa.Numeric(6, 2), nullable=True))
    op.add_column('dim_items', sa.Column('is_active_source', sa.Boolean(), nullable=True))
    op.create_index('ix_dim_items_barcode', 'dim_items', ['barcode'])


def downgrade() -> None:
    op.drop_index('ix_dim_items_barcode', table_name='dim_items')
    op.drop_column('dim_items', 'is_active_source')
    op.drop_column('dim_items', 'discount_pct')
    op.drop_column('dim_items', 'strict_sale_rel')
    op.drop_column('dim_items', 'strict_purchase_rel')
    op.drop_column('dim_items', 'strict_rel_2_to_1')
    op.drop_column('dim_items', 'rel_sale_to_1')
    op.drop_column('dim_items', 'rel_purchase_to_1')
    op.drop_column('dim_items', 'rel_2_to_1')
    op.drop_column('dim_items', 'sales_unit')
    op.drop_column('dim_items', 'purchase_unit')
    op.drop_column('dim_items', 'unit2')
    op.drop_column('dim_items', 'business_unit_name')
    op.drop_column('dim_items', 'model_name')
    op.drop_column('dim_items', 'category_3')
    op.drop_column('dim_items', 'category_2')
    op.drop_column('dim_items', 'category_1')
    op.drop_column('dim_items', 'commercial_category')
    op.drop_column('dim_items', 'use_batch')
    op.drop_column('dim_items', 'vat_label')
    op.drop_column('dim_items', 'vat_rate')
    op.drop_column('dim_items', 'main_unit')
    op.drop_column('dim_items', 'barcode')
