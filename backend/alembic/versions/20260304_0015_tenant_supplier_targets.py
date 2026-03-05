"""tenant supplier targets module

Revision ID: 20260304_0015_tenant
Revises: 20260228_0014_tenant
Create Date: 2026-03-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260304_0015_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0014_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'supplier_targets',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False, server_default='Default Target'),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=False),
        sa.Column('supplier_name', sa.String(length=255), nullable=True),
        sa.Column('target_year', sa.Integer(), nullable=False),
        sa.Column('target_amount', sa.Numeric(14, 2), nullable=False, server_default=sa.text('0')),
        sa.Column('rebate_percent', sa.Numeric(8, 4), nullable=False, server_default=sa.text('0')),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('supplier_ext_id', 'target_year', 'name', name='uq_supplier_targets_supplier_year_name'),
    )
    op.create_index('ix_supplier_targets_target_year', 'supplier_targets', ['target_year'])
    op.create_index('ix_supplier_targets_supplier_ext_id', 'supplier_targets', ['supplier_ext_id'])
    op.create_index('ix_supplier_targets_is_active', 'supplier_targets', ['is_active'])

    op.create_table(
        'supplier_target_items',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('supplier_target_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('item_external_id', sa.String(length=128), nullable=False),
        sa.Column('item_name', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['supplier_target_id'], ['supplier_targets.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('supplier_target_id', 'item_external_id', name='uq_supplier_target_items_target_item'),
    )
    op.create_index('ix_supplier_target_items_target_id', 'supplier_target_items', ['supplier_target_id'])
    op.create_index('ix_supplier_target_items_item_external_id', 'supplier_target_items', ['item_external_id'])


def downgrade() -> None:
    op.drop_index('ix_supplier_target_items_item_external_id', table_name='supplier_target_items')
    op.drop_index('ix_supplier_target_items_target_id', table_name='supplier_target_items')
    op.drop_table('supplier_target_items')

    op.drop_index('ix_supplier_targets_is_active', table_name='supplier_targets')
    op.drop_index('ix_supplier_targets_supplier_ext_id', table_name='supplier_targets')
    op.drop_index('ix_supplier_targets_target_year', table_name='supplier_targets')
    op.drop_table('supplier_targets')
