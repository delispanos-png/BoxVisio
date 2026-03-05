"""tenant inventory aggregate tables for intelligence v1

Revision ID: 20260228_0014_tenant
Revises: 20260228_0013_tenant
Create Date: 2026-02-28 21:35:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260228_0014_tenant'
down_revision = '20260228_0013_tenant'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'agg_sales_item_daily',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('item_external_id', sa.String(length=128), nullable=False),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default=sa.text('0')),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default=sa.text('0')),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default=sa.text('0')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('doc_date', 'item_external_id', name='uq_agg_sales_item_daily_item'),
    )
    op.create_index('ix_agg_sales_item_daily_doc_date', 'agg_sales_item_daily', ['doc_date'])
    op.create_index('ix_agg_sales_item_daily_item_external_id', 'agg_sales_item_daily', ['item_external_id'])
    op.create_index(
        'ix_agg_sales_item_daily_doc_item',
        'agg_sales_item_daily',
        ['doc_date', 'item_external_id'],
    )

    op.create_table(
        'agg_inventory_snapshot_daily',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('item_external_id', sa.String(length=128), nullable=False),
        sa.Column('qty_on_hand', sa.Numeric(18, 4), nullable=False, server_default=sa.text('0')),
        sa.Column('value_amount', sa.Numeric(14, 2), nullable=False, server_default=sa.text('0')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('snapshot_date', 'item_external_id', name='uq_agg_inventory_snapshot_daily_item'),
    )
    op.create_index('ix_agg_inventory_snapshot_daily_snapshot_date', 'agg_inventory_snapshot_daily', ['snapshot_date'])
    op.create_index('ix_agg_inventory_snapshot_daily_item_external_id', 'agg_inventory_snapshot_daily', ['item_external_id'])
    op.create_index(
        'ix_agg_inventory_snapshot_daily_date_item',
        'agg_inventory_snapshot_daily',
        ['snapshot_date', 'item_external_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_agg_inventory_snapshot_daily_date_item', table_name='agg_inventory_snapshot_daily')
    op.drop_index('ix_agg_inventory_snapshot_daily_item_external_id', table_name='agg_inventory_snapshot_daily')
    op.drop_index('ix_agg_inventory_snapshot_daily_snapshot_date', table_name='agg_inventory_snapshot_daily')
    op.drop_table('agg_inventory_snapshot_daily')

    op.drop_index('ix_agg_sales_item_daily_doc_item', table_name='agg_sales_item_daily')
    op.drop_index('ix_agg_sales_item_daily_item_external_id', table_name='agg_sales_item_daily')
    op.drop_index('ix_agg_sales_item_daily_doc_date', table_name='agg_sales_item_daily')
    op.drop_table('agg_sales_item_daily')
