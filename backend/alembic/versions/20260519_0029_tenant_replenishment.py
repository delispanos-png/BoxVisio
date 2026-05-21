"""tenant replenishment snapshots

Revision ID: 20260519_0029_tenant
Revises: 20260519_0028_tenant
Create Date: 2026-05-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision: str = '20260519_0029_tenant'
down_revision: Union[str, Sequence[str], None] = '20260519_0028_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'replenishment_snapshots'):
        op.create_table(
            'replenishment_snapshots',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('source_filename', sa.String(length=255), nullable=True),
            sa.Column('period_label', sa.String(length=64), nullable=True),
            sa.Column('target_stock_weeks', sa.Numeric(10, 4), nullable=False, server_default='4'),
            sa.Column('overstock_weeks', sa.Numeric(10, 4), nullable=False, server_default='12'),
            sa.Column('sales_avg_period_1_weeks', sa.Integer(), nullable=False, server_default='4'),
            sa.Column('sales_avg_period_2_weeks', sa.Integer(), nullable=False, server_default='12'),
            sa.Column('rows_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('issue_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('summary_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('imported_by', sa.String(length=255), nullable=True),
            sa.Column('imported_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    if 'ix_replenishment_snapshots_imported_at' not in _index_names(bind, 'replenishment_snapshots'):
        op.create_index('ix_replenishment_snapshots_imported_at', 'replenishment_snapshots', ['imported_at'])
    if 'ix_replenishment_snapshots_period' not in _index_names(bind, 'replenishment_snapshots'):
        op.create_index('ix_replenishment_snapshots_period', 'replenishment_snapshots', ['period_label'])

    if not _table_exists(bind, 'replenishment_lines'):
        op.create_table(
            'replenishment_lines',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('snapshot_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('replenishment_snapshots.id', ondelete='CASCADE'), nullable=False),
            sa.Column('source_row', sa.Integer(), nullable=False),
            sa.Column('item_code', sa.String(length=128), nullable=False),
            sa.Column('item_name', sa.String(length=255), nullable=True),
            sa.Column('category_1', sa.String(length=255), nullable=True),
            sa.Column('category_2', sa.String(length=255), nullable=True),
            sa.Column('category_3', sa.String(length=255), nullable=True),
            sa.Column('status_1', sa.String(length=64), nullable=True),
            sa.Column('status_2', sa.String(length=128), nullable=True),
            sa.Column('min_stock', sa.Numeric(18, 4), nullable=True),
            sa.Column('repl_moq', sa.Numeric(18, 4), nullable=True),
            sa.Column('vendor_moq', sa.Numeric(18, 4), nullable=True),
            sa.Column('purchase_price', sa.Numeric(14, 4), nullable=True),
            sa.Column('total_need_qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('total_overstock_qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('supplier_order_qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('supplier_order_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('weeks_of_stock_total', sa.Numeric(18, 4), nullable=True),
            sa.Column('store_metrics_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('raw_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.UniqueConstraint('snapshot_id', 'item_code', name='uq_replenishment_lines_snapshot_item'),
        )
    if 'ix_replenishment_lines_snapshot_item' not in _index_names(bind, 'replenishment_lines'):
        op.create_index('ix_replenishment_lines_snapshot_item', 'replenishment_lines', ['snapshot_id', 'item_code'])
    if 'ix_replenishment_lines_categories' not in _index_names(bind, 'replenishment_lines'):
        op.create_index('ix_replenishment_lines_categories', 'replenishment_lines', ['category_1', 'category_2', 'category_3'])
    if 'ix_replenishment_lines_status' not in _index_names(bind, 'replenishment_lines'):
        op.create_index('ix_replenishment_lines_status', 'replenishment_lines', ['status_1', 'status_2'])
    if 'ix_replenishment_lines_supplier_order' not in _index_names(bind, 'replenishment_lines'):
        op.create_index('ix_replenishment_lines_supplier_order', 'replenishment_lines', ['supplier_order_qty'])
    if 'ix_replenishment_lines_item_code' not in _index_names(bind, 'replenishment_lines'):
        op.create_index('ix_replenishment_lines_item_code', 'replenishment_lines', ['item_code'])

    if not _table_exists(bind, 'replenishment_data_quality_issues'):
        op.create_table(
            'replenishment_data_quality_issues',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('snapshot_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('replenishment_snapshots.id', ondelete='CASCADE'), nullable=False),
            sa.Column('severity', sa.String(length=16), nullable=False, server_default='warning'),
            sa.Column('issue_code', sa.String(length=64), nullable=False),
            sa.Column('source_row', sa.Integer(), nullable=True),
            sa.Column('item_code', sa.String(length=128), nullable=True),
            sa.Column('item_name', sa.String(length=255), nullable=True),
            sa.Column('field_name', sa.String(length=128), nullable=True),
            sa.Column('source_cell', sa.String(length=32), nullable=True),
            sa.Column('raw_value', sa.Text(), nullable=True),
            sa.Column('message', sa.Text(), nullable=False),
            sa.Column('metadata_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    if 'ix_replenishment_dq_snapshot_code' not in _index_names(bind, 'replenishment_data_quality_issues'):
        op.create_index('ix_replenishment_dq_snapshot_code', 'replenishment_data_quality_issues', ['snapshot_id', 'issue_code'])
    if 'ix_replenishment_dq_item' not in _index_names(bind, 'replenishment_data_quality_issues'):
        op.create_index('ix_replenishment_dq_item', 'replenishment_data_quality_issues', ['item_code'])


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'replenishment_data_quality_issues'):
        op.drop_table('replenishment_data_quality_issues')
    if _table_exists(bind, 'replenishment_lines'):
        op.drop_table('replenishment_lines')
    if _table_exists(bind, 'replenishment_snapshots'):
        op.drop_table('replenishment_snapshots')
