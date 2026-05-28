"""tenant market import tables for eRA and IQVIA

Revision ID: 20260522_0036_tenant
Revises: 20260520_0035_tenant
Create Date: 2026-05-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision: str = '20260522_0036_tenant'
down_revision: Union[str, Sequence[str], None] = '20260520_0035_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _create_index(bind, name: str, table: str, cols: list[str]) -> None:
    if name not in _index_names(bind, table):
        op.create_index(name, table, cols)


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'era_exploration_snapshots'):
        op.create_table(
            'era_exploration_snapshots',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('source_filename', sa.String(length=255), nullable=True),
            sa.Column('period_label', sa.String(length=64), nullable=True),
            sa.Column('rows_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('summary_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('imported_by', sa.String(length=255), nullable=True),
            sa.Column('imported_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    _create_index(bind, 'ix_era_exploration_snapshots_imported_at', 'era_exploration_snapshots', ['imported_at'])
    _create_index(bind, 'ix_era_exploration_snapshots_period', 'era_exploration_snapshots', ['period_label'])

    if not _table_exists(bind, 'era_exploration_lines'):
        op.create_table(
            'era_exploration_lines',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('snapshot_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('era_exploration_snapshots.id', ondelete='CASCADE'), nullable=False),
            sa.Column('source_row', sa.Integer(), nullable=False),
            sa.Column('brand', sa.String(length=255), nullable=True),
            sa.Column('product_name', sa.String(length=500), nullable=True),
            sa.Column('barcode', sa.Text(), nullable=True),
            sa.Column('primary_barcode', sa.String(length=64), nullable=True),
            sa.Column('barcodes_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
            sa.Column('category', sa.String(length=255), nullable=True),
            sa.Column('market_sales', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('your_sales', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('your_sales_value_ms', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('market_units', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('your_units', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('your_units_ms', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('gap_sales', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('gap_units', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('raw_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    _create_index(bind, 'ix_era_exploration_lines_snapshot_brand', 'era_exploration_lines', ['snapshot_id', 'brand'])
    _create_index(bind, 'ix_era_exploration_lines_snapshot_category', 'era_exploration_lines', ['snapshot_id', 'category'])
    _create_index(bind, 'ix_era_exploration_lines_primary_barcode', 'era_exploration_lines', ['primary_barcode'])
    _create_index(bind, 'ix_era_exploration_lines_market_sales', 'era_exploration_lines', ['snapshot_id', 'market_sales'])

    if not _table_exists(bind, 'iqvia_snapshots'):
        op.create_table(
            'iqvia_snapshots',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('source_filename', sa.String(length=255), nullable=True),
            sa.Column('period_label', sa.String(length=64), nullable=True),
            sa.Column('rows_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('summary_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('imported_by', sa.String(length=255), nullable=True),
            sa.Column('imported_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    _create_index(bind, 'ix_iqvia_snapshots_imported_at', 'iqvia_snapshots', ['imported_at'])
    _create_index(bind, 'ix_iqvia_snapshots_period', 'iqvia_snapshots', ['period_label'])

    if not _table_exists(bind, 'iqvia_lines'):
        op.create_table(
            'iqvia_lines',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('snapshot_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('iqvia_snapshots.id', ondelete='CASCADE'), nullable=False),
            sa.Column('source_row', sa.Integer(), nullable=False),
            sa.Column('category', sa.String(length=255), nullable=True),
            sa.Column('atc3', sa.String(length=255), nullable=True),
            sa.Column('otc3', sa.String(length=255), nullable=True),
            sa.Column('corporation', sa.String(length=255), nullable=True),
            sa.Column('manufacturer', sa.String(length=255), nullable=True),
            sa.Column('product', sa.String(length=500), nullable=True),
            sa.Column('pack', sa.String(length=500), nullable=True),
            sa.Column('product_label', sa.String(length=1000), nullable=True),
            sa.Column('area_code', sa.String(length=64), nullable=True),
            sa.Column('area_name', sa.String(length=255), nullable=True),
            sa.Column('territory_code', sa.String(length=64), nullable=True),
            sa.Column('territory_name', sa.String(length=255), nullable=True),
            sa.Column('units', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('values', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('avg_price', sa.Numeric(18, 4), nullable=False, server_default='0'),
            sa.Column('raw_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        )
    _create_index(bind, 'ix_iqvia_lines_snapshot_category', 'iqvia_lines', ['snapshot_id', 'category'])
    _create_index(bind, 'ix_iqvia_lines_snapshot_manufacturer', 'iqvia_lines', ['snapshot_id', 'manufacturer'])
    _create_index(bind, 'ix_iqvia_lines_snapshot_territory', 'iqvia_lines', ['snapshot_id', 'territory_name'])
    _create_index(bind, 'ix_iqvia_lines_snapshot_atc3', 'iqvia_lines', ['snapshot_id', 'atc3'])
    _create_index(bind, 'ix_iqvia_lines_snapshot_otc3', 'iqvia_lines', ['snapshot_id', 'otc3'])
    _create_index(bind, 'ix_iqvia_lines_values', 'iqvia_lines', ['snapshot_id', 'values'])


def downgrade() -> None:
    bind = op.get_bind()
    for table in ('iqvia_lines', 'iqvia_snapshots', 'era_exploration_lines', 'era_exploration_snapshots'):
        if _table_exists(bind, table):
            op.drop_table(table)
