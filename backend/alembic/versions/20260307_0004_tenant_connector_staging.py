"""tenant stream staging tables for universal connector framework

Revision ID: 20260307_0004_tenant
Revises: 20260307_0003_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0004_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0003_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _create_stream_staging_table(table_name: str, default_stream: str) -> None:
    op.create_table(
        table_name,
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_type', sa.String(length=64), nullable=False),
        sa.Column('stream', sa.String(length=64), nullable=False, server_default=default_stream),
        sa.Column('event_id', sa.String(length=128), nullable=True),
        sa.Column('external_id', sa.String(length=128), nullable=True),
        sa.Column('doc_date', sa.Date(), nullable=True),
        sa.Column('transform_status', sa.String(length=16), nullable=False, server_default='loaded'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column('ingested_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('processed_at', sa.DateTime(), nullable=True),
    )
    op.alter_column(table_name, 'stream', server_default=None)
    op.alter_column(table_name, 'transform_status', server_default=None)
    op.alter_column(table_name, 'source_payload_json', server_default=None)
    op.alter_column(table_name, 'ingested_at', server_default=None)


def _ensure_stream_staging_indexes(bind, table_name: str) -> None:
    idx = _index_names(bind, table_name)
    suffix = table_name.replace('stg_', '')
    idx_ingested = f'ix_{table_name}_ingested_at'
    idx_conn_ext = f'ix_{table_name}_connector_external'
    idx_status = f'ix_{table_name}_status'
    idx_doc_date = f'ix_{table_name}_doc_date'

    if idx_ingested not in idx:
        op.create_index(idx_ingested, table_name, ['ingested_at'])
    if idx_conn_ext not in idx:
        op.create_index(idx_conn_ext, table_name, ['connector_type', 'external_id'])
    if idx_status not in idx:
        op.create_index(idx_status, table_name, ['transform_status'])
    if idx_doc_date not in idx:
        op.create_index(idx_doc_date, table_name, ['doc_date'])


def upgrade() -> None:
    bind = op.get_bind()
    tables = [
        ('stg_sales_documents', 'sales_documents'),
        ('stg_purchase_documents', 'purchase_documents'),
        ('stg_inventory_documents', 'inventory_documents'),
        ('stg_cash_transactions', 'cash_transactions'),
        ('stg_supplier_balances', 'supplier_balances'),
        ('stg_customer_balances', 'customer_balances'),
    ]

    for table_name, default_stream in tables:
        if not _table_exists(bind, table_name):
            _create_stream_staging_table(table_name, default_stream)
        _ensure_stream_staging_indexes(bind, table_name)


def downgrade() -> None:
    bind = op.get_bind()
    tables = [
        'stg_customer_balances',
        'stg_supplier_balances',
        'stg_cash_transactions',
        'stg_inventory_documents',
        'stg_purchase_documents',
        'stg_sales_documents',
    ]
    for table_name in tables:
        if not _table_exists(bind, table_name):
            continue
        idx = _index_names(bind, table_name)
        for idx_name in (
            f'ix_{table_name}_doc_date',
            f'ix_{table_name}_status',
            f'ix_{table_name}_connector_external',
            f'ix_{table_name}_ingested_at',
        ):
            if idx_name in idx:
                op.drop_index(idx_name, table_name=table_name)
        op.drop_table(table_name)
