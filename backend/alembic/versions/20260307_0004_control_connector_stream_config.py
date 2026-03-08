"""control connector stream-aware ingestion config

Revision ID: 20260307_0004_control
Revises: 20260307_0003_control
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260307_0004_control'
down_revision: Union[str, Sequence[str], None] = '20260307_0003_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')

    if 'source_type' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('source_type', sa.String(length=32), nullable=False, server_default='sql'),
        )

    if 'supported_streams' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('supported_streams', sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
        )

    if 'enabled_streams' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('enabled_streams', sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
        )

    if 'stream_query_mapping' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('stream_query_mapping', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        )

    if 'stream_file_mapping' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('stream_file_mapping', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        )

    if 'stream_api_endpoint' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('stream_api_endpoint', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        )

    # Backfill source category using existing connector_type values.
    bind.execute(
        sa.text(
            """
            UPDATE tenant_connections
            SET source_type = CASE
                WHEN connector_type ILIKE '%api%' THEN 'api'
                WHEN connector_type ILIKE '%file%' OR connector_type ILIKE '%sftp%' OR connector_type ILIKE '%csv%' OR connector_type ILIKE '%excel%' THEN 'file'
                ELSE 'sql'
            END
            WHERE source_type IS NULL OR source_type = ''
            """
        )
    )

    # Backfill connector supported/enabled stream sets.
    bind.execute(
        sa.text(
            """
            UPDATE tenant_connections
            SET supported_streams = CASE
                WHEN connector_type = 'external_api' THEN '["sales_documents","purchase_documents"]'::json
                ELSE '["sales_documents","purchase_documents","inventory_documents","cash_transactions","supplier_balances","customer_balances"]'::json
            END
            WHERE supported_streams IS NULL OR supported_streams::text = '[]'
            """
        )
    )

    bind.execute(
        sa.text(
            """
            UPDATE tenant_connections
            SET enabled_streams = supported_streams
            WHERE enabled_streams IS NULL OR enabled_streams::text = '[]'
            """
        )
    )

    # Backfill stream->query mapping from legacy query columns.
    bind.execute(
        sa.text(
            """
            UPDATE tenant_connections
            SET stream_query_mapping = json_build_object(
                'sales_documents', COALESCE(NULLIF(sales_query_template, ''), ''),
                'purchase_documents', COALESCE(NULLIF(purchases_query_template, ''), ''),
                'inventory_documents', COALESCE(NULLIF(inventory_query_template, ''), ''),
                'cash_transactions', COALESCE(NULLIF(cashflow_query_template, ''), ''),
                'supplier_balances', COALESCE(NULLIF(supplier_balances_query_template, ''), ''),
                'customer_balances', COALESCE(NULLIF(customer_balances_query_template, ''), '')
            )
            WHERE stream_query_mapping IS NULL OR stream_query_mapping::text = '{}'
            """
        )
    )

    op.alter_column('tenant_connections', 'source_type', server_default=None)
    op.alter_column('tenant_connections', 'supported_streams', server_default=None)
    op.alter_column('tenant_connections', 'enabled_streams', server_default=None)
    op.alter_column('tenant_connections', 'stream_query_mapping', server_default=None)
    op.alter_column('tenant_connections', 'stream_file_mapping', server_default=None)
    op.alter_column('tenant_connections', 'stream_api_endpoint', server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')

    if 'stream_api_endpoint' in cols:
        op.drop_column('tenant_connections', 'stream_api_endpoint')
    if 'stream_file_mapping' in cols:
        op.drop_column('tenant_connections', 'stream_file_mapping')
    if 'stream_query_mapping' in cols:
        op.drop_column('tenant_connections', 'stream_query_mapping')
    if 'enabled_streams' in cols:
        op.drop_column('tenant_connections', 'enabled_streams')
    if 'supported_streams' in cols:
        op.drop_column('tenant_connections', 'supported_streams')
    if 'source_type' in cols:
        op.drop_column('tenant_connections', 'source_type')
