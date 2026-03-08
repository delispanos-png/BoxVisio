"""control connector connection_parameters and field mapping

Revision ID: 20260307_0005_control
Revises: 20260307_0004_control
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260307_0005_control'
down_revision: Union[str, Sequence[str], None] = '20260307_0004_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')

    if 'connection_parameters' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('connection_parameters', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        )

    if 'stream_field_mapping' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('stream_field_mapping', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        )

    bind.execute(
        sa.text(
            """
            UPDATE tenant_connections
            SET connection_parameters = json_build_object(
                'connector_type', connector_type,
                'source_type', source_type,
                'host_hint', NULL,
                'database_hint', NULL,
                'username_hint', NULL
            )
            WHERE connection_parameters IS NULL OR connection_parameters::text = '{}'
            """
        )
    )

    op.alter_column('tenant_connections', 'connection_parameters', server_default=None)
    op.alter_column('tenant_connections', 'stream_field_mapping', server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')
    if 'stream_field_mapping' in cols:
        op.drop_column('tenant_connections', 'stream_field_mapping')
    if 'connection_parameters' in cols:
        op.drop_column('tenant_connections', 'connection_parameters')
