"""encrypt sqlserver credentials in tenant_connections

Revision ID: 20260228_0012_control
Revises: 20260228_0010_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0012_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0010_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tenant_connections', sa.Column('connector_type', sa.String(length=64), nullable=True, server_default='pharmacyone_sql'))
    op.add_column('tenant_connections', sa.Column('enc_payload', sa.Text(), nullable=False, server_default=''))
    op.add_column('tenant_connections', sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')))
    op.add_column('tenant_connections', sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')))
    op.add_column('tenant_connections', sa.Column('last_test_ok_at', sa.DateTime(), nullable=True))
    op.add_column('tenant_connections', sa.Column('last_test_error', sa.Text(), nullable=True))

    op.execute(
        """
        UPDATE tenant_connections
        SET connector_type = CASE
            WHEN provider = 'pharmacyone_sqlserver' THEN 'pharmacyone_sql'
            WHEN provider IS NULL OR provider = '' THEN 'pharmacyone_sql'
            ELSE provider
        END
        """
    )
    op.alter_column('tenant_connections', 'connector_type', nullable=False, server_default=None)

    op.drop_column('tenant_connections', 'connection_string')
    op.drop_column('tenant_connections', 'encrypted_config')
    op.drop_column('tenant_connections', 'provider')


def downgrade() -> None:
    op.add_column('tenant_connections', sa.Column('provider', sa.String(length=64), nullable=False, server_default='pharmacyone_sqlserver'))
    op.add_column('tenant_connections', sa.Column('encrypted_config', sa.Text(), nullable=False, server_default=''))
    op.add_column('tenant_connections', sa.Column('connection_string', sa.Text(), nullable=False, server_default=''))

    op.execute(
        """
        UPDATE tenant_connections
        SET provider = CASE
            WHEN connector_type = 'pharmacyone_sql' THEN 'pharmacyone_sqlserver'
            ELSE connector_type
        END
        """
    )

    op.drop_column('tenant_connections', 'last_test_error')
    op.drop_column('tenant_connections', 'last_test_ok_at')
    op.drop_column('tenant_connections', 'updated_at')
    op.drop_column('tenant_connections', 'created_at')
    op.drop_column('tenant_connections', 'enc_payload')
    op.drop_column('tenant_connections', 'connector_type')
