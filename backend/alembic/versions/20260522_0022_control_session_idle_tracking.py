"""control session idle tracking

Revision ID: 20260522_0022_control
Revises: 20260421_0021_control
Create Date: 2026-05-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260522_0022_control'
down_revision: Union[str, Sequence[str], None] = '20260421_0021_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('refresh_tokens', sa.Column('last_seen_at', sa.DateTime(), nullable=True))
    op.add_column('refresh_tokens', sa.Column('last_seen_path', sa.String(length=255), nullable=True))
    op.add_column('refresh_tokens', sa.Column('last_seen_ip', sa.String(length=64), nullable=True))
    op.add_column('refresh_tokens', sa.Column('last_seen_user_agent', sa.String(length=255), nullable=True))
    op.execute("UPDATE refresh_tokens SET last_seen_at = created_at WHERE last_seen_at IS NULL")
    op.create_index('ix_refresh_tokens_last_seen_at', 'refresh_tokens', ['last_seen_at'])
    op.create_index('ix_refresh_tokens_idle_scan', 'refresh_tokens', ['revoked_at', 'expires_at', 'last_seen_at'])


def downgrade() -> None:
    op.drop_index('ix_refresh_tokens_idle_scan', table_name='refresh_tokens')
    op.drop_index('ix_refresh_tokens_last_seen_at', table_name='refresh_tokens')
    op.drop_column('refresh_tokens', 'last_seen_user_agent')
    op.drop_column('refresh_tokens', 'last_seen_ip')
    op.drop_column('refresh_tokens', 'last_seen_path')
    op.drop_column('refresh_tokens', 'last_seen_at')
