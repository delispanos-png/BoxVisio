"""control user profile and access duration fields

Revision ID: 20260301_0014_control
Revises: 20260228_0013_control
Create Date: 2026-03-01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260301_0014_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0013_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('full_name', sa.String(length=255), nullable=True))
    op.add_column('users', sa.Column('phone', sa.String(length=64), nullable=True))
    op.add_column('users', sa.Column('access_expires_at', sa.DateTime(), nullable=True))
    op.create_index('ix_users_access_expires_at', 'users', ['access_expires_at'])


def downgrade() -> None:
    op.drop_index('ix_users_access_expires_at', table_name='users')
    op.drop_column('users', 'access_expires_at')
    op.drop_column('users', 'phone')
    op.drop_column('users', 'full_name')
