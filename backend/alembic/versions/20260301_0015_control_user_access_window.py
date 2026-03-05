"""control user access window start/end

Revision ID: 20260301_0015_control
Revises: 20260301_0014_control
Create Date: 2026-03-01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260301_0015_control'
down_revision: Union[str, Sequence[str], None] = '20260301_0014_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('access_starts_at', sa.DateTime(), nullable=True))
    op.create_index('ix_users_access_starts_at', 'users', ['access_starts_at'])


def downgrade() -> None:
    op.drop_index('ix_users_access_starts_at', table_name='users')
    op.drop_column('users', 'access_starts_at')
