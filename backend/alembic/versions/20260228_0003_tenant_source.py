"""tenant source column for feature gating

Revision ID: 20260228_0003_control
Revises: 20260228_0002_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0003_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0002_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tenants', sa.Column('source', sa.String(length=32), nullable=False, server_default='external'))


def downgrade() -> None:
    op.drop_column('tenants', 'source')
