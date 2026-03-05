"""tenant sync_state table

Revision ID: 20260228_0002_tenant
Revises: 20260228_0001_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0002_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0001_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sync_state',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_type', sa.String(length=64), nullable=False),
        sa.Column('last_sync_timestamp', sa.DateTime(), nullable=True),
        sa.Column('last_sync_id', sa.String(length=128), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('connector_type', name='uq_sync_state_connector_type'),
    )


def downgrade() -> None:
    op.drop_table('sync_state')
