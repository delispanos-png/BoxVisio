"""tenant ingestion engine dead letter table

Revision ID: 20260228_0008_tenant
Revises: 20260228_0005_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0008_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0005_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'ingest_dead_letter',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_type', sa.String(length=64), nullable=False),
        sa.Column('entity', sa.String(length=32), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=True),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('error_message', sa.String(length=1024), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_ingest_dead_letter_event_id', 'ingest_dead_letter', ['event_id'])
    op.create_index(
        'ix_ingest_dead_letter_connector_entity',
        'ingest_dead_letter',
        ['connector_type', 'entity'],
    )
    op.create_index('ix_ingest_dead_letter_created_at', 'ingest_dead_letter', ['created_at'])


def downgrade() -> None:
    op.drop_index('ix_ingest_dead_letter_created_at', table_name='ingest_dead_letter')
    op.drop_index('ix_ingest_dead_letter_connector_entity', table_name='ingest_dead_letter')
    op.drop_index('ix_ingest_dead_letter_event_id', table_name='ingest_dead_letter')
    op.drop_table('ingest_dead_letter')
