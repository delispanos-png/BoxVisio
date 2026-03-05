"""tenant intelligence insights tables

Revision ID: 20260228_0012_tenant
Revises: 20260228_0011_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0012_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0011_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'insight_rules',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('rule_key', sa.String(length=64), nullable=False),
        sa.Column('description', sa.String(length=255), nullable=False),
        sa.Column('threshold_value', sa.Numeric(14, 4), nullable=False, server_default='0'),
        sa.Column('severity_default', sa.String(length=16), nullable=False, server_default='medium'),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('rule_key', name='uq_insight_rules_rule_key'),
    )
    op.create_index('ix_insight_rules_enabled', 'insight_rules', ['enabled'])

    op.create_table(
        'insights',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('insight_type', sa.String(length=64), nullable=False),
        sa.Column('severity', sa.String(length=16), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('entity_type', sa.String(length=32), nullable=True),
        sa.Column('entity_id', sa.String(length=128), nullable=True),
        sa.Column('metric_value', sa.Numeric(14, 4), nullable=True),
        sa.Column('baseline_value', sa.Numeric(14, 4), nullable=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='open'),
        sa.Column('detected_on', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_insights_created_at', 'insights', ['created_at'])
    op.create_index('ix_insights_type_severity', 'insights', ['insight_type', 'severity'])
    op.create_index('ix_insights_entity', 'insights', ['entity_type', 'entity_id'])
    op.create_index('ix_insights_status', 'insights', ['status'])
    op.create_index('ix_insights_detected_on', 'insights', ['detected_on'])

    op.create_table(
        'insight_history',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('insight_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('insights.id', ondelete='CASCADE'), nullable=False),
        sa.Column('event_type', sa.String(length=32), nullable=False, server_default='generated'),
        sa.Column('note', sa.Text(), nullable=True),
        sa.Column('metric_value', sa.Numeric(14, 4), nullable=True),
        sa.Column('baseline_value', sa.Numeric(14, 4), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_insight_history_insight_id', 'insight_history', ['insight_id'])
    op.create_index('ix_insight_history_created_at', 'insight_history', ['created_at'])


def downgrade() -> None:
    op.drop_index('ix_insight_history_created_at', table_name='insight_history')
    op.drop_index('ix_insight_history_insight_id', table_name='insight_history')
    op.drop_table('insight_history')

    op.drop_index('ix_insights_detected_on', table_name='insights')
    op.drop_index('ix_insights_status', table_name='insights')
    op.drop_index('ix_insights_entity', table_name='insights')
    op.drop_index('ix_insights_type_severity', table_name='insights')
    op.drop_index('ix_insights_created_at', table_name='insights')
    op.drop_table('insights')

    op.drop_index('ix_insight_rules_enabled', table_name='insight_rules')
    op.drop_table('insight_rules')
