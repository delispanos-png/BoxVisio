"""tenant deterministic intelligence engine v1

Revision ID: 20260228_0013_tenant
Revises: 20260228_0012_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0013_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0012_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table('insight_history')
    op.drop_table('insights')
    op.drop_table('insight_rules')

    op.create_table(
        'insight_rules',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('code', sa.Text(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('category', sa.Text(), nullable=False),
        sa.Column('severity_default', sa.String(length=16), nullable=False, server_default='warning'),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('params_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column('scope', sa.Text(), nullable=False, server_default='tenant'),
        sa.Column('schedule', sa.Text(), nullable=False, server_default='daily'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('code', name='uq_insight_rules_code'),
    )
    op.create_index('ix_insight_rules_enabled_category', 'insight_rules', ['enabled', 'category'])

    op.create_table(
        'insights',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('rule_code', sa.Text(), nullable=False),
        sa.Column('category', sa.Text(), nullable=False),
        sa.Column('severity', sa.String(length=16), nullable=False),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('entity_type', sa.Text(), nullable=True),
        sa.Column('entity_external_id', sa.Text(), nullable=True),
        sa.Column('entity_name', sa.Text(), nullable=True),
        sa.Column('period_from', sa.Date(), nullable=False),
        sa.Column('period_to', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(), nullable=True),
        sa.Column('baseline_value', sa.Numeric(), nullable=True),
        sa.Column('delta_value', sa.Numeric(), nullable=True),
        sa.Column('delta_pct', sa.Numeric(), nullable=True),
        sa.Column('metadata_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='open'),
        sa.Column('acknowledged_by', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('acknowledged_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_insights_created_at_desc', 'insights', [sa.text('created_at DESC')])
    op.create_index('ix_insights_rule_code_created_at', 'insights', ['rule_code', 'created_at'])
    op.create_index('ix_insights_status', 'insights', ['status'])
    op.create_index('ix_insights_entity', 'insights', ['entity_type', 'entity_external_id'])
    op.create_index(
        'uq_insights_rule_scope_period',
        'insights',
        [sa.text('rule_code'), sa.text("COALESCE(entity_type, '')"), sa.text("COALESCE(entity_external_id, '')"), sa.text('period_from'), sa.text('period_to')],
        unique=True,
    )

    op.create_table(
        'insight_runs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('rules_executed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('insights_created', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error', sa.Text(), nullable=True),
    )
    op.create_index('ix_insight_runs_started_at', 'insight_runs', ['started_at'])
    op.create_index('ix_insight_runs_status', 'insight_runs', ['status'])


def downgrade() -> None:
    op.drop_index('ix_insight_runs_status', table_name='insight_runs')
    op.drop_index('ix_insight_runs_started_at', table_name='insight_runs')
    op.drop_table('insight_runs')

    op.drop_index('uq_insights_rule_scope_period', table_name='insights')
    op.drop_index('ix_insights_entity', table_name='insights')
    op.drop_index('ix_insights_status', table_name='insights')
    op.drop_index('ix_insights_rule_code_created_at', table_name='insights')
    op.drop_index('ix_insights_created_at_desc', table_name='insights')
    op.drop_table('insights')

    op.drop_index('ix_insight_rules_enabled_category', table_name='insight_rules')
    op.drop_table('insight_rules')
