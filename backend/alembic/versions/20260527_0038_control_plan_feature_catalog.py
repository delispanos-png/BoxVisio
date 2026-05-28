"""control dynamic plan feature catalog

Revision ID: 20260527_0038_control
Revises: 20260522_0022_control
Create Date: 2026-05-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '20260527_0038_control'
down_revision: Union[str, Sequence[str], None] = '20260522_0022_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'plan_feature_catalog',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('feature_key', sa.String(length=64), nullable=False),
        sa.Column('label', sa.String(length=255), nullable=False),
        sa.Column('group', sa.String(length=64), nullable=False, server_default='Custom'),
        sa.Column('feature_type', sa.String(length=32), nullable=False, server_default='feature'),
        sa.Column('plan_status', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('feature_key', name='uq_plan_feature_catalog_feature_key'),
    )
    op.create_index('ix_plan_feature_catalog_feature_key', 'plan_feature_catalog', ['feature_key'])
    op.create_index('ix_plan_feature_catalog_tenant_id', 'plan_feature_catalog', ['tenant_id'])


def downgrade() -> None:
    op.drop_index('ix_plan_feature_catalog_tenant_id', table_name='plan_feature_catalog')
    op.drop_index('ix_plan_feature_catalog_feature_key', table_name='plan_feature_catalog')
    op.drop_table('plan_feature_catalog')
