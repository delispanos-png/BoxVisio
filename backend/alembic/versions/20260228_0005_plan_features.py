"""plan features table

Revision ID: 20260228_0005_control
Revises: 20260228_0004_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0005_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0004_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    plan_enum = postgresql.ENUM('standard', 'pro', 'enterprise', name='planname', create_type=False)
    plan_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'plan_features',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('plan', plan_enum, nullable=False),
        sa.Column('feature_name', sa.String(length=64), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('plan', 'feature_name', name='uq_plan_feature'),
    )

    op.execute(
        """
        INSERT INTO plan_features (plan, feature_name, enabled, created_at) VALUES
          ('standard', 'sales', true, NOW()),
          ('standard', 'purchases', false, NOW()),
          ('standard', 'inventory', false, NOW()),
          ('standard', 'cashflows', false, NOW()),
          ('pro', 'sales', true, NOW()),
          ('pro', 'purchases', true, NOW()),
          ('pro', 'inventory', false, NOW()),
          ('pro', 'cashflows', false, NOW()),
          ('enterprise', 'sales', true, NOW()),
          ('enterprise', 'purchases', true, NOW()),
          ('enterprise', 'inventory', true, NOW()),
          ('enterprise', 'cashflows', true, NOW())
        ON CONFLICT (plan, feature_name) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.drop_table('plan_features')
