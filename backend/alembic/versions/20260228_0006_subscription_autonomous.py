"""autonomous subscription lifecycle

Revision ID: 20260228_0006_control
Revises: 20260228_0005_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0006_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0005_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    sub_enum = sa.Enum('trial', 'active', 'past_due', 'suspended', 'canceled', name='subscriptionstatus')
    sub_enum.create(op.get_bind(), checkfirst=True)

    op.add_column('tenants', sa.Column('subscription_status', sub_enum, nullable=False, server_default='trial'))
    op.add_column('tenants', sa.Column('trial_ends_at', sa.DateTime(), nullable=True))
    op.add_column('tenants', sa.Column('current_period_end', sa.DateTime(), nullable=True))
    op.add_column('tenants', sa.Column('canceled_at', sa.DateTime(), nullable=True))

    op.create_table(
        'subscription_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('from_status', sa.String(length=32), nullable=True),
        sa.Column('to_status', sa.String(length=32), nullable=False),
        sa.Column('note', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_subscription_events_tenant_id', 'subscription_events', ['tenant_id'])


def downgrade() -> None:
    op.drop_index('ix_subscription_events_tenant_id', table_name='subscription_events')
    op.drop_table('subscription_events')

    op.drop_column('tenants', 'canceled_at')
    op.drop_column('tenants', 'current_period_end')
    op.drop_column('tenants', 'trial_ends_at')
    op.drop_column('tenants', 'subscription_status')

    sa.Enum(name='subscriptionstatus').drop(op.get_bind(), checkfirst=True)
