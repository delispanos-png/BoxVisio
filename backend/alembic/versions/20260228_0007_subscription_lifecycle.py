"""subscription lifecycle tables

Revision ID: 20260228_0007_control
Revises: 20260228_0006_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0007_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0006_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    plan_enum = postgresql.ENUM('standard', 'pro', 'enterprise', name='planname', create_type=False)
    sub_enum = postgresql.ENUM('trial', 'active', 'past_due', 'suspended', 'canceled', name='subscriptionstatus', create_type=False)
    plan_enum.create(op.get_bind(), checkfirst=True)
    sub_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'subscriptions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('plan', plan_enum, nullable=False),
        sa.Column('status', sub_enum, nullable=False, server_default='trial'),
        sa.Column('trial_starts_at', sa.DateTime(), nullable=True),
        sa.Column('trial_ends_at', sa.DateTime(), nullable=True),
        sa.Column('current_period_start', sa.DateTime(), nullable=True),
        sa.Column('current_period_end', sa.DateTime(), nullable=True),
        sa.Column('feature_flags', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column('canceled_at', sa.DateTime(), nullable=True),
        sa.Column('suspended_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('tenant_id', name='uq_subscriptions_tenant_id'),
    )
    op.create_index('ix_subscriptions_tenant_id', 'subscriptions', ['tenant_id'])
    op.create_index('ix_subscriptions_trial_ends_at', 'subscriptions', ['trial_ends_at'])
    op.create_index('ix_subscriptions_current_period_end', 'subscriptions', ['current_period_end'])

    op.create_table(
        'subscription_limits',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id'), nullable=False),
        sa.Column('limit_key', sa.String(length=64), nullable=False),
        sa.Column('limit_value', sa.Integer(), nullable=False),
        sa.Column('used_value', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('subscription_id', 'limit_key', name='uq_subscription_limit_key'),
    )
    op.create_index('ix_subscription_limits_subscription_id', 'subscription_limits', ['subscription_id'])

    op.create_table(
        'invoices',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id'), nullable=True),
        sa.Column('invoice_no', sa.String(length=64), nullable=False, unique=True),
        sa.Column('amount_due', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='EUR'),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='draft'),
        sa.Column('due_at', sa.DateTime(), nullable=True),
        sa.Column('paid_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_invoices_tenant_id', 'invoices', ['tenant_id'])
    op.create_index('ix_invoices_subscription_id', 'invoices', ['subscription_id'])

    op.create_table(
        'payments',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('invoice_id', sa.Integer(), sa.ForeignKey('invoices.id'), nullable=True),
        sa.Column('amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='EUR'),
        sa.Column('provider', sa.String(length=64), nullable=True),
        sa.Column('provider_ref', sa.String(length=128), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='received'),
        sa.Column('paid_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_payments_tenant_id', 'payments', ['tenant_id'])
    op.create_index('ix_payments_invoice_id', 'payments', ['invoice_id'])

    op.create_table(
        'audit_logs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=True),
        sa.Column('actor_user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('action', sa.String(length=128), nullable=False),
        sa.Column('entity_type', sa.String(length=64), nullable=False),
        sa.Column('entity_id', sa.String(length=128), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
    )
    op.create_index('ix_audit_logs_tenant_id', 'audit_logs', ['tenant_id'])
    op.create_index('ix_audit_logs_actor_user_id', 'audit_logs', ['actor_user_id'])
    op.create_index('ix_audit_logs_created_at', 'audit_logs', ['created_at'])

    op.execute(
        """
        INSERT INTO subscriptions (
            tenant_id, plan, status, trial_starts_at, trial_ends_at, current_period_start, current_period_end,
            feature_flags, canceled_at, created_at, updated_at
        )
        SELECT
            t.id,
            t.plan,
            t.subscription_status,
            CASE WHEN t.subscription_status = 'trial' THEN t.created_at ELSE NULL END,
            t.trial_ends_at,
            CASE WHEN t.subscription_status IN ('active', 'past_due') THEN t.created_at ELSE NULL END,
            t.current_period_end,
            COALESCE(t.feature_flags, '{}'::json),
            t.canceled_at,
            t.created_at,
            NOW()
        FROM tenants t
        ON CONFLICT (tenant_id) DO NOTHING;
        """
    )

    op.execute(
        """
        INSERT INTO subscription_limits (subscription_id, limit_key, limit_value, used_value, created_at, updated_at)
        SELECT s.id, 'max_users', COALESCE(p.max_users, 5), 0, NOW(), NOW()
        FROM subscriptions s
        LEFT JOIN plans p ON p.code = s.plan::text
        ON CONFLICT (subscription_id, limit_key) DO NOTHING;
        """
    )
    op.execute(
        """
        INSERT INTO subscription_limits (subscription_id, limit_key, limit_value, used_value, created_at, updated_at)
        SELECT s.id, 'max_branches', COALESCE(p.max_branches, 5), 0, NOW(), NOW()
        FROM subscriptions s
        LEFT JOIN plans p ON p.code = s.plan::text
        ON CONFLICT (subscription_id, limit_key) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.drop_index('ix_audit_logs_created_at', table_name='audit_logs')
    op.drop_index('ix_audit_logs_actor_user_id', table_name='audit_logs')
    op.drop_index('ix_audit_logs_tenant_id', table_name='audit_logs')
    op.drop_table('audit_logs')

    op.drop_index('ix_payments_invoice_id', table_name='payments')
    op.drop_index('ix_payments_tenant_id', table_name='payments')
    op.drop_table('payments')

    op.drop_index('ix_invoices_subscription_id', table_name='invoices')
    op.drop_index('ix_invoices_tenant_id', table_name='invoices')
    op.drop_table('invoices')

    op.drop_index('ix_subscription_limits_subscription_id', table_name='subscription_limits')
    op.drop_table('subscription_limits')

    op.drop_index('ix_subscriptions_current_period_end', table_name='subscriptions')
    op.drop_index('ix_subscriptions_trial_ends_at', table_name='subscriptions')
    op.drop_index('ix_subscriptions_tenant_id', table_name='subscriptions')
    op.drop_table('subscriptions')
