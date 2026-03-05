"""control extensions: plans, whmcs_services, reset fields

Revision ID: 20260228_0002_control
Revises: 20260228_0001_control
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260228_0002_control'
down_revision: Union[str, Sequence[str], None] = '20260228_0001_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'plans',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('code', sa.String(length=32), nullable=False, unique=True),
        sa.Column('display_name', sa.String(length=64), nullable=False),
        sa.Column('feature_sales', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('feature_purchases', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('feature_inventory', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('feature_cashflows', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('max_users', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('max_branches', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
    )

    op.create_table(
        'whmcs_services',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('service_id', sa.String(length=128), nullable=False, unique=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=True),
        sa.Column('product_id', sa.String(length=128), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='active'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_whmcs_services_service_id', 'whmcs_services', ['service_id'])
    op.create_index('ix_whmcs_services_tenant_id', 'whmcs_services', ['tenant_id'])

    op.add_column('users', sa.Column('reset_token', sa.String(length=128), nullable=True))
    op.add_column('users', sa.Column('reset_token_expires_at', sa.DateTime(), nullable=True))
    op.create_index('ix_users_reset_token', 'users', ['reset_token'])

    op.execute(
        """
        INSERT INTO plans (code, display_name, feature_sales, feature_purchases, feature_inventory, feature_cashflows, max_users, max_branches, is_active)
        VALUES
            ('standard', 'Standard', true, false, false, false, 5, 5, true),
            ('pro', 'Pro', true, true, false, false, 15, 20, true),
            ('enterprise', 'Enterprise', true, true, true, true, 100, 1000, true)
        ON CONFLICT (code) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.drop_index('ix_users_reset_token', table_name='users')
    op.drop_column('users', 'reset_token_expires_at')
    op.drop_column('users', 'reset_token')

    op.drop_index('ix_whmcs_services_tenant_id', table_name='whmcs_services')
    op.drop_index('ix_whmcs_services_service_id', table_name='whmcs_services')
    op.drop_table('whmcs_services')

    op.drop_table('plans')
