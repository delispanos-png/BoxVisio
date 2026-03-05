"""control init

Revision ID: 20260228_0001_control
Revises:
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0001_control'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    plan_enum = postgresql.ENUM('standard', 'pro', 'enterprise', name='planname', create_type=False)
    status_enum = postgresql.ENUM('active', 'suspended', 'terminated', name='tenantstatus', create_type=False)
    role_enum = postgresql.ENUM('cloudon_admin', 'tenant_admin', 'tenant_user', name='rolename', create_type=False)

    plan_enum.create(op.get_bind(), checkfirst=True)
    status_enum.create(op.get_bind(), checkfirst=True)
    role_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'tenants',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('slug', sa.String(length=64), nullable=False, unique=True),
        sa.Column('plan', plan_enum, nullable=False),
        sa.Column('status', status_enum, nullable=False),
        sa.Column('db_name', sa.String(length=128), nullable=False),
        sa.Column('db_user', sa.String(length=128), nullable=False),
        sa.Column('db_password', sa.String(length=255), nullable=False),
        sa.Column('whmcs_service_id', sa.String(length=128), nullable=True),
        sa.Column('whmcs_product_id', sa.String(length=128), nullable=True),
        sa.Column('feature_flags', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_tenants_whmcs_service_id', 'tenants', ['whmcs_service_id'])

    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=True),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('password_hash', sa.String(length=255), nullable=False),
        sa.Column('role', role_enum, nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('email', 'tenant_id', name='uq_user_email_tenant'),
    )
    op.create_index('ix_users_tenant_id', 'users', ['tenant_id'])

    op.create_table(
        'tenant_api_keys',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('key_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('key_secret', sa.String(length=255), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_tenant_api_keys_tenant_id', 'tenant_api_keys', ['tenant_id'])

    op.create_table(
        'tenant_connections',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
        sa.Column('provider', sa.String(length=64), nullable=False),
        sa.Column('encrypted_config', sa.Text(), nullable=False),
        sa.Column('last_sync_at', sa.DateTime(), nullable=True),
        sa.Column('sync_status', sa.String(length=64), nullable=False),
    )
    op.create_index('ix_tenant_connections_tenant_id', 'tenant_connections', ['tenant_id'])

    op.create_table(
        'whmcs_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('service_id', sa.String(length=128), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_whmcs_events_service_id', 'whmcs_events', ['service_id'])


def downgrade() -> None:
    op.drop_index('ix_whmcs_events_service_id', table_name='whmcs_events')
    op.drop_table('whmcs_events')

    op.drop_index('ix_tenant_connections_tenant_id', table_name='tenant_connections')
    op.drop_table('tenant_connections')

    op.drop_index('ix_tenant_api_keys_tenant_id', table_name='tenant_api_keys')
    op.drop_table('tenant_api_keys')

    op.drop_index('ix_users_tenant_id', table_name='users')
    op.drop_table('users')

    op.drop_index('ix_tenants_whmcs_service_id', table_name='tenants')
    op.drop_table('tenants')

    sa.Enum(name='rolename').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='tenantstatus').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='planname').drop(op.get_bind(), checkfirst=True)
