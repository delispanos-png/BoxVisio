"""control professional profiles dimension and user mapping

Revision ID: 20260308_0006_control
Revises: 20260307_0005_control
Create Date: 2026-03-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260308_0006_control'
down_revision: Union[str, Sequence[str], None] = '20260307_0005_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_names(bind) -> set[str]:
    return set(inspect(bind).get_table_names())


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    return {idx.get('name', '') for idx in inspect(bind).get_indexes(table_name)}


def _fk_names(bind, table_name: str) -> set[str]:
    return {fk.get('name', '') for fk in inspect(bind).get_foreign_keys(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    tables = _table_names(bind)

    if 'dim_professional_profiles' not in tables:
        op.create_table(
            'dim_professional_profiles',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('profile_code', sa.String(length=64), nullable=False),
            sa.Column('profile_name', sa.String(length=255), nullable=False),
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint('profile_code', name='uq_dim_professional_profiles_profile_code'),
        )
        op.create_index(
            'ix_dim_professional_profiles_profile_code',
            'dim_professional_profiles',
            ['profile_code'],
        )

    bind.execute(
        sa.text(
            """
            INSERT INTO dim_professional_profiles (profile_code, profile_name, description, created_at, updated_at)
            VALUES
              ('OWNER', 'Owner', 'Business owner profile with full cross-stream operational visibility.', NOW(), NOW()),
              ('MANAGER', 'Manager', 'General manager profile focused on executive KPIs and stream-level operations.', NOW(), NOW()),
              ('FINANCE', 'Finance', 'Finance profile focused on cash, receivables and payables.', NOW(), NOW()),
              ('INVENTORY', 'Inventory', 'Inventory profile focused on stock movements and inventory analytics.', NOW(), NOW()),
              ('SALES', 'Sales', 'Sales profile focused on sales documents and sales analytics.', NOW(), NOW())
            ON CONFLICT (profile_code) DO UPDATE
            SET
              profile_name = EXCLUDED.profile_name,
              description = EXCLUDED.description,
              updated_at = NOW()
            """
        )
    )

    user_cols = _column_names(bind, 'users')
    if 'professional_profile_id' not in user_cols:
        op.add_column('users', sa.Column('professional_profile_id', sa.Integer(), nullable=True))

    user_idx_names = _index_names(bind, 'users')
    if 'ix_users_professional_profile_id' not in user_idx_names:
        op.create_index('ix_users_professional_profile_id', 'users', ['professional_profile_id'])

    user_fk_names = _fk_names(bind, 'users')
    if 'fk_users_professional_profile_id_dim_professional_profiles' not in user_fk_names:
        op.create_foreign_key(
            'fk_users_professional_profile_id_dim_professional_profiles',
            'users',
            'dim_professional_profiles',
            ['professional_profile_id'],
            ['id'],
        )

    bind.execute(
        sa.text(
            """
            UPDATE users u
            SET professional_profile_id = p.id
            FROM dim_professional_profiles p
            WHERE u.professional_profile_id IS NULL
              AND p.profile_code = CASE
                  WHEN u.role::text = 'tenant_user' THEN 'FINANCE'
                  WHEN u.role::text = 'cloudon_admin' THEN 'OWNER'
                  ELSE 'MANAGER'
              END
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    tables = _table_names(bind)
    if 'users' in tables:
        user_fk_names = _fk_names(bind, 'users')
        if 'fk_users_professional_profile_id_dim_professional_profiles' in user_fk_names:
            op.drop_constraint('fk_users_professional_profile_id_dim_professional_profiles', 'users', type_='foreignkey')

        user_idx_names = _index_names(bind, 'users')
        if 'ix_users_professional_profile_id' in user_idx_names:
            op.drop_index('ix_users_professional_profile_id', table_name='users')

        user_cols = _column_names(bind, 'users')
        if 'professional_profile_id' in user_cols:
            op.drop_column('users', 'professional_profile_id')

    if 'dim_professional_profiles' in tables:
        idx_names = _index_names(bind, 'dim_professional_profiles')
        if 'ix_dim_professional_profiles_profile_code' in idx_names:
            op.drop_index('ix_dim_professional_profiles_profile_code', table_name='dim_professional_profiles')
        op.drop_table('dim_professional_profiles')
