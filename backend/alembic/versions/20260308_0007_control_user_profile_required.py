"""control enforce single required professional profile per user

Revision ID: 20260308_0007_control
Revises: 20260308_0006_control
Create Date: 2026-03-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260308_0007_control'
down_revision: Union[str, Sequence[str], None] = '20260308_0006_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _is_column_nullable(bind, table_name: str, column_name: str) -> bool | None:
    for col in inspect(bind).get_columns(table_name):
        if col.get('name') == column_name:
            return bool(col.get('nullable', True))
    return None


def upgrade() -> None:
    bind = op.get_bind()
    user_cols = _column_names(bind, 'users')
    if 'professional_profile_id' not in user_cols:
        return

    # Safety backfill in case legacy rows still have NULL professional profile.
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

    nullable = _is_column_nullable(bind, 'users', 'professional_profile_id')
    if nullable is True:
        op.alter_column('users', 'professional_profile_id', existing_type=sa.Integer(), nullable=False)


def downgrade() -> None:
    bind = op.get_bind()
    user_cols = _column_names(bind, 'users')
    if 'professional_profile_id' in user_cols:
        nullable = _is_column_nullable(bind, 'users', 'professional_profile_id')
        if nullable is False:
            op.alter_column('users', 'professional_profile_id', existing_type=sa.Integer(), nullable=True)
