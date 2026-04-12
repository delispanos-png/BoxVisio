"""control user company scope

Revision ID: 20260407_0012_control
Revises: 20260308_0007_control
Create Date: 2026-04-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260407_0012_control'
down_revision: Union[str, Sequence[str], None] = '20260308_0007_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    user_cols = _column_names(bind, 'users')
    if 'company_id' not in user_cols:
        op.add_column('users', sa.Column('company_id', sa.String(length=64), nullable=True))
    user_indexes = _index_names(bind, 'users')
    if 'ix_users_company_id' not in user_indexes:
        op.create_index('ix_users_company_id', 'users', ['company_id'])


def downgrade() -> None:
    bind = op.get_bind()
    user_cols = _column_names(bind, 'users')
    user_indexes = _index_names(bind, 'users')
    if 'ix_users_company_id' in user_indexes:
        op.drop_index('ix_users_company_id', table_name='users')
    if 'company_id' in user_cols:
        op.drop_column('users', 'company_id')
