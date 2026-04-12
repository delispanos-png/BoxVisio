"""control connector active flag

Revision ID: 20260408_0013_control
Revises: 20260407_0012_control
Create Date: 2026-04-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260408_0013_control'
down_revision: Union[str, Sequence[str], None] = '20260407_0012_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(bind, table_name: str) -> set[str]:
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')
    if 'is_active' not in cols:
        op.add_column(
            'tenant_connections',
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        )
        op.execute("UPDATE tenant_connections SET is_active = true WHERE is_active IS NULL")
        op.alter_column('tenant_connections', 'is_active', server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    cols = _column_names(bind, 'tenant_connections')
    if 'is_active' in cols:
        op.drop_column('tenant_connections', 'is_active')
