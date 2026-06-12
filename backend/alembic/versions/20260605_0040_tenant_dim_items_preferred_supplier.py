"""add preferred (habitual) supplier to dim_items

Revision ID: 20260605_0040_tenant
Revises: 20260528_0039_tenant
Create Date: 2026-06-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260605_0040_tenant'
down_revision: Union[str, Sequence[str], None] = '20260528_0039_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(bind, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
            ),
            {'t': table, 'c': column},
        ).first()
    )


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_column(bind, 'dim_items', 'preferred_supplier_ext_id'):
        op.add_column('dim_items', sa.Column('preferred_supplier_ext_id', sa.String(length=128), nullable=True))
    if not _has_column(bind, 'dim_items', 'preferred_supplier_name'):
        op.add_column('dim_items', sa.Column('preferred_supplier_name', sa.String(length=255), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if _has_column(bind, 'dim_items', 'preferred_supplier_name'):
        op.drop_column('dim_items', 'preferred_supplier_name')
    if _has_column(bind, 'dim_items', 'preferred_supplier_ext_id'):
        op.drop_column('dim_items', 'preferred_supplier_ext_id')
