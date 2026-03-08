"""add agreement_notes to supplier_targets

Revision ID: 20260308_0009_tenant
Revises: 20260307_0008_tenant
Create Date: 2026-03-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260308_0009_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0008_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    if not _table_exists(bind, table_name):
        return False
    cols = inspect(bind).get_columns(table_name)
    return any(str(col.get('name')) == column_name for col in cols)


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'supplier_targets'):
        return
    if not _column_exists(bind, 'supplier_targets', 'agreement_notes'):
        op.add_column('supplier_targets', sa.Column('agreement_notes', sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE supplier_targets
        SET agreement_notes = notes
        WHERE agreement_notes IS NULL
          AND notes IS NOT NULL
          AND btrim(notes) <> ''
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'supplier_targets') and _column_exists(bind, 'supplier_targets', 'agreement_notes'):
        op.drop_column('supplier_targets', 'agreement_notes')
