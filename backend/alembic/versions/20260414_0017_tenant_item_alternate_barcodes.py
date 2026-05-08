"""tenant dim_items alternate barcodes

Revision ID: 20260414_0017_tenant
Revises: 20260414_0016_tenant
Create Date: 2026-04-14
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = '20260414_0017_tenant'
down_revision: Union[str, Sequence[str], None] = '20260414_0016_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    if 'alternate_barcodes' not in _column_names(bind, 'dim_items'):
        op.add_column('dim_items', sa.Column('alternate_barcodes', sa.Text(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, 'dim_items'):
        return
    if 'alternate_barcodes' in _column_names(bind, 'dim_items'):
        op.drop_column('dim_items', 'alternate_barcodes')
