"""tenant sales documents performance index

Revision ID: 20260516_0024_tenant
Revises: 20260516_0023_tenant
Create Date: 2026-05-16
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect


revision: str = '20260516_0024_tenant'
down_revision: Union[str, Sequence[str], None] = '20260516_0023_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_sales'):
        idx = _index_names(bind, 'fact_sales')
        if 'ix_fact_sales_doc_date_document_id' not in idx:
            op.create_index('ix_fact_sales_doc_date_document_id', 'fact_sales', ['doc_date', 'document_id'])


def downgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, 'fact_sales'):
        idx = _index_names(bind, 'fact_sales')
        if 'ix_fact_sales_doc_date_document_id' in idx:
            op.drop_index('ix_fact_sales_doc_date_document_id', table_name='fact_sales')
