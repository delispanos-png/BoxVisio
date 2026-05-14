"""tenant sales document charge breakdown

Revision ID: 20260512_0022_tenant
Revises: 20260418_0020_tenant
Create Date: 2026-05-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision: str = '20260512_0022_tenant'
down_revision: Union[str, Sequence[str], None] = '20260418_0020_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    return inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if _table_exists('fact_sales_document_charges'):
        return

    op.create_table(
        'fact_sales_document_charges',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=160), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('document_id', sa.String(length=128), nullable=False),
        sa.Column('document_no', sa.String(length=128), nullable=True),
        sa.Column('document_series', sa.String(length=128), nullable=True),
        sa.Column('document_type', sa.String(length=128), nullable=True),
        sa.Column('charge_code', sa.String(length=64), nullable=False),
        sa.Column('charge_name', sa.String(length=255), nullable=True),
        sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('source_connector_id', sa.String(length=64), nullable=True),
        sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.UniqueConstraint('external_id', name='uq_fact_sales_document_charges_external_id'),
        sa.UniqueConstraint('document_id', 'charge_code', name='uq_fact_sales_document_charges_doc_charge'),
    )
    op.create_index('ix_fact_sales_document_charges_doc_date', 'fact_sales_document_charges', ['doc_date'])
    op.create_index('ix_fact_sales_document_charges_document_id', 'fact_sales_document_charges', ['document_id'])
    op.create_index('ix_fact_sales_document_charges_document_no', 'fact_sales_document_charges', ['document_no'])
    op.create_index('ix_fact_sales_document_charges_charge_code', 'fact_sales_document_charges', ['charge_code'])
    op.create_index('ix_fact_sales_document_charges_source_connector_id', 'fact_sales_document_charges', ['source_connector_id'])


def downgrade() -> None:
    if _table_exists('fact_sales_document_charges'):
        op.drop_table('fact_sales_document_charges')
