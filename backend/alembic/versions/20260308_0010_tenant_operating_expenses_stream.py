"""tenant operating expenses stream tables

Revision ID: 20260308_0010_tenant
Revises: 20260308_0009_tenant
Create Date: 2026-03-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260308_0010_tenant'
down_revision: Union[str, Sequence[str], None] = '20260308_0009_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def _create_dim_expense_categories(bind) -> None:
    if not _table_exists(bind, 'dim_expense_categories'):
        op.create_table(
            'dim_expense_categories',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('external_id', sa.String(length=128), nullable=True),
            sa.Column('category_code', sa.String(length=128), nullable=False),
            sa.Column('category_name', sa.String(length=255), nullable=False),
            sa.Column('parent_category_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_expense_categories.id'), nullable=True),
            sa.Column('level_no', sa.Integer(), nullable=False, server_default='1'),
            sa.Column('classification', sa.String(length=32), nullable=False, server_default='opex'),
            sa.Column('gl_account_code', sa.String(length=128), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('category_code', name='uq_dim_expense_categories_category_code'),
        )

    idx = _index_names(bind, 'dim_expense_categories')
    if 'ix_dim_expense_categories_external_id' not in idx:
        op.create_index('ix_dim_expense_categories_external_id', 'dim_expense_categories', ['external_id'])
    if 'ix_dim_expense_categories_category_name' not in idx:
        op.create_index('ix_dim_expense_categories_category_name', 'dim_expense_categories', ['category_name'])
    if 'ix_dim_expense_categories_parent_category_id' not in idx:
        op.create_index('ix_dim_expense_categories_parent_category_id', 'dim_expense_categories', ['parent_category_id'])
    if 'ix_dim_expense_categories_classification' not in idx:
        op.create_index('ix_dim_expense_categories_classification', 'dim_expense_categories', ['classification'])
    if 'ix_dim_expense_categories_updated_at' not in idx:
        op.create_index('ix_dim_expense_categories_updated_at', 'dim_expense_categories', ['updated_at'])


def _create_fact_expenses(bind) -> None:
    if not _table_exists(bind, 'fact_expenses'):
        op.create_table(
            'fact_expenses',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('external_id', sa.String(length=128), nullable=False),
            sa.Column('expense_date', sa.Date(), nullable=False),
            sa.Column('posting_date', sa.Date(), nullable=True),
            sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('location_id', postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column('category_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_expense_categories.id'), nullable=True),
            sa.Column('expense_category_code', sa.String(length=128), nullable=True),
            sa.Column('supplier_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_suppliers.id'), nullable=True),
            sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
            sa.Column('account_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_accounts.id'), nullable=True),
            sa.Column('account_ext_id', sa.String(length=128), nullable=True),
            sa.Column('document_type', sa.String(length=128), nullable=True),
            sa.Column('document_no', sa.String(length=128), nullable=True),
            sa.Column('cost_center', sa.String(length=128), nullable=True),
            sa.Column('payment_status', sa.String(length=32), nullable=True),
            sa.Column('due_date', sa.Date(), nullable=True),
            sa.Column('currency_code', sa.String(length=3), nullable=False, server_default='EUR'),
            sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('source_connector_id', sa.String(length=64), nullable=True),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('external_id', name='uq_fact_expenses_external_id'),
        )

    idx = _index_names(bind, 'fact_expenses')
    wanted = {
        'ix_fact_expenses_external_id': ['external_id'],
        'ix_fact_expenses_expense_date': ['expense_date'],
        'ix_fact_expenses_expense_date_branch_id': ['expense_date', 'branch_id'],
        'ix_fact_expenses_expense_date_category_id': ['expense_date', 'category_id'],
        'ix_fact_expenses_expense_date_supplier_id': ['expense_date', 'supplier_id'],
        'ix_fact_expenses_expense_date_account_id': ['expense_date', 'account_id'],
        'ix_fact_expenses_branch_category_expense_date': ['branch_id', 'category_id', 'expense_date'],
        'ix_fact_expenses_branch_ext_id': ['branch_ext_id'],
        'ix_fact_expenses_expense_category_code': ['expense_category_code'],
        'ix_fact_expenses_supplier_ext_id': ['supplier_ext_id'],
        'ix_fact_expenses_account_ext_id': ['account_ext_id'],
        'ix_fact_expenses_document_type': ['document_type'],
        'ix_fact_expenses_document_no': ['document_no'],
        'ix_fact_expenses_cost_center': ['cost_center'],
        'ix_fact_expenses_payment_status': ['payment_status'],
        'ix_fact_expenses_due_date': ['due_date'],
        'ix_fact_expenses_source_connector_id': ['source_connector_id'],
        'ix_fact_expenses_updated_at': ['updated_at'],
    }
    for idx_name, columns in wanted.items():
        if idx_name not in idx:
            op.create_index(idx_name, 'fact_expenses', columns)


def _create_staging_expenses(bind) -> None:
    if not _table_exists(bind, 'stg_expense_documents'):
        op.create_table(
            'stg_expense_documents',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('connector_type', sa.String(length=64), nullable=False),
            sa.Column('stream', sa.String(length=64), nullable=False, server_default='operating_expenses'),
            sa.Column('event_id', sa.String(length=128), nullable=True),
            sa.Column('external_id', sa.String(length=128), nullable=True),
            sa.Column('doc_date', sa.Date(), nullable=True),
            sa.Column('transform_status', sa.String(length=16), nullable=False, server_default='loaded'),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('source_payload_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column('ingested_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('processed_at', sa.DateTime(), nullable=True),
        )

    idx = _index_names(bind, 'stg_expense_documents')
    wanted = {
        'ix_stg_expense_documents_ingested_at': ['ingested_at'],
        'ix_stg_expense_documents_connector_external': ['connector_type', 'external_id'],
        'ix_stg_expense_documents_status': ['transform_status'],
        'ix_stg_expense_documents_doc_date': ['doc_date'],
    }
    for idx_name, columns in wanted.items():
        if idx_name not in idx:
            op.create_index(idx_name, 'stg_expense_documents', columns)


def _create_expense_aggregates(bind) -> None:
    if not _table_exists(bind, 'agg_expenses_daily'):
        op.create_table(
            'agg_expenses_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('expense_date', sa.Date(), nullable=False),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('expense_category_code', sa.String(length=128), nullable=True),
            sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
            sa.Column('account_ext_id', sa.String(length=128), nullable=True),
            sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint(
                'expense_date',
                'branch_ext_id',
                'expense_category_code',
                'supplier_ext_id',
                'account_ext_id',
                name='uq_agg_expenses_daily_dims',
            ),
        )
    agg_daily_idx = _index_names(bind, 'agg_expenses_daily')
    for idx_name, columns in {
        'ix_agg_expenses_daily_expense_date': ['expense_date'],
        'ix_agg_expenses_daily_branch_ext_id': ['branch_ext_id'],
        'ix_agg_expenses_daily_expense_category_code': ['expense_category_code'],
        'ix_agg_expenses_daily_supplier_ext_id': ['supplier_ext_id'],
        'ix_agg_expenses_daily_account_ext_id': ['account_ext_id'],
        'ix_agg_expenses_daily_date_branch': ['expense_date', 'branch_ext_id'],
        'ix_agg_expenses_daily_date_category': ['expense_date', 'expense_category_code'],
    }.items():
        if idx_name not in agg_daily_idx:
            op.create_index(idx_name, 'agg_expenses_daily', columns)

    if not _table_exists(bind, 'agg_expenses_monthly'):
        op.create_table(
            'agg_expenses_monthly',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('month_start', sa.Date(), nullable=False),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('expense_category_code', sa.String(length=128), nullable=True),
            sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
            sa.Column('account_ext_id', sa.String(length=128), nullable=True),
            sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint(
                'month_start',
                'branch_ext_id',
                'expense_category_code',
                'supplier_ext_id',
                'account_ext_id',
                name='uq_agg_expenses_monthly_dims',
            ),
        )
    agg_monthly_idx = _index_names(bind, 'agg_expenses_monthly')
    for idx_name, columns in {
        'ix_agg_expenses_monthly_month_start': ['month_start'],
        'ix_agg_expenses_monthly_branch_ext_id': ['branch_ext_id'],
        'ix_agg_expenses_monthly_expense_category_code': ['expense_category_code'],
        'ix_agg_expenses_monthly_supplier_ext_id': ['supplier_ext_id'],
        'ix_agg_expenses_monthly_account_ext_id': ['account_ext_id'],
        'ix_agg_expenses_monthly_month_branch': ['month_start', 'branch_ext_id'],
        'ix_agg_expenses_monthly_month_category': ['month_start', 'expense_category_code'],
    }.items():
        if idx_name not in agg_monthly_idx:
            op.create_index(idx_name, 'agg_expenses_monthly', columns)

    if not _table_exists(bind, 'agg_expenses_by_category_daily'):
        op.create_table(
            'agg_expenses_by_category_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('expense_date', sa.Date(), nullable=False),
            sa.Column('expense_category_code', sa.String(length=128), nullable=True),
            sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('expense_date', 'expense_category_code', name='uq_agg_expenses_by_category_daily_dims'),
        )
    agg_category_idx = _index_names(bind, 'agg_expenses_by_category_daily')
    for idx_name, columns in {
        'ix_agg_expenses_by_category_daily_date': ['expense_date'],
        'ix_agg_expenses_by_category_daily_expense_category_code': ['expense_category_code'],
    }.items():
        if idx_name not in agg_category_idx:
            op.create_index(idx_name, 'agg_expenses_by_category_daily', columns)

    if not _table_exists(bind, 'agg_expenses_by_branch_daily'):
        op.create_table(
            'agg_expenses_by_branch_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('expense_date', sa.Date(), nullable=False),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('amount_net', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_tax', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('amount_gross', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('expense_date', 'branch_ext_id', name='uq_agg_expenses_by_branch_daily_dims'),
        )
    agg_branch_idx = _index_names(bind, 'agg_expenses_by_branch_daily')
    for idx_name, columns in {
        'ix_agg_expenses_by_branch_daily_date': ['expense_date'],
        'ix_agg_expenses_by_branch_daily_branch_ext_id': ['branch_ext_id'],
        'ix_agg_expenses_by_branch_daily_date_branch': ['expense_date', 'branch_ext_id'],
    }.items():
        if idx_name not in agg_branch_idx:
            op.create_index(idx_name, 'agg_expenses_by_branch_daily', columns)


def upgrade() -> None:
    bind = op.get_bind()
    _create_dim_expense_categories(bind)
    _create_fact_expenses(bind)
    _create_staging_expenses(bind)
    _create_expense_aggregates(bind)


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'agg_expenses_by_branch_daily'):
        for idx_name in list(_index_names(bind, 'agg_expenses_by_branch_daily')):
            if idx_name.startswith('ix_agg_expenses_by_branch_daily'):
                op.drop_index(idx_name, table_name='agg_expenses_by_branch_daily')
        op.drop_table('agg_expenses_by_branch_daily')

    if _table_exists(bind, 'agg_expenses_by_category_daily'):
        for idx_name in list(_index_names(bind, 'agg_expenses_by_category_daily')):
            if idx_name.startswith('ix_agg_expenses_by_category_daily'):
                op.drop_index(idx_name, table_name='agg_expenses_by_category_daily')
        op.drop_table('agg_expenses_by_category_daily')

    if _table_exists(bind, 'agg_expenses_monthly'):
        for idx_name in list(_index_names(bind, 'agg_expenses_monthly')):
            if idx_name.startswith('ix_agg_expenses_monthly'):
                op.drop_index(idx_name, table_name='agg_expenses_monthly')
        op.drop_table('agg_expenses_monthly')

    if _table_exists(bind, 'agg_expenses_daily'):
        for idx_name in list(_index_names(bind, 'agg_expenses_daily')):
            if idx_name.startswith('ix_agg_expenses_daily'):
                op.drop_index(idx_name, table_name='agg_expenses_daily')
        op.drop_table('agg_expenses_daily')

    if _table_exists(bind, 'stg_expense_documents'):
        for idx_name in list(_index_names(bind, 'stg_expense_documents')):
            if idx_name.startswith('ix_stg_expense_documents'):
                op.drop_index(idx_name, table_name='stg_expense_documents')
        op.drop_table('stg_expense_documents')

    if _table_exists(bind, 'fact_expenses'):
        for idx_name in list(_index_names(bind, 'fact_expenses')):
            if idx_name.startswith('ix_fact_expenses'):
                op.drop_index(idx_name, table_name='fact_expenses')
        op.drop_table('fact_expenses')

    if _table_exists(bind, 'dim_expense_categories'):
        for idx_name in list(_index_names(bind, 'dim_expense_categories')):
            if idx_name.startswith('ix_dim_expense_categories'):
                op.drop_index(idx_name, table_name='dim_expense_categories')
        op.drop_table('dim_expense_categories')
