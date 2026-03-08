"""tenant operational streams upgrade (cash transactions + supplier balances)

Revision ID: 20260307_0001_tenant
Revises: 20260306_0020_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0001_tenant'
down_revision: Union[str, Sequence[str], None] = '20260306_0020_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'fact_cashflows'):
        cashflow_cols = _column_names(bind, 'fact_cashflows')
        if 'transaction_id' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('transaction_id', sa.String(length=128), nullable=True))
        if 'transaction_date' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('transaction_date', sa.Date(), nullable=True))
        if 'transaction_type' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('transaction_type', sa.String(length=64), nullable=True))
        if 'subcategory' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('subcategory', sa.String(length=32), nullable=True))
        if 'account_id' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('account_id', sa.String(length=128), nullable=True))
        if 'counterparty_type' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('counterparty_type', sa.String(length=32), nullable=True))
        if 'counterparty_id' not in cashflow_cols:
            op.add_column('fact_cashflows', sa.Column('counterparty_id', sa.String(length=128), nullable=True))

        cashflow_indexes = _index_names(bind, 'fact_cashflows')
        if 'ix_fact_cashflows_subcategory' not in cashflow_indexes:
            op.create_index('ix_fact_cashflows_subcategory', 'fact_cashflows', ['subcategory'])
        if 'ix_fact_cashflows_account_id' not in cashflow_indexes:
            op.create_index('ix_fact_cashflows_account_id', 'fact_cashflows', ['account_id'])

    if not _table_exists(bind, 'agg_cash_daily'):
        op.create_table(
            'agg_cash_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('doc_date', sa.Date(), nullable=False),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('subcategory', sa.String(length=32), nullable=True),
            sa.Column('transaction_type', sa.String(length=64), nullable=True),
            sa.Column('account_id', sa.String(length=128), nullable=True),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('inflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('outflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('net_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('doc_date', 'branch_ext_id', 'subcategory', 'transaction_type', 'account_id', name='uq_agg_cash_daily_dims'),
        )
        op.create_index('ix_agg_cash_daily_doc_date', 'agg_cash_daily', ['doc_date'])
        op.create_index('ix_agg_cash_daily_branch_ext_id', 'agg_cash_daily', ['branch_ext_id'])
        op.create_index('ix_agg_cash_daily_subcategory', 'agg_cash_daily', ['subcategory'])
        op.create_index('ix_agg_cash_daily_transaction_type', 'agg_cash_daily', ['transaction_type'])
        op.create_index('ix_agg_cash_daily_account_id', 'agg_cash_daily', ['account_id'])

    if not _table_exists(bind, 'agg_cash_by_type'):
        op.create_table(
            'agg_cash_by_type',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('doc_date', sa.Date(), nullable=False),
            sa.Column('subcategory', sa.String(length=32), nullable=True),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('inflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('outflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('net_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('doc_date', 'subcategory', name='uq_agg_cash_by_type_dims'),
        )
        op.create_index('ix_agg_cash_by_type_doc_date', 'agg_cash_by_type', ['doc_date'])
        op.create_index('ix_agg_cash_by_type_subcategory', 'agg_cash_by_type', ['subcategory'])

    if not _table_exists(bind, 'agg_cash_accounts'):
        op.create_table(
            'agg_cash_accounts',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('doc_date', sa.Date(), nullable=False),
            sa.Column('account_id', sa.String(length=128), nullable=True),
            sa.Column('entries', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('inflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('outflows', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('net_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('doc_date', 'account_id', name='uq_agg_cash_accounts_dims'),
        )
        op.create_index('ix_agg_cash_accounts_doc_date', 'agg_cash_accounts', ['doc_date'])
        op.create_index('ix_agg_cash_accounts_account_id', 'agg_cash_accounts', ['account_id'])

    if not _table_exists(bind, 'fact_supplier_balances'):
        op.create_table(
            'fact_supplier_balances',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('external_id', sa.String(length=128), nullable=False),
            sa.Column('supplier_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_suppliers.id'), nullable=True),
            sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
            sa.Column('balance_date', sa.Date(), nullable=False),
            sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('open_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('overdue_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_0_30', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_31_60', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_61_90', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_90_plus', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('last_payment_date', sa.Date(), nullable=True),
            sa.Column('trend_vs_previous', sa.Numeric(14, 2), nullable=True),
            sa.Column('currency', sa.String(length=3), nullable=False, server_default='EUR'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('external_id', name='uq_fact_supplier_balances_external_id'),
        )
        op.create_index('ix_fact_supplier_balances_external_id', 'fact_supplier_balances', ['external_id'])
        op.create_index('ix_fact_supplier_balances_balance_date', 'fact_supplier_balances', ['balance_date'])
        op.create_index('ix_fact_supplier_balances_supplier_ext_id', 'fact_supplier_balances', ['supplier_ext_id'])
        op.create_index('ix_fact_supplier_balances_branch_ext_id', 'fact_supplier_balances', ['branch_ext_id'])
        op.create_index('ix_fact_supplier_balances_updated_at', 'fact_supplier_balances', ['updated_at'])

    if not _table_exists(bind, 'agg_supplier_balances_daily'):
        op.create_table(
            'agg_supplier_balances_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('balance_date', sa.Date(), nullable=False),
            sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('open_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('overdue_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_0_30', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_31_60', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_61_90', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_90_plus', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('trend_vs_previous', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('suppliers', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('balance_date', 'supplier_ext_id', 'branch_ext_id', name='uq_agg_supplier_balances_daily_dims'),
        )
        op.create_index('ix_agg_supplier_balances_daily_balance_date', 'agg_supplier_balances_daily', ['balance_date'])
        op.create_index('ix_agg_supplier_balances_daily_supplier_ext_id', 'agg_supplier_balances_daily', ['supplier_ext_id'])
        op.create_index('ix_agg_supplier_balances_daily_branch_ext_id', 'agg_supplier_balances_daily', ['branch_ext_id'])


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'agg_supplier_balances_daily'):
        op.drop_index('ix_agg_supplier_balances_daily_branch_ext_id', table_name='agg_supplier_balances_daily')
        op.drop_index('ix_agg_supplier_balances_daily_supplier_ext_id', table_name='agg_supplier_balances_daily')
        op.drop_index('ix_agg_supplier_balances_daily_balance_date', table_name='agg_supplier_balances_daily')
        op.drop_table('agg_supplier_balances_daily')

    if _table_exists(bind, 'fact_supplier_balances'):
        op.drop_index('ix_fact_supplier_balances_updated_at', table_name='fact_supplier_balances')
        op.drop_index('ix_fact_supplier_balances_branch_ext_id', table_name='fact_supplier_balances')
        op.drop_index('ix_fact_supplier_balances_supplier_ext_id', table_name='fact_supplier_balances')
        op.drop_index('ix_fact_supplier_balances_balance_date', table_name='fact_supplier_balances')
        op.drop_index('ix_fact_supplier_balances_external_id', table_name='fact_supplier_balances')
        op.drop_table('fact_supplier_balances')

    if _table_exists(bind, 'agg_cash_accounts'):
        op.drop_index('ix_agg_cash_accounts_account_id', table_name='agg_cash_accounts')
        op.drop_index('ix_agg_cash_accounts_doc_date', table_name='agg_cash_accounts')
        op.drop_table('agg_cash_accounts')

    if _table_exists(bind, 'agg_cash_by_type'):
        op.drop_index('ix_agg_cash_by_type_subcategory', table_name='agg_cash_by_type')
        op.drop_index('ix_agg_cash_by_type_doc_date', table_name='agg_cash_by_type')
        op.drop_table('agg_cash_by_type')

    if _table_exists(bind, 'agg_cash_daily'):
        op.drop_index('ix_agg_cash_daily_account_id', table_name='agg_cash_daily')
        op.drop_index('ix_agg_cash_daily_transaction_type', table_name='agg_cash_daily')
        op.drop_index('ix_agg_cash_daily_subcategory', table_name='agg_cash_daily')
        op.drop_index('ix_agg_cash_daily_branch_ext_id', table_name='agg_cash_daily')
        op.drop_index('ix_agg_cash_daily_doc_date', table_name='agg_cash_daily')
        op.drop_table('agg_cash_daily')

    if _table_exists(bind, 'fact_cashflows'):
        cashflow_indexes = _index_names(bind, 'fact_cashflows')
        if 'ix_fact_cashflows_account_id' in cashflow_indexes:
            op.drop_index('ix_fact_cashflows_account_id', table_name='fact_cashflows')
        if 'ix_fact_cashflows_subcategory' in cashflow_indexes:
            op.drop_index('ix_fact_cashflows_subcategory', table_name='fact_cashflows')

        cashflow_cols = _column_names(bind, 'fact_cashflows')
        if 'counterparty_id' in cashflow_cols:
            op.drop_column('fact_cashflows', 'counterparty_id')
        if 'counterparty_type' in cashflow_cols:
            op.drop_column('fact_cashflows', 'counterparty_type')
        if 'account_id' in cashflow_cols:
            op.drop_column('fact_cashflows', 'account_id')
        if 'subcategory' in cashflow_cols:
            op.drop_column('fact_cashflows', 'subcategory')
        if 'transaction_type' in cashflow_cols:
            op.drop_column('fact_cashflows', 'transaction_type')
        if 'transaction_date' in cashflow_cols:
            op.drop_column('fact_cashflows', 'transaction_date')
        if 'transaction_id' in cashflow_cols:
            op.drop_column('fact_cashflows', 'transaction_id')
