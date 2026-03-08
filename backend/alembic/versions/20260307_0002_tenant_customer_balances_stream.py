"""tenant customer balances operational stream

Revision ID: 20260307_0002_tenant
Revises: 20260307_0001_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0002_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0001_tenant'
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

    if not _table_exists(bind, 'fact_customer_balances'):
        op.create_table(
            'fact_customer_balances',
            sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('external_id', sa.String(length=128), nullable=False),
            sa.Column('customer_ext_id', sa.String(length=128), nullable=True),
            sa.Column('customer_name', sa.String(length=255), nullable=True),
            sa.Column('balance_date', sa.Date(), nullable=False),
            sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('open_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('overdue_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_0_30', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_31_60', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_61_90', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_90_plus', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('last_collection_date', sa.Date(), nullable=True),
            sa.Column('trend_vs_previous', sa.Numeric(14, 2), nullable=True),
            sa.Column('currency', sa.String(length=3), nullable=False, server_default='EUR'),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
            sa.UniqueConstraint('external_id', name='uq_fact_customer_balances_external_id'),
        )

    fact_idx = _index_names(bind, 'fact_customer_balances')
    if 'ix_fact_customer_balances_external_id' not in fact_idx:
        op.create_index('ix_fact_customer_balances_external_id', 'fact_customer_balances', ['external_id'])
    if 'ix_fact_customer_balances_balance_date' not in fact_idx:
        op.create_index('ix_fact_customer_balances_balance_date', 'fact_customer_balances', ['balance_date'])
    if 'ix_fact_customer_balances_customer_ext_id' not in fact_idx:
        op.create_index('ix_fact_customer_balances_customer_ext_id', 'fact_customer_balances', ['customer_ext_id'])
    if 'ix_fact_customer_balances_branch_ext_id' not in fact_idx:
        op.create_index('ix_fact_customer_balances_branch_ext_id', 'fact_customer_balances', ['branch_ext_id'])
    if 'ix_fact_customer_balances_updated_at' not in fact_idx:
        op.create_index('ix_fact_customer_balances_updated_at', 'fact_customer_balances', ['updated_at'])

    if not _table_exists(bind, 'agg_customer_balances_daily'):
        op.create_table(
            'agg_customer_balances_daily',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('balance_date', sa.Date(), nullable=False),
            sa.Column('customer_ext_id', sa.String(length=128), nullable=True),
            sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
            sa.Column('open_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('overdue_balance', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_0_30', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_31_60', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_61_90', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('aging_bucket_90_plus', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('trend_vs_previous', sa.Numeric(14, 2), nullable=False, server_default='0'),
            sa.Column('customers', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('balance_date', 'customer_ext_id', 'branch_ext_id', name='uq_agg_customer_balances_daily_dims'),
        )

    agg_idx = _index_names(bind, 'agg_customer_balances_daily')
    if 'ix_agg_customer_balances_daily_balance_date' not in agg_idx:
        op.create_index('ix_agg_customer_balances_daily_balance_date', 'agg_customer_balances_daily', ['balance_date'])
    if 'ix_agg_customer_balances_daily_customer_ext_id' not in agg_idx:
        op.create_index('ix_agg_customer_balances_daily_customer_ext_id', 'agg_customer_balances_daily', ['customer_ext_id'])
    if 'ix_agg_customer_balances_daily_branch_ext_id' not in agg_idx:
        op.create_index('ix_agg_customer_balances_daily_branch_ext_id', 'agg_customer_balances_daily', ['branch_ext_id'])


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'agg_customer_balances_daily'):
        agg_idx = _index_names(bind, 'agg_customer_balances_daily')
        if 'ix_agg_customer_balances_daily_branch_ext_id' in agg_idx:
            op.drop_index('ix_agg_customer_balances_daily_branch_ext_id', table_name='agg_customer_balances_daily')
        if 'ix_agg_customer_balances_daily_customer_ext_id' in agg_idx:
            op.drop_index('ix_agg_customer_balances_daily_customer_ext_id', table_name='agg_customer_balances_daily')
        if 'ix_agg_customer_balances_daily_balance_date' in agg_idx:
            op.drop_index('ix_agg_customer_balances_daily_balance_date', table_name='agg_customer_balances_daily')
        op.drop_table('agg_customer_balances_daily')

    if _table_exists(bind, 'fact_customer_balances'):
        fact_idx = _index_names(bind, 'fact_customer_balances')
        if 'ix_fact_customer_balances_updated_at' in fact_idx:
            op.drop_index('ix_fact_customer_balances_updated_at', table_name='fact_customer_balances')
        if 'ix_fact_customer_balances_branch_ext_id' in fact_idx:
            op.drop_index('ix_fact_customer_balances_branch_ext_id', table_name='fact_customer_balances')
        if 'ix_fact_customer_balances_customer_ext_id' in fact_idx:
            op.drop_index('ix_fact_customer_balances_customer_ext_id', table_name='fact_customer_balances')
        if 'ix_fact_customer_balances_balance_date' in fact_idx:
            op.drop_index('ix_fact_customer_balances_balance_date', table_name='fact_customer_balances')
        if 'ix_fact_customer_balances_external_id' in fact_idx:
            op.drop_index('ix_fact_customer_balances_external_id', table_name='fact_customer_balances')
        op.drop_table('fact_customer_balances')
