"""tenant multi-branch alignment (branch dimension and company/branch aggregates)

Revision ID: 20260307_0005_tenant
Revises: 20260307_0004_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0005_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0004_tenant'
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


def _ensure_dim_branches_shape(bind) -> None:
    if not _table_exists(bind, 'dim_branches'):
        return

    cols = _column_names(bind, 'dim_branches')
    idx = _index_names(bind, 'dim_branches')

    if 'branch_code' not in cols:
        op.add_column('dim_branches', sa.Column('branch_code', sa.String(length=64), nullable=True))
    if 'branch_name' not in cols:
        op.add_column('dim_branches', sa.Column('branch_name', sa.String(length=255), nullable=True))
    if 'company_id' not in cols:
        op.add_column('dim_branches', sa.Column('company_id', sa.String(length=64), nullable=True))
    if 'location_metadata' not in cols:
        op.add_column('dim_branches', sa.Column('location_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    if 'ix_dim_branches_branch_code' not in idx:
        op.create_index('ix_dim_branches_branch_code', 'dim_branches', ['branch_code'])
    if 'ix_dim_branches_company_id' not in idx:
        op.create_index('ix_dim_branches_company_id', 'dim_branches', ['company_id'])

    op.execute(
        """
        UPDATE dim_branches
        SET
            branch_code = COALESCE(NULLIF(branch_code, ''), external_id),
            branch_name = COALESCE(NULLIF(branch_name, ''), name)
        """
    )


def _create_agg_sales_daily_company(bind) -> None:
    if _table_exists(bind, 'agg_sales_daily_company'):
        return
    op.create_table(
        'agg_sales_daily_company',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('branches', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('margin_pct', sa.Numeric(10, 4), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('doc_date', name='uq_agg_sales_daily_company_date'),
    )
    op.alter_column('agg_sales_daily_company', 'qty', server_default=None)
    op.alter_column('agg_sales_daily_company', 'net_value', server_default=None)
    op.alter_column('agg_sales_daily_company', 'gross_value', server_default=None)
    op.alter_column('agg_sales_daily_company', 'cost_amount', server_default=None)
    op.alter_column('agg_sales_daily_company', 'branches', server_default=None)
    op.alter_column('agg_sales_daily_company', 'margin_pct', server_default=None)
    op.alter_column('agg_sales_daily_company', 'created_at', server_default=None)
    op.alter_column('agg_sales_daily_company', 'updated_at', server_default=None)
    op.create_index('ix_agg_sales_daily_company_doc_date', 'agg_sales_daily_company', ['doc_date'])


def _create_agg_sales_daily_branch(bind) -> None:
    if _table_exists(bind, 'agg_sales_daily_branch'):
        return
    op.create_table(
        'agg_sales_daily_branch',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('contribution_pct', sa.Numeric(10, 4), nullable=False, server_default='0'),
        sa.Column('margin_pct', sa.Numeric(10, 4), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('doc_date', 'branch_ext_id', name='uq_agg_sales_daily_branch_dims'),
    )
    op.alter_column('agg_sales_daily_branch', 'qty', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'net_value', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'gross_value', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'cost_amount', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'contribution_pct', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'margin_pct', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'created_at', server_default=None)
    op.alter_column('agg_sales_daily_branch', 'updated_at', server_default=None)
    op.create_index('ix_agg_sales_daily_branch_doc_date', 'agg_sales_daily_branch', ['doc_date'])
    op.create_index('ix_agg_sales_daily_branch_branch_ext_id', 'agg_sales_daily_branch', ['branch_ext_id'])
    op.create_index('ix_agg_sales_daily_branch_date_branch', 'agg_sales_daily_branch', ['doc_date', 'branch_ext_id'])


def _create_agg_purchases_daily_company(bind) -> None:
    if _table_exists(bind, 'agg_purchases_daily_company'):
        return
    op.create_table(
        'agg_purchases_daily_company',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('branches', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('suppliers', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('doc_date', name='uq_agg_purchases_daily_company_date'),
    )
    op.alter_column('agg_purchases_daily_company', 'qty', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'net_value', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'cost_amount', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'branches', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'suppliers', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'created_at', server_default=None)
    op.alter_column('agg_purchases_daily_company', 'updated_at', server_default=None)
    op.create_index('ix_agg_purchases_daily_company_doc_date', 'agg_purchases_daily_company', ['doc_date'])


def _create_agg_purchases_daily_branch(bind) -> None:
    if _table_exists(bind, 'agg_purchases_daily_branch'):
        return
    op.create_table(
        'agg_purchases_daily_branch',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('contribution_pct', sa.Numeric(10, 4), nullable=False, server_default='0'),
        sa.Column('suppliers', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('doc_date', 'branch_ext_id', name='uq_agg_purchases_daily_branch_dims'),
    )
    op.alter_column('agg_purchases_daily_branch', 'qty', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'net_value', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'cost_amount', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'contribution_pct', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'suppliers', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'created_at', server_default=None)
    op.alter_column('agg_purchases_daily_branch', 'updated_at', server_default=None)
    op.create_index('ix_agg_purchases_daily_branch_doc_date', 'agg_purchases_daily_branch', ['doc_date'])
    op.create_index('ix_agg_purchases_daily_branch_branch_ext_id', 'agg_purchases_daily_branch', ['branch_ext_id'])
    op.create_index('ix_agg_purchases_daily_branch_date_branch', 'agg_purchases_daily_branch', ['doc_date', 'branch_ext_id'])


def _refresh_company_branch_aggregates(bind) -> None:
    if _table_exists(bind, 'agg_sales_daily_company'):
        op.execute('DELETE FROM agg_sales_daily_company')
        op.execute(
            """
            INSERT INTO agg_sales_daily_company (
                doc_date, qty, net_value, gross_value, cost_amount, branches, margin_pct, created_at, updated_at
            )
            SELECT
                fs.doc_date,
                COALESCE(SUM(fs.qty), 0),
                COALESCE(SUM(fs.net_value), 0),
                COALESCE(SUM(fs.gross_value), 0),
                COALESCE(SUM(fs.cost_amount), 0),
                COUNT(DISTINCT fs.branch_ext_id),
                CASE
                    WHEN COALESCE(SUM(fs.net_value), 0) = 0 THEN 0
                    ELSE (COALESCE(SUM(fs.net_value), 0) - COALESCE(SUM(fs.cost_amount), 0)) / COALESCE(SUM(fs.net_value), 0) * 100
                END,
                NOW(),
                NOW()
            FROM fact_sales fs
            GROUP BY fs.doc_date
            """
        )

    if _table_exists(bind, 'agg_sales_daily_branch'):
        op.execute('DELETE FROM agg_sales_daily_branch')
        op.execute(
            """
            WITH branch_rollup AS (
                SELECT
                    fs.doc_date,
                    fs.branch_ext_id,
                    COALESCE(SUM(fs.qty), 0) AS qty,
                    COALESCE(SUM(fs.net_value), 0) AS net_value,
                    COALESCE(SUM(fs.gross_value), 0) AS gross_value,
                    COALESCE(SUM(fs.cost_amount), 0) AS cost_amount
                FROM fact_sales fs
                GROUP BY fs.doc_date, fs.branch_ext_id
            ),
            day_rollup AS (
                SELECT
                    br.doc_date,
                    COALESCE(SUM(br.net_value), 0) AS total_net
                FROM branch_rollup br
                GROUP BY br.doc_date
            )
            INSERT INTO agg_sales_daily_branch (
                doc_date, branch_ext_id, qty, net_value, gross_value, cost_amount, contribution_pct, margin_pct, created_at, updated_at
            )
            SELECT
                br.doc_date,
                br.branch_ext_id,
                br.qty,
                br.net_value,
                br.gross_value,
                br.cost_amount,
                CASE WHEN dr.total_net = 0 THEN 0 ELSE br.net_value / dr.total_net * 100 END AS contribution_pct,
                CASE WHEN br.net_value = 0 THEN 0 ELSE (br.net_value - br.cost_amount) / br.net_value * 100 END AS margin_pct,
                NOW(),
                NOW()
            FROM branch_rollup br
            JOIN day_rollup dr ON dr.doc_date = br.doc_date
            """
        )

    if _table_exists(bind, 'agg_purchases_daily_company'):
        op.execute('DELETE FROM agg_purchases_daily_company')
        op.execute(
            """
            INSERT INTO agg_purchases_daily_company (
                doc_date, qty, net_value, cost_amount, branches, suppliers, created_at, updated_at
            )
            SELECT
                fp.doc_date,
                COALESCE(SUM(fp.qty), 0),
                COALESCE(SUM(fp.net_value), 0),
                COALESCE(SUM(fp.cost_amount), 0),
                COUNT(DISTINCT fp.branch_ext_id),
                COUNT(DISTINCT fp.supplier_ext_id),
                NOW(),
                NOW()
            FROM fact_purchases fp
            GROUP BY fp.doc_date
            """
        )

    if _table_exists(bind, 'agg_purchases_daily_branch'):
        op.execute('DELETE FROM agg_purchases_daily_branch')
        op.execute(
            """
            WITH branch_rollup AS (
                SELECT
                    fp.doc_date,
                    fp.branch_ext_id,
                    COALESCE(SUM(fp.qty), 0) AS qty,
                    COALESCE(SUM(fp.net_value), 0) AS net_value,
                    COALESCE(SUM(fp.cost_amount), 0) AS cost_amount,
                    COUNT(DISTINCT fp.supplier_ext_id) AS suppliers
                FROM fact_purchases fp
                GROUP BY fp.doc_date, fp.branch_ext_id
            ),
            day_rollup AS (
                SELECT
                    br.doc_date,
                    COALESCE(SUM(br.net_value), 0) AS total_net
                FROM branch_rollup br
                GROUP BY br.doc_date
            )
            INSERT INTO agg_purchases_daily_branch (
                doc_date, branch_ext_id, qty, net_value, cost_amount, contribution_pct, suppliers, created_at, updated_at
            )
            SELECT
                br.doc_date,
                br.branch_ext_id,
                br.qty,
                br.net_value,
                br.cost_amount,
                CASE WHEN dr.total_net = 0 THEN 0 ELSE br.net_value / dr.total_net * 100 END AS contribution_pct,
                br.suppliers,
                NOW(),
                NOW()
            FROM branch_rollup br
            JOIN day_rollup dr ON dr.doc_date = br.doc_date
            """
        )


def upgrade() -> None:
    bind = op.get_bind()
    _ensure_dim_branches_shape(bind)
    _create_agg_sales_daily_company(bind)
    _create_agg_sales_daily_branch(bind)
    _create_agg_purchases_daily_company(bind)
    _create_agg_purchases_daily_branch(bind)
    _refresh_company_branch_aggregates(bind)


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'agg_purchases_daily_branch'):
        idx = _index_names(bind, 'agg_purchases_daily_branch')
        for idx_name in (
            'ix_agg_purchases_daily_branch_date_branch',
            'ix_agg_purchases_daily_branch_branch_ext_id',
            'ix_agg_purchases_daily_branch_doc_date',
        ):
            if idx_name in idx:
                op.drop_index(idx_name, table_name='agg_purchases_daily_branch')
        op.drop_table('agg_purchases_daily_branch')

    if _table_exists(bind, 'agg_purchases_daily_company'):
        idx = _index_names(bind, 'agg_purchases_daily_company')
        if 'ix_agg_purchases_daily_company_doc_date' in idx:
            op.drop_index('ix_agg_purchases_daily_company_doc_date', table_name='agg_purchases_daily_company')
        op.drop_table('agg_purchases_daily_company')

    if _table_exists(bind, 'agg_sales_daily_branch'):
        idx = _index_names(bind, 'agg_sales_daily_branch')
        for idx_name in (
            'ix_agg_sales_daily_branch_date_branch',
            'ix_agg_sales_daily_branch_branch_ext_id',
            'ix_agg_sales_daily_branch_doc_date',
        ):
            if idx_name in idx:
                op.drop_index(idx_name, table_name='agg_sales_daily_branch')
        op.drop_table('agg_sales_daily_branch')

    if _table_exists(bind, 'agg_sales_daily_company'):
        idx = _index_names(bind, 'agg_sales_daily_company')
        if 'ix_agg_sales_daily_company_doc_date' in idx:
            op.drop_index('ix_agg_sales_daily_company_doc_date', table_name='agg_sales_daily_company')
        op.drop_table('agg_sales_daily_company')

    if _table_exists(bind, 'dim_branches'):
        idx = _index_names(bind, 'dim_branches')
        cols = _column_names(bind, 'dim_branches')
        if 'ix_dim_branches_company_id' in idx:
            op.drop_index('ix_dim_branches_company_id', table_name='dim_branches')
        if 'ix_dim_branches_branch_code' in idx:
            op.drop_index('ix_dim_branches_branch_code', table_name='dim_branches')
        if 'location_metadata' in cols:
            op.drop_column('dim_branches', 'location_metadata')
        if 'company_id' in cols:
            op.drop_column('dim_branches', 'company_id')
        if 'branch_name' in cols:
            op.drop_column('dim_branches', 'branch_name')
        if 'branch_code' in cols:
            op.drop_column('dim_branches', 'branch_code')
