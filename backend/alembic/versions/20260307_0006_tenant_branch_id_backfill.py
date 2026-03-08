"""tenant branch_id backfill for multi-branch facts

Revision ID: 20260307_0006_tenant
Revises: 20260307_0005_tenant
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect

revision: str = '20260307_0006_tenant'
down_revision: Union[str, Sequence[str], None] = '20260307_0005_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {col['name'] for col in inspect(bind).get_columns(table_name)}


def _backfill_branch_id(bind, table_name: str) -> None:
    cols = _column_names(bind, table_name)
    if 'branch_id' not in cols or 'branch_ext_id' not in cols:
        return
    op.execute(
        f"""
        UPDATE {table_name} f
        SET branch_id = b.id
        FROM dim_branches b
        WHERE f.branch_id IS NULL
          AND f.branch_ext_id IS NOT NULL
          AND b.external_id = f.branch_ext_id
        """
    )


def _refresh_sales_company_branch() -> None:
    op.execute(
        """
        DELETE FROM agg_sales_daily_company;
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
    op.execute(
        """
        DELETE FROM agg_sales_daily_branch;
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


def _refresh_purchases_company_branch() -> None:
    op.execute(
        """
        DELETE FROM agg_purchases_daily_company;
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
    op.execute(
        """
        DELETE FROM agg_purchases_daily_branch;
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

    if _table_exists(bind, 'fact_sales'):
        _backfill_branch_id(bind, 'fact_sales')
    if _table_exists(bind, 'fact_purchases'):
        _backfill_branch_id(bind, 'fact_purchases')
    if _table_exists(bind, 'fact_supplier_balances'):
        _backfill_branch_id(bind, 'fact_supplier_balances')
    if _table_exists(bind, 'fact_customer_balances'):
        _backfill_branch_id(bind, 'fact_customer_balances')

    if _table_exists(bind, 'agg_sales_daily_company') and _table_exists(bind, 'agg_sales_daily_branch'):
        _refresh_sales_company_branch()
    if _table_exists(bind, 'agg_purchases_daily_company') and _table_exists(bind, 'agg_purchases_daily_branch'):
        _refresh_purchases_company_branch()


def downgrade() -> None:
    # Irreversible data migration (backfill + aggregate refresh).
    pass
