"""widen agg_* surrogate primary keys from int4 to int8

agg_stock_aging_id_seq reached 2,147,483,647 on the pharmacy295 tenant and the
inventory aggregate refresh started failing outright with
SequenceGeneratorLimitExceededError. Every agg_* table is rebuilt with the same
DELETE + INSERT cycle, so they all burn through the sequence range; the two
inventory tables just got there first.

Revision ID: 20260727_0041_tenant
Revises: 20260605_0040_tenant
Create Date: 2026-07-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260727_0041_tenant'
down_revision: Union[str, Sequence[str], None] = '20260605_0040_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


AGG_TABLES = (
    'agg_cash_daily',
    'agg_cash_by_type',
    'agg_cash_accounts',
    'agg_supplier_balances_daily',
    'agg_customer_balances_daily',
    'agg_expenses_daily',
    'agg_expenses_monthly',
    'agg_expenses_by_category_daily',
    'agg_expenses_by_branch_daily',
    'agg_sales_daily',
    'agg_sales_daily_company',
    'agg_sales_daily_branch',
    'agg_sales_monthly',
    'agg_sales_item_daily',
    'agg_purchases_daily',
    'agg_purchases_daily_company',
    'agg_purchases_daily_branch',
    'agg_purchases_monthly',
    'agg_inventory_snapshot_daily',
    'agg_stock_aging',
)

# agg_inventory_snapshot_daily.id is referenced by this passthrough view, which
# blocks ALTER COLUMN TYPE and has to be dropped and rebuilt around it.
SNAPSHOT_VIEW = 'agg_inventory_snapshot'
SNAPSHOT_VIEW_SQL = (
    'CREATE VIEW agg_inventory_snapshot AS '
    'SELECT id, snapshot_date, item_external_id, qty_on_hand, value_amount, '
    'updated_at, created_at FROM agg_inventory_snapshot_daily'
)


def _id_type(bind, table: str) -> str | None:
    return bind.execute(
        sa.text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t AND column_name = 'id'"
        ),
        {'t': table},
    ).scalar()


def _has_view(bind, name: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.views "
                "WHERE table_schema = 'public' AND table_name = :v"
            ),
            {'v': name},
        ).first()
    )


def _convert(bind, table: str, to_type: str) -> None:
    op.execute(f'ALTER TABLE public.{table} ALTER COLUMN id TYPE {to_type}')
    op.execute(
        f'ALTER SEQUENCE public.{table}_id_seq AS {to_type} '
        f'MAXVALUE {9223372036854775807 if to_type == "bigint" else 2147483647}'
    )


def upgrade() -> None:
    bind = op.get_bind()
    for table in AGG_TABLES:
        if _id_type(bind, table) != 'integer':
            continue
        rebuild_view = table == 'agg_inventory_snapshot_daily' and _has_view(bind, SNAPSHOT_VIEW)
        if rebuild_view:
            op.execute(f'DROP VIEW {SNAPSHOT_VIEW}')
        _convert(bind, table, 'bigint')
        if rebuild_view:
            op.execute(SNAPSHOT_VIEW_SQL)


def downgrade() -> None:
    # Narrowing back to int4 only succeeds while every id still fits; on a tenant
    # that already overflowed it will fail loudly rather than truncate.
    bind = op.get_bind()
    for table in reversed(AGG_TABLES):
        if _id_type(bind, table) != 'bigint':
            continue
        rebuild_view = table == 'agg_inventory_snapshot_daily' and _has_view(bind, SNAPSHOT_VIEW)
        if rebuild_view:
            op.execute(f'DROP VIEW {SNAPSHOT_VIEW}')
        _convert(bind, table, 'integer')
        if rebuild_view:
            op.execute(SNAPSHOT_VIEW_SQL)
