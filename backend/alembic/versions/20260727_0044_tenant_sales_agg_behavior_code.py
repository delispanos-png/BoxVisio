"""add behavior_code as a dimension on the sales aggregates

The executive dashboard fell back to scanning fact_sales because the
sales_kpi_config participation whitelist (behaviour codes) could not be expressed
against the aggregates. Carrying behavior_code as a dimension turns that filter
into a plain WHERE on read.

Verified equivalent before rollout with a shadow aggregate built from the exact
production refresh SQL: 504 days, 5 dimensions and 19 months compared row by row
with zero differences in value or quantity. The single intended change is that
category breakdowns now use the aggregate's 3-level ITEMCAT grouping (319 values)
instead of the query path's 1-level grouping (23) — totals are unaffected.

Revision ID: 20260727_0044_tenant
Revises: 20260727_0043_tenant
Create Date: 2026-07-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260727_0044_tenant'
down_revision: Union[str, Sequence[str], None] = '20260727_0043_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# table -> (constraint name, key columns after adding behavior_code)
TARGETS = {
    'agg_sales_daily': (
        'uq_agg_sales_daily_dims',
        'doc_date, behavior_code, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id',
        'doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id',
    ),
    'agg_sales_daily_company': (
        'uq_agg_sales_daily_company_date',
        'doc_date, behavior_code',
        'doc_date',
    ),
    'agg_sales_monthly': (
        'uq_agg_sales_monthly_dims',
        'month_start, behavior_code, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id',
        'month_start, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id',
    ),
}


def _has_column(bind, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=:t AND column_name=:c"
            ),
            {'t': table, 'c': column},
        ).first()
    )


def _has_constraint(bind, table: str, name: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM pg_constraint "
                "WHERE conname=:n AND conrelid=to_regclass('public.'||:t)"
            ),
            {'n': name, 't': table},
        ).first()
    )


def upgrade() -> None:
    bind = op.get_bind()
    for table, (conname, new_cols, _old_cols) in TARGETS.items():
        if not _has_column(bind, table, 'behavior_code'):
            op.add_column(table, sa.Column('behavior_code', sa.Integer(), nullable=True))
        if _has_constraint(bind, table, conname):
            op.execute(f'ALTER TABLE public.{table} DROP CONSTRAINT {conname}')
        op.execute(
            f'ALTER TABLE public.{table} ADD CONSTRAINT {conname} '
            f'UNIQUE NULLS NOT DISTINCT ({new_cols})'
        )
    # Existing rows keep behavior_code NULL until the next refresh, which upserts
    # the new key and lets _delete_stale_aggregate_rows drop the un-keyed leftovers.


def downgrade() -> None:
    bind = op.get_bind()
    for table, (conname, _new_cols, old_cols) in TARGETS.items():
        if _has_constraint(bind, table, conname):
            op.execute(f'ALTER TABLE public.{table} DROP CONSTRAINT {conname}')
        # Collapsing the key can collide on rows that differ only by behaviour
        # code, so clear them out first — a refresh rebuilds them.
        op.execute(f'DELETE FROM public.{table}')
        op.execute(
            f'ALTER TABLE public.{table} ADD CONSTRAINT {conname} '
            f'UNIQUE NULLS NOT DISTINCT ({old_cols})'
        )
        if _has_column(bind, table, 'behavior_code'):
            op.drop_column(table, 'behavior_code')
