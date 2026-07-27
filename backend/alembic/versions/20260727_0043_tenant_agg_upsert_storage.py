"""agg_* storage changes for the upsert refresh path

Two changes that together let the aggregate refresh update rows in place instead
of deleting and re-inserting the whole window:

1. NULLS NOT DISTINCT on every agg_* uniqueness constraint that covers a
   nullable dimension. Under the default NULLS DISTINCT, ON CONFLICT never
   matches a row whose dimension is NULL — agg_sales_monthly.category_ext_id is
   NULL for every row — so each refresh would append duplicates rather than
   update. Requires PostgreSQL 15+.

2. fillfactor 80 on agg_* so the resulting updates can stay HOT and skip index
   maintenance.

Revision ID: 20260727_0043_tenant
Revises: 20260727_0042_tenant
Create Date: 2026-07-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260727_0043_tenant'
down_revision: Union[str, Sequence[str], None] = '20260727_0042_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _agg_tables(bind) -> list[str]:
    return [
        r[0]
        for r in bind.execute(
            sa.text(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relkind = 'r' AND n.nspname = 'public' AND c.relname LIKE 'agg\\_%'"
            )
        )
    ]


def _nullable_unique_indexes(bind):
    """agg_* unique indexes covering at least one nullable column."""
    return bind.execute(
        sa.text(
            """
            SELECT c.relname,
                   i.indexrelid::regclass::text,
                   COALESCE(con.conname, ''),
                   pg_get_indexdef(i.indexrelid)
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
            WHERE n.nspname = 'public'
              AND c.relname LIKE 'agg\\_%'
              AND i.indisunique AND NOT i.indisprimary
              AND NOT i.indnullsnotdistinct
              AND EXISTS (
                  SELECT 1 FROM unnest(i.indkey) k
                  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k
                  WHERE NOT a.attnotnull
              )
            """
        )
    ).all()


def upgrade() -> None:
    bind = op.get_bind()

    for table, index_ref, conname, indexdef in _nullable_unique_indexes(bind):
        cols = indexdef[indexdef.rindex('(') + 1: indexdef.rindex(')')]
        if conname:
            op.execute(f'ALTER TABLE public.{table} DROP CONSTRAINT {conname}')
            op.execute(
                f'ALTER TABLE public.{table} ADD CONSTRAINT {conname} '
                f'UNIQUE NULLS NOT DISTINCT ({cols})'
            )
        else:
            idxname = index_ref.split('.')[-1].strip('"')
            op.execute(f'DROP INDEX public.{idxname}')
            op.execute(
                f'CREATE UNIQUE INDEX {idxname} ON public.{table} '
                f'USING btree ({cols}) NULLS NOT DISTINCT'
            )

    for table in _agg_tables(bind):
        op.execute(f'ALTER TABLE public.{table} SET (fillfactor = 80)')


def downgrade() -> None:
    bind = op.get_bind()
    for table in _agg_tables(bind):
        op.execute(f'ALTER TABLE public.{table} RESET (fillfactor)')
    # NULLS NOT DISTINCT is intentionally not reverted: going back would let the
    # duplicate rows it prevents reappear on the next refresh.
