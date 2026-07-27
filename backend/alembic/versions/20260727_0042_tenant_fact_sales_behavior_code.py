"""denormalise fact_sales.behavior_code out of source_payload_json

Every sales KPI filters on the SoftOne behaviour code, which lived only inside
source_payload_json. That meant a JSON extract plus an integer cast for each of
the ~1.2M fact_sales rows on every query. The value is now stored in its own
column, backfilled here and populated at ingest time.

Query code reads COALESCE(behavior_code, <json extract>), so a row this backfill
misses — or one written by a connector that does not yet supply the field —
still resolves to exactly the same value.

Revision ID: 20260727_0042_tenant
Revises: 20260727_0041_tenant
Create Date: 2026-07-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260727_0042_tenant'
down_revision: Union[str, Sequence[str], None] = '20260727_0041_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

BATCH = 100000


def _has_column(bind, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
            ),
            {'t': table, 'c': column},
        ).first()
    )


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_column(bind, 'fact_sales', 'behavior_code'):
        op.add_column('fact_sales', sa.Column('behavior_code', sa.Integer(), nullable=True))

    # Batched so a large tenant does not hold one long transaction over the
    # whole fact table while ingest is running.
    while True:
        moved = bind.execute(
            sa.text(
                """
                WITH victims AS (
                    SELECT id FROM fact_sales
                    WHERE behavior_code IS NULL
                      AND (source_payload_json->>'source_transaction_type_id') ~ '^-?[0-9]+$'
                    LIMIT :batch
                )
                UPDATE fact_sales f
                SET behavior_code = NULLIF(
                    regexp_replace(f.source_payload_json->>'source_transaction_type_id', '[^0-9-]', '', 'g'),
                    ''
                )::int
                FROM victims v
                WHERE f.id = v.id
                """
            ),
            {'batch': BATCH},
        ).rowcount
        if not moved:
            break

    op.execute(
        'CREATE INDEX IF NOT EXISTS ix_fact_sales_behavior_code_doc_date '
        'ON fact_sales (behavior_code, doc_date)'
    )


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS ix_fact_sales_behavior_code_doc_date')
    bind = op.get_bind()
    if _has_column(bind, 'fact_sales', 'behavior_code'):
        op.drop_column('fact_sales', 'behavior_code')
