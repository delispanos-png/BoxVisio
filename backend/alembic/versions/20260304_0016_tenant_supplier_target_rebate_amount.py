"""add rebate_amount to supplier_targets

Revision ID: 20260304_0016_tenant
Revises: 20260304_0015_tenant
Create Date: 2026-03-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260304_0016_tenant'
down_revision: Union[str, Sequence[str], None] = '20260304_0015_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'supplier_targets',
        sa.Column('rebate_amount', sa.Numeric(14, 2), nullable=False, server_default=sa.text('0')),
    )


def downgrade() -> None:
    op.drop_column('supplier_targets', 'rebate_amount')

