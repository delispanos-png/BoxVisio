"""add image_url to dim_items

Revision ID: 20260306_0019_tenant
Revises: 20260306_0018_tenant
Create Date: 2026-03-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '20260306_0019_tenant'
down_revision: Union[str, Sequence[str], None] = '20260306_0018_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('dim_items', sa.Column('image_url', sa.String(length=1024), nullable=True))


def downgrade() -> None:
    op.drop_column('dim_items', 'image_url')
