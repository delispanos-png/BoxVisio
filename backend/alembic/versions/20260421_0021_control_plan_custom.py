"""control add custom plan, update plan limits

Revision ID: 20260421_0021_control
Revises: 20260408_0013_control
Create Date: 2026-04-21
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '20260421_0021_control'
down_revision: Union[str, Sequence[str], None] = '20260408_0013_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add 'custom' value to planname enum (PostgreSQL requires specific syntax)
    op.execute("ALTER TYPE planname ADD VALUE IF NOT EXISTS 'custom'")

    # Update plans table limits to match new policy
    op.execute("""
        UPDATE plans SET max_users = 3, max_branches = 1
        WHERE code = 'standard'
    """)
    op.execute("""
        UPDATE plans SET max_users = 5, max_branches = 5
        WHERE code = 'pro'
    """)
    op.execute("""
        UPDATE plans SET max_users = 50, max_branches = 10
        WHERE code = 'enterprise'
    """)

    # Insert custom plan if not exists
    op.execute("""
        INSERT INTO plans (code, display_name, feature_sales, feature_purchases,
                           feature_inventory, feature_cashflows, max_users, max_branches, is_active)
        VALUES ('custom', 'Custom', true, true, true, true, 9999, 9999, true)
        ON CONFLICT (code) DO UPDATE
            SET display_name = 'Custom',
                feature_sales = true,
                feature_purchases = true,
                feature_inventory = true,
                feature_cashflows = true,
                max_users = 9999,
                max_branches = 9999
    """)


def downgrade() -> None:
    # Restore old limits
    op.execute("UPDATE plans SET max_users = 5, max_branches = 5 WHERE code = 'standard'")
    op.execute("UPDATE plans SET max_users = 15, max_branches = 20 WHERE code = 'pro'")
    op.execute("UPDATE plans SET max_users = 100, max_branches = 1000 WHERE code = 'enterprise'")
    op.execute("DELETE FROM plans WHERE code = 'custom'")
    # Note: PostgreSQL does not support removing enum values — custom value remains in type
