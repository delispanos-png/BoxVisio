"""tenant canonical data model with uuid keys

Revision ID: 20260228_0005_tenant
Revises: 20260228_0004_tenant
Create Date: 2026-02-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '20260228_0005_tenant'
down_revision: Union[str, Sequence[str], None] = '20260228_0004_tenant'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')

    # Rebuild canonical dimensions/facts with UUID PK/FK and strict constraints.
    op.execute('DROP TABLE IF EXISTS fact_cashflows CASCADE')
    op.execute('DROP TABLE IF EXISTS fact_inventory CASCADE')
    op.execute('DROP TABLE IF EXISTS fact_purchases CASCADE')
    op.execute('DROP TABLE IF EXISTS fact_sales CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_calendar CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_items CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_suppliers CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_groups CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_categories CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_brands CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_warehouses CASCADE')
    op.execute('DROP TABLE IF EXISTS dim_branches CASCADE')

    op.create_table(
        'dim_branches',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_branches_external_id'),
    )
    op.create_index('ix_dim_branches_external_id', 'dim_branches', ['external_id'])
    op.create_index('ix_dim_branches_updated_at', 'dim_branches', ['updated_at'])

    op.create_table(
        'dim_warehouses',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_warehouses_external_id'),
    )
    op.create_index('ix_dim_warehouses_external_id', 'dim_warehouses', ['external_id'])
    op.create_index('ix_dim_warehouses_updated_at', 'dim_warehouses', ['updated_at'])

    op.create_table(
        'dim_brands',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_brands_external_id'),
    )
    op.create_index('ix_dim_brands_external_id', 'dim_brands', ['external_id'])
    op.create_index('ix_dim_brands_updated_at', 'dim_brands', ['updated_at'])

    op.create_table(
        'dim_groups',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_groups_external_id'),
    )
    op.create_index('ix_dim_groups_external_id', 'dim_groups', ['external_id'])
    op.create_index('ix_dim_groups_updated_at', 'dim_groups', ['updated_at'])

    op.create_table(
        'dim_categories',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('parent_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_categories.id'), nullable=True),
        sa.Column('level', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_categories_external_id'),
    )
    op.create_index('ix_dim_categories_external_id', 'dim_categories', ['external_id'])
    op.create_index('ix_dim_categories_updated_at', 'dim_categories', ['updated_at'])

    op.create_table(
        'dim_suppliers',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_suppliers_external_id'),
    )
    op.create_index('ix_dim_suppliers_external_id', 'dim_suppliers', ['external_id'])
    op.create_index('ix_dim_suppliers_updated_at', 'dim_suppliers', ['updated_at'])

    op.create_table(
        'dim_items',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('sku', sa.String(length=128), nullable=True),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('brand_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_brands.id'), nullable=True),
        sa.Column('category_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_categories.id'), nullable=True),
        sa.Column('group_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_groups.id'), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_dim_items_external_id'),
    )
    op.create_index('ix_dim_items_external_id', 'dim_items', ['external_id'])
    op.create_index('ix_dim_items_updated_at', 'dim_items', ['updated_at'])

    op.create_table(
        'dim_calendar',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('full_date', sa.Date(), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('quarter', sa.Integer(), nullable=False),
        sa.Column('month', sa.Integer(), nullable=False),
        sa.Column('month_name', sa.String(length=16), nullable=False),
        sa.Column('week_of_year', sa.Integer(), nullable=False),
        sa.Column('day_of_month', sa.Integer(), nullable=False),
        sa.Column('day_of_week', sa.Integer(), nullable=False),
        sa.Column('is_weekend', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('full_date', name='uq_dim_calendar_full_date'),
    )
    op.create_index('ix_dim_calendar_full_date', 'dim_calendar', ['full_date'])
    op.create_index('ix_dim_calendar_updated_at', 'dim_calendar', ['updated_at'])

    op.create_table(
        'fact_sales',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
        sa.Column('item_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_items.id'), nullable=True),
        sa.Column('warehouse_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_warehouses.id'), nullable=True),
        sa.Column('brand_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_brands.id'), nullable=True),
        sa.Column('category_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_categories.id'), nullable=True),
        sa.Column('group_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_groups.id'), nullable=True),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('profit_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_fact_sales_external_id'),
        sa.UniqueConstraint('event_id', name='uq_sales_event_id'),
    )
    op.create_index('ix_fact_sales_doc_date', 'fact_sales', ['doc_date'])
    op.create_index('ix_fact_sales_updated_at', 'fact_sales', ['updated_at'])
    op.create_index('ix_fact_sales_doc_date_branch_id', 'fact_sales', ['doc_date', 'branch_id'])

    op.create_table(
        'fact_purchases',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
        sa.Column('item_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_items.id'), nullable=True),
        sa.Column('supplier_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_suppliers.id'), nullable=True),
        sa.Column('warehouse_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_warehouses.id'), nullable=True),
        sa.Column('brand_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_brands.id'), nullable=True),
        sa.Column('category_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_categories.id'), nullable=True),
        sa.Column('group_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_groups.id'), nullable=True),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_fact_purchases_external_id'),
        sa.UniqueConstraint('event_id', name='uq_purchases_event_id'),
    )
    op.create_index('ix_fact_purchases_doc_date', 'fact_purchases', ['doc_date'])
    op.create_index('ix_fact_purchases_updated_at', 'fact_purchases', ['updated_at'])
    op.create_index('ix_fact_purchases_doc_date_branch_id', 'fact_purchases', ['doc_date', 'branch_id'])

    op.create_table(
        'fact_inventory',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
        sa.Column('item_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_items.id'), nullable=True),
        sa.Column('warehouse_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_warehouses.id'), nullable=True),
        sa.Column('qty_on_hand', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('qty_reserved', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('cost_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('value_amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_fact_inventory_external_id'),
    )
    op.create_index('ix_fact_inventory_updated_at', 'fact_inventory', ['updated_at'])
    op.create_index('ix_fact_inventory_doc_date_branch_id', 'fact_inventory', ['doc_date', 'branch_id'])

    op.create_table(
        'fact_cashflows',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('external_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('branch_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dim_branches.id'), nullable=True),
        sa.Column('entry_type', sa.String(length=32), nullable=False),
        sa.Column('amount', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='EUR'),
        sa.Column('reference_no', sa.String(length=64), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.UniqueConstraint('external_id', name='uq_fact_cashflows_external_id'),
    )
    op.create_index('ix_fact_cashflows_updated_at', 'fact_cashflows', ['updated_at'])
    op.create_index('ix_fact_cashflows_doc_date_branch_id', 'fact_cashflows', ['doc_date', 'branch_id'])


def downgrade() -> None:
    op.drop_index('ix_fact_cashflows_doc_date_branch_id', table_name='fact_cashflows')
    op.drop_index('ix_fact_cashflows_updated_at', table_name='fact_cashflows')
    op.drop_table('fact_cashflows')

    op.drop_index('ix_fact_inventory_doc_date_branch_id', table_name='fact_inventory')
    op.drop_index('ix_fact_inventory_updated_at', table_name='fact_inventory')
    op.drop_table('fact_inventory')

    op.drop_index('ix_fact_purchases_doc_date_branch_id', table_name='fact_purchases')
    op.drop_index('ix_fact_purchases_updated_at', table_name='fact_purchases')
    op.drop_index('ix_fact_purchases_doc_date', table_name='fact_purchases')
    op.drop_table('fact_purchases')

    op.drop_index('ix_fact_sales_doc_date_branch_id', table_name='fact_sales')
    op.drop_index('ix_fact_sales_updated_at', table_name='fact_sales')
    op.drop_index('ix_fact_sales_doc_date', table_name='fact_sales')
    op.drop_table('fact_sales')

    op.drop_index('ix_dim_calendar_updated_at', table_name='dim_calendar')
    op.drop_index('ix_dim_calendar_full_date', table_name='dim_calendar')
    op.drop_table('dim_calendar')

    op.drop_index('ix_dim_items_updated_at', table_name='dim_items')
    op.drop_index('ix_dim_items_external_id', table_name='dim_items')
    op.drop_table('dim_items')

    op.drop_index('ix_dim_suppliers_updated_at', table_name='dim_suppliers')
    op.drop_index('ix_dim_suppliers_external_id', table_name='dim_suppliers')
    op.drop_table('dim_suppliers')

    op.drop_index('ix_dim_groups_updated_at', table_name='dim_groups')
    op.drop_index('ix_dim_groups_external_id', table_name='dim_groups')
    op.drop_table('dim_groups')

    op.drop_index('ix_dim_categories_updated_at', table_name='dim_categories')
    op.drop_index('ix_dim_categories_external_id', table_name='dim_categories')
    op.drop_table('dim_categories')

    op.drop_index('ix_dim_brands_updated_at', table_name='dim_brands')
    op.drop_index('ix_dim_brands_external_id', table_name='dim_brands')
    op.drop_table('dim_brands')

    op.drop_index('ix_dim_warehouses_updated_at', table_name='dim_warehouses')
    op.drop_index('ix_dim_warehouses_external_id', table_name='dim_warehouses')
    op.drop_table('dim_warehouses')

    op.drop_index('ix_dim_branches_updated_at', table_name='dim_branches')
    op.drop_index('ix_dim_branches_external_id', table_name='dim_branches')
    op.drop_table('dim_branches')

    # restore legacy minimal schema (pre-canonical)
    op.create_table(
        'dim_branches',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_warehouses',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_brands',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
    )
    op.create_table(
        'dim_categories',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ext_id', sa.String(length=64), nullable=False, unique=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('level', sa.Integer(), nullable=False, server_default='1'),
    )
    op.create_table(
        'fact_sales',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.Column('gross_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.UniqueConstraint('event_id', name='uq_sales_event_id'),
    )
    op.create_index('ix_fact_sales_doc_date', 'fact_sales', ['doc_date'])

    op.create_table(
        'fact_purchases',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('doc_date', sa.Date(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('branch_ext_id', sa.String(length=64), nullable=True),
        sa.Column('warehouse_ext_id', sa.String(length=64), nullable=True),
        sa.Column('supplier_ext_id', sa.String(length=64), nullable=True),
        sa.Column('brand_ext_id', sa.String(length=64), nullable=True),
        sa.Column('category_ext_id', sa.String(length=64), nullable=True),
        sa.Column('group_ext_id', sa.String(length=64), nullable=True),
        sa.Column('item_code', sa.String(length=128), nullable=True),
        sa.Column('qty', sa.Numeric(18, 4), nullable=False, server_default='0'),
        sa.Column('net_value', sa.Numeric(14, 2), nullable=False, server_default='0'),
        sa.UniqueConstraint('event_id', name='uq_purchases_event_id'),
    )
    op.create_index('ix_fact_purchases_doc_date', 'fact_purchases', ['doc_date'])
