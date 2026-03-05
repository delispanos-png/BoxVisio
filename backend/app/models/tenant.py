from datetime import date, datetime
from uuid import UUID

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import TenantBase


class DimBranch(TenantBase):
    __tablename__ = 'dim_branches'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimWarehouse(TenantBase):
    __tablename__ = 'dim_warehouses'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimBrand(TenantBase):
    __tablename__ = 'dim_brands'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimCategory(TenantBase):
    __tablename__ = 'dim_categories'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_categories.id'), nullable=True)
    level: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimGroup(TenantBase):
    __tablename__ = 'dim_groups'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimSupplier(TenantBase):
    __tablename__ = 'dim_suppliers'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimItem(TenantBase):
    __tablename__ = 'dim_items'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    brand_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_brands.id'), nullable=True)
    category_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_categories.id'), nullable=True)
    group_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_groups.id'), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimCalendar(TenantBase):
    __tablename__ = 'dim_calendar'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    full_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False, index=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    month_name: Mapped[str] = mapped_column(String(16), nullable=False)
    week_of_year: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_month: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactSales(TenantBase):
    __tablename__ = 'fact_sales'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_sales_external_id'),
        UniqueConstraint('event_id', name='uq_sales_event_id'),
        Index('ix_fact_sales_doc_date_branch_id', 'doc_date', 'branch_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    item_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_items.id'), nullable=True)
    warehouse_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_warehouses.id'), nullable=True)
    brand_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_brands.id'), nullable=True)
    category_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_categories.id'), nullable=True)
    group_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_groups.id'), nullable=True)

    # Backward-compatible fields currently used by ingestion/KPIs.
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    item_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    gross_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    profit_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactPurchases(TenantBase):
    __tablename__ = 'fact_purchases'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_purchases_external_id'),
        UniqueConstraint('event_id', name='uq_purchases_event_id'),
        Index('ix_fact_purchases_doc_date_branch_id', 'doc_date', 'branch_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    item_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_items.id'), nullable=True)
    supplier_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_suppliers.id'), nullable=True)
    warehouse_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_warehouses.id'), nullable=True)
    brand_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_brands.id'), nullable=True)
    category_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_categories.id'), nullable=True)
    group_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_groups.id'), nullable=True)

    # Backward-compatible fields currently used by ingestion/KPIs.
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    item_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactInventory(TenantBase):
    __tablename__ = 'fact_inventory'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_inventory_external_id'),
        Index('ix_fact_inventory_doc_date_branch_id', 'doc_date', 'branch_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    item_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_items.id'), nullable=True)
    warehouse_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_warehouses.id'), nullable=True)
    qty_on_hand: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    qty_reserved: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    value_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactCashflow(TenantBase):
    __tablename__ = 'fact_cashflows'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_cashflows_external_id'),
        Index('ix_fact_cashflows_doc_date_branch_id', 'doc_date', 'branch_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    entry_type: Mapped[str] = mapped_column(String(32), nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    reference_no: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StagingIngestEvent(TenantBase):
    __tablename__ = 'staging_ingest_events'
    __table_args__ = (UniqueConstraint('event_id', 'entity', name='uq_staging_event_entity'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity: Mapped[str] = mapped_column(String(64), nullable=False)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class SyncState(TenantBase):
    __tablename__ = 'sync_state'
    __table_args__ = (UniqueConstraint('connector_type', name='uq_sync_state_connector_type'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    last_sync_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sync_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class IngestDeadLetter(TenantBase):
    __tablename__ = 'ingest_dead_letter'
    __table_args__ = (
        Index('ix_ingest_dead_letter_connector_entity', 'connector_type', 'entity'),
        Index('ix_ingest_dead_letter_created_at', 'created_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity: Mapped[str] = mapped_column(String(32), nullable=False)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_message: Mapped[str] = mapped_column(String(1024), nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggSalesDaily(TenantBase):
    __tablename__ = 'agg_sales_daily'
    __table_args__ = (
        UniqueConstraint(
            'doc_date',
            'branch_ext_id',
            'warehouse_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_sales_daily_dims',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    gross_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggSalesMonthly(TenantBase):
    __tablename__ = 'agg_sales_monthly'
    __table_args__ = (
        UniqueConstraint(
            'month_start',
            'branch_ext_id',
            'warehouse_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_sales_monthly_dims',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    month_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    gross_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggSalesItemDaily(TenantBase):
    __tablename__ = 'agg_sales_item_daily'
    __table_args__ = (
        UniqueConstraint('doc_date', 'item_external_id', name='uq_agg_sales_item_daily_item'),
        Index('ix_agg_sales_item_daily_doc_item', 'doc_date', 'item_external_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    item_external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggPurchasesDaily(TenantBase):
    __tablename__ = 'agg_purchases_daily'
    __table_args__ = (
        UniqueConstraint(
            'doc_date',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_purchases_daily_dims',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggPurchasesMonthly(TenantBase):
    __tablename__ = 'agg_purchases_monthly'
    __table_args__ = (
        UniqueConstraint(
            'month_start',
            'branch_ext_id',
            'warehouse_ext_id',
            'supplier_ext_id',
            'brand_ext_id',
            'category_ext_id',
            'group_ext_id',
            name='uq_agg_purchases_monthly_dims',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    month_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    brand_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    category_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    group_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggInventorySnapshotDaily(TenantBase):
    __tablename__ = 'agg_inventory_snapshot_daily'
    __table_args__ = (
        UniqueConstraint('snapshot_date', 'item_external_id', name='uq_agg_inventory_snapshot_daily_item'),
        Index('ix_agg_inventory_snapshot_daily_date_item', 'snapshot_date', 'item_external_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    item_external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    qty_on_hand: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    value_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class InsightRule(TenantBase):
    __tablename__ = 'insight_rules'
    __table_args__ = (
        UniqueConstraint('code', name='uq_insight_rules_code'),
        Index('ix_insight_rules_enabled_category', 'enabled', 'category'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    code: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    severity_default: Mapped[str] = mapped_column(String(16), nullable=False, default='warning')
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    params_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    scope: Mapped[str] = mapped_column(Text, nullable=False, default='tenant')
    schedule: Mapped[str] = mapped_column(Text, nullable=False, default='daily')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Insight(TenantBase):
    __tablename__ = 'insights'
    __table_args__ = (
        Index('ix_insights_created_at_desc', text('created_at DESC')),
        Index('ix_insights_rule_code_created_at', 'rule_code', 'created_at'),
        Index('ix_insights_status', 'status'),
        Index('ix_insights_entity', 'entity_type', 'entity_external_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    rule_code: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_external_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    period_from: Mapped[date] = mapped_column(Date, nullable=False)
    period_to: Mapped[date] = mapped_column(Date, nullable=False)
    value: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    baseline_value: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    delta_value: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    delta_pct: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default='open')
    acknowledged_by: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class InsightRun(TenantBase):
    __tablename__ = 'insight_runs'
    __table_args__ = (
        Index('ix_insight_runs_started_at', 'started_at'),
        Index('ix_insight_runs_status', 'status'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    rules_executed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    insights_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class SupplierTarget(TenantBase):
    __tablename__ = 'supplier_targets'
    __table_args__ = (
        UniqueConstraint('supplier_ext_id', 'target_year', 'name', name='uq_supplier_targets_supplier_year_name'),
        Index('ix_supplier_targets_target_year', 'target_year'),
        Index('ix_supplier_targets_supplier_ext_id', 'supplier_ext_id'),
        Index('ix_supplier_targets_is_active', 'is_active'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    name: Mapped[str] = mapped_column(String(255), nullable=False, default='Default Target')
    supplier_ext_id: Mapped[str] = mapped_column(String(64), nullable=False)
    supplier_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    target_amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    rebate_percent: Mapped[float] = mapped_column(Numeric(8, 4), nullable=False, default=0)
    rebate_amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SupplierTargetItem(TenantBase):
    __tablename__ = 'supplier_target_items'
    __table_args__ = (
        UniqueConstraint('supplier_target_id', 'item_external_id', name='uq_supplier_target_items_target_item'),
        Index('ix_supplier_target_items_target_id', 'supplier_target_id'),
        Index('ix_supplier_target_items_item_external_id', 'item_external_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    supplier_target_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey('supplier_targets.id', ondelete='CASCADE'),
        nullable=False,
    )
    item_external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    item_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
