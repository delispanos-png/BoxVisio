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
    branch_code: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    branch_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    location_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
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


class DimExpenseCategory(TenantBase):
    __tablename__ = 'dim_expense_categories'
    __table_args__ = (
        UniqueConstraint('category_code', name='uq_dim_expense_categories_category_code'),
        Index('ix_dim_expense_categories_external_id', 'external_id'),
        Index('ix_dim_expense_categories_category_name', 'category_name'),
        Index('ix_dim_expense_categories_parent_category_id', 'parent_category_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    category_code: Mapped[str] = mapped_column(String(128), nullable=False)
    category_name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent_category_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey('dim_expense_categories.id'),
        nullable=True,
    )
    level_no: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    classification: Mapped[str] = mapped_column(String(32), nullable=False, default='opex', index=True)
    gl_account_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
        index=True,
    )
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


class DimCustomer(TenantBase):
    __tablename__ = 'dim_customers'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    customer_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimAccount(TenantBase):
    __tablename__ = 'dim_accounts'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimDocumentType(TenantBase):
    __tablename__ = 'dim_document_types'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    stream: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimPaymentMethod(TenantBase):
    __tablename__ = 'dim_payment_methods'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DimItem(TenantBase):
    __tablename__ = 'dim_items'

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    main_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vat_rate: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    vat_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    use_batch: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    commercial_category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category_1: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category_2: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category_3: Mapped[str | None] = mapped_column(String(255), nullable=True)
    model_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    business_unit_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    unit2: Mapped[str | None] = mapped_column(String(64), nullable=True)
    purchase_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sales_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rel_2_to_1: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    rel_purchase_to_1: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    rel_sale_to_1: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    strict_rel_2_to_1: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    strict_purchase_rel: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    strict_sale_rel: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    abc_category: Mapped[str | None] = mapped_column(String(32), nullable=True)
    image_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    discount_pct: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    is_active_source: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
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
        Index('ix_fact_sales_doc_date_item_id', 'doc_date', 'item_id'),
        Index('ix_fact_sales_doc_date_customer_id', 'doc_date', 'customer_id'),
        Index('ix_fact_sales_branch_item_doc_date', 'branch_id', 'item_id', 'doc_date'),
        Index('ix_fact_sales_document_id_doc_date', 'document_id', 'doc_date'),
        Index('ix_fact_sales_document_no', 'document_no'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    item_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_items.id'), nullable=True)
    customer_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_customers.id'), nullable=True)
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
    document_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_no: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_series: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_status: Mapped[str | None] = mapped_column(String(128), nullable=True)
    eshop_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    customer_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payment_method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    shipping_method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    origin_ref: Mapped[str | None] = mapped_column(String(128), nullable=True)
    destination_ref: Mapped[str | None] = mapped_column(String(128), nullable=True)
    delivery_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_zip: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delivery_city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    delivery_area: Mapped[str | None] = mapped_column(String(128), nullable=True)
    movement_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    carrier_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    transport_medium: Mapped[str | None] = mapped_column(String(128), nullable=True)
    transport_no: Mapped[str | None] = mapped_column(String(128), nullable=True)
    route_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    loading_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    delivery_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes_2: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source_created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source_updated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    line_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    qty_executed: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    unit_price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    discount_pct: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    discount_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    vat_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    source_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

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
        Index('ix_fact_purchases_doc_date_supplier_id', 'doc_date', 'supplier_id'),
        Index('ix_fact_purchases_branch_supplier_doc_date', 'branch_id', 'supplier_id', 'doc_date'),
        Index('ix_fact_purchases_document_id_doc_date', 'document_id', 'doc_date'),
        Index('ix_fact_purchases_document_no', 'document_no'),
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
    document_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_no: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_series: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_module_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    redirect_module_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    object_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    source_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    item_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    discount1_pct: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    discount2_pct: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    discount3_pct: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    discount1_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    discount2_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    discount3_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    discount_pct: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    discount_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactInventory(TenantBase):
    __tablename__ = 'fact_inventory'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_inventory_external_id'),
        Index('ix_fact_inventory_doc_date_branch_id', 'doc_date', 'branch_id'),
        Index('ix_fact_inventory_item_id_doc_date', 'item_id', 'doc_date'),
        Index('ix_fact_inventory_warehouse_id_doc_date', 'warehouse_id', 'doc_date'),
        Index('ix_fact_inventory_document_id_doc_date', 'document_id', 'doc_date'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    item_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_items.id'), nullable=True)
    warehouse_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_warehouses.id'), nullable=True)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    warehouse_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    item_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_no: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_series: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    movement_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_module_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    redirect_module_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    object_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    source_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
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
        Index('ix_fact_cashflows_transaction_date_branch_id', 'transaction_date', 'branch_id'),
        Index('ix_fact_cashflows_transaction_date_account_id', 'transaction_date', 'account_id'),
        Index('ix_fact_cashflows_subcategory_transaction_date', 'subcategory', 'transaction_date'),
        Index('ix_fact_cashflows_subcategory', 'subcategory'),
        Index('ix_fact_cashflows_account_id', 'account_id'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    transaction_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    transaction_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    entry_type: Mapped[str] = mapped_column(String(32), nullable=False)
    transaction_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    subcategory: Mapped[str | None] = mapped_column(String(32), nullable=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    counterparty_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    counterparty_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    reference_no: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactSupplierBalance(TenantBase):
    __tablename__ = 'fact_supplier_balances'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_supplier_balances_external_id'),
        Index('ix_fact_supplier_balances_balance_date', 'balance_date'),
        Index('ix_fact_supplier_balances_balance_date_supplier_id', 'balance_date', 'supplier_id'),
        Index('ix_fact_supplier_balances_balance_date_branch_id', 'balance_date', 'branch_id'),
        Index('ix_fact_supplier_balances_supplier_ext_id', 'supplier_ext_id'),
        Index('ix_fact_supplier_balances_branch_ext_id', 'branch_ext_id'),
        Index('ix_fact_supplier_balances_updated_at', 'updated_at'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    supplier_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_suppliers.id'), nullable=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    balance_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    open_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    overdue_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_0_30: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_31_60: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_61_90: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_90_plus: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    last_payment_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    trend_vs_previous: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactCustomerBalance(TenantBase):
    __tablename__ = 'fact_customer_balances'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_customer_balances_external_id'),
        Index('ix_fact_customer_balances_balance_date', 'balance_date'),
        Index('ix_fact_customer_balances_balance_date_customer_id', 'balance_date', 'customer_id'),
        Index('ix_fact_customer_balances_balance_date_branch_id', 'balance_date', 'branch_id'),
        Index('ix_fact_customer_balances_customer_ext_id', 'customer_ext_id'),
        Index('ix_fact_customer_balances_branch_ext_id', 'branch_ext_id'),
        Index('ix_fact_customer_balances_updated_at', 'updated_at'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    customer_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_customers.id'), nullable=True)
    customer_ext_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    balance_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    open_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    overdue_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_0_30: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_31_60: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_61_90: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_90_plus: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    last_collection_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    trend_vs_previous: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FactExpense(TenantBase):
    __tablename__ = 'fact_expenses'
    __table_args__ = (
        UniqueConstraint('external_id', name='uq_fact_expenses_external_id'),
        Index('ix_fact_expenses_expense_date', 'expense_date'),
        Index('ix_fact_expenses_expense_date_branch_id', 'expense_date', 'branch_id'),
        Index('ix_fact_expenses_expense_date_category_id', 'expense_date', 'category_id'),
        Index('ix_fact_expenses_expense_date_supplier_id', 'expense_date', 'supplier_id'),
        Index('ix_fact_expenses_expense_date_account_id', 'expense_date', 'account_id'),
        Index('ix_fact_expenses_branch_category_expense_date', 'branch_id', 'category_id', 'expense_date'),
        Index('ix_fact_expenses_source_connector_id', 'source_connector_id'),
        Index('ix_fact_expenses_updated_at', 'updated_at'),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    expense_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    posting_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    branch_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_branches.id'), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    location_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    category_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey('dim_expense_categories.id'),
        nullable=True,
        index=True,
    )
    expense_category_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    supplier_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_suppliers.id'), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    account_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey('dim_accounts.id'), nullable=True, index=True)
    account_ext_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_type: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    document_no: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    cost_center: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payment_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    due_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    currency_code: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    amount_net: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_tax: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_gross: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggCashDaily(TenantBase):
    __tablename__ = 'agg_cash_daily'
    __table_args__ = (
        UniqueConstraint('doc_date', 'branch_ext_id', 'subcategory', 'transaction_type', 'account_id', name='uq_agg_cash_daily_dims'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    subcategory: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    transaction_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    outflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    net_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggCashByType(TenantBase):
    __tablename__ = 'agg_cash_by_type'
    __table_args__ = (
        UniqueConstraint('doc_date', 'subcategory', name='uq_agg_cash_by_type_dims'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    subcategory: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    outflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    net_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggCashAccounts(TenantBase):
    __tablename__ = 'agg_cash_accounts'
    __table_args__ = (
        UniqueConstraint('doc_date', 'account_id', name='uq_agg_cash_accounts_dims'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    outflows: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    net_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggSupplierBalancesDaily(TenantBase):
    __tablename__ = 'agg_supplier_balances_daily'
    __table_args__ = (
        UniqueConstraint('balance_date', 'supplier_ext_id', 'branch_ext_id', name='uq_agg_supplier_balances_daily_dims'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    balance_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    open_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    overdue_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_0_30: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_31_60: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_61_90: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_90_plus: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    trend_vs_previous: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    suppliers: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggCustomerBalancesDaily(TenantBase):
    __tablename__ = 'agg_customer_balances_daily'
    __table_args__ = (
        UniqueConstraint('balance_date', 'customer_ext_id', 'branch_ext_id', name='uq_agg_customer_balances_daily_dims'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    balance_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    customer_ext_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    open_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    overdue_balance: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_0_30: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_31_60: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_61_90: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    aging_bucket_90_plus: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    trend_vs_previous: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    customers: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggExpensesDaily(TenantBase):
    __tablename__ = 'agg_expenses_daily'
    __table_args__ = (
        UniqueConstraint(
            'expense_date',
            'branch_ext_id',
            'expense_category_code',
            'supplier_ext_id',
            'account_ext_id',
            name='uq_agg_expenses_daily_dims',
        ),
        Index('ix_agg_expenses_daily_date_branch', 'expense_date', 'branch_ext_id'),
        Index('ix_agg_expenses_daily_date_category', 'expense_date', 'expense_category_code'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    expense_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    expense_category_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    account_ext_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    amount_net: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_tax: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_gross: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggExpensesMonthly(TenantBase):
    __tablename__ = 'agg_expenses_monthly'
    __table_args__ = (
        UniqueConstraint(
            'month_start',
            'branch_ext_id',
            'expense_category_code',
            'supplier_ext_id',
            'account_ext_id',
            name='uq_agg_expenses_monthly_dims',
        ),
        Index('ix_agg_expenses_monthly_month_branch', 'month_start', 'branch_ext_id'),
        Index('ix_agg_expenses_monthly_month_category', 'month_start', 'expense_category_code'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    month_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    expense_category_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    supplier_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    account_ext_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    amount_net: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_tax: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_gross: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggExpensesByCategoryDaily(TenantBase):
    __tablename__ = 'agg_expenses_by_category_daily'
    __table_args__ = (
        UniqueConstraint('expense_date', 'expense_category_code', name='uq_agg_expenses_by_category_daily_dims'),
        Index('ix_agg_expenses_by_category_daily_date', 'expense_date'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    expense_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    expense_category_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    amount_net: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_tax: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_gross: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggExpensesByBranchDaily(TenantBase):
    __tablename__ = 'agg_expenses_by_branch_daily'
    __table_args__ = (
        UniqueConstraint('expense_date', 'branch_ext_id', name='uq_agg_expenses_by_branch_daily_dims'),
        Index('ix_agg_expenses_by_branch_daily_date_branch', 'expense_date', 'branch_ext_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    expense_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    amount_net: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_tax: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    amount_gross: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    entries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StagingIngestEvent(TenantBase):
    __tablename__ = 'staging_ingest_events'
    __table_args__ = (UniqueConstraint('event_id', 'entity', name='uq_staging_event_entity'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity: Mapped[str] = mapped_column(String(64), nullable=False)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StgSalesDocument(TenantBase):
    __tablename__ = 'stg_sales_documents'
    __table_args__ = (
        Index('ix_stg_sales_documents_ingested_at', 'ingested_at'),
        Index('ix_stg_sales_documents_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_sales_documents_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='sales_documents')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgPurchaseDocument(TenantBase):
    __tablename__ = 'stg_purchase_documents'
    __table_args__ = (
        Index('ix_stg_purchase_documents_ingested_at', 'ingested_at'),
        Index('ix_stg_purchase_documents_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_purchase_documents_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='purchase_documents')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgInventoryDocument(TenantBase):
    __tablename__ = 'stg_inventory_documents'
    __table_args__ = (
        Index('ix_stg_inventory_documents_ingested_at', 'ingested_at'),
        Index('ix_stg_inventory_documents_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_inventory_documents_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='inventory_documents')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgCashTransaction(TenantBase):
    __tablename__ = 'stg_cash_transactions'
    __table_args__ = (
        Index('ix_stg_cash_transactions_ingested_at', 'ingested_at'),
        Index('ix_stg_cash_transactions_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_cash_transactions_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='cash_transactions')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgSupplierBalance(TenantBase):
    __tablename__ = 'stg_supplier_balances'
    __table_args__ = (
        Index('ix_stg_supplier_balances_ingested_at', 'ingested_at'),
        Index('ix_stg_supplier_balances_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_supplier_balances_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='supplier_balances')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgCustomerBalance(TenantBase):
    __tablename__ = 'stg_customer_balances'
    __table_args__ = (
        Index('ix_stg_customer_balances_ingested_at', 'ingested_at'),
        Index('ix_stg_customer_balances_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_customer_balances_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='customer_balances')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class StgExpenseDocument(TenantBase):
    __tablename__ = 'stg_expense_documents'
    __table_args__ = (
        Index('ix_stg_expense_documents_ingested_at', 'ingested_at'),
        Index('ix_stg_expense_documents_connector_external', 'connector_type', 'external_id'),
        Index('ix_stg_expense_documents_status', 'transform_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False, default='operating_expenses')
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    doc_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    transform_status: Mapped[str] = mapped_column(String(16), nullable=False, default='loaded')
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class SyncState(TenantBase):
    __tablename__ = 'sync_state'
    __table_args__ = (
        UniqueConstraint('connector_type', name='uq_sync_state_connector_type'),
        Index('ix_sync_state_stream_source_connector', 'stream_code', 'source_connector_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_connector_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_sync_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sync_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class IngestBatch(TenantBase):
    __tablename__ = 'ingest_batches'
    __table_args__ = (
        UniqueConstraint('connector_type', 'stream', 'batch_id', name='uq_ingest_batches_connector_stream_batch'),
        Index('ix_ingest_batches_stream_started_at', 'stream', 'started_at'),
        Index('ix_ingest_batches_status', 'status'),
        Index('ix_ingest_batches_started_status', 'started_at', 'status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream: Mapped[str] = mapped_column(String(64), nullable=False)
    batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default='running')
    rows_read: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_loaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class IngestDeadLetter(TenantBase):
    __tablename__ = 'ingest_dead_letter'
    __table_args__ = (
        Index('ix_ingest_dead_letter_connector_entity', 'connector_type', 'entity'),
        Index('ix_ingest_dead_letter_created_at', 'created_at'),
        Index('ix_ingest_dead_letter_status_created_at', 'status', 'created_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity: Mapped[str] = mapped_column(String(32), nullable=False)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_message: Mapped[str] = mapped_column(String(1024), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default='new')
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
        Index('ix_agg_sales_daily_date_branch_category', 'doc_date', 'branch_ext_id', 'category_ext_id'),
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


class AggSalesDailyCompany(TenantBase):
    __tablename__ = 'agg_sales_daily_company'
    __table_args__ = (UniqueConstraint('doc_date', name='uq_agg_sales_daily_company_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    gross_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    branches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    margin_pct: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggSalesDailyBranch(TenantBase):
    __tablename__ = 'agg_sales_daily_branch'
    __table_args__ = (
        UniqueConstraint('doc_date', 'branch_ext_id', name='uq_agg_sales_daily_branch_dims'),
        Index('ix_agg_sales_daily_branch_date_branch', 'doc_date', 'branch_ext_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    gross_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    contribution_pct: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    margin_pct: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
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
        Index('ix_agg_sales_monthly_date_branch_category', 'month_start', 'branch_ext_id', 'category_ext_id'),
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
        Index(
            'ix_agg_purchases_daily_date_branch_category_supplier',
            'doc_date',
            'branch_ext_id',
            'category_ext_id',
            'supplier_ext_id',
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


class AggPurchasesDailyCompany(TenantBase):
    __tablename__ = 'agg_purchases_daily_company'
    __table_args__ = (UniqueConstraint('doc_date', name='uq_agg_purchases_daily_company_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    branches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    suppliers: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AggPurchasesDailyBranch(TenantBase):
    __tablename__ = 'agg_purchases_daily_branch'
    __table_args__ = (
        UniqueConstraint('doc_date', 'branch_ext_id', name='uq_agg_purchases_daily_branch_dims'),
        Index('ix_agg_purchases_daily_branch_date_branch', 'doc_date', 'branch_ext_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    net_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    cost_amount: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    contribution_pct: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    suppliers: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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
        Index(
            'ix_agg_purchases_monthly_date_branch_category_supplier',
            'month_start',
            'branch_ext_id',
            'category_ext_id',
            'supplier_ext_id',
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


class AggStockAging(TenantBase):
    __tablename__ = 'agg_stock_aging'
    __table_args__ = (
        UniqueConstraint('snapshot_date', 'item_external_id', 'branch_ext_id', name='uq_agg_stock_aging_dims'),
        Index('ix_agg_stock_aging_snapshot_branch', 'snapshot_date', 'branch_ext_id'),
        Index('ix_agg_stock_aging_days', 'days_since_last_sale'),
        Index('ix_agg_stock_aging_bucket', 'aging_bucket'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    item_external_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    branch_ext_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    qty_on_hand: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    stock_value: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    last_sale_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    days_since_last_sale: Mapped[int | None] = mapped_column(Integer, nullable=True)
    aging_bucket: Mapped[str | None] = mapped_column(String(16), nullable=True)
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
    agreement_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
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
