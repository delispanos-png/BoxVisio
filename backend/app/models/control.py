from datetime import datetime
from enum import Enum

from sqlalchemy import JSON, Boolean, DateTime, Enum as SqlEnum, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import ControlBase


class PlanName(str, Enum):
    standard = 'standard'
    pro = 'pro'
    enterprise = 'enterprise'


class TenantStatus(str, Enum):
    active = 'active'
    suspended = 'suspended'
    terminated = 'terminated'


class RoleName(str, Enum):
    cloudon_admin = 'cloudon_admin'
    tenant_admin = 'tenant_admin'
    tenant_user = 'tenant_user'


class SubscriptionStatus(str, Enum):
    trial = 'trial'
    active = 'active'
    past_due = 'past_due'
    suspended = 'suspended'
    canceled = 'canceled'


class Tenant(ControlBase):
    __tablename__ = 'tenants'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    plan: Mapped[PlanName] = mapped_column(SqlEnum(PlanName), nullable=False, default=PlanName.standard)
    status: Mapped[TenantStatus] = mapped_column(SqlEnum(TenantStatus), nullable=False, default=TenantStatus.active)

    db_name: Mapped[str] = mapped_column(String(128), nullable=False)
    db_user: Mapped[str] = mapped_column(String(128), nullable=False)
    db_password: Mapped[str] = mapped_column(String(255), nullable=False)

    whmcs_service_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    whmcs_product_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='external')
    subscription_status: Mapped[SubscriptionStatus] = mapped_column(
        SqlEnum(SubscriptionStatus), nullable=False, default=SubscriptionStatus.trial
    )
    trial_ends_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    feature_flags: Mapped[dict] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    users: Mapped[list['User']] = relationship(back_populates='tenant')
    api_keys: Mapped[list['TenantApiKey']] = relationship(back_populates='tenant')
    subscriptions: Mapped[list['Subscription']] = relationship(back_populates='tenant')


class User(ControlBase):
    __tablename__ = 'users'
    __table_args__ = (UniqueConstraint('email', 'tenant_id', name='uq_user_email_tenant'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int | None] = mapped_column(ForeignKey('tenants.id'), nullable=True, index=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[RoleName] = mapped_column(SqlEnum(RoleName), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    access_starts_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    access_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    reset_token: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    reset_token_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    tenant: Mapped[Tenant | None] = relationship(back_populates='users')


class TenantApiKey(ControlBase):
    __tablename__ = 'tenant_api_keys'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    key_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    key_secret: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    tenant: Mapped[Tenant] = relationship(back_populates='api_keys')


class TenantConnection(ControlBase):
    __tablename__ = 'tenant_connections'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False, default='pharmacyone_sql')
    enc_payload: Mapped[str] = mapped_column(Text, nullable=False, default='')
    sales_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    purchases_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    incremental_column: Mapped[str] = mapped_column(String(128), nullable=False, default='UpdatedAt')
    id_column: Mapped[str] = mapped_column(String(128), nullable=False, default='LineId')
    date_column: Mapped[str] = mapped_column(String(128), nullable=False, default='DocDate')
    branch_column: Mapped[str] = mapped_column(String(128), nullable=False, default='BranchCode')
    item_column: Mapped[str] = mapped_column(String(128), nullable=False, default='ItemCode')
    amount_column: Mapped[str] = mapped_column(String(128), nullable=False, default='NetValue')
    cost_column: Mapped[str] = mapped_column(String(128), nullable=False, default='CostValue')
    qty_column: Mapped[str] = mapped_column(String(128), nullable=False, default='Qty')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_test_ok_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_test_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sync_status: Mapped[str] = mapped_column(String(64), default='never')


class WhmcsEvent(ControlBase):
    __tablename__ = 'whmcs_events'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    service_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Plan(ControlBase):
    __tablename__ = 'plans'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(64), nullable=False)
    feature_sales: Mapped[bool] = mapped_column(Boolean, default=True)
    feature_purchases: Mapped[bool] = mapped_column(Boolean, default=False)
    feature_inventory: Mapped[bool] = mapped_column(Boolean, default=False)
    feature_cashflows: Mapped[bool] = mapped_column(Boolean, default=False)
    max_users: Mapped[int] = mapped_column(Integer, default=5)
    max_branches: Mapped[int] = mapped_column(Integer, default=5)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class PlanFeature(ControlBase):
    __tablename__ = 'plan_features'
    __table_args__ = (UniqueConstraint('plan', 'feature_name', name='uq_plan_feature'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan: Mapped[PlanName] = mapped_column(SqlEnum(PlanName), nullable=False)
    feature_name: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class SubscriptionEvent(ControlBase):
    __tablename__ = 'subscription_events'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    from_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    to_status: Mapped[str] = mapped_column(String(32), nullable=False)
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class WhmcsService(ControlBase):
    __tablename__ = 'whmcs_services'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    service_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    tenant_id: Mapped[int | None] = mapped_column(ForeignKey('tenants.id'), nullable=True, index=True)
    product_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default='active')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Subscription(ControlBase):
    __tablename__ = 'subscriptions'
    __table_args__ = (UniqueConstraint('tenant_id', name='uq_subscriptions_tenant_id'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    plan: Mapped[PlanName] = mapped_column(SqlEnum(PlanName), nullable=False, default=PlanName.standard)
    status: Mapped[SubscriptionStatus] = mapped_column(
        SqlEnum(SubscriptionStatus), nullable=False, default=SubscriptionStatus.trial
    )
    trial_starts_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    trial_ends_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    current_period_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    feature_flags: Mapped[dict] = mapped_column(JSON, default=dict)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    suspended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    tenant: Mapped[Tenant] = relationship(back_populates='subscriptions')


class SubscriptionLimit(ControlBase):
    __tablename__ = 'subscription_limits'
    __table_args__ = (UniqueConstraint('subscription_id', 'limit_key', name='uq_subscription_limit_key'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    subscription_id: Mapped[int] = mapped_column(ForeignKey('subscriptions.id'), nullable=False, index=True)
    limit_key: Mapped[str] = mapped_column(String(64), nullable=False)
    limit_value: Mapped[int] = mapped_column(Integer, nullable=False)
    used_value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Invoice(ControlBase):
    __tablename__ = 'invoices'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    subscription_id: Mapped[int | None] = mapped_column(ForeignKey('subscriptions.id'), nullable=True, index=True)
    invoice_no: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    amount_due: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='draft')
    due_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    paid_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Payment(ControlBase):
    __tablename__ = 'payments'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    invoice_id: Mapped[int | None] = mapped_column(ForeignKey('invoices.id'), nullable=True, index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default='EUR')
    provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_ref: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='received')
    paid_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AuditLog(ControlBase):
    __tablename__ = 'audit_logs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int | None] = mapped_column(ForeignKey('tenants.id'), nullable=True, index=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey('users.id'), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class RefreshToken(ControlBase):
    __tablename__ = 'refresh_tokens'
    __table_args__ = (UniqueConstraint('token_jti', name='uq_refresh_tokens_token_jti'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False, index=True)
    token_jti: Mapped[str] = mapped_column(String(128), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    replaced_by_jti: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
