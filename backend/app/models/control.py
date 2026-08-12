from datetime import datetime
from enum import Enum

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum as SqlEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import ControlBase


class PlanName(str, Enum):
    standard = 'standard'
    pro = 'pro'
    enterprise = 'enterprise'
    custom = 'custom'


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


class RuleDomain(str, Enum):
    document_type_rules = 'document_type_rules'
    source_mapping = 'source_mapping'
    kpi_participation_rules = 'kpi_participation_rules'
    intelligence_threshold_rules = 'intelligence_threshold_rules'


class OperationalStream(str, Enum):
    sales_documents = 'sales_documents'
    purchase_documents = 'purchase_documents'
    inventory_documents = 'inventory_documents'
    cash_transactions = 'cash_transactions'
    supplier_balances = 'supplier_balances'
    customer_balances = 'customer_balances'


class OverrideMode(str, Enum):
    merge = 'merge'
    replace = 'replace'
    disable = 'disable'


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


class ProfessionalProfile(ControlBase):
    __tablename__ = 'dim_professional_profiles'
    __table_args__ = (
        UniqueConstraint('profile_code', name='uq_dim_professional_profiles_profile_code'),
        Index('ix_dim_professional_profiles_profile_code', 'profile_code'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profile_code: Mapped[str] = mapped_column(String(64), nullable=False)
    profile_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    users: Mapped[list['User']] = relationship(back_populates='professional_profile')


class User(ControlBase):
    __tablename__ = 'users'
    __table_args__ = (UniqueConstraint('email', 'tenant_id', name='uq_user_email_tenant'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int | None] = mapped_column(ForeignKey('tenants.id'), nullable=True, index=True)
    company_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    professional_profile_id: Mapped[int] = mapped_column(
        ForeignKey('dim_professional_profiles.id'),
        nullable=False,
        index=True,
    )
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
    professional_profile: Mapped[ProfessionalProfile] = relationship(back_populates='users')


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
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False, default='sql_connector')
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    enc_payload: Mapped[str] = mapped_column(Text, nullable=False, default='')
    connection_parameters: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    sales_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    purchases_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    inventory_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    cashflow_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    supplier_balances_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    customer_balances_query_template: Mapped[str] = mapped_column(Text, nullable=False, default='')
    incremental_column: Mapped[str] = mapped_column(String(128), nullable=False, default='UpdatedAt')
    id_column: Mapped[str] = mapped_column(String(128), nullable=False, default='LineId')
    date_column: Mapped[str] = mapped_column(String(128), nullable=False, default='DocDate')
    branch_column: Mapped[str] = mapped_column(String(128), nullable=False, default='BranchCode')
    item_column: Mapped[str] = mapped_column(String(128), nullable=False, default='ItemCode')
    amount_column: Mapped[str] = mapped_column(String(128), nullable=False, default='NetValue')
    cost_column: Mapped[str] = mapped_column(String(128), nullable=False, default='CostValue')
    qty_column: Mapped[str] = mapped_column(String(128), nullable=False, default='Qty')
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default='sql')
    supported_streams: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    enabled_streams: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    stream_query_mapping: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    stream_field_mapping: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    stream_file_mapping: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    stream_api_endpoint: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
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


class PlanFeatureCatalog(ControlBase):
    __tablename__ = 'plan_feature_catalog'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    feature_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    group: Mapped[str] = mapped_column(String(64), nullable=False, default='Custom')
    feature_type: Mapped[str] = mapped_column(String(32), nullable=False, default='feature')
    plan_status: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    tenant_id: Mapped[int | None] = mapped_column(ForeignKey('tenants.id'), nullable=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


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
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    last_seen_path: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_seen_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_seen_user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)


class GlobalRuleSet(ControlBase):
    __tablename__ = 'global_rule_sets'
    __table_args__ = (
        UniqueConstraint('code', name='uq_global_rule_sets_code'),
        Index('ix_global_rule_sets_active_priority', 'is_active', 'priority'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    entries: Mapped[list['GlobalRuleEntry']] = relationship(back_populates='ruleset')


class GlobalRuleEntry(ControlBase):
    __tablename__ = 'global_rule_entries'
    __table_args__ = (
        UniqueConstraint('ruleset_id', 'domain', 'stream', 'rule_key', name='uq_global_rule_entries_scope'),
        Index('ix_global_rule_entries_domain_stream', 'domain', 'stream'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ruleset_id: Mapped[int] = mapped_column(ForeignKey('global_rule_sets.id'), nullable=False, index=True)
    domain: Mapped[RuleDomain] = mapped_column(SqlEnum(RuleDomain), nullable=False)
    stream: Mapped[OperationalStream] = mapped_column(SqlEnum(OperationalStream), nullable=False)
    rule_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    ruleset: Mapped[GlobalRuleSet] = relationship(back_populates='entries')


class TenantRuleOverride(ControlBase):
    __tablename__ = 'tenant_rule_overrides'
    __table_args__ = (
        UniqueConstraint('tenant_id', 'domain', 'stream', 'rule_key', name='uq_tenant_rule_override_scope'),
        Index('ix_tenant_rule_overrides_tenant_domain_stream', 'tenant_id', 'domain', 'stream'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    domain: Mapped[RuleDomain] = mapped_column(SqlEnum(RuleDomain), nullable=False)
    stream: Mapped[OperationalStream] = mapped_column(SqlEnum(OperationalStream), nullable=False)
    rule_key: Mapped[str] = mapped_column(String(128), nullable=False)
    override_mode: Mapped[OverrideMode] = mapped_column(SqlEnum(OverrideMode), nullable=False, default=OverrideMode.merge)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SavedReport(ControlBase):
    """A named, reusable Report Builder configuration (columns + group-by + filters +
    periods) stored per tenant user as the analysis-page query string. Lets users keep
    their column layout and saved reports instead of rebuilding them every time."""

    __tablename__ = 'saved_reports'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default='analysis')
    params: Mapped[str] = mapped_column(Text, nullable=False, default='')
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CopilotConfig(ControlBase):
    """Per-tenant Co-Pilot (AI assistant) configuration. The tenant supplies its OWN
    LLM API key (billed to the tenant's own account, never through us) from its admin
    panel; the key is stored encrypted with the same Fernet envelope used for connection
    secrets. Admin only toggles whether the tenant sees the Co-Pilot module (feature flag)."""

    __tablename__ = 'copilot_configs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, unique=True, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default='anthropic')
    model: Mapped[str] = mapped_column(String(64), nullable=False, default='claude-opus-5')
    # Fernet-envelope ciphertext of {"api_key": "..."} — never stored or logged in plaintext.
    api_key_enc: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Optional soft monthly ceiling on output tokens (0/None = no cap).
    max_monthly_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # 'aggregates' = only KPI/aggregate results may leave to the tenant's LLM; 'row_level'
    # also allows detail rows. Chosen per tenant; default row_level (their data, their key).
    data_scope: Mapped[str] = mapped_column(String(16), nullable=False, default='row_level')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CopilotUsage(ControlBase):
    """Per-tenant, per-month Co-Pilot token usage — used to enforce the optional
    monthly token ceiling set in the tenant's Co-Pilot settings."""

    __tablename__ = 'copilot_usage'

    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), primary_key=True)
    yyyymm: Mapped[int] = mapped_column(Integer, primary_key=True)
    in_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    out_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CopilotConversation(ControlBase):
    """A saved Co-Pilot conversation (per tenant user) — the history list on the page."""

    __tablename__ = 'copilot_conversations'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey('tenants.id'), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False, default='Συνομιλία')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CopilotMessage(ControlBase):
    """One turn (user or assistant text) inside a saved Co-Pilot conversation."""

    __tablename__ = 'copilot_messages'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey('copilot_conversations.id', ondelete='CASCADE'), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, default='')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
