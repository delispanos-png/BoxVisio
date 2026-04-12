import secrets
from datetime import datetime, timedelta
from typing import Any

from celery import Celery
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_roles
from app.core.config import settings
from app.db.control_session import get_control_db
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import (
    AuditLog,
    GlobalRuleEntry,
    GlobalRuleSet,
    OperationalStream,
    OverrideMode,
    PlanFeature,
    PlanName,
    RoleName,
    RuleDomain,
    Subscription,
    SubscriptionEvent,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantConnection,
    TenantRuleOverride,
    User,
)
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_CASHFLOW_QUERY,
    DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
    DEFAULT_GENERIC_INVENTORY_QUERY,
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
    discover_sqlserver,
    test_connection,
    test_connection_with_version,
)
from app.services.connection_secrets import build_odbc_connection_string, encrypt_sqlserver_secret, SqlServerSecret
from app.services.ingestion import enqueue_tenant_job
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, STREAM_TO_ENTITY, normalize_stream_name, normalize_stream_values
from app.services.ingestion.engine import CONNECTORS
from app.services.ingestion.sync_planner import plan_tenant_sync_jobs
from app.services.subscriptions import get_or_create_subscription, infer_default_features_for_plan, sync_tenant_from_subscription
from app.services.provisioning_wizard import run_tenant_provisioning_wizard
from app.services.querypacks import apply_querypack_to_connection, load_querypack
from app.services.rule_config import resolve_rule_payload
from app.services.intelligence_service import list_insights, list_rules as list_tenant_rules, update_rule as update_tenant_rule

router = APIRouter(prefix='/v1/admin', tags=['admin'])

celery_client = Celery('admin_sender', broker=settings.celery_broker_url)
DEFAULT_SQL_CONNECTOR = 'sql_connector'
SQL_CONNECTOR_TYPES = (DEFAULT_SQL_CONNECTOR, 'pharmacyone_sql')


class TenantWizardRequest(BaseModel):
    name: str
    slug: str
    admin_email: EmailStr
    plan: PlanName = PlanName.standard
    source: str = 'sql'
    subscription_status: SubscriptionStatus = SubscriptionStatus.trial
    trial_days: int = 14


class ConnectionCreateRequest(BaseModel):
    tenant_id: int
    connector_type: str = DEFAULT_SQL_CONNECTOR
    is_active: bool = True
    source_type: str | None = None
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)
    sales_query_template: str = DEFAULT_GENERIC_SALES_QUERY
    purchases_query_template: str = DEFAULT_GENERIC_PURCHASES_QUERY
    inventory_query_template: str = DEFAULT_GENERIC_INVENTORY_QUERY
    cashflow_query_template: str = DEFAULT_GENERIC_CASHFLOW_QUERY
    supplier_balances_query_template: str = DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY
    customer_balances_query_template: str = DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY
    updated_at_column: str = 'UpdatedAt'
    incremental_column: str = ''
    id_column: str = 'LineId'
    date_column: str = 'DocDate'
    branch_column: str = 'BranchCode'
    item_column: str = 'ItemCode'
    net_amount_column: str = 'NetValue'
    amount_column: str = ''
    cost_column: str = 'CostValue'
    qty_column: str = 'Qty'
    supported_streams: list[str] | None = None
    enabled_streams: list[str] | None = None
    stream_query_mapping: dict[str, str] = Field(default_factory=dict)
    stream_field_mapping: dict[str, dict[str, str]] = Field(default_factory=dict)
    stream_file_mapping: dict[str, dict] = Field(default_factory=dict)
    stream_api_endpoint: dict[str, str] = Field(default_factory=dict)
    connection_parameters: dict[str, Any] = Field(default_factory=dict)


class SqlServerMappingRequest(BaseModel):
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)
    sales_query_template: str = DEFAULT_GENERIC_SALES_QUERY
    purchases_query_template: str = DEFAULT_GENERIC_PURCHASES_QUERY
    inventory_query_template: str = DEFAULT_GENERIC_INVENTORY_QUERY
    cashflow_query_template: str = DEFAULT_GENERIC_CASHFLOW_QUERY
    supplier_balances_query_template: str = DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY
    customer_balances_query_template: str = DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY
    updated_at_column: str = 'UpdatedAt'
    incremental_column: str = ''
    id_column: str = 'LineId'
    date_column: str = 'DocDate'
    branch_column: str = 'BranchCode'
    item_column: str = 'ItemCode'
    net_amount_column: str = 'NetValue'
    amount_column: str = ''
    cost_column: str = 'CostValue'
    qty_column: str = 'Qty'
    is_active: bool | None = None
    source_type: str | None = None
    supported_streams: list[str] | None = None
    enabled_streams: list[str] | None = None
    stream_query_mapping: dict[str, str] = Field(default_factory=dict)
    stream_field_mapping: dict[str, dict[str, str]] = Field(default_factory=dict)
    stream_file_mapping: dict[str, dict] = Field(default_factory=dict)
    stream_api_endpoint: dict[str, str] = Field(default_factory=dict)
    connection_parameters: dict[str, Any] = Field(default_factory=dict)


class SqlServerDiscoveryRequest(BaseModel):
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)


class SqlServerTestRequest(BaseModel):
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)


class PlanUpdateRequest(BaseModel):
    plan: PlanName


class PlanFeaturesUpdateRequest(BaseModel):
    sales: bool | None = None
    purchases: bool | None = None
    inventory: bool | None = None
    cashflows: bool | None = None


class SubscriptionUpdateRequest(BaseModel):
    status: SubscriptionStatus
    current_period_end: datetime | None = None
    trial_ends_at: datetime | None = None
    note: str | None = None


class BackfillRequest(BaseModel):
    from_date: datetime
    to_date: datetime
    chunk_days: int = 7
    chunk_records: int = 1000
    include_purchases: bool = True
    include_inventory: bool = True
    include_cashflows: bool = True
    include_supplier_balances: bool = True
    include_customer_balances: bool = True


class InsightRuleUpdateRequest(BaseModel):
    enabled: bool | None = None
    severity_default: str | None = None
    params_json: dict | None = None


class GlobalRuleSetCreateRequest(BaseModel):
    code: str
    name: str
    description: str | None = None
    is_active: bool = True
    priority: int = 100


class GlobalRuleEntryUpsertRequest(BaseModel):
    ruleset_code: str = 'softone_default_v1'
    domain: RuleDomain
    stream: OperationalStream
    rule_key: str
    payload_json: dict = Field(default_factory=dict)
    is_active: bool = True


async def _get_sql_connector_connection(db: AsyncSession, tenant_id: int) -> TenantConnection | None:
    rows = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type.in_(SQL_CONNECTOR_TYPES),
            )
        )
    ).scalars().all()
    by_type = {str(row.connector_type or '').strip().lower(): row for row in rows}
    return by_type.get(DEFAULT_SQL_CONNECTOR) or by_type.get('pharmacyone_sql')


class TenantRuleOverrideUpsertRequest(BaseModel):
    domain: RuleDomain
    stream: OperationalStream
    rule_key: str
    override_mode: OverrideMode = OverrideMode.merge
    payload_json: dict = Field(default_factory=dict)
    is_active: bool = True


def _default_source_type_for_connector(connector_type: str) -> str:
    lowered = str(connector_type or '').strip().lower()
    if 'api' in lowered:
        return 'api'
    if 'file' in lowered or 'csv' in lowered or 'excel' in lowered or 'sftp' in lowered:
        return 'file'
    return 'sql'


def _default_supported_streams_for_connector(connector_type: str) -> list[str]:
    if str(connector_type or '').strip().lower() == 'external_api':
        return ['sales_documents', 'purchase_documents']
    return list(ALL_OPERATIONAL_STREAMS)


def _normalize_stream_list(values: list[str] | None, *, fallback: list[str]) -> list[str]:
    normalized = normalize_stream_values(values or [])
    return [stream for stream in normalized] if normalized else [stream for stream in fallback]


def _coerce_stream_query_mapping(
    value: dict[str, Any] | None,
    *,
    sales_query_template: str,
    purchases_query_template: str,
    inventory_query_template: str,
    cashflow_query_template: str,
    supplier_balances_query_template: str,
    customer_balances_query_template: str,
) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(value, dict):
        for key, query in value.items():
            stream = normalize_stream_values([str(key)])
            if not stream:
                continue
            if isinstance(query, str) and query.strip():
                out[stream[0]] = query

    fallback_mapping = {
        'sales_documents': sales_query_template,
        'purchase_documents': purchases_query_template,
        'inventory_documents': inventory_query_template,
        'cash_transactions': cashflow_query_template,
        'supplier_balances': supplier_balances_query_template,
        'customer_balances': customer_balances_query_template,
    }
    for stream, query in fallback_mapping.items():
        if isinstance(query, str) and query.strip() and stream not in out:
            out[stream] = query
    return out


def _coerce_stream_field_mapping(value: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    if not isinstance(value, dict):
        return out
    for stream_key, mapping in value.items():
        stream = normalize_stream_values([str(stream_key)])
        if not stream or not isinstance(mapping, dict):
            continue
        normalized_map: dict[str, str] = {}
        for canonical_field, source_field in mapping.items():
            c = str(canonical_field or '').strip()
            s = str(source_field or '').strip()
            if c and s:
                normalized_map[c] = s
        if normalized_map:
            out[stream[0]] = normalized_map
    return out


@router.get('/tenants')
async def list_tenants(
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).order_by(Tenant.id.desc()))).scalars().all()
    payload = []
    for t in tenants:
        sub = await get_or_create_subscription(db, t)
        await sync_tenant_from_subscription(db, t, sub)
        payload.append(
            {
                'id': t.id,
                'name': t.name,
                'slug': t.slug,
                'plan': sub.plan.value,
                'status': t.status.value,
                'subscription_status': sub.status.value,
                'trial_ends_at': sub.trial_ends_at,
                'current_period_end': sub.current_period_end,
                'source': t.source,
            }
        )
    await db.commit()
    return payload


@router.get('/connectors/catalog')
async def list_connector_catalog(
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    catalog = []
    for connector in CONNECTORS.values():
        catalog.append(connector.declaration())
    return {'connectors': catalog}


@router.post('/tenants/wizard')
async def tenant_wizard_create(
    payload: TenantWizardRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    result = await run_tenant_provisioning_wizard(
        db=db,
        name=payload.name,
        slug=payload.slug,
        admin_email=payload.admin_email,
        plan=payload.plan,
        source=payload.source,
        subscription_status=payload.subscription_status,
        trial_days=payload.trial_days,
    )
    if result['status'] != 'ok':
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post('/tenants')
async def create_tenant(
    payload: TenantWizardRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await tenant_wizard_create(payload=payload, _=_, db=db)


@router.patch('/tenants/{tenant_id}/plan')
async def assign_plan(
    tenant_id: int,
    payload: PlanUpdateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    sub = await get_or_create_subscription(db, tenant)
    prev = sub.plan
    tenant.plan = payload.plan
    sub.plan = payload.plan
    sub.feature_flags = infer_default_features_for_plan(payload.plan)
    await sync_tenant_from_subscription(db, tenant, sub)
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='plan_updated',
            entity_type='subscription',
            entity_id=str(sub.id),
            payload={'from': prev.value, 'to': payload.plan.value},
        )
    )
    await db.commit()
    return {'status': 'updated', 'plan': tenant.plan.value}


@router.get('/plans')
async def list_plans(
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (await db.execute(select(PlanFeature).order_by(PlanFeature.plan, PlanFeature.feature_name))).scalars().all()
    payload: dict[str, dict[str, bool]] = {}
    for r in rows:
        payload.setdefault(r.plan.value, {})[r.feature_name] = bool(r.enabled)
    return payload


@router.patch('/plans/{plan}/features')
async def update_plan_features(
    plan: PlanName,
    payload: PlanFeaturesUpdateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    updates = {
        'sales': payload.sales,
        'purchases': payload.purchases,
        'inventory': payload.inventory,
        'cashflows': payload.cashflows,
    }
    for feature, enabled in updates.items():
        if enabled is None:
            continue
        row = (
            await db.execute(
                select(PlanFeature).where(
                    PlanFeature.plan == plan,
                    PlanFeature.feature_name == feature,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            row = PlanFeature(plan=plan, feature_name=feature, enabled=enabled)
            db.add(row)
        else:
            row.enabled = enabled
    await db.commit()
    return {'status': 'updated', 'plan': plan.value}


@router.patch('/tenants/{tenant_id}/subscription')
async def update_subscription(
    tenant_id: int,
    payload: SubscriptionUpdateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    sub = await get_or_create_subscription(db, tenant)
    prev = sub.status.value
    sub.status = payload.status
    if payload.current_period_end is not None:
        sub.current_period_end = payload.current_period_end
    if payload.trial_ends_at is not None:
        sub.trial_ends_at = payload.trial_ends_at
    if payload.status == SubscriptionStatus.canceled:
        sub.canceled_at = datetime.utcnow()
    if payload.status == SubscriptionStatus.suspended:
        sub.suspended_at = datetime.utcnow()

    await sync_tenant_from_subscription(db, tenant, sub)

    db.add(
        SubscriptionEvent(
            tenant_id=tenant.id,
            from_status=prev,
            to_status=sub.status.value,
            note=payload.note,
        )
    )
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='subscription_updated',
            entity_type='subscription',
            entity_id=str(sub.id),
            payload={'from': prev, 'to': sub.status.value, 'note': payload.note},
        )
    )
    await db.commit()
    return {
        'status': 'updated',
        'tenant_id': tenant.id,
        'subscription_status': sub.status.value,
        'trial_ends_at': sub.trial_ends_at,
        'current_period_end': sub.current_period_end,
    }


@router.post('/connections')
async def create_connection(
    payload: ConnectionCreateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == payload.tenant_id,
                TenantConnection.connector_type == payload.connector_type,
            )
        )
    ).scalar_one_or_none()
    if not conn:
        conn = TenantConnection(
            tenant_id=payload.tenant_id,
            connector_type=payload.connector_type,
            sync_status='never',
        )
        db.add(conn)

    conn.is_active = bool(payload.is_active)
    conn.enc_payload = encrypt_sqlserver_secret(
        host=payload.host,
        port=payload.port,
        database=payload.database,
        username=payload.username,
        password=payload.password,
        options=payload.options,
    )
    conn.sales_query_template = payload.sales_query_template
    conn.purchases_query_template = payload.purchases_query_template
    conn.inventory_query_template = payload.inventory_query_template
    conn.cashflow_query_template = payload.cashflow_query_template
    conn.supplier_balances_query_template = payload.supplier_balances_query_template
    conn.customer_balances_query_template = payload.customer_balances_query_template
    conn.incremental_column = (payload.incremental_column or payload.updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = payload.id_column
    conn.date_column = payload.date_column
    conn.branch_column = payload.branch_column
    conn.item_column = payload.item_column
    conn.amount_column = (payload.amount_column or payload.net_amount_column).strip() or 'NetValue'
    conn.cost_column = payload.cost_column
    conn.qty_column = payload.qty_column
    conn.source_type = (payload.source_type or _default_source_type_for_connector(payload.connector_type)).strip().lower() or 'sql'
    default_supported = _default_supported_streams_for_connector(payload.connector_type)
    conn.supported_streams = _normalize_stream_list(payload.supported_streams, fallback=default_supported)
    conn.enabled_streams = _normalize_stream_list(payload.enabled_streams, fallback=conn.supported_streams)
    conn.stream_query_mapping = _coerce_stream_query_mapping(
        payload.stream_query_mapping,
        sales_query_template=conn.sales_query_template,
        purchases_query_template=conn.purchases_query_template,
        inventory_query_template=conn.inventory_query_template,
        cashflow_query_template=conn.cashflow_query_template,
        supplier_balances_query_template=conn.supplier_balances_query_template,
        customer_balances_query_template=conn.customer_balances_query_template,
    )
    conn.stream_field_mapping = _coerce_stream_field_mapping(payload.stream_field_mapping)
    conn.stream_file_mapping = payload.stream_file_mapping or {}
    conn.stream_api_endpoint = payload.stream_api_endpoint or {}
    conn.connection_parameters = payload.connection_parameters or {
        'connector_type': payload.connector_type,
        'source_type': conn.source_type,
        'host': payload.host,
        'port': payload.port,
        'database': payload.database,
        'username': payload.username,
        'options': payload.options,
    }
    conn.last_test_error = None

    await db.commit()
    return {'status': 'created', 'connection_id': conn.id}


@router.get('/tenants/{tenant_id}/sqlserver/mapping')
async def get_sqlserver_mapping(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = await _get_sql_connector_connection(db, tenant_id)
    if not conn:
        raise HTTPException(status_code=404, detail='SQL Server mapping not found')

    return {
        'tenant_id': tenant_id,
        'is_active': bool(conn.is_active),
        'has_credentials': bool(conn.enc_payload),
        'sales_query_template': conn.sales_query_template or DEFAULT_GENERIC_SALES_QUERY,
        'purchases_query_template': conn.purchases_query_template or DEFAULT_GENERIC_PURCHASES_QUERY,
        'inventory_query_template': conn.inventory_query_template or DEFAULT_GENERIC_INVENTORY_QUERY,
        'cashflow_query_template': conn.cashflow_query_template or DEFAULT_GENERIC_CASHFLOW_QUERY,
        'supplier_balances_query_template': conn.supplier_balances_query_template or DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
        'customer_balances_query_template': conn.customer_balances_query_template or DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
        'incremental_column': conn.incremental_column,
        'id_column': conn.id_column,
        'date_column': conn.date_column,
        'branch_column': conn.branch_column,
        'item_column': conn.item_column,
        'amount_column': conn.amount_column,
        'cost_column': conn.cost_column,
        'qty_column': conn.qty_column,
        'source_type': conn.source_type or _default_source_type_for_connector(DEFAULT_SQL_CONNECTOR),
        'supported_streams': conn.supported_streams or _default_supported_streams_for_connector(DEFAULT_SQL_CONNECTOR),
        'enabled_streams': conn.enabled_streams or conn.supported_streams or _default_supported_streams_for_connector(DEFAULT_SQL_CONNECTOR),
        'stream_query_mapping': conn.stream_query_mapping or {},
        'stream_field_mapping': conn.stream_field_mapping or {},
        'stream_file_mapping': conn.stream_file_mapping or {},
        'stream_api_endpoint': conn.stream_api_endpoint or {},
        'connection_parameters': conn.connection_parameters or {},
    }


@router.put('/tenants/{tenant_id}/sqlserver/mapping')
async def upsert_sqlserver_mapping(
    tenant_id: int,
    payload: SqlServerMappingRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = await _get_sql_connector_connection(db, tenant_id)
    if not conn:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type=DEFAULT_SQL_CONNECTOR,
            sync_status='never',
        )
        db.add(conn)

    if payload.is_active is not None:
        conn.is_active = bool(payload.is_active)
    conn.enc_payload = encrypt_sqlserver_secret(
        host=payload.host,
        port=payload.port,
        database=payload.database,
        username=payload.username,
        password=payload.password,
        options=payload.options,
    )
    conn.sales_query_template = payload.sales_query_template
    conn.purchases_query_template = payload.purchases_query_template
    conn.inventory_query_template = payload.inventory_query_template
    conn.cashflow_query_template = payload.cashflow_query_template
    conn.supplier_balances_query_template = payload.supplier_balances_query_template
    conn.customer_balances_query_template = payload.customer_balances_query_template
    conn.incremental_column = (payload.incremental_column or payload.updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = payload.id_column
    conn.date_column = payload.date_column
    conn.branch_column = payload.branch_column
    conn.item_column = payload.item_column
    conn.amount_column = (payload.amount_column or payload.net_amount_column).strip() or 'NetValue'
    conn.cost_column = payload.cost_column
    conn.qty_column = payload.qty_column
    conn.source_type = (payload.source_type or _default_source_type_for_connector(DEFAULT_SQL_CONNECTOR)).strip().lower() or 'sql'
    default_supported = _default_supported_streams_for_connector(DEFAULT_SQL_CONNECTOR)
    conn.supported_streams = _normalize_stream_list(payload.supported_streams, fallback=default_supported)
    conn.enabled_streams = _normalize_stream_list(payload.enabled_streams, fallback=conn.supported_streams)
    conn.stream_query_mapping = _coerce_stream_query_mapping(
        payload.stream_query_mapping,
        sales_query_template=conn.sales_query_template,
        purchases_query_template=conn.purchases_query_template,
        inventory_query_template=conn.inventory_query_template,
        cashflow_query_template=conn.cashflow_query_template,
        supplier_balances_query_template=conn.supplier_balances_query_template,
        customer_balances_query_template=conn.customer_balances_query_template,
    )
    conn.stream_field_mapping = _coerce_stream_field_mapping(payload.stream_field_mapping)
    conn.stream_file_mapping = payload.stream_file_mapping or {}
    conn.stream_api_endpoint = payload.stream_api_endpoint or {}
    conn.connection_parameters = payload.connection_parameters or {
        'connector_type': DEFAULT_SQL_CONNECTOR,
        'source_type': conn.source_type,
        'host': payload.host,
        'port': payload.port,
        'database': payload.database,
        'username': payload.username,
        'options': payload.options,
    }
    conn.last_test_error = None
    await db.commit()
    return {'status': 'updated', 'connection_id': conn.id}


@router.post('/tenants/{tenant_id}/sqlserver/discovery')
async def sqlserver_discovery(
    tenant_id: int,
    payload: SqlServerDiscoveryRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = await _get_sql_connector_connection(db, tenant_id)
    _ = conn  # explicit no-op; discovery uses provided credentials only.
    secret = SqlServerSecret(
        host=payload.host,
        port=payload.port,
        database=payload.database,
        username=payload.username,
        password=payload.password,
        options=payload.options,
    )
    connection_string = build_odbc_connection_string(secret)

    try:
        discovery = discover_sqlserver(connection_string)
    except Exception as exc:
        raise HTTPException(status_code=400, detail='discovery_failed') from exc
    return {'tenant_id': tenant_id, 'tables': discovery}


@router.post('/tenants/{tenant_id}/sqlserver/test')
async def sqlserver_test_connection(
    tenant_id: int,
    payload: SqlServerTestRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    secret = SqlServerSecret(
        host=payload.host,
        port=payload.port,
        database=payload.database,
        username=payload.username,
        password=payload.password,
        options=payload.options,
    )
    connection_string = build_odbc_connection_string(secret)
    try:
        test_connection(connection_string)
    except Exception as exc:
        conn = await _get_sql_connector_connection(db, tenant_id)
        if conn is not None:
            conn.last_test_error = 'test_failed'
            await db.commit()
        return {'status': 'error', 'detail': 'test_failed'}
    conn = await _get_sql_connector_connection(db, tenant_id)
    if conn is not None:
        conn.last_test_ok_at = datetime.utcnow()
        conn.last_test_error = None
        await db.commit()
    return {'status': 'ok'}


@router.post('/tenants/{tenant_id}/connections/test')
async def connection_test(
    tenant_id: int,
    payload: SqlServerTestRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    conn = await _get_sql_connector_connection(db, tenant_id)
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type=DEFAULT_SQL_CONNECTOR,
            sync_status='never',
        )
        db.add(conn)

    options = dict(payload.options)
    options.setdefault('Encrypt', 'yes')
    options.setdefault('TrustServerCertificate', 'yes')
    options.setdefault('Connection Timeout', '5')
    secret = SqlServerSecret(
        host=payload.host,
        port=payload.port,
        database=payload.database,
        username=payload.username,
        password=payload.password,
        options=options,
    )
    connection_string = build_odbc_connection_string(secret)

    try:
        server_version = test_connection_with_version(connection_string)
    except Exception:
        conn.last_test_error = 'test_failed'
        await db.commit()
        return {'status': 'error', 'detail': 'test_failed'}

    conn.last_test_ok_at = datetime.utcnow()
    conn.last_test_error = None
    await db.commit()
    return {'status': 'ok', 'server_version': server_version}


@router.post('/tenants/{tenant_id}/sync')
async def trigger_sync(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    planned_jobs = await plan_tenant_sync_jobs(
        db,
        tenant_id=tenant.id,
        tenant_slug=tenant.slug,
    )
    if not planned_jobs:
        raise HTTPException(status_code=400, detail='No active connectors configured for tenant')
    for job in planned_jobs:
        payload = dict(job)
        payload.setdefault('payload', {})
        payload.setdefault('attempt', 0)
        payload.setdefault('max_retries', settings.ingest_job_max_retries)
        enqueue_tenant_job(tenant.slug, payload)
    celery_client.send_task(
        'worker.tasks.drain_tenant_ingest_queue',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )
    return {
        'status': 'queued',
        'tenant': tenant.slug,
        'jobs': [f"{job['connector']}:{job['stream']}" for job in planned_jobs],
        'queue': f'ingest:{tenant.slug}',
    }


@router.post('/tenants/{tenant_id}/querypack/apply')
async def apply_default_querypack(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    conn = await _get_sql_connector_connection(db, tenant_id)
    if conn is None:
        conn = TenantConnection(tenant_id=tenant_id, connector_type=DEFAULT_SQL_CONNECTOR, sync_status='never')
        db.add(conn)

    pack = load_querypack('erp_sql', 'default')
    apply_querypack_to_connection(conn, pack)
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='querypack_applied',
            entity_type='tenant_connection',
            entity_id=str(conn.id or ''),
            payload={'querypack': pack.name, 'version': pack.version},
        )
    )
    await db.commit()
    return {'status': 'ok', 'tenant': tenant.slug, 'querypack': pack.name, 'version': pack.version}


@router.post('/tenants/{tenant_id}/backfill')
async def run_initial_backfill(
    tenant_id: int,
    payload: BackfillRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    planned_jobs = await plan_tenant_sync_jobs(
        db,
        tenant_id=tenant.id,
        tenant_slug=tenant.slug,
    )
    if not planned_jobs:
        raise HTTPException(status_code=400, detail='No active connectors configured for tenant')
    all_external = bool(planned_jobs) and all(
        str(job.get('connector') or '').strip().lower() == 'external_api'
        for job in planned_jobs
    )
    if all_external:
        chunk_records = max(100, min(10000, int(payload.chunk_records)))
        include_by_stream = {
            'sales_documents': True,
            'purchase_documents': bool(payload.include_purchases),
            'inventory_documents': bool(payload.include_inventory),
            'cash_transactions': bool(payload.include_cashflows),
            'supplier_balances': bool(payload.include_supplier_balances),
            'customer_balances': bool(payload.include_customer_balances),
        }
        queued = 0
        for job in planned_jobs:
            stream = normalize_stream_name(job.get('stream'))
            if stream and not include_by_stream.get(stream, True):
                continue
            queued_job = dict(job)
            merged_payload = dict(queued_job.get('payload') or {})
            merged_payload.update(
                {
                    'from_date': payload.from_date.date().isoformat(),
                    'to_date': payload.to_date.date().isoformat(),
                    'ignore_sync_state': True,
                    'backfill': True,
                    'limit': chunk_records,
                }
            )
            queued_job['payload'] = merged_payload
            queued_job.setdefault('attempt', 0)
            queued_job.setdefault('max_retries', settings.ingest_job_max_retries)
            enqueue_tenant_job(tenant.slug, queued_job)
            queued += 1

        if queued == 0:
            raise HTTPException(status_code=400, detail='No eligible streams selected for external API backfill')

        result = celery_client.send_task(
            'worker.tasks.drain_tenant_ingest_queue',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
        return {
            'status': 'queued',
            'tenant': tenant.slug,
            'task_id': result.id,
            'connector': 'external_api',
            'jobs': queued,
            'chunk_records': chunk_records,
        }

    result = celery_client.send_task(
        'worker.tasks.enqueue_sql_backfill',
        kwargs={
            'tenant_slug': tenant.slug,
            'from_date_str': payload.from_date.date().isoformat(),
            'to_date_str': payload.to_date.date().isoformat(),
            'chunk_days': max(1, int(payload.chunk_days)),
            'include_purchases': bool(payload.include_purchases),
            'include_inventory': bool(payload.include_inventory),
            'include_cashflows': bool(payload.include_cashflows),
            'include_supplier_balances': bool(payload.include_supplier_balances),
            'include_customer_balances': bool(payload.include_customer_balances),
        },
        queue='ingest',
    )
    return {'status': 'queued', 'tenant': tenant.slug, 'task_id': result.id}


@router.post('/users/{user_id}/invite-reset')
async def invite_reset(
    user_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.id == user_id, User.is_active.is_(True)))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail='User not found')
    user.reset_token = secrets.token_urlsafe(24)
    user.reset_token_expires_at = datetime.utcnow() + timedelta(days=2)
    await db.commit()
    return {'status': 'ok', 'reset_token': user.reset_token, 'expires_at': user.reset_token_expires_at}


@router.post('/tenants/{tenant_id}/api-keys/rotate')
async def rotate_tenant_api_keys(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')

    current_keys = (
        await db.execute(
            select(TenantApiKey).where(
                TenantApiKey.tenant_id == tenant_id,
                TenantApiKey.is_active.is_(True),
            )
        )
    ).scalars().all()
    for key in current_keys:
        key.is_active = False

    new_key_id = secrets.token_urlsafe(16)
    new_secret = secrets.token_urlsafe(32)
    db.add(
        TenantApiKey(
            tenant_id=tenant_id,
            key_id=new_key_id,
            key_secret=new_secret,
            is_active=True,
        )
    )
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='tenant_api_key_rotated',
            entity_type='tenant_api_keys',
            entity_id=str(tenant_id),
            payload={'disabled_keys': len(current_keys), 'new_key_id': new_key_id},
        )
    )
    await db.commit()
    return {'status': 'rotated', 'tenant_id': tenant_id, 'key_id': new_key_id, 'key_secret': new_secret}


@router.post('/tenants/{tenant_id}/insights/run')
async def run_tenant_insights_now(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    task = celery_client.send_task(
        'worker.tasks.generate_insights_for_tenant',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )
    return {'status': 'queued', 'tenant': tenant.slug, 'task_id': task.id}


@router.get('/tenants/{tenant_id}/insights')
async def admin_list_tenant_insights(
    tenant_id: int,
    category: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    limit: int = 100,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        return await list_insights(
            tenant_db,
            category=category,
            severity=severity,
            status=status,
            limit=limit,
        )
    return []


@router.get('/tenants/{tenant_id}/insight-rules')
async def admin_list_tenant_insight_rules(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        rows = await list_tenant_rules(tenant_db)
        return [
            {
                'code': r.code,
                'name': r.name,
                'description': r.description,
                'category': r.category,
                'severity_default': r.severity_default,
                'enabled': bool(r.enabled),
                'params_json': r.params_json,
                'scope': r.scope,
                'schedule': r.schedule,
            }
            for r in rows
        ]
    return []


@router.patch('/tenants/{tenant_id}/insight-rules/{code}')
async def admin_update_tenant_insight_rule(
    tenant_id: int,
    code: str,
    payload: InsightRuleUpdateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        ok = await update_tenant_rule(
            tenant_db,
            code=code,
            enabled=payload.enabled,
            severity_default=payload.severity_default,
            params_json=payload.params_json,
        )
        if not ok:
            raise HTTPException(status_code=404, detail='Rule not found')
        return {'status': 'updated', 'tenant': tenant.slug, 'code': code}
    raise HTTPException(status_code=500, detail='Tenant DB session failed')


@router.get('/rulesets/global')
async def list_global_rule_sets(
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (
        await db.execute(select(GlobalRuleSet).order_by(GlobalRuleSet.priority.desc(), GlobalRuleSet.code.asc()))
    ).scalars().all()
    return [
        {
            'id': r.id,
            'code': r.code,
            'name': r.name,
            'description': r.description,
            'is_active': bool(r.is_active),
            'priority': int(r.priority),
        }
        for r in rows
    ]


@router.post('/rulesets/global')
async def create_or_update_global_rule_set(
    payload: GlobalRuleSetCreateRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    row = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == payload.code))).scalar_one_or_none()
    if row is None:
        row = GlobalRuleSet(
            code=payload.code,
            name=payload.name,
            description=payload.description,
            is_active=bool(payload.is_active),
            priority=int(payload.priority),
        )
        db.add(row)
        action = 'created'
    else:
        row.name = payload.name
        row.description = payload.description
        row.is_active = bool(payload.is_active)
        row.priority = int(payload.priority)
        action = 'updated'
    await db.commit()
    return {'status': action, 'ruleset_code': payload.code}


@router.get('/rules/global')
async def list_global_rule_entries(
    ruleset_code: str | None = None,
    domain: RuleDomain | None = None,
    stream: OperationalStream | None = None,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    stmt = (
        select(GlobalRuleEntry, GlobalRuleSet)
        .join(GlobalRuleSet, GlobalRuleSet.id == GlobalRuleEntry.ruleset_id)
        .order_by(
            GlobalRuleSet.priority.desc(),
            GlobalRuleSet.code.asc(),
            GlobalRuleEntry.domain.asc(),
            GlobalRuleEntry.stream.asc(),
            GlobalRuleEntry.rule_key.asc(),
        )
    )
    if ruleset_code:
        stmt = stmt.where(GlobalRuleSet.code == ruleset_code)
    if domain:
        stmt = stmt.where(GlobalRuleEntry.domain == domain)
    if stream:
        stmt = stmt.where(GlobalRuleEntry.stream == stream)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'id': entry.id,
            'ruleset_code': ruleset.code,
            'ruleset_name': ruleset.name,
            'domain': entry.domain.value,
            'stream': entry.stream.value,
            'rule_key': entry.rule_key,
            'payload_json': entry.payload_json or {},
            'is_active': bool(entry.is_active),
        }
        for entry, ruleset in rows
    ]


@router.put('/rules/global')
async def upsert_global_rule_entry(
    payload: GlobalRuleEntryUpsertRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    ruleset = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == payload.ruleset_code))).scalar_one_or_none()
    if ruleset is None:
        ruleset = GlobalRuleSet(
            code=payload.ruleset_code,
            name=payload.ruleset_code,
            description='Auto-created from API upsert.',
            is_active=True,
            priority=100,
        )
        db.add(ruleset)
        await db.flush()

    row = (
        await db.execute(
            select(GlobalRuleEntry).where(
                GlobalRuleEntry.ruleset_id == ruleset.id,
                GlobalRuleEntry.domain == payload.domain,
                GlobalRuleEntry.stream == payload.stream,
                GlobalRuleEntry.rule_key == payload.rule_key,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        row = GlobalRuleEntry(
            ruleset_id=ruleset.id,
            domain=payload.domain,
            stream=payload.stream,
            rule_key=payload.rule_key,
            payload_json=payload.payload_json,
            is_active=bool(payload.is_active),
        )
        db.add(row)
        action = 'created'
    else:
        row.payload_json = payload.payload_json
        row.is_active = bool(payload.is_active)
        action = 'updated'
    await db.commit()
    return {'status': action, 'ruleset_code': ruleset.code, 'domain': payload.domain.value, 'stream': payload.stream.value, 'rule_key': payload.rule_key}


@router.get('/tenants/{tenant_id}/rules/overrides')
async def list_tenant_rule_overrides(
    tenant_id: int,
    domain: RuleDomain | None = None,
    stream: OperationalStream | None = None,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(status_code=404, detail='Tenant not found')
    stmt = (
        select(TenantRuleOverride)
        .where(TenantRuleOverride.tenant_id == tenant_id)
        .order_by(TenantRuleOverride.domain.asc(), TenantRuleOverride.stream.asc(), TenantRuleOverride.rule_key.asc())
    )
    if domain:
        stmt = stmt.where(TenantRuleOverride.domain == domain)
    if stream:
        stmt = stmt.where(TenantRuleOverride.stream == stream)
    rows = (await db.execute(stmt)).scalars().all()
    return [
        {
            'id': r.id,
            'tenant_id': r.tenant_id,
            'domain': r.domain.value,
            'stream': r.stream.value,
            'rule_key': r.rule_key,
            'override_mode': r.override_mode.value,
            'payload_json': r.payload_json or {},
            'is_active': bool(r.is_active),
        }
        for r in rows
    ]


@router.put('/tenants/{tenant_id}/rules/overrides')
async def upsert_tenant_rule_override(
    tenant_id: int,
    payload: TenantRuleOverrideUpsertRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(status_code=404, detail='Tenant not found')
    row = (
        await db.execute(
            select(TenantRuleOverride).where(
                TenantRuleOverride.tenant_id == tenant_id,
                TenantRuleOverride.domain == payload.domain,
                TenantRuleOverride.stream == payload.stream,
                TenantRuleOverride.rule_key == payload.rule_key,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        row = TenantRuleOverride(
            tenant_id=tenant_id,
            domain=payload.domain,
            stream=payload.stream,
            rule_key=payload.rule_key,
            override_mode=payload.override_mode,
            payload_json=payload.payload_json,
            is_active=bool(payload.is_active),
        )
        db.add(row)
        action = 'created'
    else:
        row.override_mode = payload.override_mode
        row.payload_json = payload.payload_json
        row.is_active = bool(payload.is_active)
        action = 'updated'
    await db.commit()
    return {'status': action, 'tenant_id': tenant_id, 'domain': payload.domain.value, 'stream': payload.stream.value, 'rule_key': payload.rule_key}


@router.get('/tenants/{tenant_id}/rules/resolve')
async def resolve_tenant_rule(
    tenant_id: int,
    domain: RuleDomain,
    stream: OperationalStream,
    rule_key: str,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        raise HTTPException(status_code=404, detail='Tenant not found')

    payload = await resolve_rule_payload(
        db,
        tenant_id=tenant_id,
        domain=domain,
        stream=stream,
        rule_key=rule_key,
        fallback_payload={},
    )
    return {
        'tenant_id': tenant_id,
        'domain': domain.value,
        'stream': stream.value,
        'rule_key': rule_key,
        'resolved_payload': payload,
    }
