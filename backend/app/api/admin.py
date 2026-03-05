import secrets
from datetime import datetime, timedelta

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
    PlanFeature,
    PlanName,
    RoleName,
    Subscription,
    SubscriptionEvent,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantConnection,
    User,
)
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    discover_sqlserver,
    test_connection,
    test_connection_with_version,
)
from app.services.connection_secrets import build_odbc_connection_string, encrypt_sqlserver_secret, SqlServerSecret
from app.services.ingestion import enqueue_tenant_job
from app.services.subscriptions import get_or_create_subscription, infer_default_features_for_plan, sync_tenant_from_subscription
from app.services.provisioning_wizard import run_tenant_provisioning_wizard
from app.services.querypacks import apply_querypack_to_connection, load_querypack
from app.services.intelligence_service import list_insights, list_rules as list_tenant_rules, update_rule as update_tenant_rule

router = APIRouter(prefix='/v1/admin', tags=['admin'])

celery_client = Celery('admin_sender', broker=settings.celery_broker_url)


class TenantWizardRequest(BaseModel):
    name: str
    slug: str
    admin_email: EmailStr
    plan: PlanName = PlanName.standard
    source: str = 'external'
    subscription_status: SubscriptionStatus = SubscriptionStatus.trial
    trial_days: int = 14


class ConnectionCreateRequest(BaseModel):
    tenant_id: int
    connector_type: str = 'pharmacyone_sql'
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)
    sales_query_template: str = DEFAULT_GENERIC_SALES_QUERY
    purchases_query_template: str = DEFAULT_GENERIC_PURCHASES_QUERY
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


class SqlServerMappingRequest(BaseModel):
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    options: dict[str, str] = Field(default_factory=dict)
    sales_query_template: str = DEFAULT_GENERIC_SALES_QUERY
    purchases_query_template: str = DEFAULT_GENERIC_PURCHASES_QUERY
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
    include_purchases: bool = True


class InsightRuleUpdateRequest(BaseModel):
    enabled: bool | None = None
    severity_default: str | None = None
    params_json: dict | None = None


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
    conn.incremental_column = (payload.incremental_column or payload.updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = payload.id_column
    conn.date_column = payload.date_column
    conn.branch_column = payload.branch_column
    conn.item_column = payload.item_column
    conn.amount_column = (payload.amount_column or payload.net_amount_column).strip() or 'NetValue'
    conn.cost_column = payload.cost_column
    conn.qty_column = payload.qty_column
    conn.last_test_error = None

    await db.commit()
    return {'status': 'created', 'connection_id': conn.id}


@router.get('/tenants/{tenant_id}/sqlserver/mapping')
async def get_sqlserver_mapping(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
    if not conn:
        raise HTTPException(status_code=404, detail='SQL Server mapping not found')

    return {
        'tenant_id': tenant_id,
        'has_credentials': bool(conn.enc_payload),
        'sales_query_template': conn.sales_query_template or DEFAULT_GENERIC_SALES_QUERY,
        'purchases_query_template': conn.purchases_query_template or DEFAULT_GENERIC_PURCHASES_QUERY,
        'incremental_column': conn.incremental_column,
        'id_column': conn.id_column,
        'date_column': conn.date_column,
        'branch_column': conn.branch_column,
        'item_column': conn.item_column,
        'amount_column': conn.amount_column,
        'cost_column': conn.cost_column,
        'qty_column': conn.qty_column,
    }


@router.put('/tenants/{tenant_id}/sqlserver/mapping')
async def upsert_sqlserver_mapping(
    tenant_id: int,
    payload: SqlServerMappingRequest,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
    if not conn:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type='pharmacyone_sql',
            sync_status='never',
        )
        db.add(conn)

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
    conn.incremental_column = (payload.incremental_column or payload.updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = payload.id_column
    conn.date_column = payload.date_column
    conn.branch_column = payload.branch_column
    conn.item_column = payload.item_column
    conn.amount_column = (payload.amount_column or payload.net_amount_column).strip() or 'NetValue'
    conn.cost_column = payload.cost_column
    conn.qty_column = payload.qty_column
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
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
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
        conn = (
            await db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type == 'pharmacyone_sql',
                )
            )
        ).scalar_one_or_none()
        if conn is not None:
            conn.last_test_error = 'test_failed'
            await db.commit()
        return {'status': 'error', 'detail': 'test_failed'}
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
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

    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type='pharmacyone_sql',
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

    enqueue_tenant_job(
        tenant.slug,
        {
            'connector': 'pharmacyone_sql',
            'entity': 'sales',
            'tenant_slug': tenant.slug,
            'payload': {},
            'attempt': 0,
            'max_retries': settings.ingest_job_max_retries,
        },
    )
    enqueue_tenant_job(
        tenant.slug,
        {
            'connector': 'pharmacyone_sql',
            'entity': 'purchases',
            'tenant_slug': tenant.slug,
            'payload': {},
            'attempt': 0,
            'max_retries': settings.ingest_job_max_retries,
        },
    )
    celery_client.send_task(
        'worker.tasks.drain_tenant_ingest_queue',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )
    return {'status': 'queued', 'tenant': tenant.slug, 'jobs': ['sales', 'purchases'], 'queue': f'ingest:{tenant.slug}'}


@router.post('/tenants/{tenant_id}/querypack/apply')
async def apply_default_querypack(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail='Tenant not found')
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()
    if conn is None:
        conn = TenantConnection(tenant_id=tenant_id, connector_type='pharmacyone_sql', sync_status='never')
        db.add(conn)

    pack = load_querypack('pharmacyone', 'default')
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
    result = celery_client.send_task(
        'worker.tasks.enqueue_pharmacyone_backfill',
        kwargs={
            'tenant_slug': tenant.slug,
            'from_date_str': payload.from_date.date().isoformat(),
            'to_date_str': payload.to_date.date().isoformat(),
            'chunk_days': max(1, int(payload.chunk_days)),
            'include_purchases': bool(payload.include_purchases),
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
