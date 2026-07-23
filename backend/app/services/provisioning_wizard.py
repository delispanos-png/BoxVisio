import asyncio
import secrets
import string
import subprocess
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
from psycopg import sql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import get_password_hash
from app.db.tenant_manager import tenant_db_name
from app.models.control import (
    AuditLog,
    Plan,
    PlanName,
    ProfessionalProfile,
    RoleName,
    Subscription,
    SubscriptionLimit,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantConnection,
    TenantStatus,
    User,
)
from app.services.cloudflare_dns import ensure_tenant_dns_record, tenant_hostname
from app.services.connection_secrets import encrypt_json_secret, encrypt_sqlserver_secret
from app.services.email_delivery import send_tenant_welcome_email
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_CASHFLOW_QUERY,
    DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
    DEFAULT_GENERIC_INVENTORY_QUERY,
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
)
from app.services.subscriptions import infer_default_features_for_plan


def _latest_tenant_head(alembic_ini: Path) -> str:
    """Resolve the latest *tenant* alembic head dynamically.

    The alembic version dir holds both control and tenant migrations, so ``upgrade head`` is
    ambiguous. Tenant revisions are suffixed ``_tenant`` — pick that single head from alembic's
    own head list so this never goes stale when new tenant migrations are added.
    """
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(alembic_ini))
    cfg.set_main_option('script_location', str(alembic_ini.parent / 'alembic'))
    script = ScriptDirectory.from_config(cfg)
    tenant_heads = [h for h in script.get_heads() if str(h).endswith('_tenant')]
    if len(tenant_heads) != 1:
        raise RuntimeError(
            f'expected exactly one tenant migration head, found {tenant_heads} '
            f'(all heads: {script.get_heads()})'
        )
    return tenant_heads[0]

OPERATIONAL_STREAMS = [
    'sales_documents',
    'purchase_documents',
    'inventory_documents',
    'item_master',
    'cash_transactions',
    'supplier_balances',
    'customer_balances',
    'operating_expenses',
    'supplier_orders',
]


def _softone_endpoints(base_url: str) -> dict[str, str]:
    base = str(base_url or '').strip().rstrip('/')
    if not base:
        return {}
    return {
        'all': base + '/GetAllForBI',
        'health': base + '/HealthCheckBIBridge',
        'sales_documents': base + '/GetSalesDocumentsForBI',
        'purchase_documents': base + '/GetPurchaseDocumentsForBI',
        'inventory_documents': base + '/GetInventoryDocumentsForBI',
        'item_master': base + '/GetItemMasterForBI',
        'cash_transactions': base + '/GetCashTransactionsForBI',
        'supplier_balances': base + '/GetSupplierBalancesForBI',
        'customer_balances': base + '/GetCustomerBalancesForBI',
        'operating_expenses': base + '/GetOperatingExpensesForBI',
        'supplier_orders': base + '/GetSupplierOrdersForBI',
    }


def _softone_bridge_base_url(service_or_bridge_url: str, bridge_path: str = 'JS/myWS') -> str:
    base = str(service_or_bridge_url or '').strip().rstrip('/')
    if not base:
        return ''
    clean_bridge_path = str(bridge_path or 'JS/myWS').strip().strip('/')
    if '/s1services/' in base.lower():
        return base
    if base.lower().endswith('/s1services'):
        return base + '/' + clean_bridge_path
    return base


def _rand_secret(size: int = 40) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(size))


async def _required_profile_id_by_code(db: AsyncSession, code: str) -> int:
    profile = (
        await db.execute(select(ProfessionalProfile).where(ProfessionalProfile.profile_code == str(code).upper()))
    ).scalar_one_or_none()
    if not profile:
        raise RuntimeError(f'missing professional profile seed: {code}')
    return int(profile.id)


def _admin_dsn() -> str:
    return (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname=postgres user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )


def _create_db_and_role(db_name: str, db_user: str, db_password: str) -> None:
    template_db = 'startdb'
    template_owner = 'u_startdb'
    used_template = False
    with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (db_user,))
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(db_user),
                        sql.Literal(db_password),
                    )
                )
            else:
                cur.execute(
                    sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                        sql.Identifier(db_user),
                        sql.Literal(db_password),
                    )
                )

            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (template_db,))
                if db_name != template_db and cur.fetchone() is not None:
                    cur.execute(
                        sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE {}").format(
                            sql.Identifier(db_name),
                            sql.Identifier(db_user),
                            sql.Identifier(template_db),
                        )
                    )
                    used_template = True
                else:
                    cur.execute(
                        sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(db_name), sql.Identifier(db_user))
                    )

    if used_template:
        dsn = (
            f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
            f"dbname={db_name} user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
        )
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (template_owner,))
                if cur.fetchone() is not None:
                    cur.execute(
                        sql.SQL("REASSIGN OWNED BY {} TO {}").format(
                            sql.Identifier(template_owner),
                            sql.Identifier(db_user),
                        )
                    )


def _drop_db_and_role(db_name: str, db_user: str) -> None:
    with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
            cur.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(db_user)))


def _run_tenant_migrations(db_name: str, db_user: str, db_password: str) -> None:
    tenant_url = settings.tenant_database_url_template_sync.format(
        user=db_user,
        password=db_password,
        db_name=db_name,
    )
    backend_root = Path(__file__).resolve().parents[2]
    alembic_ini = backend_root / 'alembic.ini'
    env = {
        **dict(os.environ),
        'MIGRATION_TARGET': 'tenant',
        'TENANT_MIGRATION_URL': tenant_url,
    }
    tenant_head = _latest_tenant_head(alembic_ini)
    subprocess.run(
        [sys.executable, '-m', 'alembic', '-c', str(alembic_ini), 'upgrade', tenant_head],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        cwd=str(backend_root),
    )


async def run_tenant_provisioning_wizard(
    db: AsyncSession,
    *,
    name: str,
    slug: str,
    admin_email: str,
    plan: PlanName,
    source: str,
    subscription_status: SubscriptionStatus,
    trial_days: int | None,
    max_users: int | None = None,
    send_welcome_email: bool = True,
    create_subdomain: bool = False,
    connection_type: str = 'none',
    softone_base_url: str = '',
    softone_username: str = '',
    softone_password: str = '',
    softone_app_id: str = '',
    softone_company: str = '',
    softone_branch: str = '',
    softone_module: str = '0',
    softone_refid: str = '',
    softone_bridge_path: str = 'JS/myWS',
    sql_host: str = '',
    sql_port: int = 1433,
    sql_database: str = '',
    sql_username: str = '',
    sql_password: str = '',
    sql_options: str = 'Encrypt=yes;TrustServerCertificate=yes',
) -> dict:
    source = str(source or '').strip().lower()
    if source in {'pharmacyone', 'pharmacyone_sql'}:
        source = 'sql'

    steps: list[dict] = []
    db_name = tenant_db_name(slug)
    db_user = f"u_{slug[:30]}"
    db_password = _rand_secret(24)
    api_key_secret = _rand_secret(40)
    api_key_id = secrets.token_urlsafe(16)
    invite_token = secrets.token_urlsafe(24)
    temporary_password = _rand_secret(14)
    trial_days_eff = settings.default_trial_days if trial_days is None else trial_days
    created_db = False

    def mark(step_no: int, title: str, status: str, message: str = ''):
        steps.append({'step': step_no, 'title': title, 'status': status, 'message': message})

    try:
        # STEP 1: Tenant details
        existing = (await db.execute(select(Tenant).where(Tenant.slug == slug))).scalar_one_or_none()
        if existing:
            raise ValueError(f'tenant slug already exists: {slug}')
        mark(1, 'Tenant details', 'ok')

        # STEP 2: Plan selection
        plan_row = (await db.execute(select(Plan).where(Plan.code == plan.value))).scalar_one_or_none()
        if not plan_row:
            raise ValueError(f'plan not found: {plan.value}')
        try:
            max_users_eff = int(max_users or plan_row.max_users or 1)
        except (TypeError, ValueError):
            max_users_eff = int(plan_row.max_users or 1)
        max_users_eff = max(1, min(max_users_eff, 9999))
        mark(2, 'Plan selection', 'ok', f'{plan.value}, max_users={max_users_eff}')

        # STEP 3: Data source selection (ERP/data-source agnostic)
        if source not in {'sql', 'external', 'files'}:
            raise ValueError(f'invalid source: {source}')
        mark(3, 'Data source selection', 'ok', source)

        # STEP 4: Create tenant DB
        await asyncio.to_thread(_create_db_and_role, db_name, db_user, db_password)
        created_db = True
        mark(4, 'Create tenant DB', 'ok', db_name)

        # STEP 5: Run tenant migrations
        await asyncio.to_thread(_run_tenant_migrations, db_name, db_user, db_password)
        mark(5, 'Run tenant migrations', 'ok')

        # STEP 6: Create tenant admin user + subscription
        features = infer_default_features_for_plan(plan)
        trial_ends = datetime.utcnow() + timedelta(days=trial_days_eff) if subscription_status == SubscriptionStatus.trial else None
        period_end = datetime.utcnow() + timedelta(days=30) if subscription_status in {SubscriptionStatus.active, SubscriptionStatus.past_due} else None

        tenant = Tenant(
            name=name,
            slug=slug,
            plan=plan,
            status=TenantStatus.active,
            source=source,
            subscription_status=subscription_status,
            trial_ends_at=trial_ends,
            current_period_end=period_end,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            feature_flags={
                **features,
                'contact': {
                    'contact_person': '',
                    'contact_email': str(admin_email or '').strip(),
                    'contact_phone': '',
                    'contact_mobile': '',
                    'billing_email': str(admin_email or '').strip(),
                    'technical_email': str(admin_email or '').strip(),
                    'address': '',
                    'city': '',
                    'postal_code': '',
                    'afm': '',
                    'doy': '',
                    'notes': '',
                },
            },
        )
        db.add(tenant)
        await db.flush()
        manager_profile_id = await _required_profile_id_by_code(db, 'MANAGER')

        user = User(
            tenant_id=tenant.id,
            professional_profile_id=manager_profile_id,
            email=admin_email,
            password_hash=get_password_hash(temporary_password),
            role=RoleName.tenant_admin,
            reset_token=invite_token,
            reset_token_expires_at=datetime.utcnow() + timedelta(days=2),
            is_active=True,
        )
        db.add(user)

        sub = Subscription(
            tenant_id=tenant.id,
            plan=plan,
            status=subscription_status,
            trial_starts_at=datetime.utcnow() if subscription_status == SubscriptionStatus.trial else None,
            trial_ends_at=trial_ends,
            current_period_start=datetime.utcnow() if subscription_status in {SubscriptionStatus.active, SubscriptionStatus.past_due} else None,
            current_period_end=period_end,
            feature_flags=features,
        )
        db.add(sub)
        await db.flush()
        db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_users', limit_value=max_users_eff, used_value=0))
        db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_branches', limit_value=plan_row.max_branches, used_value=0))
        mark(6, 'Create tenant admin user', 'ok', admin_email)

        # STEP 7: Generate API keys
        api_key = TenantApiKey(
            tenant_id=tenant.id,
            key_id=api_key_id,
            key_secret=api_key_secret,
            is_active=True,
        )
        db.add(api_key)
        connection_type = str(connection_type or 'none').strip().lower()
        db.add(
            AuditLog(
                tenant_id=tenant.id,
                action='tenant_provisioned_wizard',
                entity_type='tenant',
                entity_id=str(tenant.id),
                payload={
                    'steps': 10,
                    'source': source,
                    'plan': plan.value,
                    'max_users': max_users_eff,
                    'connection_type': connection_type,
                },
            )
        )
        mark(7, 'Generate API keys', 'ok')

        softone_context = {
            'app_id': str(softone_app_id or '').strip(),
            'appId': str(softone_app_id or '').strip(),
            'company': str(softone_company or '').strip(),
            'COMPANY': str(softone_company or '').strip(),
            'branch': str(softone_branch or '').strip(),
            'BRANCH': str(softone_branch or '').strip(),
            'module': str(softone_module or '0').strip() or '0',
            'MODULE': str(softone_module or '0').strip() or '0',
            'refid': str(softone_refid or '').strip(),
            'REFID': str(softone_refid or '').strip(),
        }

        softone_service_url = str(softone_base_url or '').strip().rstrip('/')
        softone_bridge_url = _softone_bridge_base_url(softone_service_url, softone_bridge_path)
        if connection_type == 'softone_api' and softone_bridge_url:
            auth_config = {
                'username': str(softone_username or '').strip(),
                **softone_context,
                'client_id_param': 'clientID',
            }
            conn = TenantConnection(
                tenant_id=tenant.id,
                connector_type='external_api',
                source_type='api',
                is_active=True,
                sync_status='never',
                supported_streams=list(OPERATIONAL_STREAMS),
                enabled_streams=list(OPERATIONAL_STREAMS),
                enc_payload=encrypt_json_secret({'password': str(softone_password or '')}),
                stream_api_endpoint=_softone_endpoints(softone_bridge_url),
                connection_parameters={
                    'connector_type': 'external_api',
                    'source_type': 'api',
                    'base_url': softone_bridge_url,
                    'service_url': softone_service_url,
                    'bridge_path': str(softone_bridge_path or 'JS/myWS').strip().strip('/'),
                    'host': softone_bridge_url,
                    'port': 443,
                    'database': 'api',
                    'username': auth_config['username'],
                    'auth_type': 'softone_login',
                    'auth_config': auth_config,
                    'verify_tls': False,
                    'retry_attempts': 2,
                    'timeout_seconds': 120,
                    'sync_interval_minutes': 5,
                    'auto_sync_enabled': False,
                },
            )
            db.add(conn)
            mark(8, 'Create SoftOne API connection', 'ok', softone_bridge_url)
        elif connection_type == 'sql_server' and str(sql_host or '').strip() and str(sql_database or '').strip():
            options_map = {}
            for part in str(sql_options or '').split(';'):
                if '=' not in part:
                    continue
                key, value = part.split('=', 1)
                key = key.strip()
                if key:
                    options_map[key] = value.strip()
            conn = TenantConnection(
                tenant_id=tenant.id,
                connector_type='sql_connector',
                source_type='sql',
                is_active=True,
                sync_status='never',
                supported_streams=list(OPERATIONAL_STREAMS),
                enabled_streams=list(OPERATIONAL_STREAMS),
                enc_payload=encrypt_sqlserver_secret(
                    host=str(sql_host or '').strip(),
                    port=int(sql_port or 1433),
                    database=str(sql_database or '').strip(),
                    username=str(sql_username or '').strip(),
                    password=str(sql_password or ''),
                    options=options_map,
                ),
                sales_query_template=DEFAULT_GENERIC_SALES_QUERY,
                purchases_query_template=DEFAULT_GENERIC_PURCHASES_QUERY,
                inventory_query_template=DEFAULT_GENERIC_INVENTORY_QUERY,
                cashflow_query_template=DEFAULT_GENERIC_CASHFLOW_QUERY,
                supplier_balances_query_template=DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
                customer_balances_query_template=DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
                incremental_column='UpdatedAt',
                id_column='LineId',
                date_column='DocDate',
                branch_column='BranchCode',
                item_column='ItemCode',
                amount_column='NetValue',
                cost_column='CostValue',
                qty_column='Qty',
                connection_parameters={
                    'connector_type': 'sql_connector',
                    'source_type': 'sql',
                    'host': str(sql_host or '').strip(),
                    'port': int(sql_port or 1433),
                    'database': str(sql_database or '').strip(),
                    'username': str(sql_username or '').strip(),
                    'options': options_map,
                    'company_id': softone_context['company'],
                    **softone_context,
                    'auth_config': softone_context,
                    'sync_interval_minutes': 5,
                    'auto_sync_enabled': False,
                },
            )
            db.add(conn)
            mark(8, 'Create SQL Server connection', 'ok', str(sql_host or '').strip())
        elif connection_type == 'softone_api':
            mark(8, 'Create SoftOne API connection', 'skipped', 'missing base URL')
        elif connection_type == 'sql_server':
            mark(8, 'Create SQL Server connection', 'skipped', 'missing SQL host/database')
        else:
            mark(8, 'Create data connection', 'skipped')

        dns_result = {'status': 'skipped'}
        if create_subdomain:
            try:
                dns_result = await asyncio.to_thread(ensure_tenant_dns_record, slug)
                mark(9, 'Create tenant subdomain', str(dns_result.get('status') or 'unknown'), tenant_hostname(slug))
            except Exception as exc:
                dns_result = {'status': 'error', 'error': str(exc)}
                mark(9, 'Create tenant subdomain', 'error', str(exc))
        else:
            mark(9, 'Create tenant subdomain', 'skipped')

        email_result = {'status': 'skipped'}
        if send_welcome_email:
            try:
                email_result = await asyncio.to_thread(
                    send_tenant_welcome_email,
                    tenant_name=name,
                    tenant_slug=slug,
                    admin_email=admin_email,
                    invite_token=invite_token,
                    temporary_password=temporary_password,
                )
                mark(10, 'Send welcome email', str(email_result.get('status') or 'unknown'), admin_email)
            except Exception as exc:
                email_result = {'status': 'error', 'error': str(exc)}
                mark(10, 'Send welcome email', 'error', str(exc))
            if str(email_result.get('status') or '').lower() != 'sent':
                reason = str(email_result.get('reason') or email_result.get('error') or email_result.get('status') or 'email_not_sent')
                raise RuntimeError(f'welcome email failed: {reason}')
        else:
            mark(10, 'Send welcome email', 'skipped')

        await db.commit()

        return {
            'status': 'ok',
            'tenant_id': tenant.id,
            'slug': slug,
            'plan': plan.value,
            'max_users': max_users_eff,
            'subscription_status': subscription_status.value,
            'invite_url': email_result.get('invite_url'),
            'email_status': email_result.get('status'),
            'dns_status': dns_result.get('status'),
            'tenant_hostname': tenant_hostname(slug) if create_subdomain else None,
            'api_key_id': api_key_id,
            'steps': steps,
        }
    except Exception as exc:
        await db.rollback()
        if created_db:
            try:
                await asyncio.to_thread(_drop_db_and_role, db_name, db_user)
            except Exception:
                pass
        mark(0, 'Rollback', 'ok', 'rolled back control DB + tenant DB resources')
        return {
            'status': 'error',
            'error': str(exc),
            'steps': steps,
        }
