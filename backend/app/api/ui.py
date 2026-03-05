import secrets
import json
import logging
import re
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import UUID

from celery import Celery
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_request_tenant, get_tenant_db, require_roles
from app.core.config import settings
from app.core.i18n import normalize_lang, tt
from app.core.security import (
    audience_for_role,
    create_access_token,
    create_refresh_token,
    expected_audience_for_host,
    get_password_hash,
    safe_decode,
    verify_password,
)
from app.models.control import RefreshToken
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
    TenantStatus,
    User,
)
from app.models.tenant import Insight
from app.services.intelligence_service import (
    insights_counts_by_severity,
    list_insights as list_tenant_insights,
    list_rules as list_tenant_rules,
    update_rule as update_tenant_rule,
)
from app.services.connection_secrets import SqlServerSecret, build_odbc_connection_string, encrypt_sqlserver_secret
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    discover_candidate_tables,
    discover_columns,
    discover_sample_rows,
    test_connection,
)
from app.services.provisioning_wizard import run_tenant_provisioning_wizard
from app.services.querypacks import apply_querypack_to_connection, load_querypack
from app.services.subscriptions import get_or_create_subscription, sync_tenant_from_subscription

router = APIRouter(tags=['ui'])
templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parents[1] / 'templates'),
    context_processors=[lambda request: {'tt': tt}],
)
templates.env.globals.setdefault('tt', tt)
celery_client = Celery('ui_sender', broker=settings.celery_broker_url)
logger = logging.getLogger(__name__)


def _login_redirect_for(user: User, host: str | None = None) -> str:
    if user.role == RoleName.cloudon_admin:
        return '/admin/dashboard'
    if host and host.lower() == 'adminpanel.boxvisio.com':
        return '/admin/dashboard'
    return '/tenant/dashboard'


def _cookie_domain_for_host(host: str) -> str | None:
    if host in {settings.admin_portal_host.lower(), settings.tenant_portal_host.lower()}:
        return host
    return None


def _normalize_theme(raw: str | None) -> str:
    val = (raw or '').strip().lower()
    return val if val in {'light', 'dark'} else 'light'


def _normalize_plan(raw: str) -> str:
    val = (raw or '').strip().lower()
    mapping = {
        'standard': 'standard',
        'std': 'standard',
        'pro': 'pro',
        'enterprise': 'enterprise',
        'ent': 'enterprise',
    }
    return mapping.get(val, val)


def _normalize_source(raw: str) -> str:
    val = (raw or '').strip().lower()
    mapping = {
        'sql': 'pharmacyone',
        'pharmacyone': 'pharmacyone',
        'pharmacyone_sql': 'pharmacyone',
        'api': 'external',
        'external': 'external',
        'external_api': 'external',
        'files': 'files',
        'file': 'files',
    }
    return mapping.get(val, val)


def _read_cpu_times() -> tuple[int, int]:
    with open('/proc/stat', encoding='utf-8') as f:
        line = f.readline().strip()
    parts = line.split()
    nums = [int(x) for x in parts[1:]]
    total = sum(nums)
    idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
    return total, idle


def _cpu_usage_percent() -> float:
    try:
        total_1, idle_1 = _read_cpu_times()
        time.sleep(0.08)
        total_2, idle_2 = _read_cpu_times()
        total_delta = max(1, total_2 - total_1)
        idle_delta = max(0, idle_2 - idle_1)
        busy = max(0.0, 1.0 - (idle_delta / total_delta))
        return round(busy * 100.0, 2)
    except Exception:
        return 0.0


def _memory_usage() -> dict[str, float]:
    try:
        meminfo: dict[str, int] = {}
        with open('/proc/meminfo', encoding='utf-8') as f:
            for line in f:
                if ':' not in line:
                    continue
                key, val = line.split(':', 1)
                meminfo[key.strip()] = int(val.strip().split()[0])  # kB
        total_kb = float(meminfo.get('MemTotal', 0))
        avail_kb = float(meminfo.get('MemAvailable', 0))
        used_kb = max(0.0, total_kb - avail_kb)
        pct = (used_kb / total_kb * 100.0) if total_kb > 0 else 0.0
        return {
            'total_gb': round(total_kb / 1024 / 1024, 2),
            'used_gb': round(used_kb / 1024 / 1024, 2),
            'free_gb': round(avail_kb / 1024 / 1024, 2),
            'percent': round(pct, 2),
        }
    except Exception:
        return {'total_gb': 0.0, 'used_gb': 0.0, 'free_gb': 0.0, 'percent': 0.0}


def _disk_usage() -> dict[str, float]:
    try:
        d = shutil.disk_usage('/')
        total_gb = d.total / 1024 / 1024 / 1024
        used_gb = d.used / 1024 / 1024 / 1024
        free_gb = d.free / 1024 / 1024 / 1024
        pct = (d.used / d.total * 100.0) if d.total > 0 else 0.0
        return {
            'total_gb': round(total_gb, 2),
            'used_gb': round(used_gb, 2),
            'free_gb': round(free_gb, 2),
            'percent': round(pct, 2),
        }
    except Exception:
        return {'total_gb': 0.0, 'used_gb': 0.0, 'free_gb': 0.0, 'percent': 0.0}


def _normalize_slug(raw: str) -> str:
    val = (raw or '').strip().lower()
    val = re.sub(r'[^a-z0-9-]+', '-', val)
    val = re.sub(r'-{2,}', '-', val).strip('-')
    return val


def _normalize_sub_status(raw: str) -> str:
    return (raw or '').strip().lower()


def _normalize_tenant_status(raw: str) -> str:
    return (raw or '').strip().lower()


def _tenant_feature_flags(tenant: Tenant) -> dict[str, bool]:
    is_enterprise = tenant.plan == PlanName.enterprise
    source = (tenant.source or '').strip().lower()
    has_pharmacyone = source in {'pharmacyone', 'sql', 'pharmacyone_sql'}
    return {
        'inventory_enabled': is_enterprise and has_pharmacyone,
        'cashflow_enabled': is_enterprise and has_pharmacyone,
    }


def _current_lang(request: Request) -> str:
    return normalize_lang(request.cookies.get('lang', 'el'))


_RULE_CATEGORY_LABELS = {
    'el': {
        'sales': 'Πωλήσεις',
        'purchases': 'Αγορές',
        'inventory': 'Απόθεμα',
        'cashflow': 'Cashflow',
    },
    'en': {
        'sales': 'Sales',
        'purchases': 'Purchases',
        'inventory': 'Inventory',
        'cashflow': 'Cashflow',
    },
}

_RULE_NAME_EL = {
    'SLS_DROP_PERIOD': 'Πτώση Τζίρου Περιόδου',
    'SLS_SPIKE_PERIOD': 'Απότομη Αύξηση Τζίρου',
    'PRF_DROP_PERIOD': 'Πτώση Κερδοφορίας',
    'MRG_DROP_POINTS': 'Μείωση Περιθωρίου (μονάδες)',
    'BR_UNDERPERFORM': 'Υποαπόδοση Καταστήματος',
    'BR_MARGIN_LOW': 'Χαμηλό Περιθώριο Καταστήματος',
    'CAT_DROP': 'Πτώση Κατηγορίας',
    'CAT_MARGIN_EROSION': 'Διάβρωση Περιθωρίου Κατηγορίας',
    'BRAND_DROP': 'Πτώση Brand',
    'TOP_DEPENDENCY': 'Εξάρτηση από Top Προϊόντα',
    'SLS_VOLATILITY': 'Υψηλή Μεταβλητότητα Πωλήσεων',
    'WEEKEND_SHIFT': 'Μεταβολή Σαββατοκύριακου',
    'PUR_SPIKE_PERIOD': 'Αύξηση Αγορών Περιόδου',
    'PUR_DROP_PERIOD': 'Μείωση Αγορών Περιόδου',
    'SUP_DEPENDENCY': 'Εξάρτηση από Προμηθευτή',
    'SUP_COST_UP': 'Αύξηση Κόστους Προμηθευτή',
    'SUP_VOLATILITY': 'Αστάθεια Τιμών Προμηθευτή',
    'PUR_MARGIN_PRESSURE': 'Πίεση Περιθωρίου από Αγορές',
    'INV_DEAD_STOCK': 'Νεκρό Απόθεμα',
    'INV_AGING_SPIKE': 'Αύξηση Παλαιού Αποθέματος',
    'INV_LOW_COVERAGE': 'Χαμηλή Κάλυψη Top Ειδών',
    'INV_OVERSTOCK_SLOW': 'Υπερβολικό Απόθεμα Αργών Ειδών',
    'DEAD_STOCK': 'Νεκρό Απόθεμα',
    'INV_AGING_SPIKE': 'Αύξηση Παλαιότητας Αποθέματος',
    'INVENTORY_VALUE_SPIKE': 'Απότομη Αύξηση Αξίας Αποθέματος',
    'LOW_COVERAGE': 'Χαμηλή Κάλυψη',
    'OVERSTOCK_RISK': 'Κίνδυνος Υπεραποθέματος',
}

_RULE_DESC_EL = {
    'SLS_DROP_PERIOD': 'Ο τζίρος της περιόδου μειώθηκε σε σχέση με την προηγούμενη περίοδο.',
    'SLS_SPIKE_PERIOD': 'Ο τζίρος της περιόδου αυξήθηκε απότομα σε σχέση με την προηγούμενη.',
    'PRF_DROP_PERIOD': 'Τα μικτά κέρδη έπεσαν σε σχέση με την προηγούμενη περίοδο.',
    'MRG_DROP_POINTS': 'Το περιθώριο κέρδους έπεσε κατά μονάδες σε σχέση με την προηγούμενη περίοδο.',
    'BR_UNDERPERFORM': 'Το κατάστημα είναι κάτω από τον μέσο όρο της εταιρείας.',
    'BR_MARGIN_LOW': 'Το περιθώριο του καταστήματος είναι χαμηλότερο από του συνόλου.',
    'CAT_DROP': 'Η κατηγορία παρουσιάζει πτώση τζίρου.',
    'CAT_MARGIN_EROSION': 'Η κατηγορία παρουσιάζει διάβρωση περιθωρίου.',
    'BRAND_DROP': 'Το brand παρουσιάζει πτώση τζίρου.',
    'TOP_DEPENDENCY': 'Υψηλή συγκέντρωση τζίρου σε λίγα κορυφαία προϊόντα.',
    'SLS_VOLATILITY': 'Υψηλή ημερήσια διακύμανση πωλήσεων.',
    'WEEKEND_SHIFT': 'Αλλαγή συμπεριφοράς πωλήσεων στο Σαββατοκύριακο.',
    'PUR_SPIKE_PERIOD': 'Οι αγορές αυξήθηκαν σε σχέση με την προηγούμενη περίοδο.',
    'PUR_DROP_PERIOD': 'Οι αγορές μειώθηκαν σε σχέση με την προηγούμενη περίοδο.',
    'SUP_DEPENDENCY': 'Υψηλή εξάρτηση αγορών από έναν προμηθευτή.',
    'SUP_COST_UP': 'Αύξηση κόστους αγορών από προμηθευτή.',
    'SUP_VOLATILITY': 'Υψηλή μεταβλητότητα κόστους προμηθευτή.',
    'PUR_MARGIN_PRESSURE': 'Αγορές αυξημένες ενώ το περιθώριο μειώνεται.',
    'INV_DEAD_STOCK': 'Απόθεμα χωρίς πωλήσεις για πολλές ημέρες και υψηλή αξία.',
    'INV_AGING_SPIKE': 'Αύξηση αξίας αποθέματος μεγάλης παλαιότητας.',
    'INV_LOW_COVERAGE': 'Χαμηλές ημέρες κάλυψης σε top είδη.',
    'INV_OVERSTOCK_SLOW': 'Υψηλή αξία σε αργοκίνητα είδη.',
    'DEAD_STOCK': 'Απόθεμα χωρίς κίνηση για Χ ημέρες και υψηλή αξία.',
    'INVENTORY_VALUE_SPIKE': 'Απότομη αύξηση της συνολικής αξίας αποθέματος.',
    'LOW_COVERAGE': 'Ημέρες κάλυψης κάτω από το όριο ασφαλείας.',
    'OVERSTOCK_RISK': 'Αξία υπεραποθέματος σε αργοκίνητα είδη.',
}


def _localize_insight_rule(rule: object, lang: str) -> dict[str, object]:
    code = getattr(rule, 'code', '')
    category = str(getattr(rule, 'category', '') or '').strip().lower()
    default_name = getattr(rule, 'name', '') or code
    default_description = getattr(rule, 'description', '') or ''
    if lang == 'el':
        name_display = _RULE_NAME_EL.get(code, default_name)
        description_display = _RULE_DESC_EL.get(code, default_description)
        category_display = _RULE_CATEGORY_LABELS['el'].get(category, category or '-')
    else:
        name_display = default_name
        description_display = default_description
        category_display = _RULE_CATEGORY_LABELS['en'].get(category, category or '-')
    return {
        'code': code,
        'name_display': name_display,
        'description_display': description_display,
        'category_display': category_display,
        'enabled': getattr(rule, 'enabled', False),
        'severity_default': getattr(rule, 'severity_default', 'warning'),
        'params_json': getattr(rule, 'params_json', {}) or {},
    }


@router.get('/set-language/{lang_code}')
async def set_language(lang_code: str, request: Request, next: str = '/'):
    lang = normalize_lang(lang_code)
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    secure_cookie = request.url.scheme == 'https' or forwarded_proto == 'https'
    response = RedirectResponse(url=next or '/', status_code=303)
    response.set_cookie(
        key='lang',
        value=lang,
        httponly=False,
        secure=secure_cookie,
        samesite='lax',
        max_age=365 * 24 * 60 * 60,
        path='/',
        domain=cookie_domain,
    )
    return response


@router.get('/set-theme/{theme_mode}')
async def set_theme(theme_mode: str, request: Request, next: str = '/'):
    mode = _normalize_theme(theme_mode)
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    secure_cookie = request.url.scheme == 'https' or forwarded_proto == 'https'
    response = RedirectResponse(url=next or '/', status_code=303)
    response.set_cookie(
        key='theme',
        value=mode,
        httponly=False,
        secure=secure_cookie,
        samesite='lax',
        max_age=365 * 24 * 60 * 60,
        path='/',
        domain=cookie_domain,
    )
    return response


def _parse_options_map(options: str) -> dict[str, str]:
    options_map: dict[str, str] = {}
    for item in [x.strip() for x in options.split(';') if x.strip()]:
        if '=' in item:
            k, v = item.split('=', 1)
            options_map[k.strip()] = v.strip()
    return options_map


async def _connections_template_context(
    db: AsyncSession,
    *,
    request: Request,
    result: dict | None = None,
    discovery: dict | None = None,
    form_values: dict | None = None,
) -> dict:
    rows = (await db.execute(select(TenantConnection).order_by(TenantConnection.id.desc()))).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    return {
        'request': request,
        'connections': rows,
        'tenants': tenants,
        'active_page': 'connections',
        'title': 'connections',
        'server_public_ip': settings.server_public_ip,
        'sqlserver_port': settings.sqlserver_default_port,
        'result': result,
        'discovery': discovery,
        'form_values': form_values or {},
    }


def _parse_date_or_none(raw: str | None):
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


async def _tenant_insight_counts(tenant: Tenant) -> dict[str, int]:
    payload = {'critical': 0, 'warning': 0, 'info': 0, 'open': 0}
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        sev = await insights_counts_by_severity(tenant_db)
        payload.update(sev)
        open_count = (
            await tenant_db.execute(select(func.count(Insight.id)).where(Insight.status == 'open'))
        ).scalar_one()
        payload['open'] = int(open_count or 0)
        break
    return payload


@router.api_route('/', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def portal_root(request: Request):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    token = request.cookies.get('access_token')
    payload = None
    if token:
        expected_aud = expected_audience_for_host(host)
        payload = safe_decode(token, audience=expected_aud, token_type='access')

    if payload:
        role = payload.get('role')
        if role == RoleName.cloudon_admin.value or host == settings.admin_portal_host.lower():
            resp = RedirectResponse(url='/admin/dashboard', status_code=302)
            resp.headers['Cache-Control'] = 'no-store'
            return resp
        resp = RedirectResponse(url='/tenant/dashboard', status_code=302)
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    if request.method.upper() == 'HEAD':
        resp = Response(status_code=200, media_type='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    resp = templates.TemplateResponse('auth/login.html', {'request': request, 'error': None})
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@router.api_route('/login', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def login_page(request: Request, error: str | None = None):
    if request.method.upper() == 'HEAD':
        resp = Response(status_code=200, media_type='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    resp = templates.TemplateResponse('auth/login.html', {'request': request, 'error': error})
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@router.post('/login')
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.email == email, User.is_active.is_(True)))).scalar_one_or_none()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            'auth/login.html',
            {'request': request, 'error': tt(request, 'invalid_credentials')},
            status_code=401,
        )

    host = (request.headers.get('host') or '').split(':')[0].lower()
    access_audience = expected_audience_for_host(host) or audience_for_role(user.role.value)
    token = create_access_token(
        subject=str(user.id),
        tenant_id=user.tenant_id,
        role=user.role.value,
        audience=access_audience,
    )
    refresh_token, refresh_jti, refresh_exp = create_refresh_token(subject=str(user.id))
    db.add(
        RefreshToken(
            user_id=user.id,
            token_jti=refresh_jti,
            expires_at=refresh_exp.replace(tzinfo=None),
            revoked_at=None,
        )
    )
    await db.commit()
    cookie_domain = _cookie_domain_for_host(host)
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    secure_cookie = request.url.scheme == 'https' or forwarded_proto == 'https'
    resp = RedirectResponse(url=_login_redirect_for(user, host=host), status_code=303)
    resp.set_cookie(
        key='access_token',
        value=token,
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
        max_age=settings.access_token_expire_minutes * 60,
        path='/',
        domain=cookie_domain,
    )
    resp.set_cookie(
        key='refresh_token',
        value=refresh_token,
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
        max_age=settings.refresh_token_expire_days * 24 * 60 * 60,
        path='/',
        domain=cookie_domain,
    )
    resp.set_cookie(
        key='csrf_token',
        value=secrets.token_urlsafe(24),
        httponly=False,
        secure=secure_cookie,
        samesite='lax',
        max_age=settings.refresh_token_expire_days * 24 * 60 * 60,
        path='/',
        domain=cookie_domain,
    )
    return resp


@router.post('/logout')
async def logout(request: Request):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    resp = RedirectResponse(url='/login', status_code=303)
    resp.delete_cookie('access_token', path='/', domain=cookie_domain)
    resp.delete_cookie('refresh_token', path='/', domain=cookie_domain)
    resp.delete_cookie('csrf_token', path='/', domain=cookie_domain)
    return resp


@router.get('/logout')
async def logout_get(request: Request):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    resp = RedirectResponse(url='/login', status_code=303)
    resp.delete_cookie('access_token', path='/', domain=cookie_domain)
    resp.delete_cookie('refresh_token', path='/', domain=cookie_domain)
    resp.delete_cookie('csrf_token', path='/', domain=cookie_domain)
    return resp


@router.get('/admin/dashboard', response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    _user=Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    total_tenants = (await db.execute(select(func.count(Tenant.id)))).scalar_one() or 0
    active_tenants = (
        await db.execute(select(func.count(Tenant.id)).where(Tenant.status == TenantStatus.active))
    ).scalar_one() or 0
    total_users = (await db.execute(select(func.count(User.id)).where(User.is_active.is_(True)))).scalar_one() or 0
    total_connections = (await db.execute(select(func.count(TenantConnection.id)))).scalar_one() or 0

    status_breakdown = {
        'trial': 0,
        'active': 0,
        'past_due': 0,
        'suspended': 0,
        'canceled': 0,
    }
    subs = (await db.execute(select(Subscription.status))).all()
    for row in subs:
        status_breakdown[row[0].value] = status_breakdown.get(row[0].value, 0) + 1

    server_info = {
        'cpu': {'percent': _cpu_usage_percent()},
        'ram': _memory_usage(),
        'disk': _disk_usage(),
    }

    return templates.TemplateResponse(
        'admin/dashboard.html',
        {
            'request': request,
            'title': 'title_admin_dashboard',
            'active_page': 'dashboard',
            'total_tenants': total_tenants,
            'active_tenants': active_tenants,
            'total_users': total_users,
            'total_connections': total_connections,
            'status_breakdown': status_breakdown,
            'server_info': server_info,
        },
    )


@router.get('/admin/server-info.json')
async def admin_server_info_json(
    _user=Depends(require_roles(RoleName.cloudon_admin)),
):
    return JSONResponse(
        {
            'cpu': {'percent': _cpu_usage_percent()},
            'ram': _memory_usage(),
            'disk': _disk_usage(),
        }
    )


@router.get('/admin/tenants', response_class=HTMLResponse)
async def admin_tenants(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (
        await db.execute(select(Tenant).where(Tenant.status != TenantStatus.terminated).order_by(Tenant.created_at.desc()))
    ).scalars().all()
    for t in tenants:
        sub = await get_or_create_subscription(db, t)
        await sync_tenant_from_subscription(db, t, sub)
    await db.commit()
    return templates.TemplateResponse(
        'admin/tenants.html',
        {
            'request': request,
            'tenants': tenants,
            'error': None,
            'provisioning_result': None,
            'active_page': 'tenants',
            'title': 'title_tenants',
        },
    )


@router.post('/admin/tenants/create')
async def admin_tenant_create(
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    admin_email: str = Form(...),
    plan: str = Form(default='standard'),
    source: str = Form(default='external'),
    subscription_status: str = Form(default='trial'),
    trial_days: int = Form(default=14),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    selected_plan = PlanName(plan)
    selected_sub = SubscriptionStatus(subscription_status)
    result = await run_tenant_provisioning_wizard(
        db=db,
        name=name,
        slug=slug,
        admin_email=admin_email,
        plan=selected_plan,
        source=source,
        subscription_status=selected_sub,
        trial_days=trial_days,
    )
    if result['status'] != 'ok':
        tenants = (
            await db.execute(select(Tenant).where(Tenant.status != TenantStatus.terminated).order_by(Tenant.created_at.desc()))
        ).scalars().all()
        for t in tenants:
            sub = await get_or_create_subscription(db, t)
            await sync_tenant_from_subscription(db, t, sub)
        await db.commit()
        return templates.TemplateResponse(
            'admin/tenants.html',
            {
                'request': request,
                'tenants': tenants,
                'error': result.get('error') or 'Provisioning failed',
                'provisioning_result': result,
                'active_page': 'tenants',
                'title': 'title_tenants',
            },
        )
    tenants = (
        await db.execute(select(Tenant).where(Tenant.status != TenantStatus.terminated).order_by(Tenant.created_at.desc()))
    ).scalars().all()
    for t in tenants:
        sub = await get_or_create_subscription(db, t)
        await sync_tenant_from_subscription(db, t, sub)
    await db.commit()
    return templates.TemplateResponse(
        'admin/tenants.html',
        {
            'request': request,
            'tenants': tenants,
            'error': None,
            'provisioning_result': result,
            'active_page': 'tenants',
            'title': 'title_tenants',
        },
    )


@router.get('/admin/tenants/{tenant_id}/edit')
async def admin_tenant_edit_get_redirect(
    request: Request,
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url='/admin/tenants?updated=0&reason=tenant_not_found', status_code=303)
    return templates.TemplateResponse(
        'admin/tenant_edit.html',
        {
            'request': request,
            'tenant': tenant,
            'active_page': 'tenants',
            'title': 'title_tenants',
            'next_url': request.query_params.get('next') or '/admin/tenants',
        },
    )


@router.post('/admin/tenants/{tenant_id}/edit')
async def admin_tenant_edit(
    tenant_id: int,
    name: str = Form(default=''),
    slug: str = Form(default=''),
    plan: str = Form(default=''),
    source: str = Form(default=''),
    tenant_status: str = Form(default=''),
    subscription_status: str = Form(default=''),
    next_url: str = Form(default='/admin/tenants'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    next_url = (next_url or '/admin/tenants').strip()
    name = (name or '').strip()
    raw_slug = (slug or '').strip()
    slug = _normalize_slug(slug)
    plan = _normalize_plan(plan)
    source = _normalize_source(source)
    tenant_status = _normalize_tenant_status(tenant_status)
    subscription_status = _normalize_sub_status(subscription_status)
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'

    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}updated=0&reason=tenant_not_found', status_code=303)

    # If user explicitly attempted to change slug but it normalizes to empty/invalid, fail loudly.
    if raw_slug and not slug:
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}updated=0&reason=bad_slug', status_code=303)

    # Resilient parsing: never fail UI save because of enum/source casing or stale values.
    try:
        selected_plan = PlanName(plan)
    except ValueError:
        selected_plan = tenant.plan

    try:
        selected_tenant_status = TenantStatus(tenant_status)
    except ValueError:
        selected_tenant_status = tenant.status

    if source not in {'pharmacyone', 'external', 'files'}:
        source = tenant.source

    try:
        selected_sub_status = SubscriptionStatus(subscription_status)
    except ValueError:
        selected_sub_status = tenant.subscription_status

    previous = {
        'name': tenant.name,
        'slug': tenant.slug,
        'plan': tenant.plan.value,
        'source': tenant.source,
        'tenant_status': tenant.status.value,
        'subscription_status': tenant.subscription_status.value,
    }

    tenant.name = name or tenant.name
    if slug:
        tenant.slug = slug
    tenant.plan = selected_plan
    tenant.source = source

    sub = await get_or_create_subscription(db, tenant)
    sub.plan = selected_plan
    sub.status = selected_sub_status
    if selected_sub_status == SubscriptionStatus.canceled and sub.canceled_at is None:
        sub.canceled_at = datetime.utcnow()
    if selected_sub_status == SubscriptionStatus.suspended and sub.suspended_at is None:
        sub.suspended_at = datetime.utcnow()
    await sync_tenant_from_subscription(db, tenant, sub)
    # Keep explicit tenant status selected from UI (do not let sync override it).
    tenant.status = selected_tenant_status

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_updated_ui',
            entity_type='tenant',
            entity_id=str(tenant.id),
            payload={
                'before': previous,
                'after': {
                    'name': tenant.name,
                    'slug': tenant.slug,
                    'plan': tenant.plan.value,
                    'source': tenant.source,
                    'tenant_status': tenant.status.value,
                    'subscription_status': sub.status.value,
                },
            },
        )
    )
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}updated=0&reason=slug_exists', status_code=303)
    except Exception:
        logger.exception('tenant_update_failed', extra={'tenant_id': tenant_id})
        await db.rollback()
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}updated=0&reason=commit_failed', status_code=303)
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}updated=1', status_code=303)


@router.post('/admin/tenants/{tenant_id}/delete')
async def admin_tenant_delete(
    tenant_id: int,
    next_url: str = Form(default='/admin/tenants'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url='/admin/tenants?deleted=0', status_code=303)

    sub = await get_or_create_subscription(db, tenant)
    sub.status = SubscriptionStatus.canceled
    if sub.canceled_at is None:
        sub.canceled_at = datetime.utcnow()

    await sync_tenant_from_subscription(db, tenant, sub)
    # Soft delete semantics: keep tenant hidden from UI lists.
    tenant.status = TenantStatus.terminated

    users = (await db.execute(select(User).where(User.tenant_id == tenant.id))).scalars().all()
    for u in users:
        u.is_active = False

    keys = (await db.execute(select(TenantApiKey).where(TenantApiKey.tenant_id == tenant.id))).scalars().all()
    for k in keys:
        k.is_active = False

    conns = (await db.execute(select(TenantConnection).where(TenantConnection.tenant_id == tenant.id))).scalars().all()
    for c in conns:
        c.sync_status = 'terminated'

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_deleted_ui_soft',
            entity_type='tenant',
            entity_id=str(tenant.id),
            payload={'status': 'terminated', 'subscription_status': 'canceled'},
        )
    )
    await db.commit()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}deleted=1', status_code=303)


@router.get('/admin/subscriptions', response_class=HTMLResponse)
async def admin_subscriptions(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).order_by(Tenant.created_at.desc()))).scalars().all()
    for t in tenants:
        sub = await get_or_create_subscription(db, t)
        await sync_tenant_from_subscription(db, t, sub)
    await db.commit()
    return templates.TemplateResponse(
        'admin/subscriptions.html',
        {
            'request': request,
            'tenants': tenants,
            'active_page': 'subscriptions',
            'title': 'title_subscriptions',
        },
    )


@router.get('/admin/subscriptions/{tenant_id}/update')
async def admin_subscription_update_get_redirect(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    # UI form endpoint; prevent raw JSON error when opened directly.
    return RedirectResponse(url='/admin/subscriptions', status_code=303)


@router.post('/admin/subscriptions/{tenant_id}/update')
async def admin_subscription_update(
    tenant_id: int,
    subscription_status: str = Form(default=''),
    note: str = Form(default=''),
    next_url: str = Form(default='/admin/subscriptions'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    subscription_status = (subscription_status or '').strip()
    note = (note or '').strip()
    next_url = (next_url or '/admin/subscriptions').strip()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/subscriptions'

    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=tenant_not_found', status_code=303)

    sub = await get_or_create_subscription(db, tenant)
    try:
        next_status = SubscriptionStatus(subscription_status or sub.status.value)
    except ValueError:
        next_status = sub.status
    prev = sub.status.value
    sub.status = next_status
    if sub.status == SubscriptionStatus.canceled:
        sub.canceled_at = datetime.utcnow()
    if sub.status == SubscriptionStatus.suspended:
        sub.suspended_at = datetime.utcnow()
    await sync_tenant_from_subscription(db, tenant, sub)

    db.add(
        SubscriptionEvent(
            tenant_id=tenant.id,
            from_status=prev,
            to_status=sub.status.value,
            note=note or None,
        )
    )
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='subscription_updated_ui',
            entity_type='subscription',
            entity_id=str(sub.id),
            payload={'from': prev, 'to': sub.status.value, 'note': note or None},
        )
    )
    try:
        await db.commit()
    except Exception:
        logger.exception('subscription_update_failed', extra={'tenant_id': tenant_id})
        await db.rollback()
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=commit_failed', status_code=303)
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}saved=1', status_code=303)


@router.get('/admin/plans', response_class=HTMLResponse)
async def admin_plans(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (await db.execute(select(PlanFeature).order_by(PlanFeature.plan, PlanFeature.feature_name))).scalars().all()
    plans: dict[str, dict[str, bool]] = {}
    for r in rows:
        plans.setdefault(r.plan.value, {})[r.feature_name] = bool(r.enabled)
    return templates.TemplateResponse(
        'admin/plans.html',
        {'request': request, 'plans': plans, 'active_page': 'plans', 'title': 'title_plan_features'},
    )


@router.post('/admin/plans/{plan}/features')
async def admin_plan_features_update(
    plan: str,
    sales: str = Form(default='0'),
    purchases: str = Form(default='0'),
    inventory: str = Form(default='0'),
    cashflows: str = Form(default='0'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    selected_plan = PlanName(plan)
    values = {
        'sales': sales == '1',
        'purchases': purchases == '1',
        'inventory': inventory == '1',
        'cashflows': cashflows == '1',
    }
    for feature, enabled in values.items():
        row = (
            await db.execute(
                select(PlanFeature).where(
                    PlanFeature.plan == selected_plan,
                    PlanFeature.feature_name == feature,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            db.add(PlanFeature(plan=selected_plan, feature_name=feature, enabled=enabled))
        else:
            row.enabled = enabled
    db.add(
        AuditLog(
            tenant_id=None,
            action='plan_features_updated_ui',
            entity_type='plan',
            entity_id=selected_plan.value,
            payload=values,
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/plans', status_code=303)


@router.get('/admin/connections', response_class=HTMLResponse)
async def admin_connections(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    context = await _connections_template_context(db, request=request, result=None, discovery=None)
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/test', response_class=HTMLResponse)
async def admin_connections_test(
    request: Request,
    tenant_id: int = Form(...),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    selected_schema: str = Form(default=''),
    selected_object: str = Form(default=''),
    sales_query_template: str = Form(default=DEFAULT_GENERIC_SALES_QUERY),
    purchases_query_template: str = Form(default=DEFAULT_GENERIC_PURCHASES_QUERY),
    updated_at_column: str = Form(default='UpdatedAt'),
    id_column: str = Form(default='LineId'),
    date_column: str = Form(default='DocDate'),
    branch_column: str = Form(default='BranchCode'),
    item_column: str = Form(default='ItemCode'),
    net_amount_column: str = Form(default='NetValue'),
    cost_column: str = Form(default='CostValue'),
    qty_column: str = Form(default='Qty'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    options_map = _parse_options_map(options)
    secret = SqlServerSecret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        options=options_map,
    )
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == 'pharmacyone_sql',
            )
        )
    ).scalar_one_or_none()

    result: dict[str, str] = {'status': 'ok', 'message': 'Connection test successful (SELECT 1).'}
    try:
        test_connection(build_odbc_connection_string(secret))
        if conn is not None:
            conn.last_test_ok_at = datetime.utcnow()
            conn.last_test_error = None
            await db.commit()
    except Exception as exc:
        result = {'status': 'error', 'message': 'Connection test failed. Verify host/port/db/user/pass and firewall allowlist.'}
        if conn is not None:
            conn.last_test_error = 'test_failed'
            await db.commit()

    context = await _connections_template_context(
        db,
        request=request,
        result=result,
        discovery=None,
        form_values={
            'tenant_id': tenant_id,
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'options': options,
            'selected_schema': selected_schema,
            'selected_object': selected_object,
            'sales_query_template': sales_query_template,
            'purchases_query_template': purchases_query_template,
            'updated_at_column': updated_at_column,
            'id_column': id_column,
            'date_column': date_column,
            'branch_column': branch_column,
            'item_column': item_column,
            'net_amount_column': net_amount_column,
            'cost_column': cost_column,
            'qty_column': qty_column,
        },
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/discovery', response_class=HTMLResponse)
async def admin_connections_discovery(
    request: Request,
    tenant_id: int = Form(...),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    selected_schema: str = Form(default=''),
    selected_object: str = Form(default=''),
    sales_query_template: str = Form(default=DEFAULT_GENERIC_SALES_QUERY),
    purchases_query_template: str = Form(default=DEFAULT_GENERIC_PURCHASES_QUERY),
    updated_at_column: str = Form(default='UpdatedAt'),
    id_column: str = Form(default='LineId'),
    date_column: str = Form(default='DocDate'),
    branch_column: str = Form(default='BranchCode'),
    item_column: str = Form(default='ItemCode'),
    net_amount_column: str = Form(default='NetValue'),
    cost_column: str = Form(default='CostValue'),
    qty_column: str = Form(default='Qty'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    options_map = _parse_options_map(options)
    secret = SqlServerSecret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        options=options_map,
    )
    discovery: dict = {'objects': [], 'selected_schema': selected_schema, 'selected_object': selected_object}
    result: dict[str, str] = {'status': 'ok', 'message': 'Discovery completed.'}
    try:
        connection_string = build_odbc_connection_string(secret)
        objects = discover_candidate_tables(connection_string)
        discovery['objects'] = objects
        if selected_schema and selected_object:
            discovery['columns'] = discover_columns(connection_string, selected_schema, selected_object)
            discovery['sample_rows'] = discover_sample_rows(connection_string, selected_schema, selected_object, top=5)
    except Exception:
        result = {'status': 'error', 'message': 'Discovery failed. Verify credentials/access and try again.'}

    context = await _connections_template_context(
        db,
        request=request,
        result=result,
        discovery=discovery,
        form_values={
            'tenant_id': tenant_id,
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'options': options,
            'selected_schema': selected_schema,
            'selected_object': selected_object,
            'sales_query_template': sales_query_template,
            'purchases_query_template': purchases_query_template,
            'updated_at_column': updated_at_column,
            'id_column': id_column,
            'date_column': date_column,
            'branch_column': branch_column,
            'item_column': item_column,
            'net_amount_column': net_amount_column,
            'cost_column': cost_column,
            'qty_column': qty_column,
        },
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/save')
async def admin_connections_save(
    tenant_id: int = Form(...),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    sales_query_template: str = Form(default=DEFAULT_GENERIC_SALES_QUERY),
    purchases_query_template: str = Form(default=DEFAULT_GENERIC_PURCHASES_QUERY),
    updated_at_column: str = Form(default='UpdatedAt'),
    incremental_column: str = Form(default=''),
    id_column: str = Form(default='LineId'),
    date_column: str = Form(default='DocDate'),
    branch_column: str = Form(default='BranchCode'),
    item_column: str = Form(default='ItemCode'),
    net_amount_column: str = Form(default='NetValue'),
    amount_column: str = Form(default=''),
    cost_column: str = Form(default='CostValue'),
    qty_column: str = Form(default='Qty'),
    selected_schema: str = Form(default=''),
    selected_object: str = Form(default=''),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url='/admin/connections?saved=0', status_code=303)

    options_map = _parse_options_map(options)

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

    conn.enc_payload = encrypt_sqlserver_secret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        options=options_map,
    )
    if selected_schema and selected_object:
        safe_schema = ''.join(ch for ch in selected_schema if ch.isalnum() or ch == '_')
        safe_object = ''.join(ch for ch in selected_object if ch.isalnum() or ch == '_')
        if safe_schema and safe_object:
            auto_query = f'SELECT * FROM [{safe_schema}].[{safe_object}]'
            if not sales_query_template.strip():
                sales_query_template = auto_query
            if not purchases_query_template.strip():
                purchases_query_template = auto_query

    conn.sales_query_template = sales_query_template
    conn.purchases_query_template = purchases_query_template
    conn.incremental_column = (incremental_column or updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = id_column
    conn.date_column = date_column
    conn.branch_column = branch_column
    conn.item_column = item_column
    conn.amount_column = (amount_column or net_amount_column).strip() or 'NetValue'
    conn.cost_column = cost_column
    conn.qty_column = qty_column
    conn.last_test_error = None

    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='connection_saved_ui',
            entity_type='tenant_connection',
            entity_id=str(conn.id or ''),
            payload={
                'connector_type': 'pharmacyone_sql',
                'selected_schema': selected_schema or None,
                'selected_object': selected_object or None,
                'mapping_profile': {
                    'branch_column': conn.branch_column,
                    'item_column': conn.item_column,
                    'qty_column': conn.qty_column,
                    'net_amount_column': conn.amount_column,
                    'cost_column': conn.cost_column,
                    'updated_at_column': conn.incremental_column,
                },
            },
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/connections?saved=1', status_code=303)


@router.post('/admin/connections/apply-pack')
async def admin_connections_apply_pack(
    tenant_id: int = Form(...),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url='/admin/connections?pack=0', status_code=303)
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
    pack = load_querypack('pharmacyone', 'default')
    apply_querypack_to_connection(conn, pack)
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='querypack_applied_ui',
            entity_type='tenant_connection',
            entity_id=str(conn.id or ''),
            payload={'querypack': pack.name, 'version': pack.version},
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/connections?pack=1', status_code=303)


@router.post('/admin/connections/backfill')
async def admin_connections_backfill(
    tenant_id: int = Form(...),
    from_date: str = Form(...),
    to_date: str = Form(...),
    chunk_days: int = Form(default=7),
    include_purchases: bool = Form(default=True),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    from_dt = _parse_date_or_none(from_date)
    to_dt = _parse_date_or_none(to_date)
    if tenant is None or from_dt is None or to_dt is None or from_dt > to_dt:
        return RedirectResponse(url='/admin/connections?backfill=0', status_code=303)

    task = celery_client.send_task(
        'worker.tasks.enqueue_pharmacyone_backfill',
        kwargs={
            'tenant_slug': tenant.slug,
            'from_date_str': from_dt.isoformat(),
            'to_date_str': to_dt.isoformat(),
            'chunk_days': max(1, int(chunk_days)),
            'include_purchases': bool(include_purchases),
        },
        queue='ingest',
    )
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='initial_backfill_queued_ui',
            entity_type='tenant_connection',
            entity_id=str(tenant_id),
            payload={
                'from_date': from_dt.isoformat(),
                'to_date': to_dt.isoformat(),
                'chunk_days': max(1, int(chunk_days)),
                'include_purchases': bool(include_purchases),
                'task_id': task.id,
            },
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/connections?backfill=1', status_code=303)


@router.get('/admin/sync-status', response_class=HTMLResponse)
async def admin_sync_status(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (
        await db.execute(
            select(TenantConnection, Tenant)
            .join(Tenant, Tenant.id == TenantConnection.tenant_id)
            .order_by(TenantConnection.last_sync_at.desc().nullslast())
        )
    ).all()
    return templates.TemplateResponse(
        'admin/sync_status.html',
        {'request': request, 'rows': rows, 'active_page': 'sync', 'title': 'title_sync_status'},
    )


@router.post('/admin/sync-status/{tenant_id}/trigger')
async def admin_sync_trigger(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant:
        celery_client.send_task(
            'worker.tasks.drain_tenant_ingest_queue',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
    return RedirectResponse(url='/admin/sync-status', status_code=303)


@router.get('/admin/insights', response_class=HTMLResponse)
async def admin_insights_overview(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).where(Tenant.status != TenantStatus.terminated).order_by(Tenant.slug.asc()))).scalars().all()
    rows = []
    totals = {'critical': 0, 'warning': 0, 'info': 0, 'open': 0}
    for tenant in tenants:
        counts = await _tenant_insight_counts(tenant)
        rows.append({'tenant': tenant, 'counts': counts})
        totals['critical'] += counts.get('critical', 0)
        totals['warning'] += counts.get('warning', 0)
        totals['info'] += counts.get('info', 0)
        totals['open'] += counts.get('open', 0)
    return templates.TemplateResponse(
        'admin/insights.html',
        {
            'request': request,
            'tenant_rows': rows,
            'totals': totals,
            'active_page': 'insights',
            'title': 'title_insights_overview',
        },
    )


@router.post('/admin/insights/{tenant_id}/run')
async def admin_insights_run_now(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant:
        celery_client.send_task(
            'worker.tasks.generate_insights_for_tenant',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
    return RedirectResponse(url='/admin/insights', status_code=303)


@router.get('/admin/insight-rules', response_class=HTMLResponse)
async def admin_insight_rules(
    request: Request,
    tenant_id: int | None = None,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    lang = _current_lang(request)
    tenants = (await db.execute(select(Tenant).where(Tenant.status != TenantStatus.terminated).order_by(Tenant.slug.asc()))).scalars().all()
    selected = None
    if tenants:
        selected = next((t for t in tenants if tenant_id and t.id == tenant_id), None) or tenants[0]
    rules = []
    if selected:
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(selected.id),
            db_name=selected.db_name,
            db_user=selected.db_user,
            db_password=selected.db_password,
        ):
            rules = await list_tenant_rules(tenant_db)
            break
    localized_rules = [_localize_insight_rule(r, lang) for r in rules]
    return templates.TemplateResponse(
        'admin/insight_rules.html',
        {
            'request': request,
            'tenants': tenants,
            'selected_tenant': selected,
            'rules': localized_rules,
            'active_page': 'insight_rules',
            'title': 'title_insight_rules',
        },
    )


@router.post('/admin/insight-rules/{tenant_id}/update')
async def admin_insight_rules_update(
    tenant_id: int,
    code: str = Form(...),
    enabled: str = Form(...),
    severity_default: str = Form(...),
    params_json: str = Form(default='{}'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant:
        try:
            parsed = json.loads(params_json or '{}')
            if not isinstance(parsed, dict):
                parsed = {}
        except Exception:
            parsed = {}
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            await update_tenant_rule(
                tenant_db,
                code=code,
                enabled=(enabled == '1'),
                severity_default=severity_default,
                params_json=parsed,
            )
            break
    return RedirectResponse(url=f'/admin/insight-rules?tenant_id={tenant_id}', status_code=303)


@router.get('/admin/insight-rules/{tenant_id}/{code}/edit', response_class=HTMLResponse)
async def admin_insight_rule_edit(
    request: Request,
    tenant_id: int,
    code: str,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    lang = _current_lang(request)
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url='/admin/insight-rules?saved=0', status_code=303)

    rules = []
    if tenant:
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            rules = await list_tenant_rules(tenant_db)
            break
    target = next((r for r in rules if getattr(r, 'code', '') == code), None)
    if not target:
        return RedirectResponse(url=f'/admin/insight-rules?tenant_id={tenant_id}&saved=0', status_code=303)
    localized = _localize_insight_rule(target, lang)
    return templates.TemplateResponse(
        'admin/insight_rule_edit.html',
        {
            'request': request,
            'tenant': tenant,
            'rule': localized,
            'active_page': 'insight_rules',
            'title': 'title_insight_rules',
        },
    )


@router.post('/admin/insight-rules/{tenant_id}/{code}/edit')
async def admin_insight_rule_edit_save(
    tenant_id: int,
    code: str,
    enabled: str = Form(...),
    severity_default: str = Form(...),
    params_json: str = Form(default='{}'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url='/admin/insight-rules?saved=0', status_code=303)

    try:
        parsed = json.loads(params_json or '{}')
        if not isinstance(parsed, dict):
            parsed = {}
    except Exception:
        parsed = {}

    updated = False
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        updated = await update_tenant_rule(
            tenant_db,
            code=code,
            enabled=(enabled == '1'),
            severity_default=severity_default,
            params_json=parsed,
        )
        break
    return RedirectResponse(
        url=f'/admin/insight-rules/{tenant_id}/{code}/edit?saved={"1" if updated else "0"}',
        status_code=303,
    )


@router.post('/admin/insight-rules/{tenant_id}/run')
async def admin_insight_rules_run(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant:
        celery_client.send_task(
            'worker.tasks.generate_insights_for_tenant',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
    return RedirectResponse(url=f'/admin/insight-rules?tenant_id={tenant_id}', status_code=303)


@router.get('/admin/users', response_class=HTMLResponse)
async def admin_users(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    users = (await db.execute(select(User).order_by(User.created_at.desc()))).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    return templates.TemplateResponse(
        'admin/users.html',
        {
            'request': request,
            'users': users,
            'tenants': tenants,
            'active_page': 'users',
            'title': 'title_user_management',
        },
    )


@router.post('/admin/users/create')
async def admin_user_create(
    full_name: str = Form(default=''),
    phone: str = Form(default=''),
    email: str = Form(...),
    role: str = Form(...),
    tenant_id: str | None = Form(default=None),
    access_starts_at: str | None = Form(default=None),
    access_expires_at: str | None = Form(default=None),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant_id_int: int | None = None
    if role != RoleName.cloudon_admin.value and tenant_id:
        tenant_id_int = int(tenant_id)
    access_start_dt = None
    access_expiry_dt = None
    raw_start = (access_starts_at or '').strip()
    raw_expiry = (access_expires_at or '').strip()
    if raw_start:
        try:
            access_start_dt = datetime.fromisoformat(raw_start)
        except ValueError:
            return RedirectResponse(url='/admin/users?updated=0&reason=bad_start', status_code=303)
    if raw_expiry:
        try:
            access_expiry_dt = datetime.fromisoformat(raw_expiry)
        except ValueError:
            return RedirectResponse(url='/admin/users?updated=0&reason=bad_expiry', status_code=303)
    if access_start_dt and access_expiry_dt and access_start_dt > access_expiry_dt:
        return RedirectResponse(url='/admin/users?updated=0&reason=bad_window', status_code=303)

    user = User(
        tenant_id=tenant_id_int,
        full_name=full_name.strip() or None,
        phone=phone.strip() or None,
        email=email,
        role=RoleName(role),
        password_hash=get_password_hash(secrets.token_urlsafe(18)),
        is_active=True,
        access_starts_at=access_start_dt,
        access_expires_at=access_expiry_dt,
    )
    db.add(user)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return RedirectResponse(url='/admin/users?updated=0&reason=email_exists', status_code=303)
    return RedirectResponse(url='/admin/users?updated=1', status_code=303)


@router.get('/admin/users/{user_id}/edit', response_class=HTMLResponse)
async def admin_user_edit_page(
    request: Request,
    user_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return RedirectResponse(url='/admin/users?updated=0&reason=user_not_found', status_code=303)
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    return templates.TemplateResponse(
        'admin/user_edit.html',
        {
            'request': request,
            'user': user,
            'tenants': tenants,
            'active_page': 'users',
            'title': 'title_user_management',
            'next_url': request.query_params.get('next') or '/admin/users',
        },
    )


@router.post('/admin/users/{user_id}/edit')
async def admin_user_edit(
    request: Request,
    user_id: int,
    full_name: str = Form(default=''),
    phone: str = Form(default=''),
    email: str = Form(...),
    role: str = Form(...),
    tenant_id: str | None = Form(default=None),
    access_starts_at: str | None = Form(default=None),
    access_expires_at: str | None = Form(default=None),
    next_url: str = Form(default='/admin/users'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/users'
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return RedirectResponse(url=f'{redirect_target}?updated=0&reason=user_not_found', status_code=303)

    try:
        selected_role = RoleName(role)
    except ValueError:
        return RedirectResponse(url=f'{redirect_target}?updated=0&reason=bad_role', status_code=303)

    tenant_id_int: int | None = None
    if selected_role != RoleName.cloudon_admin and tenant_id and str(tenant_id).strip():
        tenant_id_int = int(str(tenant_id).strip())

    start_dt = None
    expiry = None
    raw_start = (access_starts_at or '').strip()
    raw_expiry = (access_expires_at or '').strip()
    if raw_start:
        try:
            start_dt = datetime.fromisoformat(raw_start)
        except ValueError:
            return RedirectResponse(url=f'{redirect_target}?updated=0&reason=bad_start', status_code=303)
    if raw_expiry:
        try:
            expiry = datetime.fromisoformat(raw_expiry)
        except ValueError:
            return RedirectResponse(url=f'{redirect_target}?updated=0&reason=bad_expiry', status_code=303)
    if start_dt and expiry and start_dt > expiry:
        return RedirectResponse(url=f'{redirect_target}?updated=0&reason=bad_window', status_code=303)

    user.full_name = full_name.strip() or None
    user.phone = phone.strip() or None
    user.email = email.strip()
    user.role = selected_role
    user.tenant_id = tenant_id_int
    user.access_starts_at = start_dt
    user.access_expires_at = expiry
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return RedirectResponse(url=f'{redirect_target}?updated=0&reason=email_exists', status_code=303)
    return RedirectResponse(url=f'{redirect_target}?updated=1', status_code=303)


@router.post('/admin/users/{user_id}/toggle')
async def admin_user_toggle(
    user_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if user:
        user.is_active = not user.is_active
        await db.commit()
    return RedirectResponse(url='/admin/users', status_code=303)


@router.post('/admin/users/{user_id}/delete')
async def admin_user_delete(
    user_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return RedirectResponse(url='/admin/users?deleted=0', status_code=303)
    await db.delete(user)
    await db.commit()
    return RedirectResponse(url='/admin/users?deleted=1', status_code=303)


@router.get('/tenant/profile', response_class=HTMLResponse)
async def tenant_profile(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/profile.html',
        {
            'request': request,
            'tenant': tenant,
            'user': user,
            **_tenant_feature_flags(tenant),
            'active_page': 'dashboard',
            'title': 'Profile',
        },
    )


@router.post('/tenant/profile')
async def tenant_profile_update(
    full_name: str = Form(''),
    phone: str = Form(''),
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    db_user = (await db.execute(select(User).where(User.id == user.id))).scalar_one_or_none()
    if db_user:
        db_user.full_name = (full_name or '').strip() or None
        db_user.phone = (phone or '').strip() or None
        await db.commit()
    return RedirectResponse(url='/tenant/profile?saved=1', status_code=303)


@router.get('/tenant/settings', response_class=HTMLResponse)
async def tenant_settings(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/settings.html',
        {
            'request': request,
            'tenant': tenant,
            'user': user,
            **_tenant_feature_flags(tenant),
            'active_page': 'dashboard',
            'title': 'Settings',
        },
    )


@router.get('/tenant/messages', response_class=HTMLResponse)
async def tenant_messages(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (
        await db.execute(
            select(AuditLog)
            .where((AuditLog.tenant_id == tenant.id) | (AuditLog.actor_user_id == user.id))
            .order_by(AuditLog.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    return templates.TemplateResponse(
        'tenant/messages.html',
        {
            'request': request,
            'tenant': tenant,
            'user': user,
            'messages': rows,
            **_tenant_feature_flags(tenant),
            'active_page': 'dashboard',
            'title': 'Messages',
        },
    )


@router.get('/admin/profile', response_class=HTMLResponse)
async def admin_profile(
    request: Request,
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/profile.html',
        {
            'request': request,
            'user': user,
            'active_page': 'dashboard',
            'title': 'Profile',
        },
    )


@router.post('/admin/profile')
async def admin_profile_update(
    full_name: str = Form(''),
    phone: str = Form(''),
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    db_user = (await db.execute(select(User).where(User.id == user.id))).scalar_one_or_none()
    if db_user:
        db_user.full_name = (full_name or '').strip() or None
        db_user.phone = (phone or '').strip() or None
        await db.commit()
    return RedirectResponse(url='/admin/profile?saved=1', status_code=303)


@router.get('/admin/settings', response_class=HTMLResponse)
async def admin_settings(
    request: Request,
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/settings.html',
        {
            'request': request,
            'user': user,
            'active_page': 'dashboard',
            'title': 'Settings',
        },
    )


@router.get('/admin/messages', response_class=HTMLResponse)
async def admin_messages(
    request: Request,
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (
        await db.execute(
            select(AuditLog)
            .where((AuditLog.actor_user_id == user.id) | (AuditLog.tenant_id.is_(None)))
            .order_by(AuditLog.created_at.desc())
            .limit(30)
        )
    ).scalars().all()
    return templates.TemplateResponse(
        'admin/messages.html',
        {
            'request': request,
            'user': user,
            'messages': rows,
            'active_page': 'dashboard',
            'title': 'Messages',
        },
    )


@router.get('/tenant/dashboard', response_class=HTMLResponse)
async def tenant_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'dashboard',
            'title': 'title_tenant_dashboard',
        },
    )


@router.get('/tenant/sales', response_class=HTMLResponse)
async def tenant_sales_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/sales_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'sales',
            'title': 'title_sales_dashboard',
        },
    )


@router.get('/tenant/purchases', response_class=HTMLResponse)
async def tenant_purchases_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/purchases_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'purchases',
            'title': 'title_purchases_dashboard',
        },
    )


@router.get('/tenant/inventory', response_class=HTMLResponse)
async def tenant_inventory_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    feature_flags = _tenant_feature_flags(tenant)
    return templates.TemplateResponse(
        'tenant/inventory_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': not feature_flags['inventory_enabled'],
            'active_page': 'inventory',
            'title': 'title_inventory_dashboard',
        },
    )


@router.get('/tenant/items', response_class=HTMLResponse)
async def tenant_items_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    return templates.TemplateResponse(
        'tenant/items_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_as_of': to_date,
            'active_page': 'items',
            'title': 'title_items_dashboard',
        },
    )


@router.get('/tenant/supplier-targets', response_class=HTMLResponse)
async def tenant_supplier_targets_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/supplier_targets.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_year': date.today().year,
            'active_page': 'supplier_targets',
            'title': 'title_supplier_targets',
        },
    )


@router.get('/tenant/price-control', response_class=HTMLResponse)
async def tenant_price_control_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = date(to_date.year, 1, 1)
    return templates.TemplateResponse(
        'tenant/price_control.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_from': from_date,
            'default_to': to_date,
            'default_target_margin': 35.0,
            'active_page': 'price_control',
            'title': 'title_price_control',
        },
    )


@router.get('/tenant/cashflow', response_class=HTMLResponse)
async def tenant_cashflow_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    feature_flags = _tenant_feature_flags(tenant)
    return templates.TemplateResponse(
        'tenant/cashflow_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': not feature_flags['cashflow_enabled'],
            'active_page': 'cashflow',
            'title': 'title_cashflow_dashboard',
        },
    )


@router.get('/tenant/insights', response_class=HTMLResponse)
async def tenant_insights_page(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    initial_insights = await list_tenant_insights(
        tenant_db,
        category=None,
        severity=None,
        status=None,
        date_from=from_date,
        date_to=to_date,
        limit=200,
    )
    return templates.TemplateResponse(
        'tenant/insights.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'default_from': from_date,
            'default_to': to_date,
            'initial_insights': initial_insights,
            'active_page': 'insights',
            'title': 'title_insights',
        },
    )


@router.post('/tenant/insights/run-now')
async def tenant_insights_run_now(
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    celery_client.send_task(
        'worker.tasks.generate_insights_for_tenant',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )
    return RedirectResponse(url='/tenant/insights', status_code=303)


@router.post('/tenant/insights/{insight_id}/acknowledge')
async def tenant_insights_acknowledge(
    insight_id: str,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    try:
        insight_uuid = UUID(insight_id)
    except Exception:
        return RedirectResponse(url='/tenant/insights', status_code=303)
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        row = (await tenant_db.execute(select(Insight).where(Insight.id == insight_uuid))).scalar_one_or_none()
        if row:
            row.status = 'acknowledged'
            row.acknowledged_at = datetime.utcnow()
            await tenant_db.commit()
        break
    return RedirectResponse(url='/tenant/insights', status_code=303)


@router.get('/tenant/compare', response_class=HTMLResponse)
async def tenant_compare(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    today = date.today()
    a_from = today - timedelta(days=30)
    a_to = today
    b_from = today - timedelta(days=60)
    b_to = today - timedelta(days=31)
    return templates.TemplateResponse(
        'tenant/compare.html',
        {
            'request': request,
            'tenant': tenant,
            **_tenant_feature_flags(tenant),
            'a_from': a_from,
            'a_to': a_to,
            'b_from': b_from,
            'b_to': b_to,
            'active_page': 'compare',
            'title': 'title_comparison',
        },
    )
