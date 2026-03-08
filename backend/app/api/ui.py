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
from fastapi import APIRouter, Depends, Form, Query, Request
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
    GlobalRuleEntry,
    GlobalRuleSet,
    OperationalStream,
    OverrideMode,
    PlanFeature,
    PlanName,
    TenantRuleOverride,
    RoleName,
    RuleDomain,
    Subscription,
    SubscriptionEvent,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantConnection,
    TenantStatus,
    User,
    ProfessionalProfile,
)
from app.models.tenant import DimBranch, Insight
from app.services.intelligence_service import (
    insights_counts_by_severity,
    list_insights as list_tenant_insights,
    list_rules as list_tenant_rules,
    update_rule as update_tenant_rule,
)
from app.services.connection_secrets import SqlServerSecret, build_odbc_connection_string, encrypt_sqlserver_secret
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_CASHFLOW_QUERY,
    DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
    DEFAULT_GENERIC_INVENTORY_QUERY,
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
    discover_candidate_tables,
    discover_columns,
    discover_sample_rows,
    test_connection,
)
from app.services.provisioning_wizard import run_tenant_provisioning_wizard
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, STREAM_TO_ENTITY, normalize_stream_name, normalize_stream_values
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


def _default_profile_code_for_role(role: RoleName) -> str:
    if role == RoleName.cloudon_admin:
        return 'OWNER'
    if role == RoleName.tenant_user:
        return 'FINANCE'
    return 'MANAGER'


def _dashboard_redirect_for_profile_code(profile_code: str | None, role: RoleName) -> str:
    code = (profile_code or '').strip().upper() or _default_profile_code_for_role(role)
    if code == 'FINANCE':
        return '/tenant/finance-dashboard'
    return '/tenant/dashboard'


def _login_redirect_for(user: User, host: str | None = None, profile_code: str | None = None) -> str:
    if user.role == RoleName.cloudon_admin:
        return '/admin/dashboard'
    if host and host.lower() == 'adminpanel.boxvisio.com':
        return '/admin/dashboard'
    return _dashboard_redirect_for_profile_code(profile_code, user.role)


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
        'sql': 'sql',
        'pharmacyone': 'sql',
        'pharmacyone_sql': 'sql',
        'api': 'external',
        'external': 'external',
        'external_api': 'external',
        'files': 'files',
        'file': 'files',
    }
    return mapping.get(val, val)


_CASHFLOW_CATEGORY_ALIAS_MAP: dict[str, str] = {
    'customer_collections': 'customer_collections',
    'customer_collection': 'customer_collections',
    'cash_tx_customer_collections': 'customer_collections',
    'customer_collections_docs': 'customer_collections',
    'customer_collections_documents': 'customer_collections',
    'customer_transfers': 'customer_transfers',
    'customer_transfer': 'customer_transfers',
    'cash_tx_customer_transfers': 'customer_transfers',
    'supplier_payments': 'supplier_payments',
    'supplier_payment': 'supplier_payments',
    'cash_tx_supplier_payments': 'supplier_payments',
    'supplier_transfers': 'supplier_transfers',
    'supplier_transfer': 'supplier_transfers',
    'cash_tx_supplier_transfers': 'supplier_transfers',
    'financial_accounts': 'financial_accounts',
    'financial_account': 'financial_accounts',
    'cash_tx_financial_accounts': 'financial_accounts',
}

_CASHFLOW_CATEGORY_LABEL_KEY_MAP: dict[str, str] = {
    'customer_collections': 'cash_tx_customer_collections',
    'customer_transfers': 'cash_tx_customer_transfers',
    'supplier_payments': 'cash_tx_supplier_payments',
    'supplier_transfers': 'cash_tx_supplier_transfers',
    'financial_accounts': 'cash_tx_financial_accounts',
}

_CASHFLOW_CATEGORY_TITLE_KEY_MAP: dict[str, str] = {
    'customer_collections': 'title_cash_tx_customer_collections',
    'customer_transfers': 'title_cash_tx_customer_transfers',
    'supplier_payments': 'title_cash_tx_supplier_payments',
    'supplier_transfers': 'title_cash_tx_supplier_transfers',
    'financial_accounts': 'title_cash_tx_financial_accounts',
}


def _normalize_cashflow_category(raw: str | None) -> str:
    value = str(raw or '').strip().lower()
    if not value:
        return ''
    value = value.replace('-', '_').replace(' ', '_')
    return _CASHFLOW_CATEGORY_ALIAS_MAP.get(value, value)


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
    return {
        'inventory_enabled': is_enterprise,
        'cashflow_enabled': is_enterprise,
    }


async def _tenant_navigation_context(tenant: Tenant) -> dict[str, bool | int]:
    branch_count = 0
    try:
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            branch_count = int((await tenant_db.execute(select(func.count(DimBranch.id)))).scalar_one() or 0)
            break
    except Exception:
        logger.exception('tenant_navigation_context_failed', extra={'tenant_id': tenant.id})
        branch_count = 0
    return {
        **_tenant_feature_flags(tenant),
        'tenant_branch_count': branch_count,
        'tenant_has_multiple_branches': branch_count > 1,
    }


_PROFESSIONAL_PROFILE_ORDER = {
    'OWNER': 1,
    'MANAGER': 2,
    'FINANCE': 3,
    'INVENTORY': 4,
    'SALES': 5,
}

_PROFILE_INSIGHT_PRIORITY: dict[str, list[str]] = {
    'FINANCE': ['receivables', 'cashflow', 'purchases'],
    'INVENTORY': ['inventory'],
    'SALES': ['sales'],
}


def _profile_sort_key(profile: ProfessionalProfile) -> tuple[int, str]:
    code = (profile.profile_code or '').upper()
    return (_PROFESSIONAL_PROFILE_ORDER.get(code, 999), (profile.profile_name or '').lower())


async def _list_professional_profiles(db: AsyncSession) -> list[ProfessionalProfile]:
    profiles = (await db.execute(select(ProfessionalProfile))).scalars().all()
    profiles.sort(key=_profile_sort_key)
    return profiles


async def _resolve_professional_profile_id(
    db: AsyncSession,
    *,
    selected_role: RoleName,
    requested_profile_code: str | None,
) -> int:
    profiles = await _list_professional_profiles(db)
    by_code = {(p.profile_code or '').strip().upper(): p for p in profiles}
    fallback_code = _default_profile_code_for_role(selected_role)
    normalized_requested = (requested_profile_code or '').strip().upper()
    target_code = normalized_requested or fallback_code
    if target_code not in by_code:
        target_code = fallback_code
    profile = by_code.get(target_code)
    if not profile:
        raise ValueError('professional_profile_not_found')
    return int(profile.id)


def _prioritize_insights_for_profile(items: list[dict], profile_code: str | None) -> list[dict]:
    code = (profile_code or '').strip().upper()
    prioritized_categories = _PROFILE_INSIGHT_PRIORITY.get(code)
    if not prioritized_categories:
        return items

    order_map = {category: idx for idx, category in enumerate(prioritized_categories)}
    return sorted(items, key=lambda row: order_map.get(str(row.get('category') or '').lower(), 999))


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


_STREAM_LABEL_KEYS: list[tuple[str, str]] = [
    ('sales_documents', 'sales_documents_menu'),
    ('purchase_documents', 'purchases_documents_menu'),
    ('inventory_documents', 'warehouse_documents_menu'),
    ('cash_transactions', 'cash_transactions_menu'),
    ('supplier_balances', 'supplier_open_balances_menu'),
    ('customer_balances', 'customer_open_balances_menu'),
]

_BUSINESS_RULE_STREAM_LABEL_BY_VALUE: dict[str, str] = {
    value: label_key for value, label_key in _STREAM_LABEL_KEYS
}

_RULE_DOMAIN_LABEL_BY_VALUE: dict[str, str] = {
    RuleDomain.document_type_rules.value: 'Κανόνες Τύπων Παραστατικών',
    RuleDomain.source_mapping.value: 'Κανόνες Κυκλωμάτων / Query Mapping',
    RuleDomain.kpi_participation_rules.value: 'Κανόνες Συμμετοχής KPI',
    RuleDomain.intelligence_threshold_rules.value: 'Κανόνες Insights',
}

_DOCUMENT_RULE_STREAMS: list[dict[str, str]] = [
    {'value': OperationalStream.sales_documents.value, 'label': 'Πωλήσεις'},
    {'value': OperationalStream.purchase_documents.value, 'label': 'Αγορές'},
    {'value': OperationalStream.inventory_documents.value, 'label': 'Αποθήκη'},
    {'value': OperationalStream.cash_transactions.value, 'label': 'Ταμείο'},
]

_DOCUMENT_SIGN_OPTIONS: list[dict[str, str]] = [
    {'value': 'positive', 'label': 'Θετικό'},
    {'value': 'negative', 'label': 'Αρνητικό'},
    {'value': 'none', 'label': 'Κανένα'},
]

_DOCUMENT_SIGN_LABEL: dict[str, str] = {item['value']: item['label'] for item in _DOCUMENT_SIGN_OPTIONS}

_SOFTONE_DOCUMENT_RULE_TEMPLATES: list[dict[str, object]] = [
    {
        'document_type': 'Τιμολόγιο Πώλησης',
        'stream': OperationalStream.sales_documents.value,
        'include_revenue': True,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': True,
        'affects_supplier_balance': False,
        'amount_sign': 'positive',
        'quantity_sign': 'positive',
    },
    {
        'document_type': 'Απόδειξη Λιανικής',
        'stream': OperationalStream.sales_documents.value,
        'include_revenue': True,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': False,
        'affects_supplier_balance': False,
        'amount_sign': 'positive',
        'quantity_sign': 'positive',
    },
    {
        'document_type': 'Πιστωτικό Πώλησης',
        'stream': OperationalStream.sales_documents.value,
        'include_revenue': True,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': True,
        'affects_supplier_balance': False,
        'amount_sign': 'negative',
        'quantity_sign': 'negative',
    },
    {
        'document_type': 'Τιμολόγιο Αγοράς',
        'stream': OperationalStream.purchase_documents.value,
        'include_revenue': False,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': False,
        'affects_supplier_balance': True,
        'amount_sign': 'positive',
        'quantity_sign': 'positive',
    },
    {
        'document_type': 'Πιστωτικό Αγοράς',
        'stream': OperationalStream.purchase_documents.value,
        'include_revenue': False,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': False,
        'affects_supplier_balance': True,
        'amount_sign': 'negative',
        'quantity_sign': 'negative',
    },
    {
        'document_type': 'Δελτίο Εισαγωγής Αποθήκης',
        'stream': OperationalStream.inventory_documents.value,
        'include_revenue': False,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': False,
        'affects_supplier_balance': False,
        'amount_sign': 'positive',
        'quantity_sign': 'positive',
    },
    {
        'document_type': 'Δελτίο Εξαγωγής Αποθήκης',
        'stream': OperationalStream.inventory_documents.value,
        'include_revenue': False,
        'include_quantity': True,
        'include_cost': True,
        'affects_customer_balance': False,
        'affects_supplier_balance': False,
        'amount_sign': 'negative',
        'quantity_sign': 'negative',
    },
    {
        'document_type': 'Είσπραξη Πελάτη',
        'stream': OperationalStream.cash_transactions.value,
        'include_revenue': False,
        'include_quantity': False,
        'include_cost': False,
        'affects_customer_balance': True,
        'affects_supplier_balance': False,
        'amount_sign': 'negative',
        'quantity_sign': 'none',
    },
    {
        'document_type': 'Πληρωμή Προμηθευτή',
        'stream': OperationalStream.cash_transactions.value,
        'include_revenue': False,
        'include_quantity': False,
        'include_cost': False,
        'affects_customer_balance': False,
        'affects_supplier_balance': True,
        'amount_sign': 'negative',
        'quantity_sign': 'none',
    },
]


def _to_bool_flag(raw: object) -> bool:
    txt = str(raw or '').strip().lower()
    return txt in {'1', 'true', 'yes', 'on', 'ναι'}


def _normalize_sign(raw: object, *, default: str = 'none') -> str:
    txt = str(raw or '').strip().lower()
    if txt in {'positive', 'pos', 'plus', '1', '+1'}:
        return 'positive'
    if txt in {'negative', 'neg', 'minus', '-1'}:
        return 'negative'
    if txt in {'none', '0', '', 'neutral'}:
        return 'none'
    return default


def _sign_to_int(sign: str) -> int:
    normalized = _normalize_sign(sign)
    if normalized == 'positive':
        return 1
    if normalized == 'negative':
        return -1
    return 0


def _payload_bool(payload: dict, keys: list[str], *, default: bool = False) -> bool:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value) != 0
        return _to_bool_flag(value)
    return default


def _payload_sign(payload: dict, keys: list[str], *, default: str = 'none') -> str:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if isinstance(value, (int, float)):
            if float(value) > 0:
                return 'positive'
            if float(value) < 0:
                return 'negative'
            return 'none'
        return _normalize_sign(value, default=default)
    return default


def _document_rule_key(document_type: str, stream: str) -> str:
    stream_code = re.sub(r'[^A-Za-z0-9]+', '_', str(stream or '').upper()).strip('_') or 'STREAM'
    doc_code = re.sub(r'\W+', '_', str(document_type or '').strip().upper(), flags=re.UNICODE).strip('_') or 'DOC'
    return f'DOC_RULE_{stream_code}_{doc_code}'[:128]


def _build_document_rule_payload(
    *,
    document_type: str,
    include_revenue: bool,
    include_quantity: bool,
    include_cost: bool,
    affects_customer_balance: bool,
    affects_supplier_balance: bool,
    amount_sign: str,
    quantity_sign: str,
) -> dict[str, object]:
    amount_sign_norm = _normalize_sign(amount_sign)
    quantity_sign_norm = _normalize_sign(quantity_sign)
    return {
        'document_type': document_type,
        'include_revenue': include_revenue,
        'include_quantity': include_quantity,
        'include_cost': include_cost,
        'affects_customer_balance': affects_customer_balance,
        'affects_supplier_balance': affects_supplier_balance,
        'amount_sign': _sign_to_int(amount_sign_norm),
        'quantity_sign': _sign_to_int(quantity_sign_norm),
        'amount_sign_label': amount_sign_norm,
        'quantity_sign_label': quantity_sign_norm,
        'editor_version': 'document_rule_form_v1',
    }


def _read_document_rule_form(payload: dict[str, object], rule_key: str) -> dict[str, object]:
    document_type = str(payload.get('document_type') or '').strip() or str(rule_key or '').strip()
    amount_sign = _payload_sign(payload, ['amount_sign_label', 'amount_sign', 'sign'], default='none')
    quantity_sign = _payload_sign(payload, ['quantity_sign_label', 'quantity_sign', 'qty_sign'], default='none')
    return {
        'document_type': document_type,
        'include_revenue': _payload_bool(payload, ['include_revenue'], default=False),
        'include_quantity': _payload_bool(payload, ['include_quantity'], default=False),
        'include_cost': _payload_bool(payload, ['include_cost'], default=False),
        'affects_customer_balance': _payload_bool(payload, ['affects_customer_balance'], default=False),
        'affects_supplier_balance': _payload_bool(payload, ['affects_supplier_balance'], default=False),
        'amount_sign': amount_sign,
        'quantity_sign': quantity_sign,
    }


def _deep_merge_dict(base: dict[str, object], override: dict[str, object]) -> dict[str, object]:
    merged: dict[str, object] = dict(base)
    for key, value in override.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dict(current, value)
        else:
            merged[key] = value
    return merged


def _document_rule_row(
    *,
    scope: str,
    scope_label: str,
    ruleset_code: str,
    stream: str,
    stream_label: str,
    rule_key: str,
    is_active: bool,
    payload: dict[str, object],
    updated_at: object,
    tenant_id: int | None = None,
    tenant_name: str | None = None,
    override_mode: str | None = None,
) -> dict[str, object]:
    parsed = _read_document_rule_form(payload, rule_key)
    return {
        'scope': scope,
        'scope_label': scope_label,
        'ruleset_code': ruleset_code,
        'stream': stream,
        'stream_label': stream_label,
        'rule_key': rule_key,
        'document_type': parsed['document_type'],
        'include_revenue': bool(parsed['include_revenue']),
        'include_quantity': bool(parsed['include_quantity']),
        'include_cost': bool(parsed['include_cost']),
        'affects_customer_balance': bool(parsed['affects_customer_balance']),
        'affects_supplier_balance': bool(parsed['affects_supplier_balance']),
        'amount_sign': str(parsed['amount_sign']),
        'quantity_sign': str(parsed['quantity_sign']),
        'amount_sign_label': _DOCUMENT_SIGN_LABEL.get(str(parsed['amount_sign']), 'Κανένα'),
        'quantity_sign_label': _DOCUMENT_SIGN_LABEL.get(str(parsed['quantity_sign']), 'Κανένα'),
        'is_active': is_active,
        'tenant_id': tenant_id,
        'tenant_name': tenant_name,
        'override_mode': override_mode,
        'updated_at': updated_at,
        'payload_json': json.dumps(payload or {}, ensure_ascii=False, indent=2),
    }


async def _upsert_document_rule_global(
    *,
    db: AsyncSession,
    ruleset_code: str,
    stream: OperationalStream,
    rule_key: str,
    payload_json: dict[str, object],
    is_active: bool,
    replace_existing: bool = True,
) -> bool:
    ruleset = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == ruleset_code))).scalar_one_or_none()
    if ruleset is None:
        ruleset = GlobalRuleSet(
            code=ruleset_code,
            name=ruleset_code,
            description='Created from document rules form UI',
            is_active=True,
            priority=100,
        )
        db.add(ruleset)
        await db.flush()

    entry = (
        await db.execute(
            select(GlobalRuleEntry).where(
                GlobalRuleEntry.ruleset_id == ruleset.id,
                GlobalRuleEntry.domain == RuleDomain.document_type_rules,
                GlobalRuleEntry.stream == stream,
                GlobalRuleEntry.rule_key == rule_key,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        db.add(
            GlobalRuleEntry(
                ruleset_id=ruleset.id,
                domain=RuleDomain.document_type_rules,
                stream=stream,
                rule_key=rule_key,
                payload_json=payload_json,
                is_active=is_active,
            )
        )
        return True

    if not replace_existing:
        return False
    entry.payload_json = payload_json
    entry.is_active = is_active
    return True


async def _upsert_document_rule_tenant_override(
    *,
    db: AsyncSession,
    tenant_id: int,
    stream: OperationalStream,
    rule_key: str,
    payload_json: dict[str, object],
    is_active: bool,
    override_mode: OverrideMode = OverrideMode.replace,
    replace_existing: bool = True,
) -> bool:
    entry = (
        await db.execute(
            select(TenantRuleOverride).where(
                TenantRuleOverride.tenant_id == tenant_id,
                TenantRuleOverride.domain == RuleDomain.document_type_rules,
                TenantRuleOverride.stream == stream,
                TenantRuleOverride.rule_key == rule_key,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        db.add(
            TenantRuleOverride(
                tenant_id=tenant_id,
                domain=RuleDomain.document_type_rules,
                stream=stream,
                rule_key=rule_key,
                override_mode=override_mode,
                payload_json=payload_json,
                is_active=is_active,
            )
        )
        return True

    if not replace_existing:
        return False
    entry.override_mode = override_mode
    entry.payload_json = payload_json
    entry.is_active = is_active
    return True


def _stream_defaults_for_connector(connector_type: str) -> list[str]:
    if str(connector_type or '').strip().lower() == 'external_api':
        return ['sales_documents', 'purchase_documents']
    return list(ALL_OPERATIONAL_STREAMS)


def _normalize_source_type(connector_type: str, source_type: str | None = None) -> str:
    raw = (source_type or '').strip().lower()
    if raw in {'sql', 'api', 'file'}:
        return raw
    lowered = str(connector_type or '').strip().lower()
    if 'api' in lowered:
        return 'api'
    if 'file' in lowered or 'csv' in lowered or 'excel' in lowered or 'sftp' in lowered:
        return 'file'
    return 'sql'


def _normalize_stream_selection(values: list[str] | None, *, fallback: list[str]) -> list[str]:
    normalized = normalize_stream_values(values or [])
    return [stream for stream in normalized] if normalized else [stream for stream in fallback]


def _coerce_stream_query_mapping_from_values(form_values: dict) -> dict[str, str]:
    fallback = {
        'sales_documents': str(form_values.get('sales_query_template') or ''),
        'purchase_documents': str(form_values.get('purchases_query_template') or ''),
        'inventory_documents': str(form_values.get('inventory_query_template') or ''),
        'cash_transactions': str(form_values.get('cashflow_query_template') or ''),
        'supplier_balances': str(form_values.get('supplier_balances_query_template') or ''),
        'customer_balances': str(form_values.get('customer_balances_query_template') or ''),
    }
    mapping_raw = form_values.get('stream_query_mapping')
    out: dict[str, str] = {}
    if isinstance(mapping_raw, dict):
        for key, value in mapping_raw.items():
            stream = normalize_stream_name(str(key))
            if stream and isinstance(value, str) and value.strip():
                out[stream] = value
    for stream, query in fallback.items():
        if query.strip() and stream not in out:
            out[stream] = query
    return out


def _coerce_stream_field_mapping_from_json(raw: str | None) -> dict[str, dict[str, str]]:
    txt = str(raw or '').strip()
    if not txt:
        return {}
    try:
        parsed = json.loads(txt)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for stream_key, mapping in parsed.items():
        stream = normalize_stream_name(str(stream_key))
        if stream is None or not isinstance(mapping, dict):
            continue
        cleaned_map: dict[str, str] = {}
        for canonical_field, source_field in mapping.items():
            c = str(canonical_field or '').strip()
            s = str(source_field or '').strip()
            if c and s:
                cleaned_map[c] = s
        if cleaned_map:
            out[stream] = cleaned_map
    return out


def _safe_rule_domain(raw: str, fallback: RuleDomain) -> RuleDomain:
    value = str(raw or '').strip().lower()
    for domain in RuleDomain:
        if domain.value == value:
            return domain
    return fallback


def _safe_operational_stream(raw: str) -> OperationalStream:
    stream = normalize_stream_name(raw)
    if stream is None:
        return OperationalStream.sales_documents
    return OperationalStream(stream)


async def _render_business_rules_page(
    *,
    request: Request,
    db: AsyncSession,
    domain: RuleDomain,
    active_page: str,
    title: str,
    page_label_key: str,
    page_description: str,
) -> HTMLResponse:
    rulesets = (
        await db.execute(
            select(GlobalRuleSet).order_by(GlobalRuleSet.priority.desc(), GlobalRuleSet.code.asc())
        )
    ).scalars().all()
    entries = (
        await db.execute(
            select(GlobalRuleEntry, GlobalRuleSet)
            .join(GlobalRuleSet, GlobalRuleSet.id == GlobalRuleEntry.ruleset_id)
            .where(GlobalRuleEntry.domain == domain)
            .order_by(
                GlobalRuleSet.priority.desc(),
                GlobalRuleSet.code.asc(),
                GlobalRuleEntry.stream.asc(),
                GlobalRuleEntry.rule_key.asc(),
            )
        )
    ).all()
    stream_options = [
        {'value': stream.value, 'label': tt(request, _BUSINESS_RULE_STREAM_LABEL_BY_VALUE.get(stream.value, 'select'))}
        for stream in OperationalStream
    ]
    entry_rows = [
        {
            'id': entry.id,
            'ruleset_code': ruleset.code,
            'stream': entry.stream.value,
            'stream_label': tt(request, _BUSINESS_RULE_STREAM_LABEL_BY_VALUE.get(entry.stream.value, 'select')),
            'rule_key': entry.rule_key,
            'is_active': bool(entry.is_active),
            'payload_json': json.dumps(entry.payload_json or {}, ensure_ascii=False, indent=2),
            'updated_at': entry.updated_at,
        }
        for entry, ruleset in entries
    ]
    return templates.TemplateResponse(
        'admin/business_rules_domain.html',
        {
            'request': request,
            'active_page': active_page,
            'title': title,
            'page_label_key': page_label_key,
            'page_description': page_description,
            'domain_value': domain.value,
            'domain_label': _RULE_DOMAIN_LABEL_BY_VALUE.get(domain.value, domain.value),
            'stream_options': stream_options,
            'rulesets': rulesets,
            'entries': entry_rows,
            'saved': request.query_params.get('saved') == '1',
            'error_message': request.query_params.get('error') or '',
            'initial_ruleset_code': (rulesets[0].code if rulesets else 'softone_default_v1'),
        },
    )


def _doc_stream_label(stream_value: str) -> str:
    lookup = {str(item['value']): str(item['label']) for item in _DOCUMENT_RULE_STREAMS}
    return lookup.get(str(stream_value or ''), str(stream_value or ''))


def _softone_document_templates_preview() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        stream_value = str(item.get('stream') or OperationalStream.sales_documents.value)
        amount_sign = _normalize_sign(item.get('amount_sign'))
        quantity_sign = _normalize_sign(item.get('quantity_sign'))
        rows.append(
            {
                **item,
                'stream_label': _doc_stream_label(stream_value),
                'amount_sign_label': _DOCUMENT_SIGN_LABEL.get(amount_sign, amount_sign),
                'quantity_sign_label': _DOCUMENT_SIGN_LABEL.get(quantity_sign, quantity_sign),
            }
        )
    return rows


async def _render_document_type_rules_page(
    *,
    request: Request,
    db: AsyncSession,
    active_page: str,
    title: str,
    page_label_key: str,
    page_description: str,
) -> HTMLResponse:
    rulesets = (
        await db.execute(
            select(GlobalRuleSet).order_by(GlobalRuleSet.priority.desc(), GlobalRuleSet.code.asc())
        )
    ).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    tenants_map = {int(t.id): t for t in tenants}

    tenant_id_raw = str(request.query_params.get('tenant_id') or '').strip()
    selected_tenant_id: int | None = None
    if tenant_id_raw.isdigit():
        candidate = int(tenant_id_raw)
        if candidate in tenants_map:
            selected_tenant_id = candidate

    global_pairs = (
        await db.execute(
            select(GlobalRuleEntry, GlobalRuleSet)
            .join(GlobalRuleSet, GlobalRuleSet.id == GlobalRuleEntry.ruleset_id)
            .where(GlobalRuleEntry.domain == RuleDomain.document_type_rules)
            .order_by(
                GlobalRuleSet.priority.desc(),
                GlobalRuleSet.code.asc(),
                GlobalRuleEntry.stream.asc(),
                GlobalRuleEntry.rule_key.asc(),
            )
        )
    ).all()

    tenant_stmt = (
        select(TenantRuleOverride)
        .where(TenantRuleOverride.domain == RuleDomain.document_type_rules)
        .order_by(
            TenantRuleOverride.tenant_id.asc(),
            TenantRuleOverride.stream.asc(),
            TenantRuleOverride.rule_key.asc(),
        )
    )
    if selected_tenant_id is not None:
        tenant_stmt = tenant_stmt.where(TenantRuleOverride.tenant_id == selected_tenant_id)
    tenant_rows_models = (await db.execute(tenant_stmt)).scalars().all()

    global_rows: list[dict[str, object]] = []
    global_map: dict[tuple[str, str], tuple[GlobalRuleEntry, GlobalRuleSet]] = {}
    for entry, ruleset in global_pairs:
        key = (entry.stream.value, entry.rule_key)
        global_map[key] = (entry, ruleset)
        global_rows.append(
            _document_rule_row(
                scope='global',
                scope_label='Global Default',
                ruleset_code=ruleset.code,
                stream=entry.stream.value,
                stream_label=_doc_stream_label(entry.stream.value),
                rule_key=entry.rule_key,
                is_active=bool(entry.is_active),
                payload=dict(entry.payload_json or {}),
                updated_at=entry.updated_at,
            )
        )

    tenant_override_rows: list[dict[str, object]] = []
    override_map: dict[tuple[str, str], TenantRuleOverride] = {}
    for row in tenant_rows_models:
        key = (row.stream.value, row.rule_key)
        override_map[key] = row
        tenant_obj = tenants_map.get(int(row.tenant_id))
        tenant_name = tenant_obj.name if tenant_obj else f'Tenant {row.tenant_id}'
        tenant_override_rows.append(
            _document_rule_row(
                scope='tenant',
                scope_label='Tenant Override',
                ruleset_code='tenant_override',
                stream=row.stream.value,
                stream_label=_doc_stream_label(row.stream.value),
                rule_key=row.rule_key,
                is_active=bool(row.is_active),
                payload=dict(row.payload_json or {}),
                updated_at=row.updated_at,
                tenant_id=int(row.tenant_id),
                tenant_name=tenant_name,
                override_mode=row.override_mode.value if getattr(row, 'override_mode', None) else 'replace',
            )
        )

    effective_rows: list[dict[str, object]] = []
    if selected_tenant_id is not None:
        all_keys = sorted(set(global_map.keys()) | set(override_map.keys()))
        for key in all_keys:
            stream_value, rule_key = key
            global_pair = global_map.get(key)
            override_row = override_map.get(key)
            global_entry = global_pair[0] if global_pair else None
            ruleset_code = global_pair[1].code if global_pair else 'tenant_only'
            if override_row is None:
                if global_entry is None:
                    continue
                payload = dict(global_entry.payload_json or {})
                is_active = bool(global_entry.is_active)
            else:
                mode = override_row.override_mode
                override_payload = dict(override_row.payload_json or {})
                if mode == OverrideMode.disable:
                    base_document_type = ''
                    if global_entry is not None:
                        base_document_type = str((global_entry.payload_json or {}).get('document_type') or '').strip()
                    payload = {'document_type': base_document_type, 'enabled': False}
                elif mode == OverrideMode.merge and global_entry is not None:
                    payload = _deep_merge_dict(dict(global_entry.payload_json or {}), override_payload)
                else:
                    payload = override_payload
                is_active = bool(override_row.is_active)

            effective_rows.append(
                _document_rule_row(
                    scope='effective',
                    scope_label='Effective (Tenant)',
                    ruleset_code=ruleset_code,
                    stream=stream_value,
                    stream_label=_doc_stream_label(stream_value),
                    rule_key=rule_key,
                    is_active=is_active,
                    payload=payload,
                    updated_at=override_row.updated_at if override_row is not None else (global_entry.updated_at if global_entry else None),
                    tenant_id=selected_tenant_id,
                    tenant_name=tenants_map.get(selected_tenant_id).name if selected_tenant_id in tenants_map else None,
                    override_mode=override_row.override_mode.value if override_row is not None else 'global',
                )
            )

    default_form_values = {
        'document_type': '',
        'stream': OperationalStream.sales_documents.value,
        'include_revenue': '1',
        'include_quantity': '1',
        'include_cost': '1',
        'affects_customer_balance': '0',
        'affects_supplier_balance': '0',
        'amount_sign': 'positive',
        'quantity_sign': 'positive',
        'is_active': '1',
        'scope': 'global',
        'override_mode': OverrideMode.replace.value,
    }

    return templates.TemplateResponse(
        'admin/business_rules_document_type_rules.html',
        {
            'request': request,
            'active_page': active_page,
            'title': title,
            'page_label_key': page_label_key,
            'page_description': page_description,
            'domain_value': RuleDomain.document_type_rules.value,
            'rulesets': rulesets,
            'initial_ruleset_code': (rulesets[0].code if rulesets else 'softone_default_v1'),
            'stream_options': _DOCUMENT_RULE_STREAMS,
            'sign_options': _DOCUMENT_SIGN_OPTIONS,
            'softone_templates': _softone_document_templates_preview(),
            'tenants': tenants,
            'selected_tenant_id': selected_tenant_id,
            'selected_tenant_name': tenants_map.get(selected_tenant_id).name if selected_tenant_id in tenants_map else None,
            'global_rows': global_rows,
            'tenant_override_rows': tenant_override_rows,
            'effective_rows': effective_rows,
            'default_form_values': default_form_values,
            'saved': request.query_params.get('saved') == '1',
            'template_saved': request.query_params.get('template_saved') == '1',
            'wizard_applied': request.query_params.get('wizard_applied') == '1',
            'error_message': request.query_params.get('error') or '',
        },
    )


async def _connections_template_context(
    db: AsyncSession,
    *,
    request: Request,
    result: dict | None = None,
    discovery: dict | None = None,
    form_values: dict | None = None,
    active_page: str = 'connections',
    title: str = 'connections',
) -> dict:
    rows = (await db.execute(select(TenantConnection).order_by(TenantConnection.id.desc()))).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    resolved_form = dict(form_values or {})
    connector_type = str(resolved_form.get('connector_type') or 'sql_connector')
    default_supported = _stream_defaults_for_connector(connector_type)
    resolved_form.setdefault('connector_type', connector_type)
    resolved_form.setdefault('source_type', _normalize_source_type(connector_type, str(resolved_form.get('source_type') or '')))
    resolved_form.setdefault('sales_query_template', DEFAULT_GENERIC_SALES_QUERY)
    resolved_form.setdefault('purchases_query_template', DEFAULT_GENERIC_PURCHASES_QUERY)
    resolved_form.setdefault('inventory_query_template', DEFAULT_GENERIC_INVENTORY_QUERY)
    resolved_form.setdefault('cashflow_query_template', DEFAULT_GENERIC_CASHFLOW_QUERY)
    resolved_form.setdefault('supplier_balances_query_template', DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY)
    resolved_form.setdefault('customer_balances_query_template', DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY)
    resolved_form.setdefault('stream_field_mapping_json', '{}')
    resolved_form.setdefault('supported_streams', default_supported)
    enabled_default = resolved_form.get('enabled_streams')
    resolved_form['enabled_streams'] = _normalize_stream_selection(
        enabled_default if isinstance(enabled_default, list) else None,
        fallback=list(resolved_form['supported_streams']),
    )
    stream_options = [{'value': stream, 'label': tt(request, label_key)} for stream, label_key in _STREAM_LABEL_KEYS]

    return {
        'request': request,
        'connections': rows,
        'tenants': tenants,
        'active_page': active_page,
        'title': title,
        'server_public_ip': settings.server_public_ip,
        'sqlserver_port': settings.sqlserver_default_port,
        'result': result,
        'discovery': discovery,
        'form_values': resolved_form,
        'stream_options': stream_options,
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


def _render_admin_menu_placeholder(
    *,
    request: Request,
    active_page: str,
    title: str,
    page_title_key: str,
    page_description: str,
    quick_links: list[dict[str, str]] | None = None,
) -> HTMLResponse:
    return templates.TemplateResponse(
        'admin/menu_placeholder.html',
        {
            'request': request,
            'active_page': active_page,
            'title': title,
            'page_title_key': page_title_key,
            'page_description': page_description,
            'quick_links': quick_links or [],
        },
    )


def _render_tenant_menu_placeholder(
    *,
    request: Request,
    tenant: Tenant,
    active_page: str,
    title: str,
    page_title_key: str,
    page_description: str,
    nav_context: dict[str, bool | int],
) -> HTMLResponse:
    return templates.TemplateResponse(
        'tenant/menu_placeholder.html',
        {
            'request': request,
            'tenant': tenant,
            **nav_context,
            'active_page': active_page,
            'title': title,
            'page_title_key': page_title_key,
            'page_description': page_description,
            'hide_page_filters': True,
        },
    )


@router.api_route('/', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def portal_root(request: Request, db: AsyncSession = Depends(get_control_db)):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    token = request.cookies.get('access_token')
    payload = None
    if token:
        expected_aud = expected_audience_for_host(host)
        payload = safe_decode(token, audience=expected_aud, token_type='access')

    if payload:
        role_raw = str(payload.get('role') or '').strip()
        role = RoleName(role_raw) if role_raw in {r.value for r in RoleName} else RoleName.tenant_admin
        if role == RoleName.cloudon_admin or host == settings.admin_portal_host.lower():
            resp = RedirectResponse(url='/admin/dashboard', status_code=302)
            resp.headers['Cache-Control'] = 'no-store'
            return resp

        profile_code: str | None = None
        user_id = payload.get('sub')
        if user_id is not None:
            try:
                uid = int(user_id)
                row = (
                    await db.execute(
                        select(User.role, ProfessionalProfile.profile_code)
                        .outerjoin(ProfessionalProfile, User.professional_profile_id == ProfessionalProfile.id)
                        .where(User.id == uid, User.is_active.is_(True))
                    )
                ).first()
                if row:
                    role = row[0]
                    profile_code = row[1]
            except (ValueError, TypeError):
                profile_code = None

        redirect_url = _dashboard_redirect_for_profile_code(profile_code, role)
        resp = RedirectResponse(url=redirect_url, status_code=302)
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
    profile_code = None
    if user.professional_profile_id:
        profile_code = (
            await db.execute(
                select(ProfessionalProfile.profile_code).where(ProfessionalProfile.id == user.professional_profile_id)
            )
        ).scalar_one_or_none()
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
    resp = RedirectResponse(url=_login_redirect_for(user, host=host, profile_code=profile_code), status_code=303)
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

    if source not in {'sql', 'external', 'files'}:
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


@router.get('/admin/data-sources', response_class=HTMLResponse)
async def admin_data_sources(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    context = await _connections_template_context(
        db,
        request=request,
        result=None,
        discovery=None,
        active_page='data_sources',
        title='title_data_sources',
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/test', response_class=HTMLResponse)
async def admin_connections_test(
    request: Request,
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
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
    inventory_query_template: str = Form(default=DEFAULT_GENERIC_INVENTORY_QUERY),
    cashflow_query_template: str = Form(default=DEFAULT_GENERIC_CASHFLOW_QUERY),
    supplier_balances_query_template: str = Form(default=DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY),
    customer_balances_query_template: str = Form(default=DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY),
    stream_field_mapping_json: str = Form(default='{}'),
    enabled_streams: list[str] = Form(default=[]),
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
                TenantConnection.connector_type == connector_type,
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
            'connector_type': connector_type,
            'source_type': _normalize_source_type(connector_type, source_type),
            'port': port,
            'database': database,
            'username': username,
            'options': options,
            'selected_schema': selected_schema,
            'selected_object': selected_object,
            'sales_query_template': sales_query_template,
            'purchases_query_template': purchases_query_template,
            'inventory_query_template': inventory_query_template,
            'cashflow_query_template': cashflow_query_template,
            'supplier_balances_query_template': supplier_balances_query_template,
            'customer_balances_query_template': customer_balances_query_template,
            'stream_field_mapping_json': stream_field_mapping_json,
            'enabled_streams': _normalize_stream_selection(
                enabled_streams,
                fallback=_stream_defaults_for_connector(connector_type),
            ),
            'updated_at_column': updated_at_column,
            'id_column': id_column,
            'date_column': date_column,
            'branch_column': branch_column,
            'item_column': item_column,
            'net_amount_column': net_amount_column,
            'cost_column': cost_column,
            'qty_column': qty_column,
        },
        active_page=('data_sources' if source_page == 'data_sources' else 'connections'),
        title=('title_data_sources' if source_page == 'data_sources' else 'connections'),
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/discovery', response_class=HTMLResponse)
async def admin_connections_discovery(
    request: Request,
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
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
    inventory_query_template: str = Form(default=DEFAULT_GENERIC_INVENTORY_QUERY),
    cashflow_query_template: str = Form(default=DEFAULT_GENERIC_CASHFLOW_QUERY),
    supplier_balances_query_template: str = Form(default=DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY),
    customer_balances_query_template: str = Form(default=DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY),
    stream_field_mapping_json: str = Form(default='{}'),
    enabled_streams: list[str] = Form(default=[]),
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
            'connector_type': connector_type,
            'source_type': _normalize_source_type(connector_type, source_type),
            'port': port,
            'database': database,
            'username': username,
            'options': options,
            'selected_schema': selected_schema,
            'selected_object': selected_object,
            'sales_query_template': sales_query_template,
            'purchases_query_template': purchases_query_template,
            'inventory_query_template': inventory_query_template,
            'cashflow_query_template': cashflow_query_template,
            'supplier_balances_query_template': supplier_balances_query_template,
            'customer_balances_query_template': customer_balances_query_template,
            'stream_field_mapping_json': stream_field_mapping_json,
            'enabled_streams': _normalize_stream_selection(
                enabled_streams,
                fallback=_stream_defaults_for_connector(connector_type),
            ),
            'updated_at_column': updated_at_column,
            'id_column': id_column,
            'date_column': date_column,
            'branch_column': branch_column,
            'item_column': item_column,
            'net_amount_column': net_amount_column,
            'cost_column': cost_column,
            'qty_column': qty_column,
        },
        active_page=('data_sources' if source_page == 'data_sources' else 'connections'),
        title=('title_data_sources' if source_page == 'data_sources' else 'connections'),
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/save')
async def admin_connections_save(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    sales_query_template: str = Form(default=DEFAULT_GENERIC_SALES_QUERY),
    purchases_query_template: str = Form(default=DEFAULT_GENERIC_PURCHASES_QUERY),
    inventory_query_template: str = Form(default=DEFAULT_GENERIC_INVENTORY_QUERY),
    cashflow_query_template: str = Form(default=DEFAULT_GENERIC_CASHFLOW_QUERY),
    supplier_balances_query_template: str = Form(default=DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY),
    customer_balances_query_template: str = Form(default=DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY),
    stream_field_mapping_json: str = Form(default='{}'),
    enabled_streams: list[str] = Form(default=[]),
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
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url=f'{redirect_base}?saved=0', status_code=303)

    options_map = _parse_options_map(options)

    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == connector_type,
            )
        )
    ).scalar_one_or_none()
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type=connector_type,
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
    conn.inventory_query_template = inventory_query_template or DEFAULT_GENERIC_INVENTORY_QUERY
    conn.cashflow_query_template = cashflow_query_template or DEFAULT_GENERIC_CASHFLOW_QUERY
    conn.supplier_balances_query_template = (
        supplier_balances_query_template or DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY
    )
    conn.customer_balances_query_template = (
        customer_balances_query_template or DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY
    )
    conn.incremental_column = (incremental_column or updated_at_column).strip() or 'UpdatedAt'
    conn.id_column = id_column
    conn.date_column = date_column
    conn.branch_column = branch_column
    conn.item_column = item_column
    conn.amount_column = (amount_column or net_amount_column).strip() or 'NetValue'
    conn.cost_column = cost_column
    conn.qty_column = qty_column
    conn.source_type = _normalize_source_type(connector_type, source_type)
    default_supported = _stream_defaults_for_connector(connector_type)
    conn.supported_streams = default_supported
    conn.enabled_streams = _normalize_stream_selection(enabled_streams, fallback=default_supported)
    conn.stream_query_mapping = _coerce_stream_query_mapping_from_values(
        {
            'sales_query_template': conn.sales_query_template,
            'purchases_query_template': conn.purchases_query_template,
            'inventory_query_template': conn.inventory_query_template,
            'cashflow_query_template': conn.cashflow_query_template,
            'supplier_balances_query_template': conn.supplier_balances_query_template,
            'customer_balances_query_template': conn.customer_balances_query_template,
        }
    )
    conn.stream_field_mapping = _coerce_stream_field_mapping_from_json(stream_field_mapping_json)
    conn.connection_parameters = {
        'connector_type': connector_type,
        'source_type': conn.source_type,
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'options': options_map,
    }
    conn.last_test_error = None

    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='connection_saved_ui',
            entity_type='tenant_connection',
            entity_id=str(conn.id or ''),
            payload={
                'connector_type': connector_type,
                'source_type': conn.source_type,
                'enabled_streams': conn.enabled_streams,
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
                'stream_query_mapping': conn.stream_query_mapping,
                'stream_field_mapping': conn.stream_field_mapping,
            },
        )
    )
    await db.commit()
    return RedirectResponse(url=f'{redirect_base}?saved=1', status_code=303)


@router.post('/admin/connections/apply-pack')
async def admin_connections_apply_pack(
    tenant_id: int = Form(...),
    source_page: str = Form(default='connections'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url=f'{redirect_base}?pack=0', status_code=303)
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type.in_(('sql_connector', 'pharmacyone_sql')),
            )
        )
    ).scalar_one_or_none()
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type='sql_connector',
            sync_status='never',
        )
        db.add(conn)
    pack = load_querypack('erp_sql', 'default')
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
    return RedirectResponse(url=f'{redirect_base}?pack=1', status_code=303)


@router.post('/admin/connections/backfill')
async def admin_connections_backfill(
    tenant_id: int = Form(...),
    source_page: str = Form(default='connections'),
    from_date: str = Form(...),
    to_date: str = Form(...),
    chunk_days: int = Form(default=7),
    include_purchases: bool = Form(default=True),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    from_dt = _parse_date_or_none(from_date)
    to_dt = _parse_date_or_none(to_date)
    if tenant is None or from_dt is None or to_dt is None or from_dt > to_dt:
        return RedirectResponse(url=f'{redirect_base}?backfill=0', status_code=303)

    task = celery_client.send_task(
        'worker.tasks.enqueue_sql_backfill',
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
    return RedirectResponse(url=f'{redirect_base}?backfill=1', status_code=303)


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


@router.get('/admin/business-rules', response_class=HTMLResponse)
async def admin_business_rules_overview(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/business_rules_overview.html',
        {
            'request': request,
            'active_page': 'business_rules',
            'title': 'title_business_rules',
        },
    )


@router.get('/admin/business-rules/document-type-rules', response_class=HTMLResponse)
async def admin_business_rules_document_types(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await _render_document_type_rules_page(
        request=request,
        db=db,
        active_page='business_rules_document_types',
        title='title_business_rules_document_types',
        page_label_key='business_rules_document_type_rules',
        page_description='Διαχείριση classification τύπων παραστατικών, πρόσημου και impact σε έσοδα/ποσότητες/υπόλοιπα.',
    )


@router.get('/admin/business-rules/document-type-rules/help', response_class=HTMLResponse)
async def admin_business_rules_document_types_help(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/business_rules_document_type_rules_help.html',
        {
            'request': request,
            'active_page': 'business_rules_document_types',
            'title': 'title_business_rules_document_types',
        },
    )


@router.get('/admin/business-rules/document-type-rules/wizard', response_class=HTMLResponse)
async def admin_business_rules_document_types_wizard(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    return templates.TemplateResponse(
        'admin/business_rules_document_type_rules_wizard.html',
        {
            'request': request,
            'tenants': tenants,
            'stream_options': _DOCUMENT_RULE_STREAMS,
            'sign_options': _DOCUMENT_SIGN_OPTIONS,
            'softone_templates': _softone_document_templates_preview(),
            'active_page': 'business_rules_document_types',
            'title': 'title_business_rules_document_types',
            'saved': request.query_params.get('saved') == '1',
            'error_message': request.query_params.get('error') or '',
            'template_saved': request.query_params.get('template_saved') == '1',
            'wizard_applied': request.query_params.get('wizard_applied') == '1',
        },
    )


@router.post('/admin/business-rules/document-type-rules/upsert-form')
async def admin_business_rules_document_types_upsert_form(
    scope: str = Form(default='global'),
    tenant_id: str | None = Form(default=None),
    ruleset_code: str = Form(default='softone_default_v1'),
    stream_value: str = Form(default=OperationalStream.sales_documents.value),
    document_type: str = Form(default=''),
    include_revenue: str = Form(default='0'),
    include_quantity: str = Form(default='0'),
    include_cost: str = Form(default='0'),
    affects_customer_balance: str = Form(default='0'),
    affects_supplier_balance: str = Form(default='0'),
    amount_sign: str = Form(default='none'),
    quantity_sign: str = Form(default='none'),
    is_active: str = Form(default='1'),
    rule_key: str | None = Form(default=None),
    override_mode: str = Form(default='replace'),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    stream = _safe_operational_stream(stream_value)
    if stream not in {
        OperationalStream.sales_documents,
        OperationalStream.purchase_documents,
        OperationalStream.inventory_documents,
        OperationalStream.cash_transactions,
    }:
        return RedirectResponse(url=f'{redirect_base}?saved=0&error=Μη+έγκυρο+κύκλωμα+για+κανόνα+τύπου+παραστατικού', status_code=303)

    doc_type = str(document_type or '').strip()
    if not doc_type:
        return RedirectResponse(url=f'{redirect_base}?saved=0&error=Το+πεδίο+Τύπος+Παραστατικού+είναι+υποχρεωτικό', status_code=303)

    resolved_rule_key = str(rule_key or '').strip() or _document_rule_key(doc_type, stream.value)
    payload = _build_document_rule_payload(
        document_type=doc_type,
        include_revenue=_to_bool_flag(include_revenue),
        include_quantity=_to_bool_flag(include_quantity),
        include_cost=_to_bool_flag(include_cost),
        affects_customer_balance=_to_bool_flag(affects_customer_balance),
        affects_supplier_balance=_to_bool_flag(affects_supplier_balance),
        amount_sign=_normalize_sign(amount_sign),
        quantity_sign=_normalize_sign(quantity_sign),
    )
    active_flag = _to_bool_flag(is_active)

    scope_value = str(scope or '').strip().lower()
    if scope_value == 'tenant':
        if not str(tenant_id or '').strip().isdigit():
            return RedirectResponse(url=f'{redirect_base}?saved=0&error=Επίλεξε+tenant+για+tenant-specific+override', status_code=303)
        tenant_id_int = int(str(tenant_id))
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id_int))).scalar_one_or_none()
        if tenant is None:
            return RedirectResponse(url=f'{redirect_base}?saved=0&error=Tenant+δεν+βρέθηκε', status_code=303)
        mode = OverrideMode.merge if str(override_mode or '').strip().lower() == OverrideMode.merge.value else OverrideMode.replace
        await _upsert_document_rule_tenant_override(
            db=db,
            tenant_id=tenant_id_int,
            stream=stream,
            rule_key=resolved_rule_key,
            payload_json=payload,
            is_active=active_flag,
            override_mode=mode,
            replace_existing=True,
        )
        db.add(
            AuditLog(
                tenant_id=tenant_id_int,
                action='document_rule_upsert_form',
                entity_type='tenant_rule_override',
                entity_id=resolved_rule_key,
                payload={
                    'scope': 'tenant',
                    'stream': stream.value,
                    'rule_key': resolved_rule_key,
                    'override_mode': mode.value,
                },
            )
        )
        await db.commit()
        return RedirectResponse(url=f'{redirect_base}?saved=1&tenant_id={tenant_id_int}', status_code=303)

    await _upsert_document_rule_global(
        db=db,
        ruleset_code=str(ruleset_code or '').strip() or 'softone_default_v1',
        stream=stream,
        rule_key=resolved_rule_key,
        payload_json=payload,
        is_active=active_flag,
        replace_existing=True,
    )
    db.add(
        AuditLog(
            tenant_id=None,
            action='document_rule_upsert_form',
            entity_type='global_rule_entry',
            entity_id=resolved_rule_key,
            payload={
                'scope': 'global',
                'stream': stream.value,
                'rule_key': resolved_rule_key,
                'ruleset_code': str(ruleset_code or '').strip() or 'softone_default_v1',
            },
        )
    )
    await db.commit()
    return RedirectResponse(url=f'{redirect_base}?saved=1', status_code=303)


@router.post('/admin/business-rules/document-type-rules/apply-softone-template')
async def admin_business_rules_document_types_apply_softone_template(
    scope: str = Form(default='global'),
    tenant_id: str | None = Form(default=None),
    ruleset_code: str = Form(default='softone_default_v1'),
    replace_existing: str = Form(default='1'),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    allow_replace = _to_bool_flag(replace_existing)
    scope_value = str(scope or '').strip().lower()
    changed = 0

    if scope_value == 'tenant':
        if not str(tenant_id or '').strip().isdigit():
            return RedirectResponse(url=f'{redirect_base}?template_saved=0&error=Επίλεξε+tenant+για+template+override', status_code=303)
        tenant_id_int = int(str(tenant_id))
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id_int))).scalar_one_or_none()
        if tenant is None:
            return RedirectResponse(url=f'{redirect_base}?template_saved=0&error=Tenant+δεν+βρέθηκε', status_code=303)
        for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
            stream = _safe_operational_stream(str(item.get('stream') or OperationalStream.sales_documents.value))
            doc_type = str(item.get('document_type') or '').strip()
            key = _document_rule_key(doc_type, stream.value)
            payload = _build_document_rule_payload(
                document_type=doc_type,
                include_revenue=bool(item.get('include_revenue')),
                include_quantity=bool(item.get('include_quantity')),
                include_cost=bool(item.get('include_cost')),
                affects_customer_balance=bool(item.get('affects_customer_balance')),
                affects_supplier_balance=bool(item.get('affects_supplier_balance')),
                amount_sign=str(item.get('amount_sign') or 'none'),
                quantity_sign=str(item.get('quantity_sign') or 'none'),
            )
            updated = await _upsert_document_rule_tenant_override(
                db=db,
                tenant_id=tenant_id_int,
                stream=stream,
                rule_key=key,
                payload_json=payload,
                is_active=True,
                override_mode=OverrideMode.replace,
                replace_existing=allow_replace,
            )
            if updated:
                changed += 1
        db.add(
            AuditLog(
                tenant_id=tenant_id_int,
                action='document_rules_softone_template_apply',
                entity_type='tenant_rule_override',
                entity_id=str(tenant_id_int),
                payload={'scope': 'tenant', 'changed': changed, 'replace_existing': allow_replace},
            )
        )
        await db.commit()
        return RedirectResponse(url=f'{redirect_base}?template_saved=1&wizard_applied=1&tenant_id={tenant_id_int}', status_code=303)

    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        stream = _safe_operational_stream(str(item.get('stream') or OperationalStream.sales_documents.value))
        doc_type = str(item.get('document_type') or '').strip()
        key = _document_rule_key(doc_type, stream.value)
        payload = _build_document_rule_payload(
            document_type=doc_type,
            include_revenue=bool(item.get('include_revenue')),
            include_quantity=bool(item.get('include_quantity')),
            include_cost=bool(item.get('include_cost')),
            affects_customer_balance=bool(item.get('affects_customer_balance')),
            affects_supplier_balance=bool(item.get('affects_supplier_balance')),
            amount_sign=str(item.get('amount_sign') or 'none'),
            quantity_sign=str(item.get('quantity_sign') or 'none'),
        )
        updated = await _upsert_document_rule_global(
            db=db,
            ruleset_code=str(ruleset_code or '').strip() or 'softone_default_v1',
            stream=stream,
            rule_key=key,
            payload_json=payload,
            is_active=True,
            replace_existing=allow_replace,
        )
        if updated:
            changed += 1
    db.add(
        AuditLog(
            tenant_id=None,
            action='document_rules_softone_template_apply',
            entity_type='global_rule_entry',
            entity_id='softone_default_v1',
            payload={
                'scope': 'global',
                'changed': changed,
                'replace_existing': allow_replace,
                'ruleset_code': str(ruleset_code or '').strip() or 'softone_default_v1',
            },
        )
    )
    await db.commit()
    return RedirectResponse(url=f'{redirect_base}?template_saved=1&wizard_applied=1', status_code=303)


@router.get('/admin/business-rules/stream-mapping-rules', response_class=HTMLResponse)
async def admin_business_rules_stream_mapping(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await _render_business_rules_page(
        request=request,
        db=db,
        domain=RuleDomain.source_mapping,
        active_page='business_rules_stream_mapping',
        title='title_business_rules_stream_mapping',
        page_label_key='business_rules_stream_mapping_rules',
        page_description='Ορισμός κανόνων ανάθεσης παραστατικών στα επιχειρησιακά κυκλώματα (sales/purchases/inventory/cash/balances).',
    )


@router.get('/admin/business-rules/kpi-participation-rules', response_class=HTMLResponse)
async def admin_business_rules_kpi_participation(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await _render_business_rules_page(
        request=request,
        db=db,
        domain=RuleDomain.kpi_participation_rules,
        active_page='business_rules_kpi_participation',
        title='title_business_rules_kpi_participation',
        page_label_key='business_rules_kpi_participation_rules',
        page_description='Κανόνες συμμετοχής εγγράφων σε KPI (include/exclude revenue, qty, cost, sign behavior, balance impact).',
    )


@router.get('/admin/business-rules/intelligence-rules', response_class=HTMLResponse)
async def admin_business_rules_intelligence(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await _render_business_rules_page(
        request=request,
        db=db,
        domain=RuleDomain.intelligence_threshold_rules,
        active_page='business_rules_intelligence',
        title='title_business_rules_intelligence',
        page_label_key='business_rules_intelligence_rules',
        page_description='Ρύθμιση thresholds, severity και ενεργοποίησης deterministic insight κανόνων ανά stream.',
    )


@router.get('/admin/business-rules/query-mapping', response_class=HTMLResponse)
async def admin_business_rules_query_mapping(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    return await _render_business_rules_page(
        request=request,
        db=db,
        domain=RuleDomain.source_mapping,
        active_page='business_rules_query_mapping',
        title='title_business_rules_query_mapping',
        page_label_key='business_rules_query_mapping',
        page_description='Global defaults και tenant overrides για source query mappings ανά επιχειρησιακό stream.',
    )


@router.get('/admin/overview/tenant-health', response_class=HTMLResponse)
async def admin_tenant_health(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='tenant_health',
        title='title_tenant_health',
        page_title_key='tenant_health',
        page_description='Επισκόπηση υγείας tenants: κατάσταση συνδρομών, βασικά alerts και πρόσφατες αποτυχίες συγχρονισμού.',
        quick_links=[
            {'href': '/admin/tenants', 'label_key': 'tenants'},
            {'href': '/admin/subscriptions', 'label_key': 'subscriptions'},
            {'href': '/admin/sync-status', 'label_key': 'sync_status'},
        ],
    )


@router.get('/admin/data-sources/stream-mapping', response_class=HTMLResponse)
async def admin_data_sources_stream_mapping(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='data_sources_stream_mapping',
        title='title_data_sources_stream_mapping',
        page_title_key='stream_mapping',
        page_description='Δήλωσε ποια επιχειρησιακά κυκλώματα ενεργοποιεί κάθε connector και πώς γίνεται το mapping ανά stream.',
        quick_links=[
            {'href': '/admin/data-sources', 'label_key': 'data_sources'},
            {'href': '/admin/business-rules/stream-mapping-rules', 'label_key': 'business_rules_stream_mapping_rules'},
        ],
    )


@router.get('/admin/data-sources/query-mapping', response_class=HTMLResponse)
async def admin_data_sources_query_mapping(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='data_sources_query_mapping',
        title='title_data_sources_query_mapping',
        page_title_key='query_mapping',
        page_description='Αντιστοίχιση source queries σε canonical streams. Τα defaults είναι global και υποστηρίζονται tenant overrides.',
        quick_links=[
            {'href': '/admin/data-sources', 'label_key': 'data_sources'},
            {'href': '/admin/business-rules/query-mapping', 'label_key': 'business_rules_query_mapping'},
        ],
    )


@router.get('/admin/data-sources/file-imports', response_class=HTMLResponse)
async def admin_data_sources_file_imports(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='data_sources_file_imports',
        title='title_data_sources_file_imports',
        page_title_key='file_imports',
        page_description='Ρύθμιση pipelines για CSV/Excel/SFTP με stream-level mapping και κανόνες validation.',
        quick_links=[
            {'href': '/admin/data-sources', 'label_key': 'data_sources'},
        ],
    )


@router.get('/admin/business-rules/tenant-overrides', response_class=HTMLResponse)
async def admin_business_rules_tenant_overrides(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='business_rules_tenant_overrides',
        title='title_business_rules_tenant_overrides',
        page_title_key='tenant_overrides',
        page_description='Ορισμός tenant-specific overrides. Runtime resolution: tenant override -> fallback σε global default.',
        quick_links=[
            {'href': '/admin/business-rules', 'label_key': 'business_rules'},
            {'href': '/admin/tenants', 'label_key': 'tenants'},
        ],
    )


@router.get('/admin/operational-streams/sales-documents', response_class=HTMLResponse)
async def admin_operational_stream_sales_documents(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_sales_documents',
        title='title_sales_documents_dashboard',
        page_title_key='sales_documents_menu',
        page_description='Admin προβολή για validation του stream Παραστατικά Πωλήσεων (staging -> facts -> aggregates).',
    )


@router.get('/admin/operational-streams/purchase-documents', response_class=HTMLResponse)
async def admin_operational_stream_purchase_documents(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_purchase_documents',
        title='title_purchases_documents_dashboard',
        page_title_key='purchases_documents_menu',
        page_description='Admin προβολή για validation του stream Παραστατικά Αγορών (staging -> facts -> aggregates).',
    )


@router.get('/admin/operational-streams/warehouse-documents', response_class=HTMLResponse)
async def admin_operational_stream_warehouse_documents(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_warehouse_documents',
        title='title_warehouse_documents_dashboard',
        page_title_key='warehouse_documents_menu',
        page_description='Admin προβολή για validation του stream Παραστατικά Αποθήκης και inventory movements.',
    )


@router.get('/admin/operational-streams/cash-transactions', response_class=HTMLResponse)
async def admin_operational_stream_cash_transactions(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_cash_transactions',
        title='title_cashflow_dashboard',
        page_title_key='cash_transactions_menu',
        page_description='Admin προβολή για validation cash stream με 5 subcategories και κανόνες sign/impact.',
    )


@router.get('/admin/operational-streams/supplier-balances', response_class=HTMLResponse)
async def admin_operational_stream_supplier_balances(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_supplier_balances',
        title='title_suppliers_dashboard',
        page_title_key='supplier_open_balances_short',
        page_description='Admin προβολή για υποχρεώσεις προμηθευτών, aging buckets και εξέλιξη ανοικτού υπολοίπου.',
    )


@router.get('/admin/operational-streams/customer-balances', response_class=HTMLResponse)
async def admin_operational_stream_customer_balances(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_customer_balances',
        title='title_customers_dashboard',
        page_title_key='customer_open_balances_short',
        page_description='Admin προβολή για απαιτήσεις πελατών, aging buckets και trend είσπραξης.',
    )


@router.get('/admin/monitoring/jobs', response_class=HTMLResponse)
async def admin_monitoring_jobs(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='monitoring_jobs',
        title='title_monitoring_jobs',
        page_title_key='jobs',
        page_description='Παρακολούθηση ingest, aggregate και insight jobs ανά tenant/stream.',
        quick_links=[{'href': '/admin/sync-status', 'label_key': 'sync_status'}],
    )


@router.get('/admin/monitoring/dead-letter-queue', response_class=HTMLResponse)
async def admin_monitoring_dead_letter_queue(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='monitoring_dead_letter_queue',
        title='title_monitoring_dead_letter_queue',
        page_title_key='dead_letter_queue',
        page_description='Προβολή αποτυχημένων εγγραφών (DLQ) με δυνατότητα triage και επανεκτέλεσης.',
    )


@router.get('/admin/monitoring/metrics', response_class=HTMLResponse)
async def admin_monitoring_metrics(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='monitoring_metrics',
        title='title_monitoring_metrics',
        page_title_key='metrics',
        page_description='KPIs πλατφόρμας: latency ingestion, queue depth, throughput, KPI response times.',
    )


@router.get('/admin/monitoring/logs', response_class=HTMLResponse)
async def admin_monitoring_logs(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='monitoring_logs',
        title='title_monitoring_logs',
        page_title_key='logs',
        page_description='Κεντρική προβολή application/sync logs για troubleshooting ανά tenant και connector.',
    )


@router.get('/admin/settings/feature-flags', response_class=HTMLResponse)
async def admin_settings_feature_flags(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='settings_feature_flags',
        title='title_settings_feature_flags',
        page_title_key='feature_flags',
        page_description='Διαχείριση feature flags ανά προϊόν, tenant και περιβάλλον.',
        quick_links=[{'href': '/admin/plans', 'label_key': 'plan_features'}],
    )


@router.get('/admin/settings/system-defaults', response_class=HTMLResponse)
async def admin_settings_system_defaults(
    request: Request,
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/settings.html',
        {
            'request': request,
            'user': user,
            'active_page': 'settings_system_defaults',
            'title': 'title_settings_system_defaults',
        },
    )


@router.post('/admin/business-rules/global-rule/upsert')
async def admin_business_rules_global_rule_upsert(
    domain_value: str = Form(...),
    active_page: str = Form(default='business_rules'),
    redirect_to: str = Form(default='/admin/business-rules'),
    ruleset_code: str = Form(default='softone_default_v1'),
    stream_value: str = Form(default='sales_documents'),
    rule_key: str = Form(default=''),
    payload_json: str = Form(default='{}'),
    is_active: str = Form(default='1'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    domain = _safe_rule_domain(domain_value, RuleDomain.document_type_rules)
    stream = _safe_operational_stream(stream_value)
    cleaned_rule_key = str(rule_key or '').strip()
    if not cleaned_rule_key:
        return RedirectResponse(url=f'{redirect_to}?error=Το+πεδίο+rule_key+είναι+υποχρεωτικό', status_code=303)

    parsed_payload: dict = {}
    if payload_json.strip():
        try:
            loaded_payload = json.loads(payload_json)
            if isinstance(loaded_payload, dict):
                parsed_payload = loaded_payload
        except json.JSONDecodeError:
            return RedirectResponse(url=f'{redirect_to}?error=Μη+έγκυρο+JSON+στο+payload_json', status_code=303)

    ruleset = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == ruleset_code))).scalar_one_or_none()
    if ruleset is None:
        ruleset = GlobalRuleSet(
            code=ruleset_code,
            name=ruleset_code,
            description='Created from admin UI',
            is_active=True,
            priority=100,
        )
        db.add(ruleset)
        await db.flush()

    entry = (
        await db.execute(
            select(GlobalRuleEntry).where(
                GlobalRuleEntry.ruleset_id == ruleset.id,
                GlobalRuleEntry.domain == domain,
                GlobalRuleEntry.stream == stream,
                GlobalRuleEntry.rule_key == cleaned_rule_key,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        db.add(
            GlobalRuleEntry(
                ruleset_id=ruleset.id,
                domain=domain,
                stream=stream,
                rule_key=cleaned_rule_key,
                payload_json=parsed_payload,
                is_active=(is_active == '1'),
            )
        )
    else:
        entry.payload_json = parsed_payload
        entry.is_active = (is_active == '1')

    db.add(
        AuditLog(
            tenant_id=None,
            action='business_rule_global_upsert_ui',
            entity_type='global_rule_entry',
            entity_id=str(ruleset.id),
            payload={
                'domain': domain.value,
                'stream': stream.value,
                'rule_key': cleaned_rule_key,
                'active_page': active_page,
            },
        )
    )
    await db.commit()
    return RedirectResponse(url=f'{redirect_to}?saved=1', status_code=303)


@router.get('/admin/users', response_class=HTMLResponse)
async def admin_users(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    users = (await db.execute(select(User).order_by(User.created_at.desc()))).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    professional_profiles = await _list_professional_profiles(db)
    profile_name_map = {p.id: p.profile_name for p in professional_profiles}
    profile_code_map = {p.id: p.profile_code for p in professional_profiles}
    role_default_profile_code = {
        RoleName.cloudon_admin.value: _default_profile_code_for_role(RoleName.cloudon_admin),
        RoleName.tenant_admin.value: _default_profile_code_for_role(RoleName.tenant_admin),
        RoleName.tenant_user.value: _default_profile_code_for_role(RoleName.tenant_user),
    }
    return templates.TemplateResponse(
        'admin/users.html',
        {
            'request': request,
            'users': users,
            'tenants': tenants,
            'professional_profiles': professional_profiles,
            'profile_name_map': profile_name_map,
            'profile_code_map': profile_code_map,
            'role_default_profile_code': role_default_profile_code,
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
    professional_profile_code: str | None = Form(default=None),
    tenant_id: str | None = Form(default=None),
    access_starts_at: str | None = Form(default=None),
    access_expires_at: str | None = Form(default=None),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    try:
        selected_role = RoleName(role)
    except ValueError:
        return RedirectResponse(url='/admin/users?updated=0&reason=bad_role', status_code=303)

    tenant_id_int: int | None = None
    if selected_role != RoleName.cloudon_admin and tenant_id:
        tenant_id_int = int(tenant_id)
    try:
        professional_profile_id = await _resolve_professional_profile_id(
            db,
            selected_role=selected_role,
            requested_profile_code=professional_profile_code,
        )
    except ValueError:
        return RedirectResponse(url='/admin/users?updated=0&reason=bad_profile', status_code=303)
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
        professional_profile_id=professional_profile_id,
        full_name=full_name.strip() or None,
        phone=phone.strip() or None,
        email=email,
        role=selected_role,
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
    professional_profiles = await _list_professional_profiles(db)
    user_default_profile_code = _default_profile_code_for_role(user.role)
    return templates.TemplateResponse(
        'admin/user_edit.html',
        {
            'request': request,
            'user': user,
            'tenants': tenants,
            'professional_profiles': professional_profiles,
            'user_default_profile_code': user_default_profile_code,
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
    professional_profile_code: str | None = Form(default=None),
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
    try:
        professional_profile_id = await _resolve_professional_profile_id(
            db,
            selected_role=selected_role,
            requested_profile_code=professional_profile_code,
        )
    except ValueError:
        return RedirectResponse(url=f'{redirect_target}?updated=0&reason=bad_profile', status_code=303)

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
    user.professional_profile_id = professional_profile_id
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
            **(await _tenant_navigation_context(tenant)),
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
            **(await _tenant_navigation_context(tenant)),
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
            **(await _tenant_navigation_context(tenant)),
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
            'active_page': 'settings_system_defaults',
            'title': 'title_settings_system_defaults',
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
    user: User = Depends(get_current_user),
):
    ui_persona = getattr(request.state, 'ui_persona', 'manager')
    if ui_persona == 'finance':
        return RedirectResponse(url='/tenant/finance-dashboard', status_code=302)
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'dashboard',
            'title': 'title_tenant_dashboard',
        },
    )


@router.get('/tenant/finance-dashboard', response_class=HTMLResponse)
async def tenant_finance_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user: User = Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/finance_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'finance_dashboard',
            'title': 'title_finance_dashboard',
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
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'sales',
            'title': 'title_sales_dashboard',
            'documents_mode': False,
        },
    )


@router.get('/tenant/sales-documents', response_class=HTMLResponse)
async def tenant_sales_documents_dashboard(
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
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'sales_documents',
            'title': 'title_sales_documents_dashboard',
            'documents_mode': True,
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
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'purchases',
            'title': 'title_purchases_dashboard',
        },
    )


@router.get('/tenant/purchase-documents', response_class=HTMLResponse)
async def tenant_purchase_documents_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/purchase_documents_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'purchase_documents',
            'title': 'title_purchases_documents_dashboard',
        },
    )


@router.get('/tenant/warehouse-documents', response_class=HTMLResponse)
async def tenant_warehouse_documents_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/warehouse_documents_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'warehouse_documents',
            'title': 'title_warehouse_documents_dashboard',
        },
    )


@router.get('/tenant/inventory', response_class=HTMLResponse)
async def tenant_inventory_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
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
            **(await _tenant_navigation_context(tenant)),
            'default_as_of': to_date,
            'active_page': 'items',
            'title': 'title_items_dashboard',
        },
    )


@router.get('/tenant/customers', response_class=HTMLResponse)
async def tenant_customers_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=365)
    return templates.TemplateResponse(
        'tenant/customers_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'hide_page_filters': True,
            'active_page': 'customers',
            'title': 'title_customers_dashboard',
        },
    )


@router.get('/tenant/suppliers', response_class=HTMLResponse)
async def tenant_suppliers_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=365)
    return templates.TemplateResponse(
        'tenant/suppliers_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'hide_page_filters': True,
            'active_page': 'suppliers',
            'title': 'title_suppliers_dashboard',
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
            **(await _tenant_navigation_context(tenant)),
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
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'default_target_margin': 35.0,
            'active_page': 'price_control',
            'title': 'title_price_control',
        },
    )


async def _render_tenant_cashflow_dashboard(
    request: Request,
    tenant: Tenant,
    raw_category: str | None,
):
    feature_flags = await _tenant_navigation_context(tenant)
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    normalized_category = _normalize_cashflow_category(raw_category)
    is_known_category = normalized_category in _CASHFLOW_CATEGORY_LABEL_KEY_MAP
    is_accounts_mode = normalized_category == 'financial_accounts'
    is_documents_mode = is_known_category and not is_accounts_mode
    template_name = (
        'tenant/cashflow_accounts_dashboard.html'
        if is_accounts_mode
        else ('tenant/cashflow_documents_dashboard.html' if is_documents_mode else 'tenant/cashflow_dashboard.html')
    )
    return templates.TemplateResponse(
        template_name,
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'default_from': from_date,
            'default_to': to_date,
            'cashflow_category': normalized_category if is_documents_mode else '',
            'cashflow_menu_category': normalized_category if is_known_category else '',
            'cashflow_category_label_key': _CASHFLOW_CATEGORY_LABEL_KEY_MAP.get(normalized_category, 'cash_transactions_menu'),
            'cashflow_documents_mode': is_documents_mode,
            'cashflow_accounts_mode': is_accounts_mode,
            'hide_page_filters': is_accounts_mode,
            'feature_locked': not feature_flags['cashflow_enabled'],
            'active_page': 'cashflow',
            'title': _CASHFLOW_CATEGORY_TITLE_KEY_MAP.get(normalized_category, 'title_cashflow_dashboard'),
        },
    )


@router.get('/tenant/cashflow', response_class=HTMLResponse)
async def tenant_cashflow_dashboard(
    request: Request,
    category: str | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    raw_category = category or request.query_params.get('category')
    return await _render_tenant_cashflow_dashboard(request=request, tenant=tenant, raw_category=raw_category)


@router.get('/tenant/cashflow/{category_slug}', response_class=HTMLResponse)
async def tenant_cashflow_dashboard_category(
    request: Request,
    category_slug: str,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_tenant_cashflow_dashboard(request=request, tenant=tenant, raw_category=category_slug)


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
    profile_code = getattr(request.state, 'professional_profile_code', None)
    initial_insights = _prioritize_insights_for_profile(initial_insights, profile_code)
    return templates.TemplateResponse(
        'tenant/insights.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
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


async def _render_tenant_compare_page(
    *,
    request: Request,
    tenant: Tenant,
    active_page: str,
    title: str,
    compare_mode: str,
) -> HTMLResponse:
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
            **(await _tenant_navigation_context(tenant)),
            'a_from': a_from,
            'a_to': a_to,
            'b_from': b_from,
            'b_to': b_to,
            'compare_mode': compare_mode,
            'active_page': active_page,
            'title': title,
        },
    )


@router.get('/tenant/compare', response_class=HTMLResponse)
async def tenant_compare_legacy(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_tenant_compare_page(
        request=request,
        tenant=tenant,
        active_page='compare_period',
        title='title_comparison_period',
        compare_mode='period_vs_period',
    )


@router.get('/tenant/comparisons/period-vs-period', response_class=HTMLResponse)
async def tenant_compare_period_vs_period(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_tenant_compare_page(
        request=request,
        tenant=tenant,
        active_page='compare_period',
        title='title_comparison_period',
        compare_mode='period_vs_period',
    )


@router.get('/tenant/comparisons/branch-vs-branch', response_class=HTMLResponse)
async def tenant_compare_branch_vs_branch(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_tenant_compare_page(
        request=request,
        tenant=tenant,
        active_page='compare_branch',
        title='title_comparison_branch',
        compare_mode='branch_vs_branch',
    )


@router.get('/tenant/comparisons/category-vs-category', response_class=HTMLResponse)
async def tenant_compare_category_vs_category(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_tenant_compare_page(
        request=request,
        tenant=tenant,
        active_page='compare_category',
        title='title_comparison_category',
        compare_mode='category_vs_category',
    )


@router.get('/tenant/analytics/receivables-payables', response_class=HTMLResponse)
async def tenant_analytics_receivables_payables(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return _render_tenant_menu_placeholder(
        request=request,
        tenant=tenant,
        nav_context=await _tenant_navigation_context(tenant),
        active_page='analytics_receivables_payables',
        title='title_analytics_receivables_payables',
        page_title_key='analytics_receivables_payables',
        page_description='Συγκεντρωτική προβολή απαιτήσεων πελατών και υποχρεώσεων προμηθευτών για οικονομική παρακολούθηση.',
    )


@router.get('/tenant/exports/reports', response_class=HTMLResponse)
async def tenant_exports_reports(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return _render_tenant_menu_placeholder(
        request=request,
        tenant=tenant,
        nav_context=await _tenant_navigation_context(tenant),
        active_page='exports_reports',
        title='title_exports_reports',
        page_title_key='reports',
        page_description='Κεντρική λίστα διαθέσιμων reports με φίλτρα, εκτέλεση και ιστορικό εξαγωγών.',
    )


@router.get('/tenant/exports/csv-excel', response_class=HTMLResponse)
async def tenant_exports_csv_excel(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return _render_tenant_menu_placeholder(
        request=request,
        tenant=tenant,
        nav_context=await _tenant_navigation_context(tenant),
        active_page='exports_csv_excel',
        title='title_exports_csv_excel',
        page_title_key='csv_excel',
        page_description='Εξαγωγές CSV/Excel από aggregates και operational streams με συμβατή δομή για downstream χρήση.',
    )
