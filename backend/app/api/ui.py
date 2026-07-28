import secrets
import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo
from xml.sax.saxutils import escape as xml_escape

import httpx
import paramiko
from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from redis import Redis
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from app.api.deps import get_current_user, get_request_tenant, get_tenant_db, require_roles
from app.core.celery_sender import make_celery_sender
from app.core.config import app_version_detailed, settings
from app.core.help_content import circuit_for_lang as help_circuit_for_lang
from app.core.help_content import circuit_groups as help_circuit_groups
from app.core.help_content import circuits_by_id_for_lang as help_circuits_by_id
from app.core.help_content import circuits_for_lang as help_circuits
from app.core.help_content import faq_for_lang as help_faq
from app.core.help_content import kpis_for_circuit as help_kpis_for_circuit
from app.core.help_content import shot_url as help_shot_url
from app.core.help_content import task_groups_for_lang as help_task_groups
from app.core.i18n import normalize_lang, tt
from app.core.kpi_catalog import catalog_by_circuit as kpi_catalog_by_circuit
from app.core.kpi_catalog import catalog_for_lang as kpi_catalog_for_lang
from app.core.kpi_catalog import default_help as kpi_default_help
from app.core.security import (
    audience_for_role,
    create_access_token,
    create_refresh_token,
    expected_audience_for_host,
    get_password_hash,
    safe_decode,
    verify_password,
)
from app.db.control_session import ControlSessionLocal, get_control_db
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import (
    AuditLog,
    GlobalRuleEntry,
    GlobalRuleSet,
    OperationalStream,
    OverrideMode,
    Plan,
    PlanFeature,
    PlanFeatureCatalog,
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
    Invoice,
    Payment,
    RefreshToken,
    SubscriptionLimit,
    WhmcsService,
)
from app.models.tenant import DimBranch, Insight, ReplenishmentDataQualityIssue, ReplenishmentLine, ReplenishmentSnapshot
from app.services.intelligence_service import (
    insights_counts_by_severity,
    list_insights as list_tenant_insights,
    list_rules as list_tenant_rules,
    update_rule as update_tenant_rule,
)
from app.services.connection_secrets import (
    SqlServerSecret,
    build_odbc_connection_string,
    decrypt_json_secret,
    decrypt_sqlserver_secret,
    encrypt_json_secret,
    encrypt_sqlserver_secret,
)
from app.services.era_exploration import (
    clear_era_exploration_cache,
    DuplicateMarketImportError as EraDuplicateMarketImportError,
    era_period_from_filename,
    file_sha256 as era_file_sha256,
    import_era_exploration_file,
    validate_era_exploration_file,
)
from app.services.email_delivery import send_email, send_tenant_welcome_email, send_user_invite_email
from app.services.iqvia import (
    clear_iqvia_cache,
    DuplicateMarketImportError as IqviaDuplicateMarketImportError,
    file_sha256 as iqvia_file_sha256,
    import_iqvia_file,
    iqvia_period_from_filename,
    validate_iqvia_file,
)
from app.services.kpi_cache import get_or_set_cache
from app.services.replenishment import (
    FNR_OUTPUT_COLUMNS,
    build_availability_foundation,
    build_availability_brief_from_facts,
    build_destocking_brief_from_facts,
    build_fnr_excel_from_facts,
    build_replenishment_from_facts,
)
from app.services.supplier_orders import (
    SupplierOrdersFilters,
    build_supplier_orders_dashboard,
    normalize_supplier_order_settings,
    supplier_order_settings_for_tenant,
)
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
from app.services.ingestion.queueing import close_ingest_circuit
from app.services.ingestion import enqueue_tenant_job
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, STREAM_TO_ENTITY, normalize_stream_name, normalize_stream_values
from app.services.ingestion.chunking import stream_chunk_days
from app.services.ingestion.progress import begin_ingest_progress, clear_ingest_progress, get_ingest_progress, queue_depth, update_ingest_progress
from app.services.ingestion.queueing import (
    priority_pool_snapshot,
    tenant_delete_active_key,
    tenant_lock_name,
    tenant_queue_name,
    tenant_stop_key,
    tenant_throttle_key,
)
from app.services.ingestion.sync_planner import plan_tenant_sync_jobs
from app.services.kpi_cache import invalidate_tenant_cache
from app.services.querypacks import apply_querypack_to_connection, load_querypack
from app.services.subscriptions import apply_subscription_time_transitions, get_or_create_subscription, sync_tenant_from_subscription
from app.services.provisioning_wizard import _drop_db_and_role
from app.services.subscription_features import (
    ADD_ON_FEATURE_KEYS,
    SubscriptionFeature,
    SUBSCRIPTION_FEATURES,
    addon_allowed_for_plan,
    infer_subscription_feature_defaults,
    normalize_subscription_feature_flags,
)
from app.services.kpi_queries import (
    export_filter_options,
    export_item_rows,
    export_item_totals,
    sales_by_channel,
    sales_comparison_by_group,
    sales_pivot,
    normalize_document_series_labels_config,
    normalize_eshop_fulfillment_config,
    normalize_price_margin_targets_config,
)
from app.services.kpi_participation_scope import (
    reset_current_sales_kpi_participation_config,
    set_current_sales_kpi_participation_config,
)
from app.services.rule_config import resolve_rule_payload as _resolve_rule_payload

router = APIRouter(tags=['ui'])
templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parents[1] / 'templates'),
    context_processors=[lambda request: {
        'tt': tt,
        'app_version': settings.app_version,
        #  Callable, not a value: the dirty marker has to be evaluated per render.
        'app_version_detailed': app_version_detailed,
        'project_name': settings.project_name,
    }],
)
templates.env.globals.setdefault('tt', tt)
templates.env.globals.setdefault('app_version', settings.app_version)
templates.env.globals.setdefault('app_version_detailed', app_version_detailed)
templates.env.globals.setdefault('project_name', settings.project_name)
celery_client = make_celery_sender('ui_sender')
logger = logging.getLogger(__name__)
GREECE_TZ = ZoneInfo('Europe/Athens')

_CONTROL_ENV_PATH = Path(__file__).resolve().parents[3] / '.env'
_MAIL_ENV_KEYS = (
    'SMTP_HOST',
    'SMTP_PORT',
    'SMTP_USERNAME',
    'SMTP_PASSWORD',
    'SMTP_FROM_EMAIL',
    'SMTP_FROM_NAME',
    'SMTP_USE_TLS',
    'APP_PUBLIC_BASE_URL',
)
_INFRASTRUCTURE_TENANT_SLUGS = {'startdb', 'rddb'}
_INFRASTRUCTURE_TENANT_SOURCES = {'system', 'template', 'infrastructure', 'internal'}


def _is_infrastructure_tenant(tenant: Tenant | None) -> bool:
    if tenant is None:
        return False
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    source = str(tenant.source or '').strip().lower()
    slug = str(tenant.slug or '').strip().lower()
    db_name = str(tenant.db_name or '').strip()
    if bool(flags.get('system_infrastructure')):
        return True
    if source in _INFRASTRUCTURE_TENANT_SOURCES:
        return True
    if slug in _INFRASTRUCTURE_TENANT_SLUGS:
        return True
    if db_name and not db_name.startswith('bi_tenant_'):
        return True
    return False


def _parse_env_value(raw_value: str) -> str:
    value = str(raw_value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
        if raw_value.strip().startswith('"'):
            value = value.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
    return value


def _read_control_env(path: Path = _CONTROL_ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding='utf-8').splitlines()
    except FileNotFoundError:
        return values
    except Exception:
        logger.exception('control_env_read_failed', extra={'path': str(path)})
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in line:
            continue
        key, raw_value = line.split('=', 1)
        key = key.strip()
        if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key):
            values[key] = _parse_env_value(raw_value)
    return values


def _env_format_value(value: str) -> str:
    text_value = str(value or '')
    if re.fullmatch(r'[A-Za-z0-9_@./:+-]*', text_value):
        return text_value if text_value else '""'
    escaped = text_value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
    return f'"{escaped}"'


def _write_control_env(updates: dict[str, str], path: Path = _CONTROL_ENV_PATH) -> None:
    existing_lines: list[str]
    try:
        existing_lines = path.read_text(encoding='utf-8').splitlines()
    except FileNotFoundError:
        existing_lines = []

    pending = dict(updates)
    out: list[str] = []
    for line in existing_lines:
        if '=' not in line or line.lstrip().startswith('#'):
            out.append(line)
            continue
        key, _raw_value = line.split('=', 1)
        clean_key = key.strip()
        if clean_key in pending:
            out.append(f'{clean_key}={_env_format_value(pending.pop(clean_key))}')
        else:
            out.append(line)

    if pending:
        if out and out[-1].strip():
            out.append('')
        out.append('# Mail server settings managed from /admin/settings/mail-server')
        for key in _MAIL_ENV_KEYS:
            if key in pending:
                out.append(f'{key}={_env_format_value(pending.pop(key))}')
        for key in sorted(pending):
            out.append(f'{key}={_env_format_value(pending[key])}')

    path.write_text('\n'.join(out).rstrip() + '\n', encoding='utf-8')


def _smtp_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or '').strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _apply_runtime_mail_settings(values: dict[str, str]) -> None:
    settings.smtp_host = values.get('SMTP_HOST', '')
    settings.smtp_port = int(values.get('SMTP_PORT') or 587)
    settings.smtp_username = values.get('SMTP_USERNAME', '')
    settings.smtp_password = values.get('SMTP_PASSWORD', '')
    settings.smtp_from_email = values.get('SMTP_FROM_EMAIL', '')
    settings.smtp_from_name = values.get('SMTP_FROM_NAME', 'CloudOn BI')
    settings.smtp_use_tls = _smtp_bool(values.get('SMTP_USE_TLS', 'true'))
    settings.app_public_base_url = values.get('APP_PUBLIC_BASE_URL', '')


def _current_mail_settings() -> dict[str, object]:
    env_values = _read_control_env()
    password = env_values.get('SMTP_PASSWORD', settings.smtp_password)
    return {
        'smtp_host': env_values.get('SMTP_HOST', settings.smtp_host),
        'smtp_port': env_values.get('SMTP_PORT', str(settings.smtp_port or 587)),
        'smtp_username': env_values.get('SMTP_USERNAME', settings.smtp_username),
        'smtp_from_email': env_values.get('SMTP_FROM_EMAIL', settings.smtp_from_email),
        'smtp_from_name': env_values.get('SMTP_FROM_NAME', settings.smtp_from_name),
        'smtp_use_tls': _smtp_bool(env_values.get('SMTP_USE_TLS', settings.smtp_use_tls)),
        'app_public_base_url': env_values.get('APP_PUBLIC_BASE_URL', settings.app_public_base_url),
        'has_password': bool(password),
        'env_path': str(_CONTROL_ENV_PATH),
        'configured': bool(env_values.get('SMTP_HOST', settings.smtp_host))
        and bool(env_values.get('SMTP_FROM_EMAIL', settings.smtp_from_email) or env_values.get('SMTP_USERNAME', settings.smtp_username)),
    }


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


def _admin_dashboard_redirect(host: str | None = None) -> str:
    admin_path = '/admin/dashboard'
    if host and host.lower() != settings.admin_portal_host.lower():
        return f'https://{settings.admin_portal_host}{admin_path}'
    return admin_path


def _request_client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get('x-forwarded-for') or '').split(',')[0].strip()
    if forwarded_for:
        return forwarded_for
    return request.client.host if request.client else ''


def _request_user_agent(request: Request) -> str:
    return (request.headers.get('user-agent') or '')[:255]


def _preempt_tenant_tasks(
    tenant_slug: str,
    *,
    task_names: set[str] | None = None,
) -> dict[str, int]:
    revoked = {"active": 0, "reserved": 0, "scheduled": 0}
    target_names = task_names or {
        "worker.tasks.generate_insights_for_tenant",
    }
    try:
        inspector = celery_client.control.inspect(timeout=1.0)
        active = inspector.active() or {}
        reserved = inspector.reserved() or {}
        scheduled = inspector.scheduled() or {}

        def _matches(task: dict) -> bool:
            if str(task.get("name") or "") not in target_names:
                return False
            kwargs = task.get("kwargs") or {}
            if isinstance(kwargs, dict):
                return str(kwargs.get("tenant_slug") or "") == tenant_slug
            return tenant_slug in str(kwargs)

        for tasks in active.values():
            for task in tasks or []:
                if _matches(task):
                    task_id = str(task.get("id") or "").strip()
                    if task_id:
                        celery_client.control.revoke(task_id, terminate=True, signal="SIGKILL")
                        revoked["active"] += 1

        for tasks in reserved.values():
            for task in tasks or []:
                if _matches(task):
                    task_id = str(task.get("id") or "").strip()
                    if task_id:
                        celery_client.control.revoke(task_id)
                        revoked["reserved"] += 1

        for tasks in scheduled.values():
            for entry in tasks or []:
                request = (entry or {}).get("request") or {}
                if _matches(request):
                    task_id = str(request.get("id") or "").strip()
                    if task_id:
                        celery_client.control.revoke(task_id)
                        revoked["scheduled"] += 1
    except Exception:
        logger.exception("Failed to preempt tenant tasks", extra={"tenant_slug": tenant_slug, "task_names": sorted(target_names)})
    return revoked


def _preempt_tenant_insight_tasks(tenant_slug: str) -> dict[str, int]:
    return _preempt_tenant_tasks(
        tenant_slug,
        task_names={"worker.tasks.generate_insights_for_tenant"},
    )


def _login_redirect_for(user: User, host: str | None = None, profile_code: str | None = None) -> str:
    if user.role == RoleName.cloudon_admin:
        return _admin_dashboard_redirect(host)
    if host and host.lower() == settings.admin_portal_host.lower():
        return _admin_dashboard_redirect(host)
    return _dashboard_redirect_for_profile_code(profile_code, user.role)


def _tenant_slug_from_host(host: str | None) -> str | None:
    host_only = str(host or '').split(':')[0].lower()
    root = str(settings.tenant_domain_root or '').strip('.').lower()
    if root and host_only.endswith(f'.{root}'):
        slug = host_only[: -(len(root) + 1)]
        return slug or None
    return None


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


_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS = {
    'status_source': 'active_available',
    'active_last_sale_days': 60,
    'movement_window_days': 30,
    'inventory_scope_sold_days': 120,
    'fast_sales_qty_30d_min': 50,
    'slow_sales_qty_30d_max': 5,
}

_DEFAULT_AUTO_SYNC_SETTINGS = {
    'enabled': False,
    'interval_minutes': max(1, int(getattr(settings, 'incremental_sync_interval_minutes', 5) or 5)),
    'profile': 'live',
    'overlap_minutes': max(0, int(getattr(settings, 'incremental_sync_overlap_minutes', 5) or 5)),
    'recovery_days': max(1, int(getattr(settings, 'ingest_daily_recovery_days', 7) or 7)),
    'business_hours': {
        'mode': 'always',
        'start': '08:00',
        'end': '22:00',
        'timezone': 'Europe/Athens',
    },
    'stream_overrides': {},
}

_DEFAULT_DAILY_RECONCILIATION_SETTINGS = {
    'enabled': False,
    'time': '23:30',
    'timezone': 'Europe/Athens',
    'lookback_days': 1,
    'streams': [
        'sales_documents',
        'purchase_documents',
        'inventory_documents',
        'cash_transactions',
        'operating_expenses',
    ],
}

_DEFAULT_DUPLICATE_PROTECTION_SETTINGS = {
    'enabled': True,
    'mode': 'natural_key',
}

_DEFAULT_TENANT_CONTACT_SETTINGS = {
    'contact_person': '',
    'contact_email': '',
    'contact_phone': '',
    'contact_mobile': '',
    'billing_email': '',
    'technical_email': '',
    'address': '',
    'city': '',
    'postal_code': '',
    'afm': '',
    'doy': '',
    'notes': '',
}

_SYNC_PROFILE_DEFAULTS = {
    'live': {'interval_minutes': 1, 'overlap_minutes': 5, 'recovery_days': 1},
    'daily': {'interval_minutes': 1440, 'overlap_minutes': 10, 'recovery_days': 1},
    'previous_day': {'interval_minutes': 1440, 'overlap_minutes': 0, 'recovery_days': 1},
    'weekly': {'interval_minutes': 10080, 'overlap_minutes': 0, 'recovery_days': 7},
    'monthly': {'interval_minutes': 43200, 'overlap_minutes': 0, 'recovery_days': 31},
    'custom': {'interval_minutes': int(_DEFAULT_AUTO_SYNC_SETTINGS['interval_minutes']), 'overlap_minutes': int(_DEFAULT_AUTO_SYNC_SETTINGS['overlap_minutes']), 'recovery_days': int(_DEFAULT_AUTO_SYNC_SETTINGS['recovery_days'])},
}

_SYNC_STREAM_LABELS = {
    'sales_documents': 'Πωλήσεις',
    'purchase_documents': 'Αγορές',
    'inventory_documents': 'Αποθήκη',
    'cash_transactions': 'Ταμειακά',
    'supplier_balances': 'Υπόλοιπα προμηθευτών',
    'customer_balances': 'Υπόλοιπα πελατών',
    'operating_expenses': 'Έξοδα',
    'supplier_orders': 'Παραγγελίες προμηθευτών',
}

_DEFAULT_3CX_FQDN = ''
_DEFAULT_3CX_USERNAME = ''
_3CX_CONNECTOR_TYPE = '3cx_call_center'
_DEFAULT_3CX_AGENT_DIRECTORY_TEXT = ''
_DEFAULT_3CX_QUEUE_DIRECTORY_TEXT = ''
_DEFAULT_3CX_SSH_HOST = '195.201.136.200'
_DEFAULT_3CX_SSH_PORT = 22
_DEFAULT_3CX_SSH_USER = 'root'
_DEFAULT_3CX_SSH_KEY_PATH = '/opt/cloudon-bi/.secrets/3cx_id3_ed25519'
_DEFAULT_3CX_DB_NAME = 'database_single'
_DEFAULT_3CX_DB_SYNC_DAYS = 45


def _parse_int_in_range(raw: object, *, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(str(raw).strip())
    except Exception:
        parsed = int(default)
    return max(min_value, min(max_value, parsed))


def _parse_3cx_directory_text(raw: object) -> dict[str, str]:
    directory: dict[str, str] = {}
    for line in str(raw or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            directory[code_clean] = label_clean
    return directory


def _extract_3cx_extension_code(value: object) -> str:
    text_value = str(value or '').strip()
    if not text_value:
        return ''
    paren_match = re.search(r'\(([A-Za-z0-9]+)\)\s*$', text_value)
    if paren_match:
        return paren_match.group(1).strip()
    token_match = re.search(r'\b(GRP[A-Za-z0-9]*|\d{3,4})\b', text_value)
    return token_match.group(1).strip() if token_match else text_value


def _3cx_internal_extension_code(value: object) -> str:
    text_value = str(value or '').strip()
    if not text_value:
        return ''
    extension_code = _extract_3cx_extension_code(text_value)
    if re.fullmatch(r'\d{2,5}', extension_code):
        return extension_code
    if re.fullmatch(r'GRP[A-Za-z0-9]*', extension_code, flags=re.IGNORECASE):
        return extension_code
    return ''


def _3cx_has_external_endpoint(*values: object) -> bool:
    for value in values:
        text_value = str(value or '').strip()
        if not text_value or text_value in {'-', 'Άγνωστη πηγή', 'Χωρίς agent', 'Χωρίς queue'}:
            continue
        digits = re.sub(r'\D+', '', text_value)
        if len(digits) >= 6:
            return True
        if not _3cx_internal_extension_code(text_value):
            return True
    return False


def _3cx_external_endpoint(value: object) -> str:
    text_value = str(value or '').strip()
    if not text_value:
        return ''
    digits = re.sub(r'\D+', '', text_value)
    if len(digits) >= 6 and not re.search(r'\(\d{2,5}\)\s*$', text_value):
        return text_value
    if text_value and not _3cx_internal_extension_code(text_value):
        return text_value
    return ''


def _3cx_is_queue_endpoint(value: object) -> bool:
    text_value = str(value or '').strip()
    extension_code = _3cx_internal_extension_code(text_value)
    return bool(
        re.fullmatch(r'8\d{2}', extension_code)
        or extension_code.upper().startswith('GRP')
        or 'queue' in text_value.lower()
        or 'call center' in text_value.lower()
        or 'digital receptionist' in text_value.lower()
    )


def _3cx_is_auto_answer_endpoint(value: object, *directories: dict[str, str]) -> bool:
    text_value = str(value or '').strip().lower()
    if 'digital receptionist' in text_value:
        return True
    extension_code = _extract_3cx_extension_code(value)
    for directory in directories:
        if not isinstance(directory, dict):
            continue
        if extension_code and 'digital receptionist' in str(directory.get(extension_code) or '').strip().lower():
            return True
        for code, label in directory.items():
            if str(code or '').strip().lower() == str(value or '').strip().lower():
                return 'digital receptionist' in str(label or '').strip().lower()
            if str(label or '').strip().lower() == text_value:
                return 'digital receptionist' in str(label or '').strip().lower()
    return False


def _3cx_activity_ended_by(activity: object) -> str:
    text_value = str(activity or '').strip()
    if not text_value:
        return ''
    match = re.search(r'Ended by\s+(.+?)(?:$|→)', text_value, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ''


def _is_3cx_internal_only_call(
    *,
    source_number: object,
    destination_value: object,
    extension: object,
    agent: object,
    queue: object,
    did_number: object,
) -> bool:
    if _3cx_has_external_endpoint(source_number, did_number):
        return False
    source_internal = _3cx_internal_extension_code(source_number)
    destination_internal = (
        _3cx_internal_extension_code(destination_value)
        or _3cx_internal_extension_code(extension)
        or _3cx_internal_extension_code(agent)
        or _3cx_internal_extension_code(queue)
    )
    return bool(source_internal and destination_internal)


def _apply_3cx_agent_directory(
    agent_rows: object,
    directory: dict[str, str],
    *,
    target_calls_per_agent: int = 60,
) -> list[dict[str, object]]:
    if not isinstance(agent_rows, list):
        return []
    grouped: dict[str, dict[str, object]] = {}
    for row in agent_rows:
        if not isinstance(row, dict):
            continue
        extension_code = _extract_3cx_extension_code(row.get('extension') or row.get('agent'))
        display_name = directory.get(extension_code) if directory else ''
        if directory and not display_name:
            continue
        clean_row = dict(row)
        clean_row['extension'] = extension_code
        if display_name:
            clean_row['agent'] = display_name
        key = extension_code or str(clean_row.get('agent') or '')
        bucket = grouped.setdefault(
            key,
            {
                'agent': clean_row.get('agent') or 'Χωρίς agent',
                'extension': extension_code,
                'answered': 0,
                'outbound': 0,
                'inbound_answered': 0,
                'outbound_answered': 0,
                'inbound_missed': 0,
                'outbound_missed': 0,
                'inbound': 0,
                'missed': 0,
                'talk_seconds': 0,
                'calls': 0,
                'last_date': '',
                'recent': [],
            },
        )
        bucket['agent'] = clean_row.get('agent') or bucket['agent']
        bucket['extension'] = extension_code or bucket.get('extension') or ''
        for field in (
            'answered',
            'outbound',
            'inbound',
            'missed',
            'talk_seconds',
            'calls',
            'inbound_answered',
            'outbound_answered',
            'inbound_missed',
            'outbound_missed',
        ):
            bucket[field] = int(bucket.get(field) or 0) + int(clean_row.get(field) or 0)
        clean_last_date = str(clean_row.get('last_date') or clean_row.get('last_call') or '').strip()
        if clean_last_date:
            bucket['last_date'] = max(str(bucket.get('last_date') or ''), clean_last_date)
        if isinstance(clean_row.get('recent'), list):
            recent_rows = bucket.get('recent') if isinstance(bucket.get('recent'), list) else []
            recent_rows.extend([item for item in clean_row['recent'] if isinstance(item, dict)])
            recent_rows = sorted(recent_rows, key=lambda item: str(item.get('date') or ''))[-8:]
            bucket['recent'] = recent_rows
            if recent_rows:
                bucket['last_date'] = max(str(bucket.get('last_date') or ''), str(recent_rows[-1].get('date') or ''))
    out = list(grouped.values())
    for row in out:
        calls = max(1, int(row.get('calls') or 0))
        row['avg_talk_seconds'] = int(int(row.get('talk_seconds') or 0) / calls)
        row['target_pct'] = round((int(row.get('answered') or 0) / max(1, target_calls_per_agent)) * 100, 1)
    return sorted(out, key=lambda item: int(item.get('answered') or 0) + int(item.get('outbound') or 0), reverse=True)


def _apply_3cx_queue_directory(queue_rows: object, directory: dict[str, str]) -> list[dict[str, object]]:
    if not isinstance(queue_rows, list):
        return []
    out: list[dict[str, object]] = []
    for row in queue_rows:
        if not isinstance(row, dict):
            continue
        clean_row = dict(row)
        queue_code = _extract_3cx_extension_code(clean_row.get('queue'))
        if queue_code in directory:
            clean_row['queue'] = directory[queue_code]
            clean_row['extension'] = queue_code
        out.append(clean_row)
    return out


def _3cx_directory_rows(directory: dict[str, str]) -> list[dict[str, str]]:
    return [{'extension': extension, 'name': name} for extension, name in directory.items()]


def _format_3cx_directory_text(directory: dict[str, str]) -> str:
    return '\n'.join(f'{code}={label}' for code, label in directory.items() if code and label)


def _xlsx_col_name(index: int) -> str:
    name = ''
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _xlsx_cell_xml(row_index: int, col_index: int, value: object, *, style_id: int = 0) -> str:
    ref = f'{_xlsx_col_name(col_index)}{row_index}'
    style_attr = f' s="{style_id}"' if style_id else ''
    if isinstance(value, bool):
        return f'<c r="{ref}"{style_attr} t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{style_attr}><v>{value}</v></c>'
    text_value = xml_escape(str(value if value is not None else ''))
    return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{text_value}</t></is></c>'


def _build_xlsx_bytes(
    *,
    sheet_name: str,
    headers: list[str],
    rows: list[list[object]],
    column_widths: list[float] | None = None,
    title: str | None = None,
) -> bytes:
    safe_sheet_name = str(sheet_name or 'Sheet1')[:31] or 'Sheet1'
    sheet_rows: list[str] = []
    offset = 1 if title else 0
    if title:
        sheet_rows.append(f'<row r="1">{_xlsx_cell_xml(1, 1, title, style_id=1)}</row>')
    all_rows = [headers] + rows
    for i, row_values in enumerate(all_rows):
        row_index = i + 1 + offset
        is_header = i == 0
        cells = ''.join(
            _xlsx_cell_xml(
                row_index,
                col_index,
                value,
                style_id=1 if is_header else (2 if isinstance(value, (int, float)) and not isinstance(value, bool) else 3),
            )
            for col_index, value in enumerate(row_values, start=1)
        )
        sheet_rows.append(f'<row r="{row_index}">{cells}</row>')
    header_row = 1 + offset
    total_rows = len(all_rows) + offset
    dimension = f'A1:{_xlsx_col_name(max(1, len(headers)))}{max(1, total_rows)}'
    widths = column_widths or []
    cols_xml = ''.join(
        f'<col min="{idx}" max="{idx}" width="{max(8.0, float(width)):.1f}" customWidth="1"/>'
        for idx, width in enumerate(widths[: len(headers)], start=1)
    )
    auto_filter_ref = f'A{header_row}:{_xlsx_col_name(max(1, len(headers)))}{header_row}'
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<dimension ref="{dimension}"/>'
        f'<sheetViews><sheetView workbookViewId="0"><pane ySplit="{header_row}" topLeftCell="A{header_row + 1}" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="18"/>'
        f'<cols>{cols_xml}</cols>'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        f'<autoFilter ref="{auto_filter_ref}"/>'
        '<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/>'
        '</worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        f'<sheet name="{xml_escape(safe_sheet_name)}" sheetId="1" r:id="rId1"/>'
        '</sheets>'
        '</workbook>'
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="4">'
        '<font><sz val="11"/><color rgb="FF111827"/><name val="Calibri"/></font>'
        '<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>'
        '<font><sz val="11"/><color rgb="FF111827"/><name val="Calibri"/></font>'
        '<font><sz val="11"/><color rgb="FF111827"/><name val="Calibri"/></font>'
        '</fonts>'
        '<fills count="3">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FF1D4ED8"/><bgColor indexed="64"/></patternFill></fill>'
        '</fills>'
        '<borders count="2">'
        '<border><left/><right/><top/><bottom/><diagonal/></border>'
        '<border><left style="thin"><color rgb="FFD9E2F3"/></left><right style="thin"><color rgb="FFD9E2F3"/></right><top style="thin"><color rgb="FFD9E2F3"/></top><bottom style="thin"><color rgb="FFD9E2F3"/></bottom><diagonal/></border>'
        '</borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="4">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>'
        '<xf numFmtId="0" fontId="2" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>'
        '<xf numFmtId="0" fontId="3" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '<dxfs count="0"/>'
        '<tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/>'
        '</styleSheet>'
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        '</Relationships>'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml', content_types_xml)
        archive.writestr('_rels/.rels', root_rels_xml)
        archive.writestr('xl/workbook.xml', workbook_xml)
        archive.writestr('xl/_rels/workbook.xml.rels', workbook_rels_xml)
        archive.writestr('xl/styles.xml', styles_xml)
        archive.writestr('xl/worksheets/sheet1.xml', sheet_xml)
    return buffer.getvalue()


def _merge_3cx_directories(*directories: dict[str, str]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for directory in directories:
        if not isinstance(directory, dict):
            continue
        for code, label in directory.items():
            code_clean = str(code or '').strip()
            label_clean = str(label or '').strip()
            if code_clean and label_clean:
                merged[code_clean] = label_clean
    return merged


def _label_3cx_endpoint(value: object, agent_directory: dict[str, str], queue_directory: dict[str, str]) -> str:
    text_value = str(value or '').strip()
    if not text_value:
        return ''
    extension_code = _extract_3cx_extension_code(text_value)
    if extension_code in queue_directory:
        return f'{queue_directory[extension_code]} ({extension_code})'
    if extension_code in agent_directory:
        return f'{agent_directory[extension_code]} ({extension_code})'
    return text_value


def _label_3cx_call_rows(
    rows: object,
    *,
    agent_directory: dict[str, str],
    queue_directory: dict[str, str],
) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    out: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean_row = dict(row)
        for field in ('queue', 'did'):
            clean_row[field] = _label_3cx_endpoint(clean_row.get(field), agent_directory, queue_directory)
        agent_label = _label_3cx_endpoint(clean_row.get('agent'), agent_directory, queue_directory)
        if agent_label:
            clean_row['agent'] = agent_label
        # Collapse the raw ring path into one entry per extension (3CX ring-all re-rings the
        # same DN many times). answered=True if that extension ever picked up; ring time = max.
        ring_path = clean_row.get('ring_path') if isinstance(clean_row.get('ring_path'), list) else []
        by_ext: dict[str, dict[str, object]] = {}
        order: list[str] = []
        for leg in ring_path:
            if not isinstance(leg, dict):
                continue
            key = str(leg.get('ext_number') or leg.get('ext') or '').strip()
            if not key:
                continue
            if key not in by_ext:
                by_ext[key] = {'ext': leg.get('ext') or key, 'ext_number': leg.get('ext_number') or '', 'answered': False, 'ring_seconds': 0}
                order.append(key)
            entry = by_ext[key]
            if leg.get('answered'):
                entry['answered'] = True
            try:
                entry['ring_seconds'] = max(int(entry['ring_seconds']), int(leg.get('ring_seconds') or 0))
            except (TypeError, ValueError):
                pass
        rang_at = [by_ext[k] for k in order]
        clean_row['rang_at'] = rang_at
        clean_row['answered_by'] = next((entry for entry in rang_at if entry.get('answered')), None)
        out.append(clean_row)
    return out


def _label_3cx_journey_rows(
    rows: object,
    *,
    agent_directory: dict[str, str],
    queue_directory: dict[str, str],
) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    out: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean_row = dict(row)
        clean_row['from'] = _label_3cx_endpoint(clean_row.get('from'), agent_directory, queue_directory)
        clean_row['to'] = _label_3cx_endpoint(clean_row.get('to'), agent_directory, queue_directory)
        out.append(clean_row)
    return out


def _safe_tenant_3cx_return_url(raw: object, *, default: str = '/tenant/settings') -> str:
    value = str(raw or '').strip() or default
    if value.startswith('/tenant/call-center') or value.startswith('/tenant/settings'):
        return value
    return default


def _tenant_3cx_redirect(raw_return_to: object, **params: object) -> RedirectResponse:
    target = _safe_tenant_3cx_return_url(raw_return_to)
    query = urlencode({key: str(value) for key, value in params.items() if value is not None and str(value) != ''})
    if query:
        target = f'{target}{"&" if "?" in target else "?"}{query}'
    return RedirectResponse(url=target, status_code=303)


def _parse_bool_enabled(raw: object, default: bool = True) -> bool:
    if raw is None:
        return bool(default)
    if isinstance(raw, str):
        return raw.strip().lower() not in {'0', 'false', 'no', 'off', 'disabled'}
    return bool(raw)


def _json_safe_payload(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_payload(item) for item in value]
    return value


def _summarize_3cx_audit_payload(value: object) -> dict[str, object]:
    payload = value if isinstance(value, dict) else {}
    manual_import = payload.get('manual_import') if isinstance(payload.get('manual_import'), dict) else {}
    stats = payload.get('stats') if isinstance(payload.get('stats'), dict) else {}
    if not stats and isinstance(manual_import.get('stats'), dict):
        stats = manual_import.get('stats') or {}
    return {
        'configured': bool(payload.get('configured') or payload.get('fqdn') or manual_import),
        'source_mode': str(payload.get('source_mode') or manual_import.get('source_mode') or ''),
        'rows': int(manual_import.get('rows') or payload.get('rows') or 0),
        'raw_rows': int(manual_import.get('raw_rows') or payload.get('raw_rows') or 0),
        'total_calls': int(stats.get('total_calls') or 0),
        'answered_calls': int(stats.get('answered_calls') or 0),
        'missed_calls': int(stats.get('missed_calls') or 0),
        'polling_interval_minutes': payload.get('polling_interval_minutes'),
        'answer_sla_seconds': payload.get('answer_sla_seconds'),
        'target_calls_per_agent': payload.get('target_calls_per_agent'),
        'last_sync_at': _json_safe_payload(payload.get('last_sync_at')),
        'last_test_ok_at': _json_safe_payload(payload.get('last_test_ok_at')),
    }


def _tenant_contact_settings(tenant: Tenant | None) -> dict[str, str]:
    flags = tenant.feature_flags if tenant is not None else None
    source: dict[str, object] = {}
    if isinstance(flags, dict):
        cfg = flags.get('contact')
        if isinstance(cfg, dict):
            source = cfg
    out: dict[str, str] = {}
    for key, default_value in _DEFAULT_TENANT_CONTACT_SETTINGS.items():
        out[key] = str(source.get(key) or default_value or '').strip()
    return out


def _tenant_inventory_item_classification_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('inventory_item_classification_config')
        if not isinstance(cfg, dict):
            legacy = flags.get('inventory_item_classification')
            cfg = legacy if isinstance(legacy, dict) else None
        if isinstance(cfg, dict):
            source = cfg
    status_source_raw = str(source.get('status_source') or '').strip().lower()
    if status_source_raw in {'commercial', 'commercial_status', 'status'}:
        status_source = 'commercial'
    elif status_source_raw in {'active_available', 'active_stock_sales', 'softone_available'}:
        status_source = 'active_available'
    elif status_source_raw in {'active_status12', 'active_both_status', 'status12'}:
        status_source = 'active_status12'
    elif status_source_raw in {'softone', 'source', 'source_flag'}:
        status_source = 'softone'
    elif status_source_raw in {'sales_window', 'sales', 'window', 'recency'}:
        status_source = 'sales_window'
    else:
        status_source = 'active_available'
    active_days = _parse_int_in_range(
        source.get('active_last_sale_days'),
        default=_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS['active_last_sale_days'],
        min_value=1,
        max_value=3650,
    )
    movement_window_days = _parse_int_in_range(
        source.get('movement_window_days'),
        default=_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS['movement_window_days'],
        min_value=1,
        max_value=3650,
    )
    inventory_scope_sold_days = _parse_int_in_range(
        source.get('inventory_scope_sold_days'),
        default=_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS['inventory_scope_sold_days'],
        min_value=1,
        max_value=3650,
    )
    fast_min = _parse_int_in_range(
        source.get('fast_sales_qty_30d_min'),
        default=_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS['fast_sales_qty_30d_min'],
        min_value=1,
        max_value=1_000_000,
    )
    slow_max = _parse_int_in_range(
        source.get('slow_sales_qty_30d_max'),
        default=_DEFAULT_INVENTORY_ITEM_CLASSIFICATION_SETTINGS['slow_sales_qty_30d_max'],
        min_value=0,
        max_value=1_000_000,
    )
    if slow_max >= fast_min:
        slow_max = max(0, fast_min - 1)
    return {
        'status_source': status_source,
        'active_last_sale_days': active_days,
        'movement_window_days': movement_window_days,
        'inventory_scope_sold_days': inventory_scope_sold_days,
        'fast_sales_qty_30d_min': fast_min,
        'slow_sales_qty_30d_max': slow_max,
    }


def _tenant_auto_sync_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('auto_sync')
        if isinstance(cfg, dict):
            source = cfg
    profile_raw = str(source.get('profile') or _DEFAULT_AUTO_SYNC_SETTINGS['profile']).strip().lower()
    profile = profile_raw if profile_raw in _SYNC_PROFILE_DEFAULTS else 'live'
    profile_defaults = _SYNC_PROFILE_DEFAULTS.get(profile, _SYNC_PROFILE_DEFAULTS['live'])
    enabled = _parse_bool_enabled(source.get('enabled'), bool(_DEFAULT_AUTO_SYNC_SETTINGS['enabled']))
    interval = _parse_int_in_range(
        source.get('interval_minutes'),
        default=int(profile_defaults['interval_minutes']),
        min_value=1,
        max_value=43200,
    )
    overlap = _parse_int_in_range(
        source.get('overlap_minutes'),
        default=int(profile_defaults['overlap_minutes']),
        min_value=0,
        max_value=1440,
    )
    recovery_days = _parse_int_in_range(
        source.get('recovery_days'),
        default=int(profile_defaults['recovery_days']),
        min_value=1,
        max_value=366,
    )
    raw_overrides = source.get('stream_overrides') if isinstance(source.get('stream_overrides'), dict) else {}
    raw_hours = source.get('business_hours') if isinstance(source.get('business_hours'), dict) else {}
    default_hours = _DEFAULT_AUTO_SYNC_SETTINGS['business_hours']
    business_mode_raw = str(raw_hours.get('mode') or default_hours['mode']).strip().lower()
    business_mode = business_mode_raw if business_mode_raw in {'always', 'business_hours'} else 'always'

    def _clean_hhmm(raw: object, fallback: str) -> str:
        text = str(raw or fallback).strip()[:5]
        if ':' not in text:
            text = fallback
        hour_text, minute_text = (text.split(':', 1) + ['00'])[:2]
        try:
            hour = max(0, min(23, int(hour_text)))
            minute = max(0, min(59, int(minute_text)))
        except Exception:
            hour, minute = [int(part) for part in fallback.split(':', 1)]
        return f'{hour:02d}:{minute:02d}'

    business_hours = {
        'mode': business_mode,
        'start': _clean_hhmm(raw_hours.get('start'), str(default_hours['start'])),
        'end': _clean_hhmm(raw_hours.get('end'), str(default_hours['end'])),
        'timezone': str(raw_hours.get('timezone') or default_hours['timezone']).strip() or 'Europe/Athens',
    }
    stream_overrides: dict[str, dict[str, object]] = {}
    for stream in ALL_OPERATIONAL_STREAMS:
        raw_stream = raw_overrides.get(stream) if isinstance(raw_overrides, dict) else {}
        if not isinstance(raw_stream, dict):
            raw_stream = {}
        stream_overrides[stream] = {
            'enabled': _parse_bool_enabled(raw_stream.get('enabled'), True),
            'interval_minutes': _parse_int_in_range(
                raw_stream.get('interval_minutes'),
                default=interval,
                min_value=1,
                max_value=43200,
            ),
            'overlap_minutes': _parse_int_in_range(
                raw_stream.get('overlap_minutes'),
                default=overlap,
                min_value=0,
                max_value=1440,
            ),
            'recovery_days': _parse_int_in_range(
                raw_stream.get('recovery_days'),
                default=recovery_days,
                min_value=1,
                max_value=366,
            ),
        }
    return {
        'enabled': enabled,
        'profile': profile,
        'interval_minutes': interval,
        'overlap_minutes': overlap,
        'recovery_days': recovery_days,
        'business_hours': business_hours,
        'stream_overrides': stream_overrides,
        'stream_labels': _SYNC_STREAM_LABELS,
    }


def _tenant_daily_reconciliation_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('daily_reconciliation')
        if isinstance(cfg, dict):
            source = cfg
    raw_time = str(source.get('time') or _DEFAULT_DAILY_RECONCILIATION_SETTINGS['time']).strip()[:5]
    if ':' not in raw_time:
        raw_time = str(_DEFAULT_DAILY_RECONCILIATION_SETTINGS['time'])
    hour_text, minute_text = (raw_time.split(':', 1) + ['00'])[:2]
    try:
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
    except Exception:
        hour, minute = 23, 30
    source_streams = source.get('streams') if isinstance(source.get('streams'), list) else []
    streams: list[str] = []
    for raw_stream in source_streams:
        stream = normalize_stream_name(raw_stream)
        if stream in ALL_OPERATIONAL_STREAMS and stream not in streams:
            streams.append(stream)
    if not streams:
        streams = list(_DEFAULT_DAILY_RECONCILIATION_SETTINGS['streams'])
    return {
        'enabled': _parse_bool_enabled(
            source.get('enabled'),
            bool(_DEFAULT_DAILY_RECONCILIATION_SETTINGS['enabled']),
        ),
        'time': f'{hour:02d}:{minute:02d}',
        'timezone': str(source.get('timezone') or _DEFAULT_DAILY_RECONCILIATION_SETTINGS['timezone']).strip() or 'Europe/Athens',
        'lookback_days': _parse_int_in_range(
            source.get('lookback_days'),
            default=int(_DEFAULT_DAILY_RECONCILIATION_SETTINGS['lookback_days']),
            min_value=1,
            max_value=366,
        ),
        'streams': streams,
        'stream_labels': _SYNC_STREAM_LABELS,
    }


def _tenant_duplicate_protection_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('duplicate_protection')
        if isinstance(cfg, dict):
            source = cfg
    mode_raw = str(source.get('mode') or _DEFAULT_DUPLICATE_PROTECTION_SETTINGS['mode']).strip().lower()
    mode = mode_raw if mode_raw in {'event_id', 'natural_key'} else 'natural_key'
    return {
        'enabled': _parse_bool_enabled(
            source.get('enabled'),
            bool(_DEFAULT_DUPLICATE_PROTECTION_SETTINGS['enabled']),
        ),
        'mode': mode,
    }


def _tenant_eshop_fulfillment_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('eshop_fulfillment')
        if isinstance(cfg, dict):
            source = cfg
        else:
            source = {'use_defaults': False}
    else:
        source = {'use_defaults': False}
    return normalize_eshop_fulfillment_config(source)


def _tenant_document_series_labels_settings(tenant: Tenant | None) -> dict[str, str]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('document_series_labels')
        if isinstance(cfg, dict):
            source = cfg
    return normalize_document_series_labels_config(source)


def _tenant_price_margin_targets_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('price_margin_targets')
        if isinstance(cfg, dict):
            source = cfg
    return normalize_price_margin_targets_config(source)


def _tenant_business_advisor_targets_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('business_advisor_targets')
        if isinstance(cfg, dict):
            source = cfg
    try:
        inventory_coverage_days = int(str(source.get('inventory_coverage_days') or 60).strip())
    except Exception:
        inventory_coverage_days = 60
    return {'inventory_coverage_days': max(1, min(3650, inventory_coverage_days))}


def _tenant_supplier_order_settings(tenant: Tenant | None) -> dict[str, int]:
    flags = tenant.feature_flags if tenant is not None else None
    source = {}
    if isinstance(flags, dict) and isinstance(flags.get('supplier_orders'), dict):
        source = flags.get('supplier_orders') or {}
    return normalize_supplier_order_settings(source)


def _tenant_era_exploration_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source: dict[str, object] = {}
    if isinstance(flags, dict):
        cfg = flags.get('era_exploration_data_config') or flags.get('era_exploration')
        if not isinstance(cfg, dict) and isinstance(flags.get('era_exploration_data'), dict):
            cfg = flags.get('era_exploration_data')
        if isinstance(cfg, dict):
            source = cfg
    return {
        'file_path': str(source.get('file_path') or ''),
        'archive_path': str(source.get('archive_path') or ''),
        'filename': str(source.get('filename') or ''),
        'uploaded_at': str(source.get('uploaded_at') or ''),
        'uploaded_by': str(source.get('uploaded_by') or ''),
        'period': str(source.get('period') or era_period_from_filename(source.get('filename') or source.get('file_path'))),
        'rows': int(source.get('rows') or 0),
        'brands': int(source.get('brands') or 0),
        'categories': int(source.get('categories') or 0),
    }


def _tenant_iqvia_settings(tenant: Tenant | None) -> dict[str, object]:
    flags = tenant.feature_flags if tenant is not None else None
    source: dict[str, object] = {}
    if isinstance(flags, dict):
        cfg = flags.get('iqvia_config') or flags.get('iqvia')
        if isinstance(cfg, dict):
            source = cfg
    return {
        'file_path': str(source.get('file_path') or ''),
        'archive_path': str(source.get('archive_path') or ''),
        'filename': str(source.get('filename') or ''),
        'uploaded_at': str(source.get('uploaded_at') or ''),
        'uploaded_by': str(source.get('uploaded_by') or ''),
        'period': str(source.get('period') or iqvia_period_from_filename(source.get('filename') or source.get('file_path'))),
        'rows': int(source.get('rows') or 0),
        'categories': int(source.get('categories') or 0),
        'manufacturers': int(source.get('manufacturers') or 0),
        'territories': int(source.get('territories') or 0),
    }


async def _tenant_call_center_settings(
    db: AsyncSession,
    tenant_id: int,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    queues: object = '',
) -> dict[str, object]:
    conn = await _find_tenant_connection(db, tenant_id=tenant_id, connector_type=_3CX_CONNECTOR_TYPE)
    params = conn.connection_parameters if conn is not None and isinstance(conn.connection_parameters, dict) else {}
    manual_import = params.get('manual_import') if isinstance(params.get('manual_import'), dict) else {}
    agent_override_text = str(params.get('agent_directory_text') or '')
    queue_override_text = str(params.get('queue_directory_text') or '')
    auto_agent_directory = (
        manual_import.get('auto_agent_directory')
        if isinstance(manual_import.get('auto_agent_directory'), dict)
        else {}
    )
    auto_queue_directory = (
        manual_import.get('auto_queue_directory')
        if isinstance(manual_import.get('auto_queue_directory'), dict)
        else {}
    )
    agent_override_directory = _parse_3cx_directory_text(agent_override_text)
    queue_override_directory = _parse_3cx_directory_text(queue_override_text)
    agent_directory = _merge_3cx_directories(auto_agent_directory, agent_override_directory)
    queue_directory = _merge_3cx_directories(auto_queue_directory, queue_override_directory)
    secret_payload: dict[str, Any] = {}
    if conn is not None and conn.enc_payload:
        try:
            secret_payload = decrypt_json_secret(conn.enc_payload)
        except Exception:
            logger.exception('tenant_3cx_secret_decrypt_failed', extra={'tenant_id': tenant_id})
    answer_sla_seconds = _parse_int_in_range(
        params.get('answer_sla_seconds'),
        default=30,
        min_value=1,
        max_value=3600,
    )
    target_calls_per_agent = _parse_int_in_range(
        params.get('target_calls_per_agent'),
        default=60,
        min_value=1,
        max_value=1000,
    )
    view_import = _filter_3cx_manual_import(
        manual_import,
        from_date=from_date,
        to_date=to_date,
        queues=queues,
        agent_directory=agent_directory,
        queue_directory=queue_directory,
        target_calls_per_agent=target_calls_per_agent,
        answer_sla_seconds=answer_sla_seconds,
    )
    stats = dict(view_import.get('stats') if isinstance(view_import.get('stats'), dict) else {})
    if 'inbound_calls' not in stats:
        stats['inbound_calls'] = max(
            0,
            int(stats.get('total_calls') or 0) - int(stats.get('outbound_calls') or 0),
        )
    labeled_call_rows = _label_3cx_call_rows(
        view_import.get('call_rows'),
        agent_directory=agent_directory,
        queue_directory=queue_directory,
    )
    # Recompute wait/talk averages from the actual call rows — the stored stat could carry a
    # stale SUM (not AVG). Average wait over all calls; average talk over answered calls only.
    _wait_vals = [int(r.get('wait_seconds') or 0) for r in labeled_call_rows if isinstance(r, dict)]
    _talk_vals = [int(r.get('talk_seconds') or 0) for r in labeled_call_rows if isinstance(r, dict) and r.get('is_answered')]
    stats['avg_wait_seconds'] = int(round(sum(_wait_vals) / len(_wait_vals))) if _wait_vals else 0
    stats['avg_talk_seconds'] = int(round(sum(_talk_vals) / len(_talk_vals))) if _talk_vals else 0
    stats['total_wait_seconds'] = sum(_wait_vals)
    stats['total_talk_seconds'] = sum(int(r.get('talk_seconds') or 0) for r in labeled_call_rows if isinstance(r, dict))
    labeled_journey_rows = _label_3cx_journey_rows(
        view_import.get('journey_rows'),
        agent_directory=agent_directory,
        queue_directory=queue_directory,
    )
    return {
        'configured': bool(conn and conn.is_active and (str(params.get('fqdn') or '').strip() or manual_import)),
        'fqdn': str(params.get('fqdn') or ''),
        'username': str(params.get('username') or secret_payload.get('username') or ''),
        'client_id': str(params.get('client_id') or ''),
        'queue_ids': str(params.get('queue_ids') or ''),
        'team_extensions': str(params.get('team_extensions') or ''),
        'agent_directory_text': agent_override_text,
        'queue_directory_text': queue_override_text,
        'agent_override_text': agent_override_text,
        'queue_override_text': queue_override_text,
        'auto_agent_directory_text': _format_3cx_directory_text(auto_agent_directory),
        'auto_queue_directory_text': _format_3cx_directory_text(auto_queue_directory),
        'auto_agent_directory_rows': _3cx_directory_rows(auto_agent_directory),
        'auto_queue_directory_rows': _3cx_directory_rows(auto_queue_directory),
        'agent_directory_rows': _3cx_directory_rows(agent_directory),
        'queue_directory_rows': _3cx_directory_rows(queue_directory),
        'polling_interval_minutes': _parse_int_in_range(
            params.get('polling_interval_minutes'),
            default=5,
            min_value=1,
            max_value=1440,
        ),
        'answer_sla_seconds': answer_sla_seconds,
        'target_calls_per_agent': target_calls_per_agent,
        'has_password': bool(secret_payload.get('password')),
        'last_sync_at': conn.last_sync_at if conn is not None else None,
        'last_test_ok_at': conn.last_test_ok_at if conn is not None else None,
        'last_test_error': conn.last_test_error if conn is not None else None,
        'sync_status': str(conn.sync_status if conn is not None else 'never'),
        'manual_import': view_import,
        'stats': stats,
        'call_rows': labeled_call_rows,
        'daily_rows': view_import.get('daily_rows') if isinstance(view_import.get('daily_rows'), list) else [],
        'weekly_rows': view_import.get('weekly_rows') if isinstance(view_import.get('weekly_rows'), list) else [],
        'did_weekly_rows': view_import.get('did_weekly_rows') if isinstance(view_import.get('did_weekly_rows'), list) else [],
        'queue_rows': _apply_3cx_queue_directory(view_import.get('queue_rows'), queue_directory),
        'did_rows': view_import.get('did_rows') if isinstance(view_import.get('did_rows'), list) else [],
        'agent_rows': _apply_3cx_agent_directory(
            view_import.get('agent_rows'),
            agent_directory,
            target_calls_per_agent=target_calls_per_agent,
        ),
        'source_rows': view_import.get('source_rows') if isinstance(view_import.get('source_rows'), list) else [],
        'journey_rows': labeled_journey_rows,
    }


def _parse_3cx_date(raw: object) -> str:
    text_value = str(raw or '').strip()
    if not text_value:
        return ''
    for fmt in (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y %H:%M',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M',
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%m/%d/%Y',
    ):
        try:
            return datetime.strptime(text_value[:19], fmt).date().isoformat()
        except Exception:
            continue
    iso_match = re.search(r'\d{4}-\d{2}-\d{2}', text_value)
    if iso_match:
        return iso_match.group(0)
    dmy_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text_value)
    if dmy_match:
        day, month, year = dmy_match.groups()
        return f'{int(year):04d}-{int(month):02d}-{int(day):02d}'
    return ''


def _parse_3cx_filter_date_value(raw: object) -> date | None:
    text_value = str(raw or '').strip()
    if not text_value:
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(text_value[:10], fmt).date()
        except ValueError:
            continue
    parsed = _parse_3cx_date(text_value)
    if parsed:
        try:
            return datetime.strptime(parsed, '%Y-%m-%d').date()
        except ValueError:
            return None
    return None


def _parse_3cx_datetime_value(raw: object) -> datetime | None:
    text_value = str(raw or '').strip()
    if not text_value:
        return None
    normalized = text_value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y %H:%M:%S'):
        try:
            return datetime.strptime(text_value[:19], fmt)
        except ValueError:
            continue
    return None


def _split_3cx_filter_terms(raw: object) -> list[str]:
    return [
        item.strip().lower()
        for item in re.split(r'[,;\n|]+', str(raw or ''))
        if item.strip()
    ]


def _3cx_filter_directory_tokens(value: object, *directories: dict[str, str]) -> list[str]:
    text_value = str(value or '').strip()
    if not text_value:
        return []
    tokens = [text_value]
    extension_code = _extract_3cx_extension_code(text_value)
    for directory in directories:
        if not isinstance(directory, dict):
            continue
        if extension_code and directory.get(extension_code):
            tokens.append(str(directory[extension_code]))
        for code, label in directory.items():
            label_value = str(label or '').strip()
            if label_value and label_value.lower() == text_value.lower():
                tokens.append(str(code))
    return tokens


def _3cx_filter_search_text(
    row: dict[str, object],
    fields: tuple[str, ...],
    *,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> str:
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    tokens: list[str] = []
    for field in fields:
        value = row.get(field)
        if field in {'agent', 'extension', 'from', 'to', 'final_destination'}:
            tokens.extend(_3cx_filter_directory_tokens(value, agent_directory, queue_directory))
        elif field == 'queue':
            tokens.extend(_3cx_filter_directory_tokens(value, queue_directory, agent_directory))
        else:
            text_value = str(value or '').strip()
            if text_value:
                tokens.append(text_value)
        if field == 'direction_label':
            if bool(row.get('is_outbound')):
                tokens.extend(['outbound', 'out', 'εξερχόμενη', 'εξερχομενη', 'exerchomeni'])
            else:
                tokens.extend(['inbound', 'in', 'εισερχόμενη', 'εισερχομενη', 'eiserchomeni'])
        if field == 'status_label':
            if bool(row.get('is_answered')):
                tokens.extend(['answered', 'answer', 'απαντημένη', 'απαντημενη', 'apantimeni'])
            if bool(row.get('is_missed')):
                tokens.extend(['missed', 'lost', 'χαμένη', 'χαμενη', 'xameni'])
    return ' '.join(tokens).lower()


def _3cx_filter_endpoint_codes(
    row: dict[str, object],
    fields: tuple[str, ...],
    *,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> set[str]:
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    endpoint_fields = {'agent', 'extension', 'queue', 'from', 'to', 'final_destination'}
    codes: set[str] = set()
    for field in fields:
        if field not in endpoint_fields:
            continue
        text_value = str(row.get(field) or '').strip()
        if not text_value:
            continue
        endpoint_code = _3cx_internal_extension_code(text_value)
        if endpoint_code:
            codes.add(endpoint_code.lower())
        for directory in (agent_directory, queue_directory):
            if not isinstance(directory, dict):
                continue
            if endpoint_code and directory.get(endpoint_code):
                codes.add(endpoint_code.lower())
            for code, label in directory.items():
                if str(label or '').strip().lower() == text_value.lower():
                    codes.add(str(code or '').strip().lower())
    return {code for code in codes if code}


def _3cx_matches_filter_terms(
    row: dict[str, object],
    terms: list[str],
    fields: tuple[str, ...],
    *,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> bool:
    if not terms:
        return True
    exact_endpoint_terms = [
        term
        for term in terms
        if re.fullmatch(r'(?:grp[a-z0-9]*|\d{2,5})', term, flags=re.IGNORECASE)
    ]
    text_terms = [term for term in terms if term not in exact_endpoint_terms]
    if exact_endpoint_terms:
        endpoint_codes = _3cx_filter_endpoint_codes(
            row,
            fields,
            agent_directory=agent_directory,
            queue_directory=queue_directory,
        )
        if any(term.lower() in endpoint_codes for term in exact_endpoint_terms):
            return True
        if not text_terms:
            return False
    searchable = _3cx_filter_search_text(
        row,
        fields,
        agent_directory=agent_directory,
        queue_directory=queue_directory,
    )
    return any(term in searchable for term in text_terms)


def _normalize_3cx_call_log_only_view(manual_import: dict[str, object]) -> dict[str, object]:
    if not manual_import.get('master_report'):
        return manual_import
    normalized = dict(manual_import)
    normalized['source_mode'] = 'call_log_only'
    filename = str(normalized.get('filename') or '').strip()
    filename_parts = [part.strip() for part in filename.split('+') if part.strip()]
    if len(filename_parts) > 1:
        master_filename = next(
            (
                part for part in filename_parts
                if 'call_report' in part.lower() or 'call log' in part.lower()
            ),
            filename_parts[0],
        )
        normalized['filename'] = master_filename
        if not isinstance(normalized.get('excluded_reports'), list):
            normalized['excluded_reports'] = [{'filename': part} for part in filename_parts if part != master_filename]
    return normalized


def _3cx_journey_hop_order_key(hop: dict[str, object], index: int) -> tuple[int, float, int]:
    try:
        seq = int(hop.get('seq') or 0)
    except Exception:
        seq = 0
    parsed_time = _parse_3cx_datetime_value(hop.get('time'))
    if parsed_time is not None:
        try:
            time_key = parsed_time.timestamp()
        except Exception:
            time_key = 0.0
    else:
        time_key = 0.0
    return (seq, time_key, index)


def _3cx_final_journey_hops(journey_rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    final_hops: dict[str, tuple[tuple[int, float, int], dict[str, object]]] = {}
    for index, hop in enumerate(journey_rows):
        if not isinstance(hop, dict):
            continue
        call_id = str(hop.get('call_id') or '').strip()
        if not call_id:
            continue
        order_key = _3cx_journey_hop_order_key(hop, index)
        existing = final_hops.get(call_id)
        if existing is None or order_key >= existing[0]:
            final_hops[call_id] = (order_key, hop)
    return {call_id: hop for call_id, (_, hop) in final_hops.items()}


def _aggregate_3cx_call_rows(
    call_rows: list[dict[str, object]],
    *,
    target_calls_per_agent: int = 60,
    answer_sla_seconds: int = 30,
) -> dict[str, object]:
    total_calls = answered_calls = missed_calls = outbound_calls = total_wait_seconds = wait_count = 0
    inbound_answered_calls = outbound_answered_calls = inbound_missed_calls = outbound_missed_calls = 0
    daily: dict[str, dict[str, object]] = {}
    weekly: dict[str, dict[str, object]] = {}
    did_weekly: dict[str, dict[str, object]] = {}
    queues: dict[str, dict[str, object]] = {}
    dids: dict[str, dict[str, object]] = {}
    agents: dict[str, dict[str, object]] = {}
    sources: dict[str, dict[str, object]] = {}

    for call in call_rows:
        if not isinstance(call, dict):
            continue
        call_date = str(call.get('date') or '')
        queue = str(call.get('queue') or 'Χωρίς queue')
        agent = str(call.get('agent') or 'Χωρίς agent')
        extension = str(call.get('extension') or '')
        source_number = str(call.get('source') or 'Άγνωστη πηγή')
        did_number = str(call.get('did') or '')
        duration_seconds = int(call.get('duration_seconds') or 0)
        talk_seconds = int(call.get('talk_seconds') or 0)
        wait_seconds = int(call.get('wait_seconds') or 0)
        is_outbound = bool(call.get('is_outbound'))
        is_missed = bool(call.get('is_missed'))
        is_answered = bool(call.get('is_answered'))
        is_redirected = bool(call.get('is_redirected'))
        status_label = str(call.get('status_label') or ('Χαμένη' if is_missed else 'Απαντ.'))
        direction_label = str(call.get('direction_label') or ('Εξ.' if is_outbound else 'Εισ.'))

        total_calls += 1
        answered_calls += 1 if is_answered else 0
        missed_calls += 1 if is_missed else 0
        outbound_calls += 1 if is_outbound else 0
        if is_outbound:
            outbound_answered_calls += 1 if is_answered else 0
            outbound_missed_calls += 1 if is_missed else 0
        else:
            inbound_answered_calls += 1 if is_answered else 0
            inbound_missed_calls += 1 if is_missed else 0
        if wait_seconds:
            total_wait_seconds += wait_seconds
            wait_count += 1

        if call_date:
            bucket = daily.setdefault(call_date, {'date': call_date, 'calls': 0, 'answered': 0, 'missed': 0, 'outbound': 0})
            bucket['calls'] = int(bucket['calls']) + 1
            bucket['answered'] = int(bucket['answered']) + (1 if is_answered else 0)
            bucket['missed'] = int(bucket['missed']) + (1 if is_missed else 0)
            bucket['outbound'] = int(bucket.get('outbound') or 0) + (1 if is_outbound else 0)
            try:
                parsed_call_date = datetime.strptime(call_date, '%Y-%m-%d').date()
            except ValueError:
                parsed_call_date = None
            if parsed_call_date is not None and not is_outbound:
                iso_year, iso_week, _ = parsed_call_date.isocalendar()
                week_start = parsed_call_date - timedelta(days=parsed_call_date.weekday())
                week_end = week_start + timedelta(days=6)
                week_key = f'{iso_year}-W{iso_week:02d}'
                week_bucket = weekly.setdefault(
                    week_key,
                    {
                        'week': week_key,
                        'week_start': week_start.isoformat(),
                        'week_end': week_end.isoformat(),
                        'inbound_calls': 0,
                        'answered': 0,
                        'missed_calls': 0,
                        'wait_seconds_total': 0,
                        'wait_count': 0,
                        'talk_seconds_total': 0,
                        'talk_count': 0,
                        'redirected_calls': 0,
                        'sources': {},
                    },
                )
                week_bucket['inbound_calls'] = int(week_bucket['inbound_calls']) + 1
                week_bucket['answered'] = int(week_bucket['answered']) + (1 if is_answered else 0)
                week_bucket['missed_calls'] = int(week_bucket['missed_calls']) + (1 if is_missed else 0)
                week_bucket['wait_seconds_total'] = int(week_bucket['wait_seconds_total']) + wait_seconds
                week_bucket['wait_count'] = int(week_bucket['wait_count']) + 1
                if is_answered:
                    week_bucket['talk_seconds_total'] = int(week_bucket['talk_seconds_total']) + talk_seconds
                    week_bucket['talk_count'] = int(week_bucket['talk_count']) + 1
                week_bucket['redirected_calls'] = int(week_bucket['redirected_calls']) + (1 if is_redirected else 0)
                week_sources = week_bucket.get('sources') if isinstance(week_bucket.get('sources'), dict) else {}
                week_sources[source_number] = int(week_sources.get(source_number) or 0) + 1
                week_bucket['sources'] = week_sources

                did_key = did_number or 'Χωρίς DID'
                did_week_key = f'{did_key}|{week_key}'
                did_week_bucket = did_weekly.setdefault(
                    did_week_key,
                    {
                        'did': did_key,
                        'week': week_key,
                        'week_start': week_start.isoformat(),
                        'week_end': week_end.isoformat(),
                        'inbound_calls': 0,
                        'answered': 0,
                        'missed_calls': 0,
                        'wait_seconds_total': 0,
                        'wait_count': 0,
                        'talk_seconds_total': 0,
                        'talk_count': 0,
                        'redirected_calls': 0,
                        'sources': {},
                        'queues': {},
                    },
                )
                did_week_bucket['inbound_calls'] = int(did_week_bucket['inbound_calls']) + 1
                did_week_bucket['answered'] = int(did_week_bucket['answered']) + (1 if is_answered else 0)
                did_week_bucket['missed_calls'] = int(did_week_bucket['missed_calls']) + (1 if is_missed else 0)
                did_week_bucket['wait_seconds_total'] = int(did_week_bucket['wait_seconds_total']) + wait_seconds
                did_week_bucket['wait_count'] = int(did_week_bucket['wait_count']) + 1
                if is_answered:
                    did_week_bucket['talk_seconds_total'] = int(did_week_bucket['talk_seconds_total']) + talk_seconds
                    did_week_bucket['talk_count'] = int(did_week_bucket['talk_count']) + 1
                did_week_bucket['redirected_calls'] = int(did_week_bucket['redirected_calls']) + (1 if is_redirected else 0)
                did_week_sources = did_week_bucket.get('sources') if isinstance(did_week_bucket.get('sources'), dict) else {}
                did_week_sources[source_number] = int(did_week_sources.get(source_number) or 0) + 1
                did_week_bucket['sources'] = did_week_sources
                did_week_queues = did_week_bucket.get('queues') if isinstance(did_week_bucket.get('queues'), dict) else {}
                did_week_queues[queue] = int(did_week_queues.get(queue) or 0) + 1
                did_week_bucket['queues'] = did_week_queues

        queue_bucket = queues.setdefault(queue, {'queue': queue, 'calls': 0, 'answered': 0, 'missed': 0, 'sla': 0})
        queue_bucket['calls'] = int(queue_bucket['calls']) + 1
        queue_bucket['answered'] = int(queue_bucket['answered']) + (1 if is_answered else 0)
        queue_bucket['missed'] = int(queue_bucket['missed']) + (1 if is_missed else 0)
        if is_answered and wait_seconds and wait_seconds <= answer_sla_seconds:
            queue_bucket['sla'] = int(queue_bucket['sla']) + 1

        if not is_outbound:
            did_key = did_number or 'Χωρίς DID'
            did_bucket = dids.setdefault(
                did_key,
                {'did': did_key, 'calls': 0, 'answered': 0, 'missed': 0, 'sla': 0, 'queues': {}, 'sources': {}},
            )
            did_bucket['calls'] = int(did_bucket['calls']) + 1
            did_bucket['answered'] = int(did_bucket['answered']) + (1 if is_answered else 0)
            did_bucket['missed'] = int(did_bucket['missed']) + (1 if is_missed else 0)
            if is_answered and wait_seconds and wait_seconds <= answer_sla_seconds:
                did_bucket['sla'] = int(did_bucket['sla']) + 1
            did_queues = did_bucket.get('queues') if isinstance(did_bucket.get('queues'), dict) else {}
            did_queues[queue] = int(did_queues.get(queue) or 0) + 1
            did_bucket['queues'] = did_queues
            did_sources = did_bucket.get('sources') if isinstance(did_bucket.get('sources'), dict) else {}
            did_sources[source_number] = int(did_sources.get(source_number) or 0) + 1
            did_bucket['sources'] = did_sources

        source_key = f'{source_number}|{did_number}|{queue}'
        source_bucket = sources.setdefault(
            source_key,
            {
                'source': source_number,
                'did': did_number,
                'queue': queue,
                'calls': 0,
                'answered': 0,
                'missed': 0,
                'outbound': 0,
                'talk_seconds': 0,
                'last_call': '',
                'agents': {},
                'recent': [],
            },
        )
        source_bucket['calls'] = int(source_bucket['calls']) + 1
        source_bucket['answered'] = int(source_bucket['answered']) + (1 if is_answered else 0)
        source_bucket['missed'] = int(source_bucket['missed']) + (1 if is_missed else 0)
        source_bucket['outbound'] = int(source_bucket['outbound']) + (1 if is_outbound else 0)
        source_bucket['talk_seconds'] = int(source_bucket['talk_seconds']) + duration_seconds
        if call_date:
            source_bucket['last_call'] = max(str(source_bucket.get('last_call') or ''), call_date)
        source_agents = source_bucket.get('agents') if isinstance(source_bucket.get('agents'), dict) else {}
        agent_key = f'{agent}|{extension}'
        agent_source_bucket = source_agents.setdefault(
            agent_key,
            {'agent': agent, 'extension': extension, 'calls': 0, 'talk_seconds': 0},
        )
        agent_source_bucket['calls'] = int(agent_source_bucket['calls']) + 1
        agent_source_bucket['talk_seconds'] = int(agent_source_bucket['talk_seconds']) + duration_seconds
        source_bucket['agents'] = source_agents
        recent_calls = source_bucket.get('recent') if isinstance(source_bucket.get('recent'), list) else []
        if len(recent_calls) < 5:
            recent_calls.append(
                {
                    'date': call_date,
                    'agent': agent,
                    'extension': extension,
                    'direction': direction_label,
                    'duration_seconds': duration_seconds,
                    'status': status_label,
                }
            )
            source_bucket['recent'] = recent_calls

        agent_bucket = agents.setdefault(
            agent_key,
            {
                'agent': agent,
                'extension': extension,
                'answered': 0,
                'outbound': 0,
                'inbound': 0,
                'missed': 0,
                'talk_seconds': 0,
                'calls': 0,
                'last_date': '',
                'recent': [],
            },
        )
        agent_bucket['calls'] = int(agent_bucket['calls']) + 1
        agent_bucket['answered'] = int(agent_bucket['answered']) + (1 if is_answered else 0)
        agent_bucket['outbound'] = int(agent_bucket['outbound']) + (1 if is_outbound else 0)
        agent_bucket['inbound'] = int(agent_bucket['inbound']) + (0 if is_outbound else 1)
        agent_bucket['missed'] = int(agent_bucket['missed']) + (1 if is_missed else 0)
        if is_outbound:
            agent_bucket['outbound_answered'] = int(agent_bucket.get('outbound_answered') or 0) + (1 if is_answered else 0)
            agent_bucket['outbound_missed'] = int(agent_bucket.get('outbound_missed') or 0) + (1 if is_missed else 0)
        else:
            agent_bucket['inbound_answered'] = int(agent_bucket.get('inbound_answered') or 0) + (1 if is_answered else 0)
            agent_bucket['inbound_missed'] = int(agent_bucket.get('inbound_missed') or 0) + (1 if is_missed else 0)
        agent_bucket['talk_seconds'] = int(agent_bucket['talk_seconds']) + duration_seconds
        if call_date:
            agent_bucket['last_date'] = max(str(agent_bucket.get('last_date') or ''), call_date)
        agent_recent = agent_bucket.get('recent') if isinstance(agent_bucket.get('recent'), list) else []
        agent_recent.append(
            {
                'date': call_date,
                'source': source_number,
                'queue': queue,
                'direction': direction_label,
                'duration_seconds': duration_seconds,
                'status': status_label,
            }
        )
        agent_bucket['recent'] = agent_recent[-8:]

    for queue_row in queues.values():
        calls = max(1, int(queue_row['calls']))
        queue_row['answer_pct'] = round((int(queue_row.get('answered') or 0) / calls) * 100, 1)
        queue_row['sla_pct'] = round((int(queue_row.get('sla') or 0) / calls) * 100, 1)
    for did_row in dids.values():
        calls = max(1, int(did_row.get('calls') or 0))
        did_row['answer_pct'] = round((int(did_row.get('answered') or 0) / calls) * 100, 1)
        did_row['sla_pct'] = round((int(did_row.get('sla') or 0) / calls) * 100, 1)
        did_queues = did_row.get('queues') if isinstance(did_row.get('queues'), dict) else {}
        did_sources = did_row.get('sources') if isinstance(did_row.get('sources'), dict) else {}
        did_row['top_queue'] = max(did_queues.items(), key=lambda item: int(item[1]))[0] if did_queues else ''
        did_row['unique_sources'] = len(did_sources)
        did_row.pop('queues', None)
        did_row.pop('sources', None)
    for agent_row in agents.values():
        calls = max(1, int(agent_row.get('calls') or 0))
        agent_row['avg_talk_seconds'] = int(int(agent_row.get('talk_seconds') or 0) / calls)
        agent_row['target_pct'] = round((int(agent_row.get('answered') or 0) / max(1, target_calls_per_agent)) * 100, 1)
    for week_row in weekly.values():
        inbound_calls = max(1, int(week_row.get('inbound_calls') or 0))
        answered = int(week_row.get('answered') or 0)
        missed = int(week_row.get('missed_calls') or 0)
        wait_count = max(1, int(week_row.get('wait_count') or 0))
        talk_count = max(1, int(week_row.get('talk_count') or 0))
        redirected = int(week_row.get('redirected_calls') or 0)
        sources_for_week = week_row.get('sources') if isinstance(week_row.get('sources'), dict) else {}
        repeat_calls = sum(int(count) for count in sources_for_week.values() if int(count) > 1)
        week_row['answer_rate_pct'] = round((answered / inbound_calls) * 100, 1)
        week_row['avg_waiting_time_seconds'] = int(int(week_row.get('wait_seconds_total') or 0) / wait_count)
        week_row['avg_talking_time_seconds'] = int(int(week_row.get('talk_seconds_total') or 0) / talk_count)
        week_row['abandonment_rate_pct'] = round((missed / inbound_calls) * 100, 1)
        week_row['call_repeat_rate_pct'] = round((repeat_calls / inbound_calls) * 100, 1)
        week_row['call_redirected_pct'] = round((redirected / inbound_calls) * 100, 1)
        week_row.pop('wait_seconds_total', None)
        week_row.pop('wait_count', None)
        week_row.pop('talk_seconds_total', None)
        week_row.pop('talk_count', None)
        week_row.pop('redirected_calls', None)
        week_row.pop('sources', None)
    for did_week_row in did_weekly.values():
        inbound_calls = max(1, int(did_week_row.get('inbound_calls') or 0))
        answered = int(did_week_row.get('answered') or 0)
        missed = int(did_week_row.get('missed_calls') or 0)
        wait_count = max(1, int(did_week_row.get('wait_count') or 0))
        talk_count = max(1, int(did_week_row.get('talk_count') or 0))
        redirected = int(did_week_row.get('redirected_calls') or 0)
        sources_for_did_week = did_week_row.get('sources') if isinstance(did_week_row.get('sources'), dict) else {}
        queues_for_did_week = did_week_row.get('queues') if isinstance(did_week_row.get('queues'), dict) else {}
        repeat_calls = sum(int(count) for count in sources_for_did_week.values() if int(count) > 1)
        did_week_row['answer_rate_pct'] = round((answered / inbound_calls) * 100, 1)
        did_week_row['avg_waiting_time_seconds'] = int(int(did_week_row.get('wait_seconds_total') or 0) / wait_count)
        did_week_row['avg_talking_time_seconds'] = int(int(did_week_row.get('talk_seconds_total') or 0) / talk_count)
        did_week_row['abandonment_rate_pct'] = round((missed / inbound_calls) * 100, 1)
        did_week_row['call_repeat_rate_pct'] = round((repeat_calls / inbound_calls) * 100, 1)
        did_week_row['call_redirected_pct'] = round((redirected / inbound_calls) * 100, 1)
        did_week_row['unique_sources'] = len(sources_for_did_week)
        did_week_row['top_queue'] = max(queues_for_did_week.items(), key=lambda item: int(item[1]))[0] if queues_for_did_week else ''
        did_week_row.pop('wait_seconds_total', None)
        did_week_row.pop('wait_count', None)
        did_week_row.pop('talk_seconds_total', None)
        did_week_row.pop('talk_count', None)
        did_week_row.pop('redirected_calls', None)
        did_week_row.pop('sources', None)
        did_week_row.pop('queues', None)

    source_rows: list[dict[str, object]] = []
    for source_row in sources.values():
        calls = max(1, int(source_row.get('calls') or 0))
        source_agents = source_row.get('agents') if isinstance(source_row.get('agents'), dict) else {}
        agent_rows = []
        for agent_source_row in source_agents.values():
            agent_calls = max(1, int(agent_source_row.get('calls') or 0))
            agent_rows.append(
                {
                    **agent_source_row,
                    'avg_talk_seconds': int(int(agent_source_row.get('talk_seconds') or 0) / agent_calls),
                }
            )
        clean_source_row = dict(source_row)
        clean_source_row['avg_talk_seconds'] = int(int(source_row.get('talk_seconds') or 0) / calls)
        clean_source_row['agents'] = sorted(agent_rows, key=lambda item: int(item.get('calls') or 0), reverse=True)[:5]
        source_rows.append(clean_source_row)

    return {
        'stats': {
            'total_calls': total_calls,
            'answered_calls': answered_calls,
            'missed_calls': missed_calls,
            'inbound_calls': total_calls - outbound_calls,
            'outbound_calls': outbound_calls,
            'inbound_answered_calls': inbound_answered_calls,
            'outbound_answered_calls': outbound_answered_calls,
            'inbound_missed_calls': inbound_missed_calls,
            'outbound_missed_calls': outbound_missed_calls,
            'answer_rate_pct': round((answered_calls / total_calls) * 100, 1) if total_calls else 0,
            'avg_wait_seconds': int(total_wait_seconds / wait_count) if wait_count else 0,
        },
        'daily_rows': sorted(daily.values(), key=lambda item: str(item.get('date') or '')),
        'weekly_rows': sorted(weekly.values(), key=lambda item: str(item.get('week') or '')),
        'did_weekly_rows': sorted(
            did_weekly.values(),
            key=lambda item: (
                -int((dids.get(str(item.get('did') or '')) or {}).get('calls') or 0),
                str(item.get('did') or ''),
                str(item.get('week') or ''),
            ),
        ),
        'queue_rows': sorted(queues.values(), key=lambda item: int(item.get('calls') or 0), reverse=True)[:20],
        'did_rows': sorted(dids.values(), key=lambda item: int(item.get('calls') or 0), reverse=True)[:50],
        'agent_rows': sorted(agents.values(), key=lambda item: int(item.get('answered') or 0), reverse=True)[:50],
        'source_rows': sorted(source_rows, key=lambda item: int(item.get('calls') or 0), reverse=True)[:50],
    }


def _aggregate_3cx_journey_queue_rows(
    journey_rows: list[dict[str, object]],
    *,
    answer_sla_seconds: int = 30,
    queue_terms: list[str] | None = None,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    queue_terms = queue_terms or []
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    queues: dict[str, dict[str, object]] = {}
    final_answered_by_call_id: dict[str, bool] = {}
    seen_queue_calls: set[tuple[str, str]] = set()
    for call_id, hop in _3cx_final_journey_hops(journey_rows).items():
        status = str(hop.get('status') or '').strip().lower()
        destination = str(hop.get('to') or '').strip()
        final_answered_by_call_id[call_id] = status == 'answered' and not _3cx_is_auto_answer_endpoint(
            destination,
            queue_directory,
            agent_directory,
        )
    for hop in journey_rows:
        if not isinstance(hop, dict):
            continue
        queue = str(hop.get('to') or '').strip()
        if not queue or not _3cx_is_queue_endpoint(queue):
            continue
        if queue_terms and not _3cx_matches_filter_terms(
            hop,
            queue_terms,
            ('from', 'to', 'direction', 'status', 'activity'),
            agent_directory=agent_directory,
            queue_directory=queue_directory,
        ):
            continue
        call_id = str(hop.get('call_id') or '').strip()
        if not call_id:
            continue
        queue_key = _extract_3cx_extension_code(queue) or queue
        pair_key = (queue_key, call_id)
        if pair_key in seen_queue_calls:
            continue
        seen_queue_calls.add(pair_key)
        status = str(hop.get('status') or '').strip().lower()
        ringing_seconds = int(hop.get('ringing_seconds') or 0)
        is_auto_answer = _3cx_is_auto_answer_endpoint(queue, queue_directory, agent_directory) and status == 'answered'
        final_answered = final_answered_by_call_id.get(call_id, False)
        is_answered = final_answered
        is_missed = not final_answered
        bucket = queues.setdefault(queue, {'queue': queue, 'calls': 0, 'answered': 0, 'missed': 0, 'sla': 0, 'auto_answered': 0})
        bucket['calls'] = int(bucket['calls']) + 1
        bucket['answered'] = int(bucket['answered']) + (1 if is_answered else 0)
        bucket['missed'] = int(bucket['missed']) + (1 if is_missed else 0)
        bucket['auto_answered'] = int(bucket.get('auto_answered') or 0) + (1 if is_auto_answer else 0)
        if is_answered and ringing_seconds <= answer_sla_seconds:
            bucket['sla'] = int(bucket['sla']) + 1
    for queue_row in queues.values():
        calls = max(1, int(queue_row.get('calls') or 0))
        queue_row['answer_pct'] = round((int(queue_row.get('answered') or 0) / calls) * 100, 1)
        queue_row['sla_pct'] = round((int(queue_row.get('sla') or 0) / calls) * 100, 1)
    return sorted(queues.values(), key=lambda item: int(item.get('calls') or 0), reverse=True)[:50]


def _normalize_3cx_auto_answer_call_rows(
    call_rows: list[dict[str, object]],
    *,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    normalized_rows: list[dict[str, object]] = []
    for row in call_rows:
        if not isinstance(row, dict):
            continue
        clean_row = dict(row)
        final_destination = clean_row.get('final_destination') or clean_row.get('queue')
        if _3cx_is_auto_answer_endpoint(final_destination, queue_directory, agent_directory):
            clean_row['is_answered'] = False
            clean_row['is_missed'] = True
            clean_row['duration_seconds'] = 0
            clean_row['status_label'] = 'Χαμένη'
        normalized_rows.append(clean_row)
    return normalized_rows


def _apply_3cx_journey_outcomes_to_call_rows(
    call_rows: list[dict[str, object]],
    journey_rows: list[dict[str, object]],
    *,
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    final_outcomes: dict[str, bool] = {}
    for call_id, hop in _3cx_final_journey_hops(journey_rows).items():
        status = str(hop.get('status') or '').strip().lower()
        destination = str(hop.get('to') or '').strip()
        final_outcomes[call_id] = status == 'answered' and not _3cx_is_auto_answer_endpoint(
            destination,
            queue_directory,
            agent_directory,
        )
    normalized_rows = _normalize_3cx_auto_answer_call_rows(
        call_rows,
        agent_directory=agent_directory,
        queue_directory=queue_directory,
    )
    out: list[dict[str, object]] = []
    for row in normalized_rows:
        clean_row = dict(row)
        call_id = str(clean_row.get('call_id') or clean_row.get('fingerprint') or '').strip()
        if call_id in final_outcomes:
            is_answered = bool(final_outcomes[call_id])
            clean_row['is_answered'] = is_answered
            clean_row['is_missed'] = not is_answered
            clean_row['status_label'] = 'Απαντ.' if is_answered else 'Χαμένη'
            if not is_answered:
                clean_row['duration_seconds'] = 0
        out.append(clean_row)
    return out


def _filter_3cx_manual_import(
    manual_import: dict[str, object],
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    queues: object = '',
    agent_directory: dict[str, str] | None = None,
    queue_directory: dict[str, str] | None = None,
    target_calls_per_agent: int = 60,
    answer_sla_seconds: int = 30,
) -> dict[str, object]:
    manual_import = _normalize_3cx_call_log_only_view(manual_import)
    call_rows = manual_import.get('call_rows')
    queue_terms = _split_3cx_filter_terms(queues)
    agent_directory = agent_directory or {}
    queue_directory = queue_directory or {}
    if not isinstance(call_rows, list):
        if from_date is None and to_date is None and not queue_terms:
            return manual_import
        filtered = dict(manual_import)
        if isinstance(manual_import.get('daily_rows'), list) and (from_date is not None or to_date is not None):
            daily_rows = []
            for row in manual_import.get('daily_rows') or []:
                if not isinstance(row, dict):
                    continue
                row_date = _parse_3cx_filter_date_value(row.get('date'))
                if from_date is not None and (row_date is None or row_date < from_date):
                    continue
                if to_date is not None and (row_date is None or row_date > to_date):
                    continue
                daily_rows.append(row)
            stats = dict(manual_import.get('stats') if isinstance(manual_import.get('stats'), dict) else {})
            total_calls = sum(int(row.get('calls') or 0) for row in daily_rows)
            answered_calls = sum(int(row.get('answered') or 0) for row in daily_rows)
            missed_calls = sum(int(row.get('missed') or 0) for row in daily_rows)
            outbound_calls = sum(int(row.get('outbound') or 0) for row in daily_rows)
            stats.update(
                {
                    'total_calls': total_calls,
                    'answered_calls': answered_calls,
                    'missed_calls': missed_calls,
                    'outbound_calls': outbound_calls,
                    'answer_rate_pct': round((answered_calls / total_calls) * 100, 1) if total_calls else 0,
                }
            )
            filtered['daily_rows'] = daily_rows
            filtered['stats'] = stats
            filtered['rows'] = total_calls
        if queue_terms:
            queue_rows = [
                row for row in (manual_import.get('queue_rows') if isinstance(manual_import.get('queue_rows'), list) else [])
                if isinstance(row, dict)
                and _3cx_matches_filter_terms(
                    row,
                    queue_terms,
                    ('queue', 'extension', 'agent', 'source', 'did'),
                    agent_directory=agent_directory,
                    queue_directory=queue_directory,
                )
            ]
            agent_rows = [
                row for row in (manual_import.get('agent_rows') if isinstance(manual_import.get('agent_rows'), list) else [])
                if isinstance(row, dict)
                and _3cx_matches_filter_terms(
                    row,
                    queue_terms,
                    ('agent', 'extension', 'queue'),
                    agent_directory=agent_directory,
                    queue_directory=queue_directory,
                )
            ]
            source_rows = [
                row for row in (manual_import.get('source_rows') if isinstance(manual_import.get('source_rows'), list) else [])
                if isinstance(row, dict)
                and _3cx_matches_filter_terms(
                    row,
                    queue_terms,
                    ('queue', 'extension', 'agent', 'source', 'did'),
                    agent_directory=agent_directory,
                    queue_directory=queue_directory,
                )
            ]
            filtered['queue_rows'] = queue_rows
            filtered['agent_rows'] = agent_rows
            filtered['source_rows'] = source_rows
            if from_date is None and to_date is None:
                driver_rows = agent_rows if agent_rows and not queue_rows else queue_rows
                total_calls = sum(int(row.get('calls') or 0) for row in driver_rows)
                answered_calls = sum(int(row.get('answered') or 0) for row in driver_rows)
                missed_calls = sum(int(row.get('missed') or 0) for row in driver_rows)
                stats = dict(filtered.get('stats') if isinstance(filtered.get('stats'), dict) else {})
                stats.update(
                    {
                        'total_calls': total_calls,
                        'answered_calls': answered_calls,
                        'missed_calls': missed_calls,
                        'answer_rate_pct': round((answered_calls / total_calls) * 100, 1) if total_calls else 0,
                    }
                )
                filtered['stats'] = stats
                filtered['rows'] = total_calls
        filtered['filters_applied'] = True
        filtered['legacy_filter_mode'] = True
        return filtered
    if from_date is None and to_date is None and not queue_terms:
        if manual_import.get('master_report') and isinstance(manual_import.get('journey_rows'), list):
            refreshed = dict(manual_import)
            refreshed_call_rows = _apply_3cx_journey_outcomes_to_call_rows(
                call_rows,
                manual_import.get('journey_rows') or [],
                agent_directory=agent_directory,
                queue_directory=queue_directory,
            )
            refreshed['call_rows'] = refreshed_call_rows
            refreshed.update(
                _aggregate_3cx_call_rows(
                    refreshed_call_rows,
                    target_calls_per_agent=target_calls_per_agent,
                    answer_sla_seconds=answer_sla_seconds,
                )
            )
            refreshed['queue_rows'] = _aggregate_3cx_journey_queue_rows(
                manual_import.get('journey_rows') or [],
                answer_sla_seconds=answer_sla_seconds,
                agent_directory=agent_directory,
                queue_directory=queue_directory,
            )
            return refreshed
        refreshed = dict(manual_import)
        refreshed.update(
            _aggregate_3cx_call_rows(
                [dict(row) for row in call_rows if isinstance(row, dict)],
                target_calls_per_agent=target_calls_per_agent,
                answer_sla_seconds=answer_sla_seconds,
            )
        )
        return refreshed

    journeys_by_call_id: dict[str, list[dict[str, object]]] = {}
    if queue_terms and isinstance(manual_import.get('journey_rows'), list):
        for journey in manual_import.get('journey_rows') or []:
            if not isinstance(journey, dict):
                continue
            call_id = str(journey.get('call_id') or '').strip()
            if call_id:
                journeys_by_call_id.setdefault(call_id, []).append(journey)

    filtered_rows: list[dict[str, object]] = []
    for row in call_rows:
        if not isinstance(row, dict):
            continue
        call_date = _parse_3cx_filter_date_value(row.get('date'))
        if from_date is not None and (call_date is None or call_date < from_date):
            continue
        if to_date is not None and (call_date is None or call_date > to_date):
            continue
        if queue_terms:
            row_matches = _3cx_matches_filter_terms(
                row,
                queue_terms,
                (
                    'call_id',
                    'fingerprint',
                    'queue',
                    'extension',
                    'agent',
                    'source',
                    'did',
                    'final_destination',
                    'direction_label',
                    'status_label',
                ),
                agent_directory=agent_directory,
                queue_directory=queue_directory,
            )
            call_id = str(row.get('call_id') or row.get('fingerprint') or '').strip()
            journey_matches = any(
                _3cx_matches_filter_terms(
                    journey,
                    queue_terms,
                    ('call_id', 'from', 'to', 'direction', 'status', 'activity'),
                    agent_directory=agent_directory,
                    queue_directory=queue_directory,
                )
                for journey in journeys_by_call_id.get(call_id, [])
            )
            if not row_matches and not journey_matches:
                continue
        filtered_rows.append(row)

    filtered = dict(manual_import)
    filtered['call_rows'] = filtered_rows
    filtered_call_ids = {str(row.get('call_id') or row.get('fingerprint') or '') for row in filtered_rows if isinstance(row, dict)}
    if isinstance(manual_import.get('journey_rows'), list):
        filtered['journey_rows'] = [
            row for row in manual_import.get('journey_rows') or []
            if isinstance(row, dict) and str(row.get('call_id') or '') in filtered_call_ids
        ]
    if filtered.get('master_report') and isinstance(filtered.get('journey_rows'), list):
        filtered_rows = _apply_3cx_journey_outcomes_to_call_rows(
            filtered_rows,
            filtered.get('journey_rows') or [],
            agent_directory=agent_directory,
            queue_directory=queue_directory,
        )
        filtered['call_rows'] = filtered_rows
    filtered['rows'] = len(filtered_rows)
    filtered['filtered_rows'] = len(filtered_rows)
    filtered['filters_applied'] = True
    filtered.update(
        _aggregate_3cx_call_rows(
            filtered_rows,
            target_calls_per_agent=target_calls_per_agent,
            answer_sla_seconds=answer_sla_seconds,
        )
    )
    if filtered.get('master_report') and isinstance(filtered.get('journey_rows'), list):
        filtered['queue_rows'] = _aggregate_3cx_journey_queue_rows(
            filtered.get('journey_rows') or [],
            answer_sla_seconds=answer_sla_seconds,
            queue_terms=queue_terms,
            agent_directory=agent_directory,
            queue_directory=queue_directory,
        )
    return filtered


def _3cx_ssh_db_config(params: dict[str, object]) -> dict[str, object]:
    return {
        'host': str(params.get('ssh_host') or _DEFAULT_3CX_SSH_HOST).strip(),
        'port': _parse_int_in_range(params.get('ssh_port'), default=_DEFAULT_3CX_SSH_PORT, min_value=1, max_value=65535),
        'user': str(params.get('ssh_user') or _DEFAULT_3CX_SSH_USER).strip(),
        'key_path': str(params.get('ssh_key_path') or _DEFAULT_3CX_SSH_KEY_PATH).strip(),
        'db_name': str(params.get('db_name') or _DEFAULT_3CX_DB_NAME).strip(),
        'sync_days': _parse_int_in_range(params.get('ssh_sync_days'), default=_DEFAULT_3CX_DB_SYNC_DAYS, min_value=1, max_value=3660),
        'sync_start_date': str(params.get('ssh_sync_start_date') or '').strip(),
    }


def _run_3cx_ssh_command_sync(ssh_config: dict[str, object], command: str, *, timeout_seconds: int = 60) -> str:
    key_path = Path(str(ssh_config.get('key_path') or ''))
    if not key_path.exists():
        raise FileNotFoundError(f'3CX SSH key not found: {key_path}')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=str(ssh_config.get('host') or ''),
            port=int(ssh_config.get('port') or 22),
            username=str(ssh_config.get('user') or 'root'),
            key_filename=str(key_path),
            look_for_keys=False,
            allow_agent=False,
            timeout=15,
            banner_timeout=15,
            auth_timeout=15,
        )
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout_seconds)
        del stdin
        out = stdout.read().decode('utf-8', errors='replace')
        err = stderr.read().decode('utf-8', errors='replace')
        status = stdout.channel.recv_exit_status()
        if status != 0:
            raise RuntimeError(f'3CX SSH command failed ({status}): {err.strip() or out.strip()}')
        return out
    finally:
        client.close()


def _fetch_3cx_db_snapshot_sync(
    *,
    params: dict[str, object],
    user_identity: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> dict[str, object]:
    ssh_config = _3cx_ssh_db_config(params)
    athens_tz = ZoneInfo('Europe/Athens')
    today_athens = datetime.now(athens_tz).date()
    sync_days = int(ssh_config.get('sync_days') or _DEFAULT_3CX_DB_SYNC_DAYS)
    configured_start_date = _parse_3cx_filter_date_value(ssh_config.get('sync_start_date'))
    start_date = from_date or configured_start_date or (today_athens - timedelta(days=sync_days - 1))
    end_date = to_date or today_athens
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    start_literal = f'{start_date.isoformat()} 00:00:00+03'
    end_exclusive = end_date + timedelta(days=1)
    end_literal = f'{end_exclusive.isoformat()} 00:00:00+03'
    db_name = str(ssh_config.get('db_name') or _DEFAULT_3CX_DB_NAME)
    answer_sla_seconds = _parse_int_in_range(
        params.get('answer_sla_seconds'),
        default=30,
        min_value=1,
        max_value=3600,
    )
    sql = f"""
WITH bounds AS (
    SELECT
        timestamp with time zone '{start_literal}' AS start_at,
        timestamp with time zone '{end_literal}' AS end_at
),
queue_names AS (
    SELECT dn.value AS q_num, q.name AS q_name
    FROM public.queue q
    JOIN public.dn dn ON dn.iddn = q.fkiddn
),
agent_directory AS (
    SELECT
        uv.dn::text AS code,
        coalesce(nullif(uv.display_name, ''), uv.dn)::text AS label
    FROM public.users_view uv
    WHERE nullif(uv.dn::text, '') IS NOT NULL
),
call_handling_directory AS (
    SELECT
        qv.dn::text AS code,
        coalesce(nullif(qv.display_name, ''), qv.dn)::text AS label
    FROM public.queue_view qv
    WHERE nullif(qv.dn::text, '') IS NOT NULL
    UNION ALL
    SELECT
        dn.value::text AS code,
        (coalesce(nullif(ivr.name, ''), dn.value) || ' (Digital Receptionist)')::text AS label
    FROM public.ivr ivr
    JOIN public.dn dn ON dn.iddn = ivr.fkiddn
    WHERE nullif(dn.value::text, '') IS NOT NULL
    UNION ALL
    SELECT
        rgv.dn::text AS code,
        (coalesce(nullif(rgv.display_name, ''), rgv.dn) || ' (Ring Group)')::text AS label
    FROM public.ring_groups_view rgv
    WHERE nullif(rgv.dn::text, '') IS NOT NULL
),
cdr_base AS (
    SELECT
        coalesce(main_call_history_id, call_history_id)::text AS root_id,
        cdr.*
    FROM public.cdroutput cdr
    CROSS JOIN bounds b
    WHERE cdr.cdr_started_at >= b.start_at
      AND cdr.cdr_started_at < b.end_at
      AND coalesce(main_call_history_id, call_history_id) IS NOT NULL
),
root_calls AS (
    SELECT
        root_id,
        min(cdr_started_at) AS started_at,
        max(cdr_ended_at) AS ended_at,
        count(*)::int AS hop_count,
        bool_or(source_entity_type = 'external_line' AND destination_entity_type IN ('inbound_routing', 'ivr', 'queue', 'extension', 'ring_group_ring_all', 'voicemail')) AS inbound_signal,
        bool_or(source_entity_type = 'extension' AND destination_entity_type IN ('outbound_rule', 'external_line')) AS outbound_signal,
        bool_or(destination_entity_type = 'queue') AS has_queue,
        bool_or(destination_entity_type = 'extension') AS has_extension,
        bool_or(destination_entity_type = 'extension' AND cdr_answered_at IS NOT NULL) AS inbound_human_answered,
        bool_or(source_entity_type = 'extension' AND destination_entity_type = 'external_line' AND cdr_answered_at IS NOT NULL) AS outbound_answered,
        min(cdr_answered_at) FILTER (WHERE destination_entity_type = 'extension' AND cdr_answered_at IS NOT NULL) AS first_extension_answered_at,
        sum(greatest(0, extract(epoch FROM coalesce(cdr_ended_at, cdr_answered_at) - cdr_answered_at)::int)) FILTER (WHERE cdr_answered_at IS NOT NULL AND (destination_entity_type = 'extension' OR destination_entity_type = 'external_line')) AS talk_seconds,
        bool_or(
            creation_method = 'transfer'
            OR (
                creation_method = 'divert'
                AND creation_forward_reason IN ('no_answer', 'out_of_office', 'busy', 'forward_all', 'not_registered')
            )
            OR termination_reason = 'redirected'
        ) AS was_redirected
    FROM cdr_base
    GROUP BY root_id
),
first_hop AS (
    SELECT DISTINCT ON (root_id)
        root_id,
        source_entity_type,
        source_dn_number,
        source_dn_name,
        source_participant_name,
        source_participant_phone_number,
        source_participant_trunk_did,
        source_participant_group_name,
        destination_entity_type,
        destination_dn_number,
        destination_dn_name,
        destination_participant_name,
        destination_participant_phone_number,
        destination_participant_trunk_did,
        destination_participant_group_name
    FROM cdr_base
    ORDER BY root_id, cdr_started_at, cdr_id
),
answered_extension AS (
    SELECT DISTINCT ON (root_id)
        root_id,
        destination_dn_number AS extension,
        destination_dn_name AS agent
    FROM cdr_base
    WHERE destination_entity_type = 'extension'
      AND cdr_answered_at IS NOT NULL
    ORDER BY root_id, cdr_answered_at, cdr_started_at
),
ring_legs AS (
    -- Every extension the call rang at, in order, with whether it picked up.
    -- For a missed call these are the internal extensions that rang and let it drop;
    -- for an answered call the one with answered=true is who actually answered.
    SELECT
        root_id,
        jsonb_agg(jsonb_build_object(
            'ext', coalesce(nullif(destination_dn_name, ''), nullif(destination_dn_number, ''), 'Άγνωστο'),
            'ext_number', nullif(destination_dn_number, ''),
            'answered', cdr_answered_at IS NOT NULL,
            'ring_seconds', greatest(0, extract(epoch FROM coalesce(cdr_answered_at, cdr_ended_at) - cdr_started_at)::int)
        ) ORDER BY cdr_started_at, cdr_id) AS legs,
        count(*)::int AS ring_count
    FROM cdr_base
    WHERE destination_entity_type = 'extension'
    GROUP BY root_id
),
outbound_target AS (
    SELECT DISTINCT ON (root_id)
        root_id,
        source_dn_number AS extension,
        source_dn_name AS agent,
        destination_participant_phone_number AS external_number
    FROM cdr_base
    WHERE source_entity_type = 'extension'
      AND destination_entity_type = 'external_line'
    ORDER BY root_id, cdr_started_at DESC
),
did_by_call AS (
    SELECT DISTINCT ON (root_id)
        root_id AS call_id,
        coalesce(
            nullif(destination_participant_phone_number, ''),
            nullif(destination_dn_number, ''),
            nullif(source_participant_trunk_did, '')
        ) AS called_number
    FROM cdr_base
    WHERE root_id IS NOT NULL
      AND destination_entity_type = 'inbound_routing'
    ORDER BY root_id, cdr_started_at
),
last_q AS (
    SELECT DISTINCT ON (qc.q_num, qc.call_history_id)
        qc.call_history_id::text AS call_id,
        qc.q_num,
        coalesce(qn.q_name, qc.q_num) AS q_name,
        qc.time_start,
        qc.time_end,
        extract(epoch FROM coalesce(qc.ts_waiting, interval '0 seconds'))::int AS wait_seconds,
        extract(epoch FROM coalesce(qc.ts_servicing, interval '0 seconds'))::int AS service_seconds,
        qc.from_userpart,
        qc.from_displayname,
        qc.to_dialednum,
        did.called_number,
        qc.to_dn,
        qc.call_result,
        qc.reason_noanswerdesc,
        qc.reason_faildesc
    FROM public.callcent_queuecalls qc
    LEFT JOIN queue_names qn ON qn.q_num = qc.q_num
    LEFT JOIN did_by_call did ON did.call_id = qc.call_history_id::text
    CROSS JOIN bounds b
    WHERE qc.time_start >= b.start_at
      AND qc.time_start < b.end_at
      AND qc.call_history_id IS NOT NULL
      AND coalesce(qc.is_visible, true)
    ORDER BY qc.q_num, qc.call_history_id, qc.time_end DESC NULLS LAST, qc.time_start DESC
),
primary_queue AS (
    SELECT DISTINCT ON (call_id)
        call_id,
        q_num,
        q_name,
        time_start,
        time_end,
        wait_seconds,
        service_seconds
    FROM last_q
    ORDER BY call_id, time_end DESC NULLS LAST, time_start DESC
),
queue_summary AS (
    SELECT
        q_num,
        q_name,
        count(*)::int AS calls,
        count(*) FILTER (WHERE service_seconds > 0)::int AS answered,
        count(*) FILTER (WHERE service_seconds <= 0)::int AS missed,
        count(*) FILTER (WHERE service_seconds > 0 AND wait_seconds <= {answer_sla_seconds})::int AS sla,
        round(100.0 * count(*) FILTER (WHERE service_seconds > 0) / nullif(count(*), 0), 1) AS answer_pct
    FROM last_q
    GROUP BY q_num, q_name
),
call_payload AS (
    SELECT coalesce(jsonb_agg(jsonb_build_object(
        'date', to_char(rc.started_at AT TIME ZONE 'Europe/Athens', 'YYYY-MM-DD'),
        'raw_call_time', to_char(rc.started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF'),
        'call_id', rc.root_id,
        'queue', CASE
            WHEN pq.q_num IS NOT NULL THEN pq.q_name || ' (' || pq.q_num || ')'
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN 'Outbound'
            ELSE 'Κανάλι: ' || coalesce(
                nullif(btrim(split_part(coalesce(fh.source_participant_name, ''), ':', 2)), ''),
                nullif(fh.source_participant_group_name, ''),
                nullif(fh.source_dn_name, ''),
                nullif(did.called_number, ''),
                nullif(fh.destination_dn_name, ''),
                'Χωρίς κανάλι'
            )
        END,
        'inbound_channel', CASE
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN ''
            ELSE coalesce(
                nullif(btrim(split_part(coalesce(fh.source_participant_name, ''), ':', 2)), ''),
                nullif(fh.source_participant_group_name, ''),
                nullif(fh.source_dn_name, ''),
                nullif(did.called_number, ''),
                nullif(fh.destination_dn_name, ''),
                'Χωρίς κανάλι'
            )
        END,
        'trunk', coalesce(nullif(fh.source_dn_name, ''), nullif(fh.source_dn_number, ''), ''),
        'agent', coalesce(nullif(ae.agent, ''), nullif(ot.agent, ''), nullif(ae.extension, ''), nullif(ot.extension, ''), 'Χωρίς agent'),
        'extension', coalesce(nullif(ae.extension, ''), nullif(ot.extension, ''), ''),
        'source', CASE
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN coalesce(nullif(ot.extension, ''), nullif(fh.source_dn_number, ''), 'Άγνωστη πηγή')
            ELSE coalesce(nullif(fh.source_participant_phone_number, ''), nullif(fh.source_dn_number, ''), 'Άγνωστη πηγή')
        END,
        'did', CASE
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN coalesce(nullif(ot.external_number, ''), nullif(fh.destination_participant_phone_number, ''), '')
            ELSE coalesce(nullif(did.called_number, ''), nullif(fh.destination_participant_phone_number, ''), nullif(fh.destination_dn_number, ''), '')
        END,
        'duration_seconds', greatest(0, extract(epoch FROM coalesce(rc.ended_at, rc.started_at) - rc.started_at)::int),
        'talk_seconds', coalesce(rc.talk_seconds, 0),
        'wait_seconds', CASE
            WHEN pq.call_id IS NOT NULL THEN pq.wait_seconds
            WHEN rc.first_extension_answered_at IS NOT NULL THEN greatest(0, extract(epoch FROM rc.first_extension_answered_at - rc.started_at)::int)
            ELSE 0
        END,
        'is_outbound', rc.outbound_signal AND NOT rc.inbound_signal,
        'is_answered', CASE
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN rc.outbound_answered
            WHEN pq.call_id IS NOT NULL THEN pq.service_seconds > 0
            ELSE rc.inbound_human_answered
        END,
        'is_missed', NOT CASE
            WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN rc.outbound_answered
            WHEN pq.call_id IS NOT NULL THEN pq.service_seconds > 0
            ELSE rc.inbound_human_answered
        END,
        'direction_label', CASE WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN 'Εξ.' ELSE 'Εισ.' END,
        'status_label', CASE
            WHEN CASE
                WHEN rc.outbound_signal AND NOT rc.inbound_signal THEN rc.outbound_answered
                WHEN pq.call_id IS NOT NULL THEN pq.service_seconds > 0
                ELSE rc.inbound_human_answered
            END THEN 'Απαντ.' ELSE 'Χαμένη' END,
        'fingerprint', rc.root_id,
        'journey_legs', rc.hop_count,
        'is_redirected', rc.was_redirected,
        'final_destination', coalesce(pq.q_num, fh.destination_dn_number, ''),
        'ring_path', coalesce(rl.legs, '[]'::jsonb),
        'ring_count', coalesce(rl.ring_count, 0),
        'source_type', '3cx_cdroutput'
    ) ORDER BY rc.started_at), '[]'::jsonb) AS rows
    FROM root_calls rc
    JOIN first_hop fh ON fh.root_id = rc.root_id
    LEFT JOIN answered_extension ae ON ae.root_id = rc.root_id
    LEFT JOIN outbound_target ot ON ot.root_id = rc.root_id
    LEFT JOIN did_by_call did ON did.call_id = rc.root_id
    LEFT JOIN primary_queue pq ON pq.call_id = rc.root_id
    LEFT JOIN ring_legs rl ON rl.root_id = rc.root_id
    WHERE rc.inbound_signal OR rc.outbound_signal
),
queue_payload AS (
    SELECT coalesce(jsonb_agg(jsonb_build_object(
        'queue', q_name || ' (' || q_num || ')',
        'extension', q_num,
        'calls', calls,
        'answered', answered,
        'missed', missed,
        'sla', sla,
        'auto_answered', 0,
        'answer_pct', answer_pct,
        'sla_pct', round(100.0 * sla / nullif(calls, 0), 1)
    ) ORDER BY calls DESC), '[]'::jsonb) AS rows
    FROM queue_summary
),
meta_payload AS (
    SELECT jsonb_build_object(
        'start_date', '{start_date.isoformat()}',
        'end_date', '{end_date.isoformat()}',
        'raw_cdr_hops', (SELECT count(*) FROM cdr_base),
        'raw_queue_events', (SELECT count(*) FROM public.callcent_queuecalls qc CROSS JOIN bounds b WHERE qc.time_start >= b.start_at AND qc.time_start < b.end_at),
        'canonical_calls', (SELECT count(*) FROM root_calls WHERE inbound_signal OR outbound_signal),
        'unique_queue_calls', (SELECT count(*) FROM last_q),
        'queue_count', (SELECT count(*) FROM queue_summary),
        'source_max_time', (SELECT max(time_start) FROM last_q)
    ) AS meta
),
directory_payload AS (
    SELECT jsonb_build_object(
        'agent_directory', coalesce(
            (SELECT jsonb_object_agg(code, label ORDER BY code) FROM agent_directory),
            '{{}}'::jsonb
        ),
        'queue_directory', coalesce(
            (SELECT jsonb_object_agg(code, label ORDER BY code) FROM call_handling_directory),
            '{{}}'::jsonb
        )
    ) AS rows
)
SELECT jsonb_build_object(
    'call_rows', (SELECT rows FROM call_payload),
    'queue_rows', (SELECT rows FROM queue_payload),
    'directory', (SELECT rows FROM directory_payload),
    'meta', (SELECT meta FROM meta_payload)
)::text;
"""
    remote_command = (
        f"sudo -u postgres psql -d {shlex.quote(db_name)} -AtX --set ON_ERROR_STOP=1 "
        f"-c {shlex.quote(sql)}"
    )
    raw_output = _run_3cx_ssh_command_sync(ssh_config, remote_command, timeout_seconds=120).strip()
    if not raw_output:
        raise ValueError('3CX DB returned empty payload.')
    payload = json.loads(raw_output)
    call_rows = [row for row in payload.get('call_rows') or [] if isinstance(row, dict)]
    queue_rows = [row for row in payload.get('queue_rows') or [] if isinstance(row, dict)]
    directory_payload = payload.get('directory') if isinstance(payload.get('directory'), dict) else {}
    auto_agent_directory = (
        directory_payload.get('agent_directory')
        if isinstance(directory_payload.get('agent_directory'), dict)
        else {}
    )
    auto_queue_directory = (
        directory_payload.get('queue_directory')
        if isinstance(directory_payload.get('queue_directory'), dict)
        else {}
    )
    aggregated = _aggregate_3cx_call_rows(call_rows)
    meta = payload.get('meta') if isinstance(payload.get('meta'), dict) else {}
    manual_import: dict[str, object] = {
        'filename': f"3CX DB via SSH ({start_date.isoformat()}..{end_date.isoformat()})",
        'uploaded_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'uploaded_by': user_identity or 'system',
        'source_mode': '3cx_db_ssh',
        'source_sha256': hashlib.sha256(raw_output.encode('utf-8')).hexdigest(),
        'import_fingerprints': [hashlib.sha256(raw_output.encode('utf-8')).hexdigest()],
        'rows': len(call_rows),
        'raw_rows': int(meta.get('raw_cdr_hops') or len(call_rows)),
        'duplicate_rows': 0,
        'internal_rows': 0,
        'master_report': False,
        'db_snapshot': True,
        'excluded_reports': [],
        'sync_period': {'from': start_date.isoformat(), 'to': end_date.isoformat()},
        'source_meta': meta,
        'auto_agent_directory': auto_agent_directory,
        'auto_queue_directory': auto_queue_directory,
        'auto_agent_directory_text': _format_3cx_directory_text(auto_agent_directory),
        'auto_queue_directory_text': _format_3cx_directory_text(auto_queue_directory),
        'call_rows': call_rows,
        'journey_rows': [],
    }
    manual_import.update(aggregated)
    manual_import['queue_rows'] = queue_rows
    return manual_import


def _3cx_call_row_key(row: dict[str, object]) -> str:
    return str(row.get('call_id') or row.get('fingerprint') or '').strip()


def _3cx_snapshot_max_date(snapshot: dict[str, object]) -> date | None:
    max_date: date | None = None
    rows = snapshot.get('call_rows') if isinstance(snapshot.get('call_rows'), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_date = _parse_3cx_filter_date_value(row.get('date'))
        if row_date is not None and (max_date is None or row_date > max_date):
            max_date = row_date
    return max_date


def _merge_3cx_db_snapshots(
    existing: dict[str, object],
    incoming: dict[str, object],
    *,
    params: dict[str, object],
) -> dict[str, object]:
    existing_rows = [row for row in (existing.get('call_rows') or []) if isinstance(row, dict)]
    incoming_rows = [row for row in (incoming.get('call_rows') or []) if isinstance(row, dict)]
    merged_by_key: dict[str, dict[str, object]] = {}
    fallback_index = 0
    for row in existing_rows + incoming_rows:
        key = _3cx_call_row_key(row)
        if not key:
            fallback_index += 1
            key = f'fallback:{fallback_index}'
        merged_by_key[key] = dict(row)
    merged_rows = sorted(
        merged_by_key.values(),
        key=lambda row: (
            str(row.get('raw_call_time') or ''),
            str(row.get('date') or ''),
            _3cx_call_row_key(row),
        ),
    )
    answer_sla_seconds = _parse_int_in_range(
        params.get('answer_sla_seconds'),
        default=30,
        min_value=1,
        max_value=3600,
    )
    target_calls_per_agent = _parse_int_in_range(
        params.get('target_calls_per_agent'),
        default=60,
        min_value=1,
        max_value=1000,
    )
    aggregated = _aggregate_3cx_call_rows(
        merged_rows,
        target_calls_per_agent=target_calls_per_agent,
        answer_sla_seconds=answer_sla_seconds,
    )
    incoming_meta = incoming.get('source_meta') if isinstance(incoming.get('source_meta'), dict) else {}
    existing_period = existing.get('sync_period') if isinstance(existing.get('sync_period'), dict) else {}
    incoming_period = incoming.get('sync_period') if isinstance(incoming.get('sync_period'), dict) else {}
    configured_start = str(params.get('ssh_sync_start_date') or '').strip()
    period_from = configured_start or str(existing_period.get('from') or incoming_period.get('from') or '')
    period_to = str(incoming_period.get('to') or existing_period.get('to') or '')
    merged: dict[str, object] = {
        **incoming,
        'filename': f"3CX DB incremental ({period_from or '-'}..{period_to or '-'})",
        'uploaded_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'source_mode': '3cx_db_ssh',
        'source_sha256': hashlib.sha256(json.dumps(merged_rows, sort_keys=True, default=str).encode('utf-8')).hexdigest(),
        'import_fingerprints': [hashlib.sha256(json.dumps(merged_rows, sort_keys=True, default=str).encode('utf-8')).hexdigest()],
        'rows': len(merged_rows),
        'duplicate_rows': max(0, len(existing_rows) + len(incoming_rows) - len(merged_rows)),
        'sync_period': {'from': period_from, 'to': period_to},
        'source_meta': {
            **incoming_meta,
            'incremental': True,
            'previous_rows': len(existing_rows),
            'incoming_rows': len(incoming_rows),
            'merged_rows': len(merged_rows),
            'overlap_duplicate_rows': max(0, len(existing_rows) + len(incoming_rows) - len(merged_rows)),
        },
        'call_rows': merged_rows,
        'journey_rows': [],
    }
    merged.update(aggregated)
    merged['queue_rows'] = aggregated.get('queue_rows') if isinstance(aggregated.get('queue_rows'), list) else []
    # Raw CDR hop counts overlap during incremental sync, so keep the best known snapshot value
    # instead of adding the overlap on every 5-minute run.
    merged['raw_rows'] = max(int(existing.get('raw_rows') or 0), int(incoming.get('raw_rows') or 0), len(merged_rows))
    return merged


async def _sync_3cx_db_snapshot(
    db: AsyncSession,
    *,
    tenant_id: int,
    user_identity: str,
    from_date: date | None = None,
    to_date: date | None = None,
    incremental: bool = False,
) -> dict[str, object]:
    conn = await _find_tenant_connection(db, tenant_id=tenant_id, connector_type=_3CX_CONNECTOR_TYPE)
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type=_3CX_CONNECTOR_TYPE,
            source_type='ssh_db',
            is_active=True,
            supported_streams=['call_center_kpis'],
            enabled_streams=['call_center_kpis'],
        )
        db.add(conn)
        await db.flush()
    params = dict(conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {})
    params.setdefault('ssh_host', _DEFAULT_3CX_SSH_HOST)
    params.setdefault('ssh_port', _DEFAULT_3CX_SSH_PORT)
    params.setdefault('ssh_user', _DEFAULT_3CX_SSH_USER)
    params.setdefault('ssh_key_path', _DEFAULT_3CX_SSH_KEY_PATH)
    params.setdefault('db_name', _DEFAULT_3CX_DB_NAME)
    params.setdefault('ssh_sync_days', _DEFAULT_3CX_DB_SYNC_DAYS)
    params.setdefault('agent_directory_text', _DEFAULT_3CX_AGENT_DIRECTORY_TEXT)
    params.setdefault('queue_directory_text', _DEFAULT_3CX_QUEUE_DIRECTORY_TEXT)
    params.setdefault('answer_sla_seconds', 30)
    params.setdefault('target_calls_per_agent', 60)
    existing_manual_import = params.get('manual_import') if isinstance(params.get('manual_import'), dict) else {}
    incremental_from_date = from_date
    if incremental and from_date is None and existing_manual_import:
        latest_existing_date = _3cx_snapshot_max_date(existing_manual_import)
        if latest_existing_date is not None:
            configured_start = _parse_3cx_filter_date_value(params.get('ssh_sync_start_date'))
            incremental_from_date = latest_existing_date - timedelta(days=1)
            if configured_start is not None and incremental_from_date < configured_start:
                incremental_from_date = configured_start
    fetched_import = await asyncio.to_thread(
        _fetch_3cx_db_snapshot_sync,
        params=params,
        user_identity=user_identity,
        from_date=incremental_from_date,
        to_date=to_date,
    )
    manual_import = (
        _merge_3cx_db_snapshots(existing_manual_import, fetched_import, params=params)
        if incremental and existing_manual_import
        else fetched_import
    )
    params['manual_import'] = manual_import
    params['source_mode'] = '3cx_db_ssh'
    params['import_mode'] = 'ssh_db_snapshot'
    conn.connection_parameters = params
    conn.source_type = 'ssh_db'
    conn.is_active = True
    conn.sync_status = 'ok'
    conn.last_sync_at = datetime.utcnow()
    conn.last_test_ok_at = datetime.utcnow()
    conn.last_test_error = None
    flag_modified(conn, 'connection_parameters')
    db.add(conn)
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='tenant_3cx_ssh_db_synced',
            entity_type='tenant_connection',
            entity_id=str(conn.id),
            payload={
                'filename': manual_import.get('filename'),
                'rows': manual_import.get('rows'),
                'raw_rows': manual_import.get('raw_rows'),
                'stats': manual_import.get('stats'),
                'source_mode': manual_import.get('source_mode'),
            },
        )
    )
    await db.commit()
    return manual_import


async def _run_3cx_manual_sync_check(db: AsyncSession, tenant: Tenant) -> dict[str, object]:
    conn = await _find_tenant_connection(db, tenant_id=tenant.id, connector_type=_3CX_CONNECTOR_TYPE)
    if conn is None or not conn.is_active:
        return {'ok': False, 'message': 'Δεν υπάρχει ενεργή ρύθμιση 3CX για τον tenant.'}
    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    try:
        manual_import = await _sync_3cx_db_snapshot(
            db,
            tenant_id=tenant.id,
            user_identity='manual_sync',
        )
    except Exception as exc:
        logger.exception('tenant_3cx_ssh_db_sync_failed', extra={'tenant_id': tenant.id})
        message = f'Αποτυχία SSH 3CX DB sync: {exc.__class__.__name__}'
        conn.sync_status = 'failed'
        conn.last_test_error = message
        await db.commit()
        return {'ok': False, 'message': message}

    stats = manual_import.get('stats') if isinstance(manual_import.get('stats'), dict) else {}
    return {
        'ok': True,
        'message': (
            f"3CX DB sync OK: {int(stats.get('total_calls') or 0)} κλήσεις, "
            f"{int(stats.get('answered_calls') or 0)} answered, "
            f"{int(stats.get('missed_calls') or 0)} missed."
        ),
    }


def _save_era_exploration_upload(
    tenant: Tenant,
    upload: UploadFile,
    user_identity: str,
) -> tuple[dict[str, object] | None, str | None]:
    uploaded_name = Path(upload.filename or '').name
    if not uploaded_name.lower().endswith('.xlsx'):
        return None, 'era_file_type'
    upload_dir = Path('/opt/cloudon-bi/uploads/tenants') / tenant.slug / 'era_exploration'
    upload_dir.mkdir(parents=True, exist_ok=True)
    target_path = upload_dir / 'current.xlsx'
    tmp_path = upload_dir / f'.{int(time.time())}.uploading.xlsx'
    try:
        with tmp_path.open('wb') as out_file:
            shutil.copyfileobj(upload.file, out_file)
        validation = validate_era_exploration_file(tmp_path)
        checksum = era_file_sha256(tmp_path)
        period = str(validation.get('period') or era_period_from_filename(uploaded_name) or 'unknown')
        archive_path = upload_dir / f'{period}.xlsx'
        tmp_path.replace(archive_path)
        shutil.copyfile(archive_path, target_path)
        clear_era_exploration_cache()
        return {
            'file_path': str(target_path),
            'archive_path': str(archive_path),
            'filename': uploaded_name,
            'uploaded_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
            'uploaded_by': user_identity or 'tenant',
            'period': period,
            'source_sha256': checksum,
            'rows': int(validation.get('rows') or 0),
            'brands': int(validation.get('brands') or 0),
            'categories': int(validation.get('categories') or 0),
        }, None
    except ValueError as exc:
        logger.warning('era_exploration_upload_invalid', extra={'tenant_id': tenant.id, 'reason': str(exc)})
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, 'era_file_invalid'
    except Exception:
        logger.exception('era_exploration_upload_failed', extra={'tenant_id': tenant.id})
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, 'era_file_upload_failed'


def _save_iqvia_upload(
    tenant: Tenant,
    upload: UploadFile,
    user_identity: str,
) -> tuple[dict[str, object] | None, str | None]:
    uploaded_name = Path(upload.filename or '').name
    if not uploaded_name.lower().endswith('.xlsx'):
        return None, 'iqvia_file_type'
    upload_dir = Path('/opt/cloudon-bi/uploads/tenants') / tenant.slug / 'iqvia'
    upload_dir.mkdir(parents=True, exist_ok=True)
    target_path = upload_dir / 'current.xlsx'
    tmp_path = upload_dir / f'.{int(time.time())}.uploading.xlsx'
    try:
        with tmp_path.open('wb') as out_file:
            shutil.copyfileobj(upload.file, out_file)
        validation = validate_iqvia_file(tmp_path)
        checksum = iqvia_file_sha256(tmp_path)
        period = str(validation.get('period') or iqvia_period_from_filename(uploaded_name) or 'unknown')
        archive_path = upload_dir / f'{period}.xlsx'
        tmp_path.replace(archive_path)
        shutil.copyfile(archive_path, target_path)
        clear_iqvia_cache()
        return {
            'file_path': str(target_path),
            'archive_path': str(archive_path),
            'filename': uploaded_name,
            'uploaded_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
            'uploaded_by': user_identity or 'tenant',
            'period': period,
            'source_sha256': checksum,
            'rows': int(validation.get('rows') or 0),
            'categories': int(validation.get('categories') or 0),
            'manufacturers': int(validation.get('manufacturers') or 0),
            'territories': int(validation.get('territories') or 0),
        }, None
    except ValueError as exc:
        logger.warning('iqvia_upload_invalid', extra={'tenant_id': tenant.id, 'reason': str(exc)})
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, 'iqvia_file_invalid'
    except Exception:
        logger.exception('iqvia_upload_failed', extra={'tenant_id': tenant.id})
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, 'iqvia_file_upload_failed'


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


def _apply_docker_df_row(result: dict[str, Any], row: dict[str, Any]) -> None:
    typ = str(row.get('Type') or row.get('type') or '').lower()
    size = str(row.get('Size') or row.get('size') or '')
    reclaimable = str(row.get('Reclaimable') or row.get('reclaimable') or '')
    if typ == 'images':
        result['images_size'] = size
        result['images_reclaimable'] = reclaimable
    elif typ == 'local volumes':
        result['volumes_size'] = size
    elif typ == 'build cache':
        result['build_cache_size'] = size


def _docker_cache_status() -> dict[str, Any]:
    result: dict[str, Any] = {
        'available': False,
        'images_size': '',
        'images_reclaimable': '',
        'volumes_size': '',
        'build_cache_size': '',
        'next_run': 'Κυριακή 03:30 UTC (+ έως 30λ.)',
        'timer_active': False,
        'last_log': '',
        'error': '',
    }
    docker_bin = shutil.which('docker')
    if not docker_bin:
        result['error'] = 'docker_not_found'
    else:
        try:
            proc = subprocess.run(
                [docker_bin, 'system', 'df', '--format', 'json'],
                check=False,
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0:
                result['available'] = True
                for line in proc.stdout.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    _apply_docker_df_row(result, row)
            else:
                result['error'] = (proc.stderr or proc.stdout or 'docker_system_df_failed').strip()[:240]
        except Exception as exc:
            result['error'] = str(exc)[:240]

    if not result['available']:
        df_snapshot = Path('/opt/cloudon-bi/artifacts/ops/docker_cleanup_df.jsonl')
        if df_snapshot.exists():
            try:
                for line in df_snapshot.read_text(encoding='utf-8', errors='ignore').splitlines():
                    if not line.strip():
                        continue
                    _apply_docker_df_row(result, json.loads(line))
                result['available'] = bool(result['images_size'] or result['volumes_size'] or result['build_cache_size'])
                result['error'] = ''
            except Exception:
                pass

    systemctl_bin = shutil.which('systemctl')
    if systemctl_bin:
        try:
            timer = subprocess.run(
                [systemctl_bin, 'show', 'cloudon-docker-cleanup.timer', '--property=ActiveState,NextElapseUSecRealtime', '--no-pager'],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if timer.returncode == 0:
                for line in timer.stdout.splitlines():
                    key, _, value = line.partition('=')
                    if key == 'ActiveState':
                        result['timer_active'] = value.strip() == 'active'
                    elif key == 'NextElapseUSecRealtime':
                        result['next_run'] = value.strip()
            if not result['timer_active'] and Path('/opt/cloudon-bi/infra/systemd/cloudon-docker-cleanup.timer').exists():
                result['timer_active'] = True
        except Exception:
            if Path('/opt/cloudon-bi/infra/systemd/cloudon-docker-cleanup.timer').exists():
                result['timer_active'] = True
    elif Path('/opt/cloudon-bi/infra/systemd/cloudon-docker-cleanup.timer').exists():
        result['timer_active'] = True

    log_path = Path('/opt/cloudon-bi/artifacts/ops/docker_cleanup.prom')
    if log_path.exists():
        try:
            result['last_log'] = log_path.read_text(encoding='utf-8', errors='ignore')[-1000:]
        except Exception:
            result['last_log'] = ''
    return result


def _normalize_slug(raw: str) -> str:
    val = (raw or '').strip().lower()
    val = re.sub(r'[^a-z0-9-]+', '-', val)
    val = re.sub(r'-{2,}', '-', val).strip('-')
    return val


_PLAN_FEATURE_STATUSES = {'none', 'included', 'extra', 'custom'}


def _normalize_feature_key(raw: str, fallback: str) -> str:
    source = raw or fallback
    val = (source or '').strip().lower()
    val = re.sub(r'[^a-z0-9_ -]+', '', val)
    val = re.sub(r'[\s-]+', '_', val).strip('_')
    val = re.sub(r'_{2,}', '_', val)
    if not val:
        val = 'custom_feature'
    if val[0].isdigit():
        val = f'f_{val}'
    return val[:64]


def _catalog_status(row: PlanFeatureCatalog, plan: PlanName) -> str:
    statuses = row.plan_status if isinstance(row.plan_status, dict) else {}
    status = str(statuses.get(plan.value) or 'none').strip().lower()
    return status if status in _PLAN_FEATURE_STATUSES else 'none'


def _catalog_feature_allowed(row: PlanFeatureCatalog, plan: PlanName, tenant_id: int | None = None) -> bool:
    if row.tenant_id is not None and int(row.tenant_id) != int(tenant_id or 0):
        return False
    if row.tenant_id is not None and plan != PlanName.custom:
        return False
    return _catalog_status(row, plan) in {'included', 'extra', 'custom'}


def _catalog_feature_default(row: PlanFeatureCatalog, plan: PlanName, tenant_id: int | None = None) -> bool:
    if not _catalog_feature_allowed(row, plan, tenant_id):
        return False
    return _catalog_status(row, plan) in {'included', 'custom'}


def _catalog_feature_is_addon(row: PlanFeatureCatalog) -> bool:
    statuses = row.plan_status if isinstance(row.plan_status, dict) else {}
    if row.tenant_id is not None:
        return True
    return any(str(value).strip().lower() == 'extra' for value in statuses.values())


def _catalog_minimum_plan(row: PlanFeatureCatalog) -> str:
    if row.tenant_id is not None:
        return 'Custom υλοποίηση συγκεκριμένου πελάτη'
    statuses = row.plan_status if isinstance(row.plan_status, dict) else {}
    labels = {'standard': 'Standard', 'pro': 'Pro', 'enterprise': 'Enterprise', 'custom': 'Custom'}
    enabled = [labels[key] for key, value in statuses.items() if str(value).strip().lower() in {'included', 'extra', 'custom'} and key in labels]
    return ', '.join(enabled) if enabled else 'Δεν έχει ενεργοποιηθεί σε πλάνο'


def _catalog_to_subscription_feature(row: PlanFeatureCatalog) -> SubscriptionFeature:
    return SubscriptionFeature(
        key=row.feature_key,
        label=row.label,
        group=row.group or ('Custom πελάτη' if row.tenant_id is not None else 'Custom'),
        menu_keys=(),
        path_prefixes=(),
        default_standard=_catalog_feature_default(row, PlanName.standard),
        default_pro=_catalog_feature_default(row, PlanName.pro),
        default_enterprise=_catalog_feature_default(row, PlanName.enterprise),
        default_custom=_catalog_feature_default(row, PlanName.custom),
        minimum_plan=_catalog_minimum_plan(row),
        addon=_catalog_feature_is_addon(row),
    )


def _normalize_catalog_status(raw: str) -> str:
    status = (raw or 'none').strip().lower()
    return status if status in _PLAN_FEATURE_STATUSES else 'none'


def _normalize_sub_status(raw: str) -> str:
    return (raw or '').strip().lower()


def _normalize_tenant_status(raw: str) -> str:
    return (raw or '').strip().lower()


def _tenant_feature_flags(tenant: Tenant) -> dict[str, bool]:
    from app.services.plan_rules import is_feature_enabled
    return {
        'inventory_enabled': is_feature_enabled(tenant, 'inventory'),
        'cashflow_enabled': is_feature_enabled(tenant, 'cashflows'),
        'supplier_targets_enabled': is_feature_enabled(tenant, 'supplier_targets'),
        'replenishment_enabled': is_feature_enabled(tenant, 'replenishment'),
        'supplier_orders_enabled': is_feature_enabled(tenant, 'supplier_orders'),
    }


async def _schedule_document_rule_refresh(
    *,
    db: AsyncSession,
    tenant_ids: list[int],
    stream: OperationalStream,
) -> None:
    if not tenant_ids:
        return
    entity = STREAM_TO_ENTITY.get(stream.value)
    for tenant_id in sorted({int(x) for x in tenant_ids if x}):
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if tenant is None:
            continue
        await invalidate_tenant_cache(str(tenant.id))
        if entity:
            try:
                celery_client.send_task(
                    'worker.tasks.refresh_aggregates_for_entity',
                    args=[tenant.slug, entity, None, None],
                )
            except Exception:
                logger.exception(
                    'document_rule_refresh_enqueue_failed',
                    extra={'tenant_id': tenant.id, 'tenant_slug': tenant.slug, 'stream': stream.value},
                )


async def _tenant_navigation_context(tenant: Tenant) -> dict[str, bool | int | str | None]:
    branch_count = 0
    last_sync_at: datetime | None = None
    last_sync_utc: datetime | None = None
    last_sync_greece: datetime | None = None
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
    try:
        async with ControlSessionLocal() as control_db:
            # "Last sync" must reflect the tenant's ACTUAL freshest data pull. Pick the active
            # connection with the most recent last_sync_at regardless of source type — the old
            # code preferred source_type='sql', so an API tenant with a stray/stale SQL connector
            # showed a stale timestamp even though the API connector had just synced.
            connection = (
                await control_db.execute(
                    select(TenantConnection)
                    .where(
                        TenantConnection.tenant_id == tenant.id,
                        TenantConnection.is_active.is_(True),
                    )
                    .order_by(TenantConnection.last_sync_at.desc().nullslast(), TenantConnection.updated_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            last_sync_at = connection.last_sync_at if connection is not None else None
    except Exception:
        logger.exception('tenant_navigation_sync_context_failed', extra={'tenant_id': tenant.id})
        last_sync_at = None
    if last_sync_at is not None:
        last_sync_utc = (
            last_sync_at.replace(tzinfo=timezone.utc)
            if last_sync_at.tzinfo is None
            else last_sync_at.astimezone(timezone.utc)
        )
        last_sync_greece = last_sync_utc.astimezone(GREECE_TZ)
    last_sync_display = last_sync_greece.strftime('%d/%m/%Y %H:%M') if last_sync_greece else 'Μη διαθέσιμο'
    last_sync_title = (
        f'Τελευταίος συγχρονισμός: {last_sync_greece.strftime("%d/%m/%Y %H:%M:%S")} (ώρα Ελλάδας)'
        if last_sync_greece
        else 'Δεν έχει καταγραφεί ακόμη συγχρονισμός'
    )
    return {
        **_tenant_feature_flags(tenant),
        'tenant_branch_count': branch_count,
        'tenant_has_multiple_branches': branch_count > 1,
        'tenant_softone_last_sync_at': last_sync_greece,
        'tenant_softone_last_sync_iso': last_sync_utc.isoformat() if last_sync_utc else None,
        'tenant_softone_last_sync_display': last_sync_display,
        'tenant_softone_last_sync_title': last_sync_title,
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


def _stringify_options_map(options: dict | None) -> str:
    if not isinstance(options, dict) or not options:
        return 'Encrypt=yes;TrustServerCertificate=yes'
    chunks: list[str] = []
    for key, value in options.items():
        k = str(key or '').strip()
        v = str(value or '').strip()
        if k:
            chunks.append(f'{k}={v}')
    return ';'.join(chunks) if chunks else 'Encrypt=yes;TrustServerCertificate=yes'


async def _find_tenant_connection(
    db: AsyncSession,
    *,
    tenant_id: int,
    connector_type: str,
) -> TenantConnection | None:
    selected_connector = str(connector_type or '').strip().lower() or 'sql_connector'
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == selected_connector,
            )
        )
    ).scalar_one_or_none()
    if conn is None and selected_connector == 'sql_connector':
        conn = (
            await db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type == 'pharmacyone_sql',
                )
            )
        ).scalar_one_or_none()
    elif conn is None and selected_connector == 'pharmacyone_sql':
        conn = (
            await db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type == 'sql_connector',
                )
            )
        ).scalar_one_or_none()
    return conn


def _resolve_secret_password(password_input: str | None, conn: TenantConnection | None) -> str:
    provided = str(password_input or '')
    if provided.strip():
        return provided
    if not conn or not conn.enc_payload:
        return ''
    try:
        secret = decrypt_sqlserver_secret(conn.enc_payload)
    except Exception:
        return ''
    return str(secret.password or '')


def _to_int_or_none(raw: object) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


_STREAM_LABEL_KEYS: list[tuple[str, str]] = [
    ('sales_documents', 'sales_documents_menu'),
    ('purchase_documents', 'purchases_documents_menu'),
    ('inventory_documents', 'warehouse_documents_menu'),
    ('cash_transactions', 'cash_transactions_menu'),
    ('operating_expenses', 'operating_expenses_menu'),
    ('supplier_orders', 'supplier_orders_menu'),
    ('supplier_balances', 'supplier_open_balances_menu'),
    ('customer_balances', 'customer_open_balances_menu'),
]

_BUSINESS_RULE_STREAM_LABEL_BY_VALUE: dict[str, str] = {
    value: label_key for value, label_key in _STREAM_LABEL_KEYS
}

_RULE_DOMAIN_LABEL_BY_VALUE: dict[str, str] = {
    RuleDomain.document_type_rules.value: 'Κανόνες Τύπων Παραστατικών',
    RuleDomain.source_mapping.value: 'Κανόνες Κυκλωμάτων / Query Rules',
    RuleDomain.kpi_participation_rules.value: 'Κανόνες Συμμετοχής KPI',
    RuleDomain.intelligence_threshold_rules.value: 'Κανόνες Insights',
}

_DOCUMENT_RULE_STREAMS: list[dict[str, str]] = [
    {'value': OperationalStream.sales_documents.value, 'label': 'Πωλήσεις'},
    {'value': OperationalStream.purchase_documents.value, 'label': 'Αγορές'},
    {'value': OperationalStream.inventory_documents.value, 'label': 'Αποθήκη'},
    {'value': OperationalStream.cash_transactions.value, 'label': 'Ταμείο'},
]

_CONTROL_OPERATIONAL_STREAM_VALUES: set[str] = {item.value for item in OperationalStream}

_DOCUMENT_SIGN_OPTIONS: list[dict[str, str]] = [
    {'value': 'positive', 'label': 'Θετικό'},
    {'value': 'negative', 'label': 'Αρνητικό'},
    {'value': 'none', 'label': 'Κανένα'},
]

_DOCUMENT_SIGN_LABEL: dict[str, str] = {item['value']: item['label'] for item in _DOCUMENT_SIGN_OPTIONS}

_SOFTONE_DOCUMENT_RULE_TEMPLATES: list[dict[str, object]] = [
    {
        'behavior_code': '102',
        'behavior_label': 'Τιμολόγιο πώλησης',
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
        'behavior_code': '131',
        'behavior_label': 'Απόδειξη λιανικής',
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
        'behavior_code': '151',
        'behavior_label': 'Πιστωτικό τιμολόγιο επιστροφής',
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
        'behavior_code': '1251',
        'behavior_label': 'Αγορές προμηθευτών',
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
        'behavior_code': '1281',
        'behavior_label': 'Πιστωτικό αγοράς / επιστροφή',
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
        'behavior_code': '101',
        'behavior_label': 'Δελτίο αποστολής',
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
        'behavior_code': '154',
        'behavior_label': 'Δελτίο επιστροφής',
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
        'behavior_code': '1381',
        'behavior_label': 'Είσπραξη πελάτη',
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
        'behavior_code': '1281',
        'behavior_label': 'Πληρωμή προμηθευτή',
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


def _softone_behavior_catalog() -> dict[tuple[str, str], dict[str, str]]:
    out: dict[tuple[str, str], dict[str, str]] = {}
    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        stream = str(item.get('stream') or '').strip()
        behavior_code_raw = str(item.get('behavior_code') or '').strip()
        behavior_code = re.sub(r'[^A-Za-z0-9_-]+', '', behavior_code_raw)[:32]
        if not stream or not behavior_code:
            continue
        out[(stream, behavior_code)] = {
            'document_type': str(item.get('document_type') or '').strip(),
            'behavior_label': str(item.get('behavior_label') or '').strip(),
        }
    return out


_SOFTONE_BEHAVIOR_CATALOG: dict[tuple[str, str], dict[str, str]] = _softone_behavior_catalog()


def _softone_document_type_options() -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        name = str(item.get('document_type') or '').strip()
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(name)
    return ordered


def _softone_document_options() -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        document_type = str(item.get('document_type') or '').strip()
        behavior_code = _normalize_behavior_code(item.get('behavior_code'))
        behavior_label = str(item.get('behavior_label') or '').strip()
        stream_value = str(item.get('stream') or '').strip()
        if not document_type:
            continue
        key = (document_type.casefold(), behavior_code, stream_value)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'document_type': document_type,
                'behavior_code': behavior_code,
                'behavior_label': behavior_label,
                'stream_value': stream_value,
                'stream_label': _doc_stream_label(stream_value),
            }
        )
    out.sort(key=lambda x: (x['stream_label'], x['document_type']))
    return out


def _softone_canonical_names(
    *,
    stream_value: str,
    behavior_code: str,
    document_type: str,
    behavior_label: str,
) -> tuple[str, str]:
    catalog_row = _SOFTONE_BEHAVIOR_CATALOG.get((stream_value, behavior_code))
    if catalog_row is None:
        return document_type, behavior_label
    canonical_doc_type = str(catalog_row.get('document_type') or '').strip() or document_type
    canonical_behavior_label = str(catalog_row.get('behavior_label') or '').strip() or behavior_label
    return canonical_doc_type, canonical_behavior_label


def _tenant_document_ruleset_code(tenant: Tenant | None) -> str:
    if tenant is None:
        return ''
    raw = tenant.feature_flags
    if not isinstance(raw, dict):
        return ''
    return str(raw.get('document_type_ruleset_code') or '').strip()


def _to_bool_flag(raw: object) -> bool:
    txt = str(raw or '').strip().lower()
    return txt in {'1', 'true', 'yes', 'on', 'ναι'}


def _normalize_behavior_code(raw: object) -> str:
    txt = str(raw or '').strip()
    if not txt:
        return ''
    cleaned = re.sub(r'[^A-Za-z0-9_-]+', '', txt)
    return cleaned[:32]


def _normalize_sign(raw: object, *, default: str = 'none') -> str:
    txt = str(raw or '').strip().lower()
    if txt in {'positive', 'pos', 'plus', '1', '+1'}:
        return 'positive'
    if txt in {'negative', 'neg', 'minus', '-1'}:
        return 'negative'
    if txt in {'none', '0', '', 'neutral'}:
        return 'none'
    return default


def _infer_behavior_code_from_payload_or_key(payload: dict[str, object], rule_key: str) -> str:
    candidates = [
        str(payload.get('source_document_type_code') or '').strip(),
        str(payload.get('document_type') or '').strip(),
        str(rule_key or '').strip(),
    ]
    for txt in candidates:
        if not txt:
            continue
        m = re.search(r'(?i)(?:sales|purchase|inventory)[^0-9]*_([0-9]{2,6})', txt)
        if m:
            return _normalize_behavior_code(m.group(1))
        m2 = re.search(r'(?i)BHV[_-]?([A-Za-z0-9-]{1,32})', txt)
        if m2:
            return _normalize_behavior_code(m2.group(1))
    return ''


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


def _document_rule_key(document_type: str, stream: str, behavior_code: str | None = None) -> str:
    stream_code = re.sub(r'[^A-Za-z0-9]+', '_', str(stream or '').upper()).strip('_') or 'STREAM'
    behavior = _normalize_behavior_code(behavior_code)
    behavior_part = f'_BHV_{behavior}' if behavior else ''
    doc_code = re.sub(r'\W+', '_', str(document_type or '').strip().upper(), flags=re.UNICODE).strip('_') or 'DOC'
    return f'DOC_RULE_{stream_code}{behavior_part}_{doc_code}'[:128]


def _build_document_rule_payload(
    *,
    behavior_code: str,
    behavior_label: str | None,
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
    behavior_code_norm = _normalize_behavior_code(behavior_code)
    behavior_label_norm = str(behavior_label or '').strip()
    return {
        'behavior_code': behavior_code_norm,
        'behavior_label': behavior_label_norm or None,
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
    behavior_code = _normalize_behavior_code(
        payload.get('behavior_code') or payload.get('softone_behavior') or payload.get('source_transaction_type_id')
    )
    if not behavior_code:
        behavior_code = _infer_behavior_code_from_payload_or_key(payload, rule_key)
    amount_sign = _payload_sign(payload, ['amount_sign_label', 'amount_sign', 'sign'], default='none')
    quantity_sign = _payload_sign(payload, ['quantity_sign_label', 'quantity_sign', 'qty_sign'], default='none')
    return {
        'behavior_code': behavior_code,
        'behavior_label': str(payload.get('behavior_label') or '').strip(),
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
    behavior_code = str(parsed['behavior_code'] or '')
    behavior_label = str(parsed['behavior_label'] or '')
    if behavior_code and not behavior_label:
        behavior_meta = _SOFTONE_BEHAVIOR_CATALOG.get((stream, behavior_code), {})
        behavior_label = str(behavior_meta.get('behavior_label') or '').strip()
    return {
        'scope': scope,
        'scope_label': scope_label,
        'ruleset_code': ruleset_code,
        'stream': stream,
        'stream_label': stream_label,
        'rule_key': rule_key,
        'behavior_code': behavior_code,
        'behavior_label': behavior_label,
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
        'item_master': str((form_values.get('stream_query_mapping') or {}).get('item_master') or ''),
        'cash_transactions': str(form_values.get('cashflow_query_template') or ''),
        'supplier_balances': str(form_values.get('supplier_balances_query_template') or ''),
        'customer_balances': str(form_values.get('customer_balances_query_template') or ''),
        'supplier_orders': str((form_values.get('stream_query_mapping') or {}).get('supplier_orders') or ''),
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
            'deleted': request.query_params.get('deleted') == '1',
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


def _sum_sign_to_label(value: object) -> str:
    try:
        num = float(value or 0)
    except Exception:
        return 'none'
    if num > 0:
        return 'positive'
    if num < 0:
        return 'negative'
    return 'none'


def _cash_type_to_softone_label(value: object) -> str:
    txt = str(value or '').strip()
    lookup = {
        'customer_collections': 'Εισπράξεις Πελατών',
        'customer_transfers': 'Μεταφορές Πελατών',
        'supplier_payments': 'Πληρωμές Προμηθευτών',
        'supplier_transfers': 'Μεταφορές Προμηθευτών',
        'financial_accounts': 'Εσωτερικές Μεταφορές',
    }
    return lookup.get(txt, txt or 'Κίνηση Ταμείου')


async def _discover_tenant_softone_rules(
    tenant_slug: str,
    *,
    db_name: str,
    db_user: str,
    db_password: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    async for tenant_db in get_tenant_db_session(tenant_slug, db_name, db_user, db_password):
        sales_rows = (
            await tenant_db.execute(
                text(
                    """
                    SELECT
                        NULLIF(BTRIM(COALESCE(f.source_payload_json->>'source_transaction_type_id', '')), '') AS behavior_code,
                        COALESCE(NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_series_name', '')), ''), '') AS softone_name,
                        COALESCE(
                            NULLIF(BTRIM(f.document_type), ''),
                            NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_type', '')), ''),
                            'sales_documents'
                        ) AS document_type_code,
                        COUNT(*)::int AS row_count,
                        COALESCE(SUM(f.net_value), 0)::numeric AS total_amount,
                        COALESCE(SUM(f.qty), 0)::numeric AS total_qty,
                        COALESCE(SUM(f.cost_amount), 0)::numeric AS total_cost,
                        SUM(CASE WHEN NULLIF(BTRIM(COALESCE(f.customer_code, '')), '') IS NOT NULL THEN 1 ELSE 0 END)::int AS customer_hits
                    FROM fact_sales f
                    GROUP BY 1, 2, 3
                    ORDER BY COUNT(*) DESC
                    LIMIT 500
                    """
                )
            )
        ).mappings().all()
        for row in sales_rows:
            behavior_code = str(row.get('behavior_code') or '').strip()
            behavior_meta = _SOFTONE_BEHAVIOR_CATALOG.get((OperationalStream.sales_documents.value, behavior_code), {})
            softone_name = str(row.get('softone_name') or '').strip()
            code_name = str(row.get('document_type_code') or '').strip()
            rows.append(
                {
                    'stream': OperationalStream.sales_documents.value,
                    'stream_label': _doc_stream_label(OperationalStream.sales_documents.value),
                    'behavior_code': behavior_code,
                    'behavior_label': str(behavior_meta.get('behavior_label') or '').strip(),
                    'document_type': softone_name or code_name,
                    'source_document_type_code': code_name,
                    'include_revenue': True,
                    'include_quantity': True,
                    'include_cost': True,
                    'affects_customer_balance': bool((row.get('customer_hits') or 0) > 0),
                    'affects_supplier_balance': False,
                    'amount_sign': _sum_sign_to_label(row.get('total_amount')),
                    'quantity_sign': _sum_sign_to_label(row.get('total_qty')),
                    'row_count': int(row.get('row_count') or 0),
                }
            )

        purchases_rows = (
            await tenant_db.execute(
                text(
                    """
                    SELECT
                        COALESCE(NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_series_name', '')), ''), '') AS softone_name,
                        COALESCE(
                            NULLIF(BTRIM(f.document_type), ''),
                            NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_type', '')), ''),
                            'purchase_documents'
                        ) AS document_type_code,
                        COUNT(*)::int AS row_count,
                        COALESCE(SUM(f.net_value), 0)::numeric AS total_amount,
                        COALESCE(SUM(f.qty), 0)::numeric AS total_qty,
                        COALESCE(SUM(f.cost_amount), 0)::numeric AS total_cost,
                        SUM(CASE WHEN NULLIF(BTRIM(COALESCE(f.supplier_ext_id, '')), '') IS NOT NULL THEN 1 ELSE 0 END)::int AS supplier_hits
                    FROM fact_purchases f
                    GROUP BY 1, 2
                    ORDER BY COUNT(*) DESC
                    LIMIT 500
                    """
                )
            )
        ).mappings().all()
        for row in purchases_rows:
            softone_name = str(row.get('softone_name') or '').strip()
            code_name = str(row.get('document_type_code') or '').strip()
            rows.append(
                {
                    'stream': OperationalStream.purchase_documents.value,
                    'stream_label': _doc_stream_label(OperationalStream.purchase_documents.value),
                    'behavior_code': '',
                    'behavior_label': '',
                    'document_type': softone_name or code_name,
                    'source_document_type_code': code_name,
                    'include_revenue': False,
                    'include_quantity': True,
                    'include_cost': True,
                    'affects_customer_balance': False,
                    'affects_supplier_balance': bool((row.get('supplier_hits') or 0) > 0),
                    'amount_sign': _sum_sign_to_label(row.get('total_amount')),
                    'quantity_sign': _sum_sign_to_label(row.get('total_qty')),
                    'row_count': int(row.get('row_count') or 0),
                }
            )

        inventory_rows = (
            await tenant_db.execute(
                text(
                    """
                    SELECT
                        COALESCE(NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_series_name', '')), ''), '') AS softone_name,
                        COALESCE(
                            NULLIF(BTRIM(f.document_type), ''),
                            NULLIF(BTRIM(COALESCE(f.source_payload_json->>'document_type', '')), ''),
                            'inventory_documents'
                        ) AS document_type_code,
                        COUNT(*)::int AS row_count,
                        COALESCE(SUM(f.value_amount), 0)::numeric AS total_amount
                    FROM fact_inventory f
                    GROUP BY 1, 2
                    ORDER BY COUNT(*) DESC
                    LIMIT 500
                    """
                )
            )
        ).mappings().all()
        for row in inventory_rows:
            softone_name = str(row.get('softone_name') or '').strip()
            code_name = str(row.get('document_type_code') or '').strip()
            rows.append(
                {
                    'stream': OperationalStream.inventory_documents.value,
                    'stream_label': _doc_stream_label(OperationalStream.inventory_documents.value),
                    'behavior_code': '',
                    'behavior_label': '',
                    'document_type': softone_name or code_name,
                    'source_document_type_code': code_name,
                    'include_revenue': False,
                    'include_quantity': True,
                    'include_cost': True,
                    'affects_customer_balance': False,
                    'affects_supplier_balance': False,
                    'amount_sign': _sum_sign_to_label(row.get('total_amount')),
                    'quantity_sign': 'none',
                    'row_count': int(row.get('row_count') or 0),
                }
            )

        cash_rows = (
            await tenant_db.execute(
                text(
                    """
                    SELECT
                        COALESCE(NULLIF(BTRIM(f.transaction_type), ''), NULLIF(BTRIM(f.entry_type), ''), 'cash_transaction') AS document_type,
                        COUNT(*)::int AS row_count,
                        COALESCE(SUM(f.amount), 0)::numeric AS total_amount,
                        SUM(CASE WHEN LOWER(COALESCE(f.counterparty_type, '')) = 'customer' THEN 1 ELSE 0 END)::int AS customer_hits,
                        SUM(CASE WHEN LOWER(COALESCE(f.counterparty_type, '')) = 'supplier' THEN 1 ELSE 0 END)::int AS supplier_hits
                    FROM fact_cashflows f
                    GROUP BY 1
                    ORDER BY COUNT(*) DESC
                    LIMIT 500
                    """
                )
            )
        ).mappings().all()
        for row in cash_rows:
            raw_type = str(row.get('document_type') or '').strip()
            rows.append(
                {
                    'stream': OperationalStream.cash_transactions.value,
                    'stream_label': _doc_stream_label(OperationalStream.cash_transactions.value),
                    'behavior_code': '',
                    'behavior_label': '',
                    'document_type': _cash_type_to_softone_label(raw_type),
                    'source_document_type_code': raw_type,
                    'include_revenue': False,
                    'include_quantity': False,
                    'include_cost': False,
                    'affects_customer_balance': bool((row.get('customer_hits') or 0) > 0),
                    'affects_supplier_balance': bool((row.get('supplier_hits') or 0) > 0),
                    'amount_sign': _sum_sign_to_label(row.get('total_amount')),
                    'quantity_sign': 'none',
                    'row_count': int(row.get('row_count') or 0),
                }
            )

    for row in rows:
        amount_sign = _normalize_sign(row.get('amount_sign'))
        qty_sign = _normalize_sign(row.get('quantity_sign'))
        row['amount_sign'] = amount_sign
        row['quantity_sign'] = qty_sign
        row['amount_sign_label'] = _DOCUMENT_SIGN_LABEL.get(amount_sign, amount_sign)
        row['quantity_sign_label'] = _DOCUMENT_SIGN_LABEL.get(qty_sign, qty_sign)

    rows.sort(key=lambda x: (str(x.get('stream') or ''), -int(x.get('row_count') or 0), str(x.get('document_type') or '')))
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
    tenant_override_counts = {
        int(row[0]): int(row[1])
        for row in (
            await db.execute(
                select(TenantRuleOverride.tenant_id, func.count(TenantRuleOverride.id))
                .where(TenantRuleOverride.domain == RuleDomain.document_type_rules)
                .group_by(TenantRuleOverride.tenant_id)
            )
        ).all()
    }

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
    global_map_all: dict[tuple[str, str], tuple[GlobalRuleEntry, GlobalRuleSet]] = {}
    global_map_by_ruleset: dict[str, dict[tuple[str, str], tuple[GlobalRuleEntry, GlobalRuleSet]]] = {}
    for entry, ruleset in global_pairs:
        key = (entry.stream.value, entry.rule_key)
        if key not in global_map_all:
            global_map_all[key] = (entry, ruleset)
        by_set = global_map_by_ruleset.setdefault(str(ruleset.code), {})
        if key not in by_set:
            by_set[key] = (entry, ruleset)
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

    tenants_with_custom_rules: list[dict[str, object]] = []
    for tenant_obj in tenants:
        tenant_id_int = int(tenant_obj.id)
        overrides_count = int(tenant_override_counts.get(tenant_id_int, 0))
        tenant_ruleset_code = _tenant_document_ruleset_code(tenant_obj)
        if overrides_count <= 0 and not tenant_ruleset_code:
            continue
        tenants_with_custom_rules.append(
            {
                'tenant_id': tenant_id_int,
                'tenant_name': tenant_obj.name,
                'tenant_slug': tenant_obj.slug,
                'overrides_count': overrides_count,
                'ruleset_code': tenant_ruleset_code,
            }
        )

    effective_rows: list[dict[str, object]] = []
    tenant_observed_softone_rows: list[dict[str, object]] = []
    tenant_observed_error = ''
    selected_tenant_ruleset_code = ''
    if selected_tenant_id is not None:
        selected_tenant_obj = tenants_map.get(selected_tenant_id)
        if selected_tenant_obj is not None:
            selected_tenant_ruleset_code = _tenant_document_ruleset_code(selected_tenant_obj)
            try:
                tenant_observed_softone_rows = await _discover_tenant_softone_rules(
                    selected_tenant_obj.slug,
                    db_name=selected_tenant_obj.db_name,
                    db_user=selected_tenant_obj.db_user,
                    db_password=selected_tenant_obj.db_password,
                )
            except Exception as exc:
                logger.warning('tenant_softone_discovery_failed tenant=%s error=%s', selected_tenant_obj.slug, exc)
                tenant_observed_error = 'Αδυναμία ανάγνωσης κανόνων SoftOne από τον tenant.'
        effective_global_map = (
            global_map_by_ruleset.get(selected_tenant_ruleset_code, {})
            if selected_tenant_ruleset_code
            else global_map_all
        )
        all_keys = sorted(set(effective_global_map.keys()) | set(override_map.keys()))
        for key in all_keys:
            stream_value, rule_key = key
            global_pair = effective_global_map.get(key)
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
        'behavior_code': '',
        'behavior_label': '',
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
            'softone_doc_type_options': _softone_document_type_options(),
            'softone_doc_options': _softone_document_options(),
            'tenants': tenants,
            'selected_tenant_id': selected_tenant_id,
            'selected_tenant_name': tenants_map.get(selected_tenant_id).name if selected_tenant_id in tenants_map else None,
            'selected_tenant_ruleset_code': selected_tenant_ruleset_code,
            'tenant_observed_softone_rows': tenant_observed_softone_rows,
            'tenant_observed_error': tenant_observed_error,
            'tenants_with_custom_rules': tenants_with_custom_rules,
            'global_rows': global_rows,
            'tenant_override_rows': tenant_override_rows,
            'effective_rows': effective_rows,
            'default_form_values': default_form_values,
            'saved': request.query_params.get('saved') == '1',
            'deleted': request.query_params.get('deleted') == '1',
            'ruleset_saved': request.query_params.get('ruleset_saved') == '1',
            'tenant_overrides_cleared': request.query_params.get('tenant_overrides_cleared') == '1',
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
    selected_tenant_id = _to_int_or_none(resolved_form.get('tenant_id'))
    if selected_tenant_id is None:
        selected_tenant_id = _to_int_or_none(request.query_params.get('tenant_id'))
    if selected_tenant_id is None and tenants:
        selected_tenant_id = int(tenants[0].id)

    last_backfill = None
    if selected_tenant_id is not None:
        last_backfill = (
            await db.execute(
                select(AuditLog)
                .where(
                    AuditLog.tenant_id == selected_tenant_id,
                    AuditLog.action == 'initial_backfill_queued_ui',
                )
                .order_by(AuditLog.created_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    query_connector = str(request.query_params.get('connector_type') or '').strip()
    connector_type = str(resolved_form.get('connector_type') or query_connector or 'sql_connector')
    resolved_form['tenant_id'] = selected_tenant_id
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
    resolved_form.setdefault('stream_api_endpoint_json', '{}')
    resolved_form.setdefault('is_active', True)
    resolved_form.setdefault('supported_streams', default_supported)
    resolved_form.setdefault('has_saved_password', False)

    # On initial page load (no explicit posted form), hydrate fields from the
    # selected tenant+connector record so settings stay tenant-specific in UI.
    if not form_values and selected_tenant_id is not None:
        selected_connector = str(resolved_form.get('connector_type') or 'sql_connector').strip().lower()
        conn = (
            await db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == selected_tenant_id,
                    TenantConnection.connector_type == selected_connector,
                )
            )
        ).scalar_one_or_none()
        if conn is None and selected_connector == 'sql_connector':
            conn = (
                await db.execute(
                    select(TenantConnection).where(
                        TenantConnection.tenant_id == selected_tenant_id,
                        TenantConnection.connector_type == 'pharmacyone_sql',
                    )
                )
            ).scalar_one_or_none()
        elif conn is None and selected_connector == 'pharmacyone_sql':
            conn = (
                await db.execute(
                    select(TenantConnection).where(
                        TenantConnection.tenant_id == selected_tenant_id,
                        TenantConnection.connector_type == 'sql_connector',
                    )
                )
            ).scalar_one_or_none()

        if conn is not None:
            params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
            auth_cfg = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
            options_map = params.get('options') if isinstance(params.get('options'), dict) else {}
            resolved_form.update(
                {
                    'connector_type': conn.connector_type or selected_connector,
                    'source_type': _normalize_source_type(
                        str(conn.connector_type or selected_connector),
                        str(conn.source_type or ''),
                    ),
                    'is_active': bool(getattr(conn, 'is_active', True)),
                    'host': str(params.get('base_url') or params.get('host') or ''),
                    'base_url': str(params.get('base_url') or params.get('host') or ''),
                    'port': str(params.get('port') or '1433'),
                    'database': str(params.get('database') or ''),
                    'username': str(auth_cfg.get('username') or params.get('username') or ''),
                    'api_auth_type': str(params.get('auth_type') or 'softone_login'),
                    'api_app_id': str(auth_cfg.get('app_id') or auth_cfg.get('appId') or ''),
                    'api_company': str(auth_cfg.get('company') or auth_cfg.get('COMPANY') or ''),
                    'api_branch': str(auth_cfg.get('branch') or auth_cfg.get('BRANCH') or ''),
                    'api_module': str(auth_cfg.get('module') or auth_cfg.get('MODULE') or '0'),
                    'api_refid': str(auth_cfg.get('refid') or auth_cfg.get('REFID') or ''),
                    'options': _stringify_options_map(options_map),
                    'sales_query_template': conn.sales_query_template or DEFAULT_GENERIC_SALES_QUERY,
                    'purchases_query_template': conn.purchases_query_template or DEFAULT_GENERIC_PURCHASES_QUERY,
                    'inventory_query_template': conn.inventory_query_template or DEFAULT_GENERIC_INVENTORY_QUERY,
                    'cashflow_query_template': conn.cashflow_query_template or DEFAULT_GENERIC_CASHFLOW_QUERY,
                    'supplier_balances_query_template': conn.supplier_balances_query_template or DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
                    'customer_balances_query_template': conn.customer_balances_query_template or DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
                    'updated_at_column': conn.incremental_column or 'UpdatedAt',
                    'incremental_column': conn.incremental_column or 'UpdatedAt',
                    'id_column': conn.id_column or 'LineId',
                    'date_column': conn.date_column or 'DocDate',
                    'branch_column': conn.branch_column or 'BranchCode',
                    'item_column': conn.item_column or 'ItemCode',
                    'net_amount_column': conn.amount_column or 'NetValue',
                    'amount_column': conn.amount_column or 'NetValue',
                    'cost_column': conn.cost_column or 'CostValue',
                    'qty_column': conn.qty_column or 'Qty',
                    'stream_field_mapping_json': json.dumps(conn.stream_field_mapping or {}, ensure_ascii=False, indent=2),
                    'stream_api_endpoint_json': json.dumps(conn.stream_api_endpoint or {}, ensure_ascii=False, indent=2),
                    'has_saved_password': bool(conn.enc_payload) or bool(auth_cfg.get('password')),
                    'supported_streams': _normalize_stream_selection(
                        conn.supported_streams if isinstance(conn.supported_streams, list) else None,
                        fallback=_stream_defaults_for_connector(str(conn.connector_type or selected_connector)),
                    ),
                    'enabled_streams': _normalize_stream_selection(
                        conn.enabled_streams if isinstance(conn.enabled_streams, list) else None,
                        fallback=_stream_defaults_for_connector(str(conn.connector_type or selected_connector)),
                    ),
                }
            )

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
        'last_backfill': last_backfill,
    }


def _parse_date_or_none(raw: str | None):
    if not raw:
        return None
    raw = str(raw).strip()
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _connections_redirect_url(
    base_path: str,
    *,
    tenant_id: int | None = None,
    connector_type: str | None = None,
    **flags: object,
) -> str:
    params: dict[str, str] = {}
    if tenant_id is not None:
        params['tenant_id'] = str(int(tenant_id))
    if connector_type and str(connector_type).strip():
        params['connector_type'] = str(connector_type).strip()
    for key, value in flags.items():
        if value is None:
            continue
        params[str(key)] = str(value)
    if not params:
        return base_path
    return f'{base_path}?{urlencode(params)}'


def _sanitize_chunk_records(raw_value: int) -> int:
    return max(100, min(10000, int(raw_value)))


def _enqueue_external_backfill_jobs(
    *,
    tenant_slug: str,
    planned_jobs: list[dict[str, object]],
    from_dt: date,
    to_dt: date,
    chunk_records: int,
    chunk_days: int,
    include_purchases: bool,
) -> tuple[int, int]:
    record_limit = _sanitize_chunk_records(chunk_records)
    default_chunk_days = max(1, int(chunk_days))
    queued = 0
    batches = 0

    def _iter_chunks(start_date: date, end_date: date, step_days: int):
        current = start_date
        step = max(1, int(step_days))
        while current <= end_date:
            chunk_end = min(current + timedelta(days=step - 1), end_date)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    for job in planned_jobs:
        stream = normalize_stream_name(job.get('stream'))
        if stream == 'purchase_documents' and not include_purchases:
            continue

        effective_chunk_days = stream_chunk_days(stream, default_chunk_days)
        for chunk_from, chunk_to in _iter_chunks(from_dt, to_dt, effective_chunk_days):
            queued_job = dict(job)
            merged_payload = dict(queued_job.get('payload') or {})
            merged_payload.update(
                {
                    'from_date': chunk_from.isoformat(),
                    'to_date': chunk_to.isoformat(),
                    'ignore_sync_state': True,
                    'backfill': True,
                    'limit': record_limit,
                }
            )
            queued_job['payload'] = merged_payload
            queued_job.setdefault('attempt', 0)
            queued_job.setdefault('max_retries', settings.ingest_job_max_retries)
            enqueue_tenant_job(tenant_slug, queued_job)
            queued += 1
            batches += 1

    return queued, batches


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
            resp = RedirectResponse(url=_admin_dashboard_redirect(host), status_code=302)
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
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    if request.method.upper() == 'HEAD':
        resp = Response(status_code=200, media_type='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    resp = templates.TemplateResponse('auth/login.html', {'request': request, 'error': error})
    resp.headers['Cache-Control'] = 'no-store'
    if host == settings.admin_portal_host.lower():
        for domain in (None, cookie_domain, settings.tenant_portal_host.lower(), '.boxvisio.com'):
            resp.delete_cookie('access_token', path='/', domain=domain)
            resp.delete_cookie('refresh_token', path='/', domain=domain)
            resp.delete_cookie('csrf_token', path='/', domain=domain)
    return resp


@router.get('/invite', response_class=HTMLResponse)
async def invite_password_page(request: Request, token: str = Query(default='')):
    return templates.TemplateResponse('auth/invite.html', {'request': request, 'token': token, 'error': None})


@router.post('/invite')
async def invite_password_submit(
    request: Request,
    token: str = Form(default=''),
    password: str = Form(default=''),
    db: AsyncSession = Depends(get_control_db),
):
    token = str(token or '').strip()
    password = str(password or '')
    if len(password) < 8:
        return templates.TemplateResponse(
            'auth/invite.html',
            {'request': request, 'token': token, 'error': 'Ο κωδικός πρέπει να έχει τουλάχιστον 8 χαρακτήρες.'},
            status_code=400,
        )
    # Resolve by token only: an invited user is inactive until they set a password, so we must
    # not require is_active here (the single-use, 48h-expiring token is the proof of invite).
    user = (await db.execute(select(User).where(User.reset_token == token))).scalar_one_or_none()
    if not user:
        return templates.TemplateResponse(
            'auth/invite.html',
            {'request': request, 'token': token, 'error': 'Μη έγκυρη πρόσκληση.'},
            status_code=400,
        )
    if not user.reset_token_expires_at or user.reset_token_expires_at < datetime.utcnow():
        return templates.TemplateResponse(
            'auth/invite.html',
            {'request': request, 'token': token, 'error': 'Η πρόσκληση έχει λήξει.'},
            status_code=400,
        )
    user.password_hash = get_password_hash(password)
    # Setting the password via the invite link activates the account.
    user.is_active = True
    user.reset_token = None
    user.reset_token_expires_at = None
    await db.commit()
    return RedirectResponse(url='/login?created=1', status_code=303)


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
    if host == settings.admin_portal_host.lower() and user.role != RoleName.cloudon_admin:
        return templates.TemplateResponse(
            'auth/login.html',
            {'request': request, 'error': 'Το admin panel επιτρέπει μόνο CloudOn admin χρήστες.'},
            status_code=403,
        )

    host_tenant_slug = _tenant_slug_from_host(host)
    if host_tenant_slug and user.tenant_id is not None:
        user_tenant_slug = (
            await db.execute(select(Tenant.slug).where(Tenant.id == user.tenant_id))
        ).scalar_one_or_none()
        if str(user_tenant_slug or '').lower() != host_tenant_slug:
            return templates.TemplateResponse(
                'auth/login.html',
                {'request': request, 'error': tt(request, 'invalid_credentials')},
                status_code=401,
            )
    # Concurrent-session licence (block model): a tenant user may not start a NEW session once the
    # tenant's purchased simultaneous connections are all in use. The user's own already-active
    # sessions never count against them (re-login / second device of the same person is fine).
    if user.role in (RoleName.tenant_admin, RoleName.tenant_user) and user.tenant_id is not None:
        _login_tenant = (
            await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))
        ).scalar_one_or_none()
        if _login_tenant is not None:
            _max_concurrent = await _tenant_max_concurrent_sessions(
                db, await get_or_create_subscription(db, _login_tenant)
            )
            if _max_concurrent > 0:
                _active_users = await _tenant_active_session_user_ids(db, user.tenant_id)
                if user.id not in _active_users and len(_active_users) >= _max_concurrent:
                    await db.commit()
                    return templates.TemplateResponse(
                        'auth/login.html',
                        {
                            'request': request,
                            'error': (
                                f'Έχει καλυφθεί το όριο ταυτόχρονων συνδέσεων ({_max_concurrent}). '
                                'Αποσυνδεθείτε από άλλη συσκευή/χρήστη και δοκιμάστε ξανά.'
                            ),
                        },
                        status_code=403,
                    )
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
        company_id=user.company_id,
        audience=access_audience,
    )
    refresh_token, refresh_jti, refresh_exp = create_refresh_token(subject=str(user.id))
    db.add(
        RefreshToken(
            user_id=user.id,
            token_jti=refresh_jti,
            expires_at=refresh_exp.replace(tzinfo=None),
            revoked_at=None,
            last_seen_at=datetime.utcnow(),
            last_seen_path='/login',
            last_seen_ip=_request_client_ip(request),
            last_seen_user_agent=_request_user_agent(request),
        )
    )
    db.add(
        AuditLog(
            tenant_id=user.tenant_id,
            actor_user_id=user.id,
            action='auth_login_success',
            entity_type='auth_session',
            entity_id=refresh_jti,
            payload={
                'host': host,
                'audience': access_audience,
                'ip': _request_client_ip(request),
                'user_agent': _request_user_agent(request),
                'refresh_expires_at': refresh_exp.isoformat(),
            },
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


async def _revoke_refresh_cookie_session(request: Request, db: AsyncSession, *, host: str) -> None:
    refresh_cookie = request.cookies.get('refresh_token')
    if not refresh_cookie:
        return
    try:
        payload = safe_decode(refresh_cookie, token_type='refresh')
    except Exception:
        return
    if not payload:
        return
    jti = str(payload.get('jti') or '').strip()
    subject = str(payload.get('sub') or '').strip()
    if not jti:
        return
    token_row = (
        await db.execute(
            select(RefreshToken)
            .where(RefreshToken.token_jti == jti)
            .limit(1)
        )
    ).scalar_one_or_none()
    if not token_row:
        return
    now = datetime.utcnow()
    if token_row.revoked_at is None:
        token_row.revoked_at = now
    actor_user_id = int(subject) if subject.isdigit() else token_row.user_id
    user = (
        await db.execute(select(User).where(User.id == actor_user_id).limit(1))
    ).scalar_one_or_none()
    db.add(
        AuditLog(
            tenant_id=user.tenant_id if user else None,
            actor_user_id=actor_user_id,
            action='auth_logout',
            entity_type='auth_session',
            entity_id=jti,
            payload={
                'host': host,
                'ip': _request_client_ip(request),
                'user_agent': _request_user_agent(request),
            },
        )
    )
    await db.commit()


@router.post('/logout')
async def logout(request: Request, db: AsyncSession = Depends(get_control_db)):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    await _revoke_refresh_cookie_session(request, db, host=host)
    resp = RedirectResponse(url='/login', status_code=303)
    resp.delete_cookie('access_token', path='/', domain=cookie_domain)
    resp.delete_cookie('refresh_token', path='/', domain=cookie_domain)
    resp.delete_cookie('csrf_token', path='/', domain=cookie_domain)
    return resp


@router.get('/logout')
async def logout_get(request: Request, db: AsyncSession = Depends(get_control_db)):
    host = (request.headers.get('host') or '').split(':')[0].lower()
    cookie_domain = _cookie_domain_for_host(host)
    await _revoke_refresh_cookie_session(request, db, host=host)
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
        'docker': _docker_cache_status(),
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
            'docker': _docker_cache_status(),
        }
    )


@router.post('/admin/docker-cleanup/run')
async def admin_docker_cleanup_run(
    _user=Depends(require_roles(RoleName.cloudon_admin)),
):
    script_path = Path('/opt/cloudon-bi/scripts/docker_cache_cleanup.sh')
    if not script_path.exists():
        return JSONResponse(status_code=404, content={'ok': False, 'error': 'cleanup_script_not_found'})
    if not shutil.which('docker'):
        trigger_path = Path('/opt/cloudon-bi/artifacts/ops/docker_cleanup.request')
        trigger_path.parent.mkdir(parents=True, exist_ok=True)
        trigger_path.write_text(datetime.now(timezone.utc).isoformat(), encoding='utf-8')
        return JSONResponse(
            content={
                'ok': True,
                'queued': True,
                'message': 'Manual cleanup requested. Host systemd path will run cloudon-docker-cleanup.service.',
                'docker': _docker_cache_status(),
            }
        )
    try:
        proc = await asyncio.create_subprocess_exec(
            str(script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
                'DOCKER_CLEANUP_METRICS_FILE': '/opt/cloudon-bi/artifacts/ops/docker_cleanup.prom',
            },
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return JSONResponse(status_code=504, content={'ok': False, 'error': 'cleanup_timeout'})
        output = (stdout or b'').decode('utf-8', errors='ignore')
        err = (stderr or b'').decode('utf-8', errors='ignore')
        return JSONResponse(
            status_code=200 if proc.returncode == 0 else 500,
            content={
                'ok': proc.returncode == 0,
                'returncode': proc.returncode,
                'output': output[-5000:],
                'error': err[-2000:],
                'docker': _docker_cache_status(),
            },
        )
    except Exception as exc:
        logger.exception('admin_docker_cleanup_run_failed')
        return JSONResponse(status_code=500, content={'ok': False, 'error': str(exc)})


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
        await apply_subscription_time_transitions(db, t, sub)
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
    max_users: int = Form(default=5),
    send_welcome_email: bool = Form(default=True),
    create_subdomain: bool = Form(default=False),
    connection_type: str = Form(default='none'),
    softone_base_url: str = Form(default=''),
    softone_username: str = Form(default=''),
    softone_password: str = Form(default=''),
    softone_app_id: str = Form(default=''),
    softone_company: str = Form(default=''),
    softone_branch: str = Form(default=''),
    softone_module: str = Form(default='0'),
    softone_refid: str = Form(default=''),
    softone_bridge_path: str = Form(default='JS/myWS'),
    sql_host: str = Form(default=''),
    sql_port: int = Form(default=1433),
    sql_database: str = Form(default=''),
    sql_username: str = Form(default=''),
    sql_password: str = Form(default=''),
    sql_options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    try:
        selected_plan = PlanName(str(plan or '').strip().lower())
        selected_sub = SubscriptionStatus(str(subscription_status or '').strip().lower())
    except ValueError:
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
                'error': 'Invalid plan or subscription status.',
                'provisioning_result': None,
                'active_page': 'tenants',
                'title': 'title_tenants',
            },
            status_code=400,
        )
    clean_slug = str(slug or '').strip().lower()
    existing_tenant = (await db.execute(select(Tenant).where(Tenant.slug == clean_slug))).scalar_one_or_none()
    if existing_tenant is not None:
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
                'provisioning_result': {
                    'status': 'exists',
                    'slug': existing_tenant.slug,
                    'tenant_id': existing_tenant.id,
                    'tenant_name': existing_tenant.name,
                },
                'active_page': 'tenants',
                'title': 'title_tenants',
            },
        )
    result = await run_tenant_provisioning_wizard(
        db=db,
        name=name,
        slug=clean_slug,
        admin_email=admin_email,
        plan=selected_plan,
        source=source,
        subscription_status=selected_sub,
        trial_days=trial_days,
        max_users=max_users,
        send_welcome_email=bool(send_welcome_email),
        create_subdomain=bool(create_subdomain),
        connection_type=connection_type,
        softone_base_url=softone_base_url,
        softone_username=softone_username,
        softone_password=softone_password,
        softone_app_id=softone_app_id,
        softone_company=softone_company,
        softone_branch=softone_branch,
        softone_module=softone_module,
        softone_refid=softone_refid,
        softone_bridge_path=softone_bridge_path,
        sql_host=sql_host,
        sql_port=sql_port,
        sql_database=sql_database,
        sql_username=sql_username,
        sql_password=sql_password,
        sql_options=sql_options,
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


@router.post('/admin/tenants/{tenant_id}/sync-item-status')
async def admin_tenant_sync_item_status(
    tenant_id: int,
    next_url: str = Form(default='/admin/tenants'),
    admin_user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url=f'{redirect_target}{sep}status_sync=0&reason=tenant_not_found', status_code=303)
    try:
        celery_client.send_task(
            'worker.tasks.refresh_item_status_for_tenant',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
    except Exception:  # noqa: BLE001
        logger.exception('admin_tenant_sync_item_status_failed', extra={'tenant_id': tenant_id})
        return RedirectResponse(url=f'{redirect_target}{sep}status_sync=0&reason=trigger_failed', status_code=303)
    return RedirectResponse(url=f'{redirect_target}{sep}status_sync=1', status_code=303)


@router.post('/admin/tenants/{tenant_id}/resend-welcome')
async def admin_tenant_resend_welcome(
    tenant_id: int,
    next_url: str = Form(default='/admin/tenants'),
    admin_user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url=f'{redirect_target}{sep}credentials_sent=0&reason=tenant_not_found', status_code=303)
    if _is_infrastructure_tenant(tenant):
        return RedirectResponse(url=f'{redirect_target}{sep}credentials_sent=0&reason=system_tenant', status_code=303)
    target = (
        await db.execute(
            select(User)
            .where(
                User.tenant_id == tenant.id,
                User.role == RoleName.tenant_admin,
                User.is_active.is_(True),
            )
            .order_by(User.id.asc())
        )
    ).scalar_one_or_none()
    if target is None:
        target = (
            await db.execute(
                select(User)
                .where(User.tenant_id == tenant.id, User.role == RoleName.tenant_admin)
                .order_by(User.id.asc())
            )
        ).scalar_one_or_none()
    if target is None or not str(target.email or '').strip():
        return RedirectResponse(url=f'{redirect_target}{sep}credentials_sent=0&reason=admin_user_not_found', status_code=303)

    invite_token = secrets.token_urlsafe(24)
    temporary_password = secrets.token_urlsafe(12)
    target.password_hash = get_password_hash(temporary_password)
    target.reset_token = invite_token
    target.reset_token_expires_at = datetime.utcnow() + timedelta(days=2)
    target.is_active = True

    try:
        email_result = await asyncio.to_thread(
            send_tenant_welcome_email,
            tenant_name=tenant.name,
            tenant_slug=tenant.slug,
            admin_email=target.email,
            invite_token=invite_token,
            temporary_password=temporary_password,
        )
        email_status = str(email_result.get('status') or 'unknown')
    except Exception as exc:
        await db.rollback()
        logging.exception('admin_tenant_resend_welcome_failed', extra={'tenant_id': tenant.id})
        return RedirectResponse(
            url=f'{redirect_target}{sep}{urlencode({"credentials_sent": "0", "reason": "email_failed", "message": str(exc)[:160]})}',
            status_code=303,
        )
    if email_status != 'sent':
        await db.rollback()
        reason = str(email_result.get('reason') or email_status or 'email_not_sent')
        return RedirectResponse(
            url=f'{redirect_target}{sep}{urlencode({"credentials_sent": "0", "reason": reason, "message": reason[:160]})}',
            status_code=303,
        )

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            actor_user_id=admin_user.id,
            action='tenant_welcome_credentials_resent',
            entity_type='user',
            entity_id=str(target.id),
            payload={'email': target.email, 'email_status': email_status},
        )
    )
    await db.commit()
    return RedirectResponse(
        url=f'{redirect_target}{sep}{urlencode({"credentials_sent": "1", "email": target.email})}',
        status_code=303,
    )


@router.get('/admin/tenants/{tenant_id}/resend-welcome')
async def admin_tenant_resend_welcome_get(
    tenant_id: int,
    next_url: str = Query(default='/admin/tenants'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(
        url=f'{redirect_target}{sep}credentials_sent=0&reason=resend_requires_post&tenant_id={tenant_id}',
        status_code=303,
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
    auto_sync = _tenant_auto_sync_settings(tenant)
    flags = tenant.feature_flags if isinstance(tenant.feature_flags, dict) else {}
    if not isinstance(flags.get('auto_sync'), dict):
        primary_conn = (
            await db.execute(
                select(TenantConnection)
                .where(TenantConnection.tenant_id == tenant.id)
                .order_by(TenantConnection.is_active.desc(), TenantConnection.updated_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if primary_conn is not None:
            params = primary_conn.connection_parameters if isinstance(primary_conn.connection_parameters, dict) else {}
            auto_sync = dict(auto_sync)
            auto_sync['enabled'] = _parse_bool_enabled(params.get('auto_sync_enabled'), True)
            auto_sync['interval_minutes'] = _parse_int_in_range(
                params.get('sync_interval_minutes'),
                default=int(auto_sync['interval_minutes']),
                min_value=1,
                max_value=1440,
            )
    tenant_contact = _tenant_contact_settings(tenant)
    tenant_admin_user = (
        await db.execute(
            select(User)
            .where(User.tenant_id == tenant.id, User.role == RoleName.tenant_admin)
            .order_by(User.is_active.desc(), User.id.asc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if tenant_admin_user is not None and str(tenant_admin_user.email or '').strip():
        admin_email = str(tenant_admin_user.email or '').strip()
        if not tenant_contact.get('contact_email'):
            tenant_contact['contact_email'] = admin_email
        if not tenant_contact.get('billing_email'):
            tenant_contact['billing_email'] = admin_email
        if not tenant_contact.get('technical_email'):
            tenant_contact['technical_email'] = admin_email
    return templates.TemplateResponse(
        'admin/tenant_edit.html',
        {
            'request': request,
            'tenant': tenant,
            'tenant_contact': tenant_contact,
            'inventory_item_classification': _tenant_inventory_item_classification_settings(tenant),
            'auto_sync': auto_sync,
            'daily_reconciliation': _tenant_daily_reconciliation_settings(tenant),
            'duplicate_protection': _tenant_duplicate_protection_settings(tenant),
            'eshop_fulfillment': _tenant_eshop_fulfillment_settings(tenant),
            'document_series_labels': _tenant_document_series_labels_settings(tenant),
            'price_margin_targets': _tenant_price_margin_targets_settings(tenant),
            'business_advisor_targets': _tenant_business_advisor_targets_settings(tenant),
            'era_exploration_data': _tenant_era_exploration_settings(tenant),
            'call_center_3cx': await _tenant_call_center_settings(db, tenant.id),
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
    contact_person: str = Form(default=''),
    contact_email: str = Form(default=''),
    contact_phone: str = Form(default=''),
    contact_mobile: str = Form(default=''),
    billing_email: str = Form(default=''),
    technical_email: str = Form(default=''),
    contact_address: str = Form(default=''),
    contact_city: str = Form(default=''),
    contact_postal_code: str = Form(default=''),
    contact_afm: str = Form(default=''),
    contact_doy: str = Form(default=''),
    contact_notes: str = Form(default=''),
    auto_sync_enabled: bool = Form(default=False),
    auto_sync_profile: str = Form(default='live'),
    auto_sync_interval_minutes: str = Form(default='5'),
    auto_sync_overlap_minutes: str = Form(default='5'),
    auto_sync_recovery_days: str = Form(default='7'),
    auto_sync_business_hours_mode: str = Form(default='always'),
    auto_sync_business_hours_start: str = Form(default='08:00'),
    auto_sync_business_hours_end: str = Form(default='22:00'),
    auto_sync_business_hours_timezone: str = Form(default='Europe/Athens'),
    stream_sync_enabled: list[str] = Form(default=[]),
    stream_sync_interval_minutes: list[str] = Form(default=[]),
    stream_sync_overlap_minutes: list[str] = Form(default=[]),
    stream_sync_recovery_days: list[str] = Form(default=[]),
    daily_reconciliation_enabled: bool = Form(default=False),
    daily_reconciliation_time: str = Form(default='23:30'),
    daily_reconciliation_timezone: str = Form(default='Europe/Athens'),
    daily_reconciliation_lookback_days: str = Form(default='1'),
    daily_reconciliation_streams: list[str] = Form(default=[]),
    duplicate_protection_enabled: bool = Form(default=False),
    duplicate_protection_mode: str = Form(default='natural_key'),
    status_source: str = Form(default='softone'),
    active_last_sale_days: str = Form(default='60'),
    movement_window_days: str = Form(default='30'),
    inventory_scope_sold_days: str = Form(default='90'),
    fast_sales_qty_30d_min: str = Form(default='50'),
    slow_sales_qty_30d_max: str = Form(default='5'),
    pickup_warehouses_text: str = Form(default=''),
    store_warehouses_text: str = Form(default=''),
    pure_eshop_warehouses_text: str = Form(default=''),
    three_pl_warehouses_text: str = Form(default=''),
    shipping_method_labels_text: str = Form(default=''),
    sales_series_channel_labels_text: str = Form(default=''),
    document_series_labels_text: str = Form(default=''),
    default_margin_pct: str = Form(default='35'),
    category_margin_targets_text: str = Form(default=''),
    group_margin_targets_text: str = Form(default=''),
    inventory_coverage_days: str = Form(default='60'),
    physical_branch_names_text: str = Form(default=''),
    call_center_fqdn: str = Form(default=''),
    call_center_username: str = Form(default=''),
    call_center_password: str = Form(default=''),
    call_center_client_id: str = Form(default=''),
    call_center_queue_ids: str = Form(default=''),
    call_center_team_extensions: str = Form(default=''),
    call_center_agent_directory_text: str = Form(default=''),
    call_center_queue_directory_text: str = Form(default=''),
    call_center_polling_interval_minutes: str = Form(default='5'),
    call_center_answer_sla_seconds: str = Form(default='30'),
    call_center_target_calls_per_agent: str = Form(default='60'),
    era_exploration_file: UploadFile | None = File(default=None),
    next_url: str = Form(default='/admin/tenants'),
    admin_user: object = Depends(require_roles(RoleName.cloudon_admin)),
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
        'contact': _tenant_contact_settings(tenant),
        'auto_sync': _tenant_auto_sync_settings(tenant),
        'daily_reconciliation': _tenant_daily_reconciliation_settings(tenant),
        'duplicate_protection': _tenant_duplicate_protection_settings(tenant),
        'inventory_item_classification': _tenant_inventory_item_classification_settings(tenant),
        'eshop_fulfillment': _tenant_eshop_fulfillment_settings(tenant),
        'document_series_labels': _tenant_document_series_labels_settings(tenant),
        'price_margin_targets': _tenant_price_margin_targets_settings(tenant),
        'business_advisor_targets': _tenant_business_advisor_targets_settings(tenant),
        'era_exploration_data': _tenant_era_exploration_settings(tenant),
        'call_center_3cx': await _tenant_call_center_settings(db, tenant.id),
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

    settings_payload = _tenant_inventory_item_classification_settings(tenant)
    status_source_clean = str(status_source or '').strip().lower()
    if status_source_clean in {'commercial', 'commercial_status', 'status'}:
        settings_payload['status_source'] = 'commercial'
    elif status_source_clean in {'active_available', 'active_stock_sales', 'softone_available'}:
        settings_payload['status_source'] = 'active_available'
    elif status_source_clean in {'active_status12', 'active_both_status', 'status12'}:
        settings_payload['status_source'] = 'active_status12'
    elif status_source_clean in {'softone', 'source', 'source_flag'}:
        settings_payload['status_source'] = 'softone'
    else:
        settings_payload['status_source'] = 'sales_window'
    settings_payload['active_last_sale_days'] = _parse_int_in_range(
        active_last_sale_days,
        default=settings_payload['active_last_sale_days'],
        min_value=1,
        max_value=3650,
    )
    settings_payload['movement_window_days'] = _parse_int_in_range(
        movement_window_days,
        default=settings_payload['movement_window_days'],
        min_value=1,
        max_value=3650,
    )
    settings_payload['inventory_scope_sold_days'] = _parse_int_in_range(
        inventory_scope_sold_days,
        default=settings_payload['inventory_scope_sold_days'],
        min_value=1,
        max_value=3650,
    )
    settings_payload['fast_sales_qty_30d_min'] = _parse_int_in_range(
        fast_sales_qty_30d_min,
        default=settings_payload['fast_sales_qty_30d_min'],
        min_value=1,
        max_value=1_000_000,
    )
    settings_payload['slow_sales_qty_30d_max'] = _parse_int_in_range(
        slow_sales_qty_30d_max,
        default=settings_payload['slow_sales_qty_30d_max'],
        min_value=0,
        max_value=1_000_000,
    )
    if settings_payload['slow_sales_qty_30d_max'] >= settings_payload['fast_sales_qty_30d_min']:
        settings_payload['slow_sales_qty_30d_max'] = max(0, settings_payload['fast_sales_qty_30d_min'] - 1)

    contact_payload = {
        'contact_person': str(contact_person or '').strip(),
        'contact_email': str(contact_email or '').strip(),
        'contact_phone': str(contact_phone or '').strip(),
        'contact_mobile': str(contact_mobile or '').strip(),
        'billing_email': str(billing_email or '').strip(),
        'technical_email': str(technical_email or '').strip(),
        'address': str(contact_address or '').strip(),
        'city': str(contact_city or '').strip(),
        'postal_code': str(contact_postal_code or '').strip(),
        'afm': str(contact_afm or '').strip(),
        'doy': str(contact_doy or '').strip(),
        'notes': str(contact_notes or '').strip(),
    }

    auto_sync_payload = _tenant_auto_sync_settings(tenant)
    profile_clean = str(auto_sync_profile or '').strip().lower()
    profile_clean = profile_clean if profile_clean in _SYNC_PROFILE_DEFAULTS else 'live'
    profile_defaults = _SYNC_PROFILE_DEFAULTS.get(profile_clean, _SYNC_PROFILE_DEFAULTS['live'])
    auto_sync_payload['enabled'] = bool(auto_sync_enabled)
    auto_sync_payload['profile'] = profile_clean
    auto_sync_payload['interval_minutes'] = _parse_int_in_range(
        auto_sync_interval_minutes,
        default=int(profile_defaults['interval_minutes']),
        min_value=1,
        max_value=43200,
    )
    auto_sync_payload['overlap_minutes'] = _parse_int_in_range(
        auto_sync_overlap_minutes,
        default=int(profile_defaults['overlap_minutes']),
        min_value=0,
        max_value=1440,
    )
    auto_sync_payload['recovery_days'] = _parse_int_in_range(
        auto_sync_recovery_days,
        default=int(profile_defaults['recovery_days']),
        min_value=1,
        max_value=366,
    )
    current_business_hours = auto_sync_payload.get('business_hours') if isinstance(auto_sync_payload.get('business_hours'), dict) else {}

    def _clean_form_hhmm(raw: object, fallback: str) -> str:
        text = str(raw or fallback).strip()[:5]
        if ':' not in text:
            text = fallback
        hour_text, minute_text = (text.split(':', 1) + ['00'])[:2]
        try:
            hour = max(0, min(23, int(hour_text)))
            minute = max(0, min(59, int(minute_text)))
        except Exception:
            hour, minute = [int(part) for part in fallback.split(':', 1)]
        return f'{hour:02d}:{minute:02d}'

    business_hours_mode = str(auto_sync_business_hours_mode or '').strip().lower()
    auto_sync_payload['business_hours'] = {
        'mode': business_hours_mode if business_hours_mode in {'always', 'business_hours'} else 'always',
        'start': _clean_form_hhmm(
            auto_sync_business_hours_start,
            str(current_business_hours.get('start') or '08:00'),
        ),
        'end': _clean_form_hhmm(
            auto_sync_business_hours_end,
            str(current_business_hours.get('end') or '22:00'),
        ),
        'timezone': str(auto_sync_business_hours_timezone or current_business_hours.get('timezone') or 'Europe/Athens').strip() or 'Europe/Athens',
    }
    enabled_streams = {normalize_stream_name(v) for v in stream_sync_enabled}
    stream_overrides: dict[str, dict[str, object]] = {}
    intervals = list(stream_sync_interval_minutes or [])
    overlaps = list(stream_sync_overlap_minutes or [])
    recovery_days_values = list(stream_sync_recovery_days or [])
    for idx, stream in enumerate(ALL_OPERATIONAL_STREAMS):
        stream_overrides[stream] = {
            'enabled': stream in enabled_streams,
            'interval_minutes': _parse_int_in_range(
                intervals[idx] if idx < len(intervals) else '',
                default=int(auto_sync_payload['interval_minutes']),
                min_value=1,
                max_value=43200,
            ),
            'overlap_minutes': _parse_int_in_range(
                overlaps[idx] if idx < len(overlaps) else '',
                default=int(auto_sync_payload['overlap_minutes']),
                min_value=0,
                max_value=1440,
            ),
            'recovery_days': _parse_int_in_range(
                recovery_days_values[idx] if idx < len(recovery_days_values) else '',
                default=int(auto_sync_payload['recovery_days']),
                min_value=1,
                max_value=366,
            ),
        }
    auto_sync_payload['stream_overrides'] = stream_overrides
    auto_sync_payload['stream_labels'] = _SYNC_STREAM_LABELS

    reconciliation_payload = _tenant_daily_reconciliation_settings(tenant)
    raw_reconcile_time = str(daily_reconciliation_time or '').strip()[:5]
    if ':' not in raw_reconcile_time:
        raw_reconcile_time = str(reconciliation_payload['time'])
    hour_text, minute_text = (raw_reconcile_time.split(':', 1) + ['00'])[:2]
    try:
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
    except Exception:
        hour, minute = 23, 30
    selected_reconciliation_streams: list[str] = []
    for raw_stream in daily_reconciliation_streams or []:
        stream = normalize_stream_name(raw_stream)
        if stream in ALL_OPERATIONAL_STREAMS and stream not in selected_reconciliation_streams:
            selected_reconciliation_streams.append(stream)
    if not selected_reconciliation_streams:
        selected_reconciliation_streams = list(reconciliation_payload['streams'])
    reconciliation_payload = {
        'enabled': bool(daily_reconciliation_enabled),
        'time': f'{hour:02d}:{minute:02d}',
        'timezone': str(daily_reconciliation_timezone or 'Europe/Athens').strip() or 'Europe/Athens',
        'lookback_days': _parse_int_in_range(
            daily_reconciliation_lookback_days,
            default=int(reconciliation_payload['lookback_days']),
            min_value=1,
            max_value=366,
        ),
        'streams': selected_reconciliation_streams,
        'stream_labels': _SYNC_STREAM_LABELS,
    }

    duplicate_protection_payload = _tenant_duplicate_protection_settings(tenant)
    duplicate_mode_clean = str(duplicate_protection_mode or '').strip().lower()
    duplicate_protection_payload['enabled'] = bool(duplicate_protection_enabled)
    duplicate_protection_payload['mode'] = duplicate_mode_clean if duplicate_mode_clean in {'event_id', 'natural_key'} else 'natural_key'

    pickup_map: dict[str, str] = {}
    for line in str(pickup_warehouses_text or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            pickup_map[code_clean] = label_clean
    store_warehouse_map: dict[str, str] = {}
    for line in str(store_warehouses_text or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            store_warehouse_map[code_clean] = label_clean
    shipping_method_label_map: dict[str, str] = {}
    for line in str(shipping_method_labels_text or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            shipping_method_label_map[code_clean] = label_clean
    sales_series_channel_label_map: dict[str, str] = {}
    for line in str(sales_series_channel_labels_text or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            sales_series_channel_label_map[code_clean] = label_clean
    current_document_series_labels = _tenant_document_series_labels_settings(tenant)
    document_series_label_map: dict[str, str] = {}
    for line in str(document_series_labels_text or '').splitlines():
        raw_line = str(line or '').strip()
        if not raw_line:
            continue
        if '=' in raw_line:
            code, label = raw_line.split('=', 1)
        elif ':' in raw_line:
            code, label = raw_line.split(':', 1)
        else:
            continue
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            document_series_label_map[code_clean] = label_clean
    if not document_series_label_map:
        document_series_label_map = dict(current_document_series_labels or {})

    current_price_margin_targets = _tenant_price_margin_targets_settings(tenant)

    def _parse_margin_target_map(raw: str, fallback: dict[str, float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for line in str(raw or '').splitlines():
            raw_line = str(line or '').strip()
            if not raw_line:
                continue
            if '=' in raw_line:
                key, value = raw_line.split('=', 1)
            elif ':' in raw_line:
                key, value = raw_line.split(':', 1)
            else:
                continue
            key_clean = str(key or '').strip()
            value_clean = str(value or '').strip().replace(',', '.')
            try:
                pct = max(0.0, min(95.0, float(value_clean)))
            except Exception:
                continue
            if key_clean:
                out[key_clean] = pct
        return out if str(raw or '').strip() else dict(fallback or {})

    try:
        default_margin_value = max(0.0, min(95.0, float(str(default_margin_pct or '').replace(',', '.'))))
    except Exception:
        default_margin_value = float(current_price_margin_targets.get('default_margin_pct') or 35.0)
    price_margin_targets_payload = normalize_price_margin_targets_config(
        {
            'default_margin_pct': default_margin_value,
            'category_margin_pct': _parse_margin_target_map(
                category_margin_targets_text,
                dict(current_price_margin_targets.get('category_margin_pct') or {}),
            ),
            'group_margin_pct': _parse_margin_target_map(
                group_margin_targets_text,
                dict(current_price_margin_targets.get('group_margin_pct') or {}),
            ),
        }
    )
    current_business_advisor_targets = _tenant_business_advisor_targets_settings(tenant)
    business_advisor_targets_payload = {
        'inventory_coverage_days': _parse_int_in_range(
            inventory_coverage_days,
            default=int(current_business_advisor_targets.get('inventory_coverage_days') or 60),
            min_value=1,
            max_value=3650,
        )
    }

    physical_branch_names: list[str] = []
    seen_physical_branch_names: set[str] = set()
    for line in str(physical_branch_names_text or '').splitlines():
        clean = str(line or '').strip()
        if clean and clean not in seen_physical_branch_names:
            seen_physical_branch_names.add(clean)
            physical_branch_names.append(clean)
    def _csv_to_list(raw: str) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in str(raw or '').replace('\n', ',').split(','):
            clean = str(item or '').strip()
            if clean and clean not in seen:
                seen.add(clean)
                out.append(clean)
        return out

    eshop_fulfillment_payload = normalize_eshop_fulfillment_config(
        {
            'use_defaults': False,
            'pickup_warehouses': pickup_map,
            'store_warehouses': store_warehouse_map,
            'pure_eshop_warehouses': _csv_to_list(pure_eshop_warehouses_text),
            'three_pl_warehouses': _csv_to_list(three_pl_warehouses_text),
            'shipping_method_labels': shipping_method_label_map,
            'sales_series_channel_labels': sales_series_channel_label_map,
            'physical_branch_names': physical_branch_names,
        }
    )

    flags = dict(tenant.feature_flags or {})
    flags['auto_sync'] = auto_sync_payload
    flags['daily_reconciliation'] = reconciliation_payload
    flags['duplicate_protection'] = duplicate_protection_payload
    # Persist the classification config under the non-managed '..._config' key so it survives
    # subscription syncs (the managed 'inventory_item_classification' is just the on/off feature flag).
    flags['inventory_item_classification_config'] = settings_payload
    flags['eshop_fulfillment'] = eshop_fulfillment_payload
    flags['document_series_labels'] = normalize_document_series_labels_config(document_series_label_map)
    flags['price_margin_targets'] = price_margin_targets_payload
    flags['business_advisor_targets'] = business_advisor_targets_payload
    flags['contact'] = contact_payload
    era_upload_payload = _tenant_era_exploration_settings(tenant)
    if era_exploration_file is not None and era_exploration_file.filename:
        era_saved_payload, era_error = _save_era_exploration_upload(
            tenant,
            era_exploration_file,
            str(getattr(admin_user, 'email', '') or getattr(admin_user, 'username', '') or 'admin'),
        )
        if era_error:
            sep = '&' if '?' in redirect_target else '?'
            return RedirectResponse(url=f'{redirect_target}{sep}updated=0&reason={era_error}', status_code=303)
        era_upload_payload = era_saved_payload or era_upload_payload
    if era_upload_payload.get('file_path'):
        flags['era_exploration_data_config'] = era_upload_payload
    tenant.feature_flags = flags

    tenant_connections = (
        await db.execute(select(TenantConnection).where(TenantConnection.tenant_id == tenant.id))
    ).scalars().all()
    for conn in tenant_connections:
        conn_params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
        conn.connection_parameters = {
            **conn_params,
            'auto_sync_enabled': bool(auto_sync_payload['enabled']),
            'sync_interval_minutes': int(auto_sync_payload['interval_minutes']),
            'auto_sync_profile': str(auto_sync_payload['profile']),
            'sync_overlap_minutes': int(auto_sync_payload['overlap_minutes']),
            'sync_recovery_days': int(auto_sync_payload['recovery_days']),
            'auto_sync_business_hours': auto_sync_payload['business_hours'],
            'stream_sync_overrides': auto_sync_payload['stream_overrides'],
            'daily_reconciliation': {
                'enabled': bool(reconciliation_payload['enabled']),
                'time': str(reconciliation_payload['time']),
                'timezone': str(reconciliation_payload['timezone']),
                'lookback_days': int(reconciliation_payload['lookback_days']),
                'streams': reconciliation_payload['streams'],
            },
        }
        db.add(conn)

    call_center_fqdn_base = str(call_center_fqdn or '').strip().rstrip('/')
    call_center_fqdn_clean = (call_center_fqdn_base + '/') if call_center_fqdn_base else ''
    if call_center_fqdn_clean and not call_center_fqdn_clean.startswith(('https://', 'http://')):
        call_center_fqdn_clean = f'https://{call_center_fqdn_clean}'
    call_center_payload = {
        'fqdn': call_center_fqdn_clean,
        'username': str(call_center_username or '').strip(),
        'client_id': str(call_center_client_id or '').strip(),
        'queue_ids': str(call_center_queue_ids or '').strip(),
        'team_extensions': str(call_center_team_extensions or '').strip(),
        'agent_directory_text': str(call_center_agent_directory_text or '').strip(),
        'queue_directory_text': str(call_center_queue_directory_text or '').strip(),
        'polling_interval_minutes': _parse_int_in_range(
            call_center_polling_interval_minutes,
            default=5,
            min_value=1,
            max_value=1440,
        ),
        'answer_sla_seconds': _parse_int_in_range(
            call_center_answer_sla_seconds,
            default=30,
            min_value=1,
            max_value=3600,
        ),
        'target_calls_per_agent': _parse_int_in_range(
            call_center_target_calls_per_agent,
            default=60,
            min_value=1,
            max_value=1000,
        ),
    }
    call_center_conn = await _find_tenant_connection(db, tenant_id=tenant.id, connector_type=_3CX_CONNECTOR_TYPE)
    existing_call_center_params = (
        call_center_conn.connection_parameters
        if call_center_conn is not None and isinstance(call_center_conn.connection_parameters, dict)
        else {}
    )
    for preserved_key in ('fqdn', 'username', 'client_id', 'queue_ids', 'team_extensions'):
        if not str(call_center_payload.get(preserved_key) or '').strip():
            call_center_payload[preserved_key] = str(existing_call_center_params.get(preserved_key) or '').strip()
    if isinstance(existing_call_center_params.get('manual_import'), dict):
        call_center_payload['manual_import'] = existing_call_center_params['manual_import']
    existing_call_center_secret: dict[str, Any] = {}
    if call_center_conn is not None and call_center_conn.enc_payload:
        try:
            existing_call_center_secret = decrypt_json_secret(call_center_conn.enc_payload)
        except Exception:
            existing_call_center_secret = {}
    call_center_secret = {
        'username': call_center_payload['username'],
        'password': (
            str(call_center_password or '')
            if str(call_center_password or '').strip()
            else str(existing_call_center_secret.get('password') or '')
        ),
    }
    call_center_has_config = any(
        str(call_center_payload.get(key) or '').strip()
        for key in (
            'fqdn',
            'username',
            'client_id',
            'queue_ids',
            'team_extensions',
            'agent_directory_text',
            'queue_directory_text',
        )
    ) or bool(call_center_payload.get('manual_import'))
    if call_center_conn is None:
        if call_center_has_config:
            call_center_conn = TenantConnection(
                tenant_id=tenant.id,
                connector_type=_3CX_CONNECTOR_TYPE,
                source_type='ssh_db',
                is_active=True,
                supported_streams=['call_center_kpis'],
                enabled_streams=['call_center_kpis'],
            )
            db.add(call_center_conn)
    if call_center_conn is not None:
        call_center_conn.is_active = bool(call_center_has_config)
        call_center_conn.source_type = str(call_center_conn.source_type or 'ssh_db')
        call_center_conn.connection_parameters = call_center_payload if call_center_has_config else {}
        call_center_conn.enc_payload = encrypt_json_secret(call_center_secret) if call_center_has_config else ''
        call_center_conn.sync_status = call_center_conn.sync_status or ('configured' if call_center_has_config else 'disabled')
        db.add(call_center_conn)

    audit_payload = {
        'before': {
            **previous,
            'call_center_3cx': _summarize_3cx_audit_payload(previous.get('call_center_3cx')),
        },
        'after': {
            'name': tenant.name,
            'slug': tenant.slug,
            'plan': tenant.plan.value,
            'source': tenant.source,
            'tenant_status': tenant.status.value,
            'subscription_status': sub.status.value,
            'contact': contact_payload,
            'auto_sync': auto_sync_payload,
            'daily_reconciliation': reconciliation_payload,
            'duplicate_protection': duplicate_protection_payload,
            'inventory_item_classification': settings_payload,
            'eshop_fulfillment': eshop_fulfillment_payload,
            'document_series_labels': flags['document_series_labels'],
            'price_margin_targets': price_margin_targets_payload,
            'business_advisor_targets': business_advisor_targets_payload,
            'era_exploration_data': era_upload_payload,
            'call_center_3cx': _summarize_3cx_audit_payload(
                {**call_center_payload, 'password': '***' if call_center_secret.get('password') else ''}
            ),
        },
    }
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_updated_ui',
            entity_type='tenant',
            entity_id=str(tenant.id),
            payload=_json_safe_payload(audit_payload),
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
    if _is_infrastructure_tenant(tenant):
        redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}deleted=0&reason=system_tenant', status_code=303)

    tenant_db_name_value = tenant.db_name
    tenant_db_user_value = tenant.db_user
    tenant_user_ids = (
        await db.execute(select(User.id).where(User.tenant_id == tenant.id))
    ).scalars().all()
    subscription_ids = (
        await db.execute(select(Subscription.id).where(Subscription.tenant_id == tenant.id))
    ).scalars().all()
    invoice_ids = (
        await db.execute(select(Invoice.id).where(Invoice.tenant_id == tenant.id))
    ).scalars().all()

    if tenant_user_ids:
        await db.execute(delete(RefreshToken).where(RefreshToken.user_id.in_(tenant_user_ids)))
        await db.execute(update(AuditLog).where(AuditLog.actor_user_id.in_(tenant_user_ids)).values(actor_user_id=None))

    if invoice_ids:
        await db.execute(delete(Payment).where(Payment.invoice_id.in_(invoice_ids)))
    await db.execute(delete(Payment).where(Payment.tenant_id == tenant.id))
    if subscription_ids:
        await db.execute(delete(SubscriptionLimit).where(SubscriptionLimit.subscription_id.in_(subscription_ids)))
    await db.execute(delete(Invoice).where(Invoice.tenant_id == tenant.id))
    await db.execute(delete(SubscriptionEvent).where(SubscriptionEvent.tenant_id == tenant.id))
    await db.execute(delete(Subscription).where(Subscription.tenant_id == tenant.id))
    await db.execute(delete(TenantApiKey).where(TenantApiKey.tenant_id == tenant.id))
    await db.execute(delete(TenantConnection).where(TenantConnection.tenant_id == tenant.id))
    await db.execute(delete(TenantRuleOverride).where(TenantRuleOverride.tenant_id == tenant.id))
    await db.execute(update(WhmcsService).where(WhmcsService.tenant_id == tenant.id).values(tenant_id=None))
    await db.execute(update(AuditLog).where(AuditLog.tenant_id == tenant.id).values(tenant_id=None))
    await db.execute(delete(User).where(User.tenant_id == tenant.id))
    await db.delete(tenant)
    await db.commit()

    try:
        await asyncio.to_thread(_drop_db_and_role, tenant_db_name_value, tenant_db_user_value)
    except Exception:
        logging.exception(
            "Failed to drop tenant database after control hard delete",
            extra={'tenant_id': tenant_id, 'db_name': tenant_db_name_value, 'db_user': tenant_db_user_value},
        )
        redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
        sep = '&' if '?' in redirect_target else '?'
        return RedirectResponse(url=f'{redirect_target}{sep}deleted=1&db_deleted=0', status_code=303)

    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}deleted=1&db_deleted=1', status_code=303)


@router.get('/admin/tenants/{tenant_id}/delete')
async def admin_tenant_delete_get(
    tenant_id: int,
    next_url: str = Query(default='/admin/tenants'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/tenants'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(
        url=f'{redirect_target}{sep}deleted=0&reason=delete_requires_post&tenant_id={tenant_id}',
        status_code=303,
    )


@router.get('/admin/subscriptions', response_class=HTMLResponse)
async def admin_subscriptions(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).order_by(Tenant.created_at.desc()))).scalars().all()
    catalog_rows = (
        await db.execute(select(PlanFeatureCatalog).where(PlanFeatureCatalog.is_active == True).order_by(PlanFeatureCatalog.group, PlanFeatureCatalog.label))
    ).scalars().all()
    dynamic_subscription_features = tuple(_catalog_to_subscription_feature(row) for row in catalog_rows)
    all_subscription_features = (*SUBSCRIPTION_FEATURES, *dynamic_subscription_features)
    catalog_by_key = {row.feature_key: row for row in catalog_rows}
    subscriptions_by_tenant: dict[int, Subscription] = {}
    subscription_features_by_tenant: dict[int, dict[str, bool]] = {}
    subscription_limits_by_tenant: dict[int, dict[str, object]] = {}
    subscription_feature_allowed_by_tenant: dict[int, dict[str, bool]] = {}
    subscription_customer_feature_keys_by_tenant: dict[int, set[str]] = {}
    for t in tenants:
        sub = await get_or_create_subscription(db, t)
        await sync_tenant_from_subscription(db, t, sub)
        limit_context = await _tenant_user_license_context(db, t)
        feature_values = normalize_subscription_feature_flags(sub.plan, sub.feature_flags)
        allowed_values: dict[str, bool] = {}
        raw_flags = dict(sub.feature_flags or {})
        for row in catalog_rows:
            allowed = _catalog_feature_allowed(row, sub.plan, int(t.id))
            feature_values[row.feature_key] = bool(raw_flags.get(row.feature_key, _catalog_feature_default(row, sub.plan, int(t.id)))) if allowed else False
            allowed_values[row.feature_key] = allowed
        subscriptions_by_tenant[int(t.id)] = sub
        subscription_features_by_tenant[int(t.id)] = feature_values
        subscription_limits_by_tenant[int(t.id)] = limit_context
        subscription_feature_allowed_by_tenant[int(t.id)] = allowed_values
        subscription_customer_feature_keys_by_tenant[int(t.id)] = {row.feature_key for row in catalog_rows if row.tenant_id == t.id}
    await db.commit()
    plan_choices = [
        {'value': PlanName.standard.value, 'label': 'Standard'},
        {'value': PlanName.pro.value, 'label': 'Pro'},
        {'value': PlanName.enterprise.value, 'label': 'Enterprise'},
        {'value': PlanName.custom.value, 'label': 'Custom'},
    ]
    addon_feature_keys = set(ADD_ON_FEATURE_KEYS) | {row.feature_key for row in catalog_rows if _catalog_feature_is_addon(row)}
    addon_allowed_by_plan = {
        plan.value: {
            feature.key: (addon_allowed_for_plan(plan, feature.key) if feature.key not in catalog_by_key else _catalog_feature_allowed(catalog_by_key[feature.key], plan))
            for feature in all_subscription_features
            if feature.key in addon_feature_keys
        }
        for plan in PlanName
    }
    return templates.TemplateResponse(
        'admin/subscriptions.html',
        {
            'request': request,
            'tenants': tenants,
            'subscriptions_by_tenant': subscriptions_by_tenant,
            'subscription_features_by_tenant': subscription_features_by_tenant,
            'subscription_feature_allowed_by_tenant': subscription_feature_allowed_by_tenant,
            'subscription_customer_feature_keys_by_tenant': subscription_customer_feature_keys_by_tenant,
            'subscription_limits_by_tenant': subscription_limits_by_tenant,
            'subscription_features': all_subscription_features,
            'plan_choices': plan_choices,
            'addon_feature_keys': addon_feature_keys,
            'addon_allowed_by_plan': addon_allowed_by_plan,
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
    plan: str = Form(default=''),
    subscription_status: str = Form(default=''),
    max_users: str = Form(default=''),
    max_concurrent_sessions: str = Form(default=''),
    max_branches: str = Form(default=''),
    enabled_features: list[str] = Form(default=[]),
    custom_agreement_notes: str = Form(default=''),
    custom_exclusive_implementations: str = Form(default=''),
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
    prev_plan = sub.plan
    try:
        selected_plan = PlanName(str(plan or sub.plan.value).strip().lower())
    except ValueError:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=invalid_plan', status_code=303)
    sub.plan = selected_plan

    limit_context = await _tenant_user_license_context(db, tenant)
    requested_max_users_raw = (max_users or '').strip()
    requested_max_users = int(limit_context['max_users'])
    if requested_max_users_raw:
        try:
            requested_max_users = int(requested_max_users_raw)
        except ValueError:
            return RedirectResponse(url=f'{redirect_target}?saved=0&reason=bad_max_users', status_code=303)
        requested_max_users = max(1, min(requested_max_users, 9999))
        if requested_max_users < int(limit_context['active_users']):
            return RedirectResponse(url=f'{redirect_target}?saved=0&reason=max_users_below_active', status_code=303)
        limit_obj = limit_context.get('limit')
        if isinstance(limit_obj, SubscriptionLimit):
            limit_obj.limit_value = requested_max_users
            limit_obj.used_value = int(limit_context['active_users'])
    # Purchased simultaneous connections (enforced at login). Separate from max_users (accounts).
    requested_concurrent_raw = (max_concurrent_sessions or '').strip()
    if requested_concurrent_raw:
        try:
            requested_concurrent = max(1, min(int(requested_concurrent_raw), 9999))
        except ValueError:
            return RedirectResponse(url=f'{redirect_target}?saved=0&reason=bad_max_concurrent', status_code=303)
        conc_row = (
            await db.execute(
                select(SubscriptionLimit).where(
                    SubscriptionLimit.subscription_id == sub.id,
                    SubscriptionLimit.limit_key == 'max_concurrent_sessions',
                )
            )
        ).scalar_one_or_none()
        if conc_row is None:
            conc_row = SubscriptionLimit(
                subscription_id=sub.id,
                limit_key='max_concurrent_sessions',
                limit_value=requested_concurrent,
                used_value=0,
            )
            db.add(conc_row)
        else:
            conc_row.limit_value = requested_concurrent
    requested_max_branches_raw = (max_branches or '').strip()
    requested_max_branches = int(limit_context.get('max_branches') or 1)
    if requested_max_branches_raw:
        try:
            requested_max_branches = int(requested_max_branches_raw)
        except ValueError:
            return RedirectResponse(url=f'{redirect_target}?saved=0&reason=bad_max_branches', status_code=303)
        requested_max_branches = max(1, min(requested_max_branches, 9999))
        branch_limit_obj = limit_context.get('branch_limit')
        if isinstance(branch_limit_obj, SubscriptionLimit):
            branch_limit_obj.limit_value = requested_max_branches
            branch_limit_obj.used_value = int(limit_context.get('active_branches') or 0)
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
    selected_features = {str(item) for item in (enabled_features or [])}
    catalog_rows = (
        await db.execute(select(PlanFeatureCatalog).where(PlanFeatureCatalog.is_active == True).order_by(PlanFeatureCatalog.group, PlanFeatureCatalog.label))
    ).scalars().all()
    sub.feature_flags = {
        feature.key: feature.key in selected_features
        for feature in (*SUBSCRIPTION_FEATURES, *(_catalog_to_subscription_feature(row) for row in catalog_rows))
    }
    for row in catalog_rows:
        if not _catalog_feature_allowed(row, selected_plan, int(tenant.id)):
            sub.feature_flags[row.feature_key] = False
    custom_agreement_notes = (custom_agreement_notes or '').strip()
    if selected_plan == PlanName.custom and custom_agreement_notes:
        sub.feature_flags['custom_agreement_notes'] = custom_agreement_notes
    custom_exclusive_implementations = (custom_exclusive_implementations or '').strip()
    if selected_plan == PlanName.custom and custom_exclusive_implementations:
        sub.feature_flags['custom_exclusive_implementations'] = custom_exclusive_implementations
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
            payload={
                'from': prev,
                'to': sub.status.value,
                'from_plan': prev_plan.value,
                'to_plan': sub.plan.value,
                'note': note or None,
                'feature_flags': sub.feature_flags,
                'max_users': requested_max_users,
                'max_branches': requested_max_branches,
            },
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


@router.post('/admin/subscriptions/{tenant_id}/temporary-upgrade')
async def admin_subscription_temporary_upgrade(
    tenant_id: int,
    target_plan: str = Form(default='enterprise'),
    trial_days: int = Form(default=14),
    note: str = Form(default=''),
    next_url: str = Form(default='/admin/subscriptions'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    note = (note or '').strip()
    next_url = (next_url or '/admin/subscriptions').strip()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/subscriptions'
    try:
        selected_plan = PlanName(target_plan)
    except ValueError:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=invalid_plan', status_code=303)
    if selected_plan not in {PlanName.pro, PlanName.enterprise, PlanName.custom}:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=invalid_trial_plan', status_code=303)
    trial_days = max(1, min(int(trial_days or 14), 90))

    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=tenant_not_found', status_code=303)

    sub = await get_or_create_subscription(db, tenant)
    now = datetime.utcnow()
    expires_at = now + timedelta(days=trial_days)
    previous_plan = sub.plan
    previous_status = sub.status
    previous_current_period_end = sub.current_period_end
    previous_trial_ends_at = sub.trial_ends_at
    previous_flags = dict(sub.feature_flags or {})
    previous_flags.pop('_temporary_upgrade', None)

    sub.plan = selected_plan
    sub.status = SubscriptionStatus.active
    sub.current_period_start = now
    sub.current_period_end = expires_at
    sub.feature_flags = {
        **infer_subscription_feature_defaults(selected_plan),
        '_temporary_upgrade': {
            'active': True,
            'started_at': now.isoformat(),
            'expires_at': expires_at.isoformat(),
            'temporary_plan': selected_plan.value,
            'original_plan': previous_plan.value,
            'original_status': previous_status.value,
            'original_feature_flags': previous_flags,
            'original_current_period_end': previous_current_period_end.isoformat() if previous_current_period_end else None,
            'original_trial_ends_at': previous_trial_ends_at.isoformat() if previous_trial_ends_at else None,
            'days': trial_days,
            'note': note or None,
        },
    }
    await sync_tenant_from_subscription(db, tenant, sub)
    db.add(
        SubscriptionEvent(
            tenant_id=tenant.id,
            from_status=previous_status.value,
            to_status=sub.status.value,
            note=f'Temporary upgrade to {selected_plan.value} for {trial_days} days' + (f' - {note}' if note else ''),
        )
    )
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='subscription_temporary_upgrade_started',
            entity_type='subscription',
            entity_id=str(sub.id),
            payload={
                'from_plan': previous_plan.value,
                'to_plan': selected_plan.value,
                'from_status': previous_status.value,
                'to_status': sub.status.value,
                'expires_at': expires_at.isoformat(),
                'days': trial_days,
                'note': note or None,
            },
        )
    )
    try:
        await db.commit()
    except Exception:
        logger.exception('subscription_temporary_upgrade_failed', extra={'tenant_id': tenant_id})
        await db.rollback()
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=commit_failed', status_code=303)
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}trial_upgrade=1', status_code=303)


@router.post('/admin/subscriptions/{tenant_id}/temporary-upgrade/cancel')
async def admin_subscription_temporary_upgrade_cancel(
    tenant_id: int,
    note: str = Form(default=''),
    next_url: str = Form(default='/admin/subscriptions'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    note = (note or '').strip()
    next_url = (next_url or '/admin/subscriptions').strip()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/subscriptions'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=tenant_not_found', status_code=303)
    sub = await get_or_create_subscription(db, tenant)
    flags = dict(sub.feature_flags or {})
    temporary_upgrade = flags.get('_temporary_upgrade') if isinstance(flags.get('_temporary_upgrade'), dict) else None
    if not temporary_upgrade or not temporary_upgrade.get('active'):
        return RedirectResponse(url=f'{redirect_target}?saved=0&reason=no_temporary_upgrade', status_code=303)

    previous_plan = sub.plan
    previous_status = sub.status
    try:
        sub.plan = PlanName(str(temporary_upgrade.get('original_plan') or tenant.plan.value))
    except ValueError:
        sub.plan = PlanName.standard
    try:
        sub.status = SubscriptionStatus(str(temporary_upgrade.get('original_status') or tenant.subscription_status.value))
    except ValueError:
        sub.status = SubscriptionStatus.active
    original_feature_flags = temporary_upgrade.get('original_feature_flags')
    sub.feature_flags = dict(original_feature_flags) if isinstance(original_feature_flags, dict) else {}
    original_current_period_end = str(temporary_upgrade.get('original_current_period_end') or '').strip()
    if original_current_period_end:
        try:
            sub.current_period_end = datetime.fromisoformat(original_current_period_end)
        except ValueError:
            sub.current_period_end = None
    else:
        sub.current_period_end = None
    original_trial_ends_at = str(temporary_upgrade.get('original_trial_ends_at') or '').strip()
    if original_trial_ends_at:
        try:
            sub.trial_ends_at = datetime.fromisoformat(original_trial_ends_at)
        except ValueError:
            pass
    await sync_tenant_from_subscription(db, tenant, sub)
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='subscription_temporary_upgrade_cancelled',
            entity_type='subscription',
            entity_id=str(sub.id),
            payload={
                'from_plan': previous_plan.value,
                'to_plan': sub.plan.value,
                'from_status': previous_status.value,
                'to_status': sub.status.value,
                'note': note or None,
            },
        )
    )
    await db.commit()
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}trial_upgrade_cancelled=1', status_code=303)


@router.get('/admin/plans', response_class=HTMLResponse)
async def admin_plans(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    rows = (await db.execute(select(PlanFeature).order_by(PlanFeature.plan, PlanFeature.feature_name))).scalars().all()
    catalog_rows = (
        await db.execute(select(PlanFeatureCatalog).where(PlanFeatureCatalog.is_active == True).order_by(PlanFeatureCatalog.group, PlanFeatureCatalog.label))
    ).scalars().all()
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name))).scalars().all()
    plans: dict[str, dict[str, bool]] = {}
    for plan_name in PlanName:
        plans[plan_name.value] = normalize_subscription_feature_flags(plan_name, {})
    for r in rows:
        plans.setdefault(r.plan.value, {})[r.feature_name] = bool(r.enabled)
    for row in catalog_rows:
        for plan_name in PlanName:
            plans.setdefault(plan_name.value, {})[row.feature_key] = _catalog_feature_default(row, plan_name)
    return templates.TemplateResponse(
        'admin/plans.html',
        {
            'request': request,
            'plans': plans,
            'subscription_features': SUBSCRIPTION_FEATURES,
            'dynamic_feature_catalog': catalog_rows,
            'dynamic_subscription_features': tuple(_catalog_to_subscription_feature(row) for row in catalog_rows),
            'plan_status_choices': [
                {'value': 'none', 'label': 'Όχι'},
                {'value': 'included', 'label': 'Περιλαμβάνεται'},
                {'value': 'extra', 'label': 'Extra'},
                {'value': 'custom', 'label': 'Custom / πελάτη'},
            ],
            'tenants': tenants,
            'active_page': 'plans',
            'title': 'title_plan_features',
        },
    )


async def _cascade_plan_feature_flags_to_subscriptions(
    db: AsyncSession,
    *,
    plan: PlanName,
    feature_values: dict[str, bool],
    tenant_id: int | None = None,
) -> int:
    stmt = (
        select(Subscription, Tenant)
        .join(Tenant, Tenant.id == Subscription.tenant_id)
        .where(Subscription.plan == plan)
    )
    if tenant_id is not None:
        stmt = stmt.where(Tenant.id == tenant_id)
    rows = (
        await db.execute(stmt)
    ).all()
    updated = 0
    for sub, tenant in rows:
        flags = dict(sub.feature_flags or {})
        changed = False
        for feature_key, enabled in feature_values.items():
            if flags.get(feature_key) != bool(enabled):
                flags[feature_key] = bool(enabled)
                changed = True
        if not changed:
            continue
        sub.feature_flags = flags
        flag_modified(sub, 'feature_flags')
        await sync_tenant_from_subscription(db, tenant, sub)
        flag_modified(tenant, 'feature_flags')
        await invalidate_tenant_cache(str(tenant.id))
        updated += 1
    return updated


@router.post('/admin/plans/{plan}/features')
async def admin_plan_features_update(
    request: Request,
    plan: str,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    selected_plan = PlanName(plan)
    form = await request.form()
    enabled_features = {str(item) for item in form.getlist('enabled_features')}
    values = {feature.key: feature.key in enabled_features for feature in SUBSCRIPTION_FEATURES}
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
    updated_subscriptions = await _cascade_plan_feature_flags_to_subscriptions(
        db,
        plan=selected_plan,
        feature_values=values,
    )
    db.add(
        AuditLog(
            tenant_id=None,
            action='plan_features_cascaded_ui',
            entity_type='plan',
            entity_id=selected_plan.value,
            payload={'updated_subscriptions': updated_subscriptions},
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/plans', status_code=303)


@router.post('/admin/plans/{plan}/features/{feature_key}/toggle')
async def admin_plan_feature_toggle(
    plan: str,
    feature_key: str,
    next_url: str = Form(default='/admin/plans'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    try:
        selected_plan = PlanName(plan)
    except ValueError:
        return RedirectResponse(url='/admin/plans?saved=0&reason=invalid_plan', status_code=303)
    feature_keys = {feature.key for feature in SUBSCRIPTION_FEATURES}
    if feature_key not in feature_keys:
        return RedirectResponse(url='/admin/plans?saved=0&reason=feature_not_found', status_code=303)

    defaults = normalize_subscription_feature_flags(selected_plan, {})
    row = (
        await db.execute(
            select(PlanFeature).where(
                PlanFeature.plan == selected_plan,
                PlanFeature.feature_name == feature_key,
            )
        )
    ).scalar_one_or_none()
    current_enabled = bool(row.enabled) if row is not None else bool(defaults.get(feature_key))
    next_enabled = not current_enabled
    if row is None:
        db.add(PlanFeature(plan=selected_plan, feature_name=feature_key, enabled=next_enabled))
    else:
        row.enabled = next_enabled
    db.add(
        AuditLog(
            tenant_id=None,
            action='plan_feature_toggled_ui',
            entity_type='plan',
            entity_id=selected_plan.value,
            payload={'feature': feature_key, 'enabled': next_enabled},
        )
    )
    updated_subscriptions = await _cascade_plan_feature_flags_to_subscriptions(
        db,
        plan=selected_plan,
        feature_values={feature_key: next_enabled},
    )
    db.add(
        AuditLog(
            tenant_id=None,
            action='plan_feature_toggle_cascaded_ui',
            entity_type='plan',
            entity_id=selected_plan.value,
            payload={'feature': feature_key, 'enabled': next_enabled, 'updated_subscriptions': updated_subscriptions},
        )
    )
    await db.commit()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/plans'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}saved=1', status_code=303)


@router.post('/admin/plans/custom-features')
async def admin_plan_custom_feature_create(
    label: str = Form(default=''),
    feature_key: str = Form(default=''),
    group: str = Form(default='Custom'),
    feature_type: str = Form(default='feature'),
    status_standard: str = Form(default='none'),
    status_pro: str = Form(default='none'),
    status_enterprise: str = Form(default='none'),
    status_custom: str = Form(default='custom'),
    tenant_id: str = Form(default=''),
    description: str = Form(default=''),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    label = (label or '').strip()
    if not label:
        return RedirectResponse(url='/admin/plans?saved=0&reason=missing_label', status_code=303)
    tenant_id_int: int | None = None
    if (tenant_id or '').strip():
        try:
            tenant_id_int = int(tenant_id)
        except ValueError:
            return RedirectResponse(url='/admin/plans?saved=0&reason=bad_tenant', status_code=303)
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id_int))).scalar_one_or_none()
        if tenant is None:
            return RedirectResponse(url='/admin/plans?saved=0&reason=tenant_not_found', status_code=303)
    base_key = _normalize_feature_key(feature_key, label)
    candidate = base_key
    suffix = 2
    while (await db.execute(select(PlanFeatureCatalog).where(PlanFeatureCatalog.feature_key == candidate))).scalar_one_or_none() is not None:
        tail = f'_{suffix}'
        candidate = f'{base_key[:64 - len(tail)]}{tail}'
        suffix += 1
    if tenant_id_int is not None:
        plan_status = {
            PlanName.standard.value: 'none',
            PlanName.pro.value: 'none',
            PlanName.enterprise.value: 'none',
            PlanName.custom.value: 'custom',
        }
    else:
        plan_status = {
            PlanName.standard.value: _normalize_catalog_status(status_standard),
            PlanName.pro.value: _normalize_catalog_status(status_pro),
            PlanName.enterprise.value: _normalize_catalog_status(status_enterprise),
            PlanName.custom.value: _normalize_catalog_status(status_custom),
        }
    row = PlanFeatureCatalog(
        feature_key=candidate,
        label=label,
        group=(group or 'Custom').strip()[:64],
        feature_type=_normalize_feature_key(feature_type, 'feature')[:32],
        plan_status=plan_status,
        tenant_id=tenant_id_int,
        description=(description or '').strip() or None,
        is_active=True,
    )
    db.add(row)
    updated_subscriptions = 0
    for plan_name in PlanName:
        enabled = _normalize_catalog_status(plan_status.get(plan_name.value, 'none')) != 'none'
        if tenant_id_int is not None and plan_name != PlanName.custom:
            enabled = False
        updated_subscriptions += await _cascade_plan_feature_flags_to_subscriptions(
            db,
            plan=plan_name,
            feature_values={candidate: enabled},
            tenant_id=tenant_id_int,
        )
    db.add(
        AuditLog(
            tenant_id=tenant_id_int,
            action='plan_custom_feature_created_ui',
            entity_type='plan_feature_catalog',
            entity_id=candidate,
            payload={
                'label': label,
                'group': row.group,
                'feature_type': row.feature_type,
                'plan_status': plan_status,
                'tenant_id': tenant_id_int,
                'updated_subscriptions': updated_subscriptions,
            },
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/plans?saved=1', status_code=303)


@router.post('/admin/plans/custom-features/{feature_key}/archive')
async def admin_plan_custom_feature_archive(
    feature_key: str,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    row = (
        await db.execute(select(PlanFeatureCatalog).where(PlanFeatureCatalog.feature_key == feature_key))
    ).scalar_one_or_none()
    if row is None:
        return RedirectResponse(url='/admin/plans?saved=0&reason=feature_not_found', status_code=303)
    row.is_active = False
    db.add(
        AuditLog(
            tenant_id=row.tenant_id,
            action='plan_custom_feature_archived_ui',
            entity_type='plan_feature_catalog',
            entity_id=row.feature_key,
            payload={'label': row.label},
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/plans?saved=1', status_code=303)


@router.post('/admin/plans/custom-features/{feature_key}/toggle')
async def admin_plan_custom_feature_toggle(
    feature_key: str,
    plan: str = Form(default=''),
    next_url: str = Form(default='/admin/plans'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    try:
        selected_plan = PlanName(plan)
    except ValueError:
        return RedirectResponse(url='/admin/plans?saved=0&reason=invalid_plan', status_code=303)
    row = (
        await db.execute(
            select(PlanFeatureCatalog).where(
                PlanFeatureCatalog.feature_key == feature_key,
                PlanFeatureCatalog.is_active == True,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return RedirectResponse(url='/admin/plans?saved=0&reason=feature_not_found', status_code=303)
    if row.tenant_id is not None and selected_plan != PlanName.custom:
        return RedirectResponse(url='/admin/plans?saved=0&reason=customer_feature_custom_only', status_code=303)

    statuses = dict(row.plan_status or {})
    current_status = _normalize_catalog_status(str(statuses.get(selected_plan.value) or 'none'))
    if current_status == 'none':
        statuses[selected_plan.value] = 'custom' if selected_plan == PlanName.custom else 'included'
    else:
        statuses[selected_plan.value] = 'none'
    row.plan_status = statuses
    flag_modified(row, 'plan_status')
    next_enabled = statuses[selected_plan.value] != 'none'
    updated_subscriptions = await _cascade_plan_feature_flags_to_subscriptions(
        db,
        plan=selected_plan,
        feature_values={row.feature_key: next_enabled},
        tenant_id=row.tenant_id,
    )
    db.add(
        AuditLog(
            tenant_id=row.tenant_id,
            action='plan_custom_feature_toggled_ui',
            entity_type='plan_feature_catalog',
            entity_id=row.feature_key,
            payload={
                'plan': selected_plan.value,
                'status': statuses[selected_plan.value],
                'updated_subscriptions': updated_subscriptions,
            },
        )
    )
    await db.commit()
    redirect_target = next_url if next_url.startswith('/admin/') else '/admin/plans'
    sep = '&' if '?' in redirect_target else '?'
    return RedirectResponse(url=f'{redirect_target}{sep}saved=1', status_code=303)


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
    is_active: bool = Form(default=False),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(default=''),
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
    conn = await _find_tenant_connection(db, tenant_id=tenant_id, connector_type=connector_type)
    resolved_password = _resolve_secret_password(password, conn)
    if not resolved_password:
        result = {'status': 'error', 'message': 'Missing password. Fill password or save connection with credentials first.'}
        context = await _connections_template_context(
            db,
            request=request,
            result=result,
            discovery=None,
            form_values={
                'tenant_id': tenant_id,
                'host': host,
                'connector_type': connector_type,
                'is_active': bool(is_active),
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
                'has_saved_password': bool(conn and conn.enc_payload),
            },
            active_page=('data_sources' if source_page == 'data_sources' else 'connections'),
            title=('title_data_sources' if source_page == 'data_sources' else 'connections'),
        )
        return templates.TemplateResponse('admin/connections.html', context)

    secret = SqlServerSecret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=resolved_password,
        options=options_map,
    )

    result: dict[str, str] = {'status': 'ok', 'message': 'Connection test successful (SELECT 1).'}
    try:
        test_connection(build_odbc_connection_string(secret))
        if conn is not None:
            conn.last_test_ok_at = datetime.utcnow()
            conn.last_test_error = None
            await db.commit()
        # Successful test — close any open circuit breaker for this tenant so
        # the drain will resume once the connection is saved and a sync triggered.
        _test_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if _test_tenant:
            close_ingest_circuit(str(_test_tenant.slug))
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
            'is_active': bool(is_active),
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
            'has_saved_password': bool(conn and conn.enc_payload),
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
    is_active: bool = Form(default=False),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(default=''),
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
    conn = await _find_tenant_connection(db, tenant_id=tenant_id, connector_type=connector_type)
    resolved_password = _resolve_secret_password(password, conn)
    discovery: dict = {'objects': [], 'selected_schema': selected_schema, 'selected_object': selected_object}
    if not resolved_password:
        result = {'status': 'error', 'message': 'Missing password. Fill password or save connection with credentials first.'}
        context = await _connections_template_context(
            db,
            request=request,
            result=result,
            discovery=discovery,
            form_values={
                'tenant_id': tenant_id,
                'host': host,
                'connector_type': connector_type,
                'is_active': bool(is_active),
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
                'has_saved_password': bool(conn and conn.enc_payload),
            },
            active_page=('data_sources' if source_page == 'data_sources' else 'connections'),
            title=('title_data_sources' if source_page == 'data_sources' else 'connections'),
        )
        return templates.TemplateResponse('admin/connections.html', context)

    secret = SqlServerSecret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=resolved_password,
        options=options_map,
    )
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
            'is_active': bool(is_active),
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
            'has_saved_password': bool(conn and conn.enc_payload),
        },
        active_page=('data_sources' if source_page == 'data_sources' else 'connections'),
        title=('title_data_sources' if source_page == 'data_sources' else 'connections'),
    )
    return templates.TemplateResponse('admin/connections.html', context)


@router.post('/admin/connections/save')
async def admin_connections_save(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    is_active: bool = Form(default=False),
    source_type: str = Form(default='sql'),
    source_page: str = Form(default='connections'),
    host: str = Form(...),
    port: int = Form(default=1433),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(default=''),
    options: str = Form(default='Encrypt=yes;TrustServerCertificate=yes'),
    sales_query_template: str = Form(default=DEFAULT_GENERIC_SALES_QUERY),
    purchases_query_template: str = Form(default=DEFAULT_GENERIC_PURCHASES_QUERY),
    inventory_query_template: str = Form(default=DEFAULT_GENERIC_INVENTORY_QUERY),
    cashflow_query_template: str = Form(default=DEFAULT_GENERIC_CASHFLOW_QUERY),
    supplier_balances_query_template: str = Form(default=DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY),
    customer_balances_query_template: str = Form(default=DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY),
    stream_field_mapping_json: str = Form(default='{}'),
    stream_api_endpoint_json: str = Form(default='{}'),
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
    sync_interval_minutes: int = Form(default=5),
    api_auth_type: str = Form(default='softone_login'),
    api_app_id: str = Form(default=''),
    api_company: str = Form(default=''),
    api_branch: str = Form(default=''),
    api_module: str = Form(default='0'),
    api_refid: str = Form(default=''),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                saved=0,
            ),
            status_code=303,
        )

    options_map = _parse_options_map(options)

    conn = await _find_tenant_connection(db, tenant_id=tenant_id, connector_type=connector_type)
    if conn is None:
        conn = TenantConnection(
            tenant_id=tenant_id,
            connector_type=connector_type,
            sync_status='never',
        )
        db.add(conn)

    normalized_source_type = _normalize_source_type(connector_type, source_type)
    if normalized_source_type == 'api' or str(connector_type).strip().lower() == 'external_api':
        existing_params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
        existing_auth = existing_params.get('auth_config') if isinstance(existing_params.get('auth_config'), dict) else {}
        existing_secret: dict[str, object] = {}
        if conn.enc_payload:
            try:
                existing_secret = decrypt_json_secret(conn.enc_payload)
            except Exception:
                existing_secret = {}
        resolved_password = (
            (password or '').strip()
            or str(existing_secret.get('password') or '').strip()
            or str(existing_auth.get('password') or '').strip()
        )
        if not resolved_password:
            return RedirectResponse(
                url=_connections_redirect_url(
                    redirect_base,
                    tenant_id=tenant_id,
                    connector_type=connector_type,
                    saved=0,
                ),
                status_code=303,
            )

        base_url = str(host or '').strip().rstrip('/')
        api_endpoints = _coerce_stream_field_mapping_from_json(stream_api_endpoint_json)
        if not api_endpoints and base_url:
            api_endpoints = {
                'all': base_url + '/GetAllForBI',
                'health': base_url + '/HealthCheckBIBridge',
                'sales_documents': base_url + '/GetSalesDocumentsForBI',
                'purchase_documents': base_url + '/GetPurchaseDocumentsForBI',
                'inventory_documents': base_url + '/GetInventoryDocumentsForBI',
                'item_master': base_url + '/GetItemMasterForBI',
                'cash_transactions': base_url + '/GetCashTransactionsForBI',
                'supplier_balances': base_url + '/GetSupplierBalancesForBI',
                'customer_balances': base_url + '/GetCustomerBalancesForBI',
                'operating_expenses': base_url + '/GetOperatingExpensesForBI',
                'supplier_orders': base_url + '/GetSupplierOrdersForBI',
            }

        conn.is_active = bool(is_active)
        conn.source_type = 'api'
        conn.enc_payload = encrypt_json_secret({'password': resolved_password})
        conn.sales_query_template = ''
        conn.purchases_query_template = ''
        conn.inventory_query_template = ''
        conn.cashflow_query_template = ''
        conn.supplier_balances_query_template = ''
        conn.customer_balances_query_template = ''
        conn.incremental_column = (incremental_column or updated_at_column).strip() or 'UpdatedAt'
        conn.id_column = id_column
        conn.date_column = date_column
        conn.branch_column = branch_column
        conn.item_column = item_column
        conn.amount_column = (amount_column or net_amount_column).strip() or 'NetValue'
        conn.cost_column = cost_column
        conn.qty_column = qty_column
        default_supported = _stream_defaults_for_connector(connector_type)
        conn.supported_streams = default_supported
        conn.enabled_streams = _normalize_stream_selection(enabled_streams, fallback=default_supported)
        conn.stream_query_mapping = {}
        conn.stream_field_mapping = _coerce_stream_field_mapping_from_json(stream_field_mapping_json)
        conn.stream_api_endpoint = api_endpoints
        conn.connection_parameters = {
            **{k: v for k, v in existing_params.items() if k in {'sync_defaults', 'company_id', 'enable_operating_expenses', 'auto_sync_enabled'}},
            'connector_type': connector_type,
            'source_type': 'api',
            'base_url': base_url,
            'host': base_url,
            'port': 443,
            'database': database or 'api',
            'username': username,
            'auth_type': (api_auth_type or 'softone_login').strip() or 'softone_login',
            'auth_config': {
                'username': username,
                'app_id': api_app_id.strip(),
                'company': api_company.strip(),
                'branch': api_branch.strip(),
                'module': (api_module or '0').strip() or '0',
                'refid': api_refid.strip(),
                'client_id_param': 'clientID',
            },
            'verify_tls': False,
            'retry_attempts': 2,
            'timeout_seconds': 120,
            'sync_interval_minutes': max(1, sync_interval_minutes),
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
                    'is_active': conn.is_active,
                    'source_type': conn.source_type,
                    'enabled_streams': conn.enabled_streams,
                    'stream_api_endpoint': conn.stream_api_endpoint,
                },
            )
        )
        if conn.is_active:
            await db.execute(
                update(TenantConnection)
                .where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.id != conn.id,
                    TenantConnection.connector_type.in_(['sql_connector', 'pharmacyone_sql']),
                )
                .values(is_active=False)
            )
            tenant.source = 'external'
            db.add(tenant)
        await db.commit()
        _save_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if _save_tenant:
            close_ingest_circuit(str(_save_tenant.slug))
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                saved=1,
            ),
            status_code=303,
        )

    resolved_password = _resolve_secret_password(password, conn)
    if not resolved_password:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                saved=0,
            ),
            status_code=303,
        )

    conn.is_active = bool(is_active)
    conn.enc_payload = encrypt_sqlserver_secret(
        host=host,
        port=port,
        database=database,
        username=username,
        password=resolved_password,
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
    _api_ep = _coerce_stream_field_mapping_from_json(stream_api_endpoint_json)
    if _api_ep:
        conn.stream_api_endpoint = _api_ep
    existing_params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    preserved_keys = {'sync_defaults', 'company_id', 'auth_config', 'enable_operating_expenses', 'auto_sync_enabled'}
    conn.connection_parameters = {
        **{k: v for k, v in existing_params.items() if k in preserved_keys},
        'connector_type': connector_type,
        'source_type': conn.source_type,
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'options': options_map,
        'sync_interval_minutes': max(1, sync_interval_minutes),
    }
    conn.last_test_error = None
    if conn.is_active:
        await db.execute(
            update(TenantConnection)
            .where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.id != conn.id,
                TenantConnection.connector_type == 'external_api',
            )
            .values(is_active=False)
        )
        tenant.source = 'sql'
        db.add(tenant)

    db.add(
        AuditLog(
            tenant_id=tenant_id,
            action='connection_saved_ui',
            entity_type='tenant_connection',
            entity_id=str(conn.id or ''),
            payload={
                'connector_type': connector_type,
                'is_active': conn.is_active,
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
    # Saving a connection means the admin has fixed or updated credentials —
    # clear any open circuit breaker so the next sync attempt is allowed through.
    _save_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if _save_tenant:
        close_ingest_circuit(str(_save_tenant.slug))
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            saved=1,
        ),
        status_code=303,
    )


@router.post('/admin/connections/apply-pack')
async def admin_connections_apply_pack(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                pack=0,
            ),
            status_code=303,
        )
    conn = (
        await db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type.in_(('sql_connector', 'pharmacyone_sql')),
            )
        )
    ).scalar_one_or_none()
    if conn is None:
        # Querypacks are SQL-connector query templates. If this tenant's data source is the
        # external API, applying a querypack is meaningless — and fabricating a sql_connector here
        # is exactly what produced duplicate connectors (an API tenant ending up with a stray
        # empty sql_connector). Refuse instead of creating one.
        api_conn = (
            await db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type == 'external_api',
                )
            )
        ).scalar_one_or_none()
        if api_conn is not None:
            return RedirectResponse(
                url=_connections_redirect_url(
                    redirect_base,
                    tenant_id=tenant_id,
                    connector_type='external_api',
                    pack=0,
                    reason='querypack_sql_only',
                ),
                status_code=303,
            )
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
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            pack=1,
        ),
        status_code=303,
    )


@router.post('/admin/connections/backfill')
async def admin_connections_backfill(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    from_date: str = Form(...),
    to_date: str = Form(...),
    chunk_records: int = Form(default=1000),
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
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                backfill=0,
            ),
            status_code=303,
        )
    Redis.from_url(settings.redis_url, decode_responses=True).delete(
        tenant_stop_key(tenant.slug),
        tenant_delete_active_key(tenant.slug),
    )

    planned_jobs = await plan_tenant_sync_jobs(
        db,
        tenant_id=tenant.id,
        tenant_slug=tenant.slug,
        preferred_connector=connector_type,
    )
    if not planned_jobs:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                backfill=0,
            ),
            status_code=303,
        )
    all_external = bool(planned_jobs) and all(
        str(job.get('connector') or '').strip().lower() == 'external_api'
        for job in planned_jobs
    )
    selected_chunk_records = _sanitize_chunk_records(chunk_records)
    selected_chunk_days = max(1, int(chunk_days))
    task_name = 'worker.tasks.enqueue_sql_backfill'
    queued_jobs = None
    queued_batches = None

    if all_external:
        queue_before = queue_depth(tenant.slug)
        queued, batches = _enqueue_external_backfill_jobs(
            tenant_slug=tenant.slug,
            planned_jobs=planned_jobs,
            from_dt=from_dt,
            to_dt=to_dt,
            chunk_records=selected_chunk_records,
            chunk_days=selected_chunk_days,
            include_purchases=bool(include_purchases),
        )
        if queued == 0:
            return RedirectResponse(
                url=_connections_redirect_url(
                    redirect_base,
                    tenant_id=tenant_id,
                    connector_type=connector_type,
                    backfill=0,
                ),
                status_code=303,
            )
        task_name = 'worker.tasks.drain_tenant_ingest_queue'
        task = celery_client.send_task(
            task_name,
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
        queued_jobs = queued
        queued_batches = batches
        begin_ingest_progress(
            tenant_slug=tenant.slug,
            operation='backfill',
            status='running',
            total_jobs=int(queued),
            start_queue_depth=queue_before + int(queued),
            target_queue_depth=queue_before,
            from_date=from_dt.isoformat(),
            to_date=to_dt.isoformat(),
            chunk_records=selected_chunk_records,
            chunk_days=selected_chunk_days,
            connector=connector_type,
        )
    else:
        begin_ingest_progress(
            tenant_slug=tenant.slug,
            operation='backfill',
            status='queued',
            total_jobs=0,
            start_queue_depth=queue_depth(tenant.slug),
            target_queue_depth=0,
            from_date=from_dt.isoformat(),
            to_date=to_dt.isoformat(),
            chunk_days=selected_chunk_days,
            connector=connector_type,
        )
        task = celery_client.send_task(
            task_name,
            kwargs={
                'tenant_slug': tenant.slug,
                'from_date_str': from_dt.isoformat(),
                'to_date_str': to_dt.isoformat(),
                'chunk_days': selected_chunk_days,
                'limit': selected_chunk_records,
                'include_purchases': bool(include_purchases),
                'include_supplier_orders': True,
                'operation': 'backfill',
            },
            queue='default',
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
                'chunk_records': selected_chunk_records,
                'chunk_days': selected_chunk_days,
                'include_purchases': bool(include_purchases),
                'connector_mode': 'external_api' if all_external else 'sql_connector',
                'queued_jobs': queued_jobs,
                'queued_batches': queued_batches,
                'task_name': task_name,
                'task_id': task.id,
            },
        )
    )
    await db.commit()
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            backfill=1,
        ),
        status_code=303,
    )


async def _enqueue_delete_only_task(
    *,
    tenant: Tenant | None,
    tenant_id: int,
    connector_type: str,
    source_page: str,
    confirm_text: str,
    delete_from_date: str | None,
    delete_to_date: str | None,
    delete_all: bool,
    include_notifications: bool,
    db: AsyncSession,
) -> RedirectResponse:
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    if tenant is None:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                delete_data=0,
            ),
            status_code=303,
        )
    if str(confirm_text or '').strip().upper() != 'RESET':
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                delete_data=0,
            ),
            status_code=303,
        )

    from_dt = _parse_date_or_none(delete_from_date)
    to_dt = _parse_date_or_none(delete_to_date)
    scoped_delete = (not bool(delete_all)) and bool((delete_from_date or '').strip() and (delete_to_date or '').strip())
    if scoped_delete and (from_dt is None or to_dt is None or from_dt > to_dt):
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                delete_data=0,
            ),
            status_code=303,
        )
    if not scoped_delete:
        from_dt = None
        to_dt = None

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    existing_delete_active = bool(redis.get(tenant_delete_active_key(tenant.slug)))
    existing_progress = get_ingest_progress(tenant.slug)
    existing_delete_running = (
        str(existing_progress.get('operation') or '') == 'delete'
        and str(existing_progress.get('status') or '') in {'queued', 'running'}
    )
    if existing_delete_active or existing_delete_running:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                delete_data=1,
            ),
            status_code=303,
        )
    stop_ttl_seconds = max(300, int(getattr(settings, 'ingest_stop_ttl_seconds', 3600) or 3600))
    delete_ttl_seconds = max(stop_ttl_seconds, 24 * 60 * 60)
    redis.set(tenant_stop_key(tenant.slug), datetime.utcnow().isoformat(), ex=stop_ttl_seconds)
    redis.set(tenant_delete_active_key(tenant.slug), datetime.utcnow().isoformat(), ex=delete_ttl_seconds)
    preempted_insight_tasks = _preempt_tenant_insight_tasks(tenant.slug)
    queue_before = int(redis.llen(tenant_queue_name(tenant.slug)))
    lock_active = bool(redis.get(tenant_lock_name(tenant.slug)))
    cleared_runtime_keys = int(
        redis.delete(
            tenant_queue_name(tenant.slug),
            f'dlq:{tenant.slug}',
            tenant_throttle_key(tenant.slug),
        )
    )

    task_name = 'worker.tasks.delete_tenant_data_only'
    try:
        task = celery_client.send_task(
            task_name,
            kwargs={
                'tenant_slug': tenant.slug,
                'from_date_str': from_dt.isoformat() if from_dt else None,
                'to_date_str': to_dt.isoformat() if to_dt else None,
                'include_notifications': bool(include_notifications),
            },
            queue='delete',
        )
        begin_ingest_progress(
            tenant_slug=tenant.slug,
            operation='delete',
            status='queued',
            total_jobs=1,
            start_queue_depth=1,
            target_queue_depth=0,
            from_date=from_dt.isoformat() if from_dt else None,
            to_date=to_dt.isoformat() if to_dt else None,
        )
    except Exception:
        redis.delete(tenant_delete_active_key(tenant.slug))
        clear_ingest_progress(tenant.slug)
        raise

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_delete_data_queued_ui',
            entity_type='tenant_connection',
            entity_id=str(tenant.id),
            payload={
                'task_name': task_name,
                'task_id': task.id,
                'queue_before_delete': queue_before,
                'lock_active_on_submit': lock_active,
                'stop_requested_before_delete': True,
                'stop_ttl_seconds': stop_ttl_seconds,
                'preempted_insight_tasks': preempted_insight_tasks,
                'cleared_runtime_keys': cleared_runtime_keys,
                'delete_mode': 'date_range' if from_dt and to_dt else 'full',
                'from_date': from_dt.isoformat() if from_dt else None,
                'to_date': to_dt.isoformat() if to_dt else None,
                'include_notifications': bool(include_notifications),
            },
        )
    )
    await db.commit()
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            delete_data=1,
        ),
        status_code=303,
    )


@router.post('/admin/connections/delete-data')
async def admin_connections_delete_data(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    confirm_text: str = Form(default=''),
    delete_from_date: str | None = Form(default=None),
    delete_to_date: str | None = Form(default=None),
    delete_all: bool = Form(default=False),
    include_notifications: bool = Form(default=False),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    return await _enqueue_delete_only_task(
        tenant=tenant,
        tenant_id=tenant_id,
        connector_type=connector_type,
        source_page=source_page,
        confirm_text=confirm_text,
        delete_from_date=delete_from_date,
        delete_to_date=delete_to_date,
        delete_all=delete_all,
        include_notifications=include_notifications,
        db=db,
    )


@router.post('/admin/connections/reset-sync')
async def admin_connections_reset_sync_alias(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    confirm_text: str = Form(default=''),
    delete_from_date: str | None = Form(default=None),
    delete_to_date: str | None = Form(default=None),
    delete_all: bool = Form(default=False),
    include_notifications: bool = Form(default=False),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    # Backward-compatible alias: old route now performs delete-only.
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    return await _enqueue_delete_only_task(
        tenant=tenant,
        tenant_id=tenant_id,
        connector_type=connector_type,
        source_page=source_page,
        confirm_text=confirm_text,
        delete_from_date=delete_from_date,
        delete_to_date=delete_to_date,
        delete_all=delete_all,
        include_notifications=include_notifications,
        db=db,
    )


@router.post('/admin/connections/recover-sync')
async def admin_connections_recover_sync(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    from_date: str = Form(default=''),
    to_date: str = Form(default=''),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                recover=0,
            ),
            status_code=303,
        )
    from_dt = _parse_date_or_none(from_date)
    to_dt = _parse_date_or_none(to_date)

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    queue_left = int(queue_depth(tenant.slug))
    cleared_lock = int(redis.delete(tenant_lock_name(tenant.slug)))
    cleared_throttle = int(redis.delete(tenant_throttle_key(tenant.slug)))
    cleared_stop = int(redis.delete(tenant_stop_key(tenant.slug), tenant_delete_active_key(tenant.slug)))

    task_id = None
    action = 'noop'
    if queue_left > 0:
        begin_ingest_progress(
            tenant_slug=tenant.slug,
            operation='recovery_sync',
            status='queued',
            total_jobs=queue_left,
            start_queue_depth=queue_left,
            target_queue_depth=0,
            from_date=from_dt.isoformat() if from_dt else None,
            to_date=to_dt.isoformat() if to_dt else None,
        )
        task = celery_client.send_task(
            'worker.tasks.drain_tenant_ingest_queue',
            kwargs={'tenant_slug': tenant.slug},
            queue='ingest',
        )
        task_id = task.id
        action = 'resume_drain'
    else:
        update_ingest_progress(tenant.slug, status='completed')

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_sync_recovery_triggered_ui',
            entity_type='tenant_connection',
            entity_id=str(tenant.id),
            payload={
                'queue_left': queue_left,
                'requested_from_date': from_dt.isoformat() if from_dt else None,
                'requested_to_date': to_dt.isoformat() if to_dt else None,
                'cleared_lock': cleared_lock,
                'cleared_throttle': cleared_throttle,
                'cleared_stop': cleared_stop,
                'recovery_action': action,
                'task_id': task_id,
            },
        )
    )
    await db.commit()
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            recover=1,
        ),
        status_code=303,
    )


@router.post('/admin/connections/stop-sync')
async def admin_connections_stop_sync(
    tenant_id: int = Form(...),
    connector_type: str = Form(default='sql_connector'),
    source_page: str = Form(default='connections'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = '/admin/data-sources' if source_page == 'data_sources' else '/admin/connections'
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(
            url=_connections_redirect_url(
                redirect_base,
                tenant_id=tenant_id,
                connector_type=connector_type,
                stop_sync=0,
            ),
            status_code=303,
        )

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    stop_ttl_seconds = max(300, int(getattr(settings, 'ingest_stop_ttl_seconds', 3600) or 3600))
    queue_before = int(redis.llen(tenant_queue_name(tenant.slug)))
    redis.set(tenant_stop_key(tenant.slug), datetime.utcnow().isoformat(), ex=stop_ttl_seconds)
    preempted_tasks = _preempt_tenant_tasks(
        tenant.slug,
        task_names={
            'worker.tasks.generate_insights_for_tenant',
            'worker.tasks.drain_tenant_ingest_queue',
            'worker.tasks.enqueue_incremental_sync',
            'worker.tasks.enqueue_sql_backfill',
            'worker.tasks.enqueue_pharmacyone_backfill',
            'worker.tasks.refresh_aggregates_for_entity',
        },
    )
    cleared_queue = int(redis.delete(tenant_queue_name(tenant.slug)))
    cleared_throttle = int(redis.delete(tenant_throttle_key(tenant.slug)))
    cleared_lock = int(redis.delete(tenant_lock_name(tenant.slug)))
    queue_after = int(redis.llen(tenant_queue_name(tenant.slug)))
    has_active_preemptions = any(int(preempted_tasks.get(k) or 0) > 0 for k in ("active", "reserved", "scheduled"))
    if queue_after == 0 and not has_active_preemptions:
        clear_ingest_progress(tenant.slug)
    else:
        update_ingest_progress(tenant.slug, status='stopped', error='stopped_by_user')

    db.add(
        AuditLog(
            tenant_id=tenant.id,
            action='tenant_sync_stop_requested_ui',
            entity_type='tenant_connection',
            entity_id=str(tenant.id),
            payload={
                'queue_before': queue_before,
                'queue_after': queue_after,
                'cleared_queue': cleared_queue,
                'cleared_throttle': cleared_throttle,
                'cleared_lock': cleared_lock,
                'stop_ttl_seconds': stop_ttl_seconds,
                'preempted_tasks': preempted_tasks,
            },
        )
    )
    await db.commit()
    return RedirectResponse(
        url=_connections_redirect_url(
            redirect_base,
            tenant_id=tenant_id,
            connector_type=connector_type,
            stop_sync=1,
        ),
        status_code=303,
    )


@router.get('/admin/connections/progress')
async def admin_connections_progress(
    tenant_id: int = Query(...),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return JSONResponse({'status': 'not_found', 'tenant_id': tenant_id}, status_code=404)
    payload = get_ingest_progress(tenant.slug)
    now = datetime.utcnow()
    updated_raw = payload.get('updated_at')
    heartbeat_age_seconds = None
    if isinstance(updated_raw, str) and updated_raw.strip():
        try:
            updated_dt = datetime.fromisoformat(updated_raw.strip())
            heartbeat_age_seconds = max(0, int((now - updated_dt).total_seconds()))
        except Exception:
            heartbeat_age_seconds = None
    status = str(payload.get('status') or '')
    queue_left = int(payload.get('current_queue_depth') or 0)
    lock_active = bool(payload.get('lock_active'))
    stuck_threshold_seconds = max(60, int(getattr(settings, 'ingest_stuck_heartbeat_seconds', 180) or 180))
    is_stuck = bool(
        status == 'running'
        and queue_left > 0
        and heartbeat_age_seconds is not None
        and heartbeat_age_seconds >= stuck_threshold_seconds
    )
    payload['heartbeat_age_seconds'] = heartbeat_age_seconds
    payload['is_stuck'] = is_stuck
    payload['auto_recovery_enabled'] = bool(getattr(settings, 'ingest_auto_recover_enabled', True))
    payload['stuck_threshold_seconds'] = stuck_threshold_seconds
    payload['stuck_hint'] = (
        (
            f'Η διαδικασία δεν έχει heartbeat πάνω από {stuck_threshold_seconds}s. '
            'Θα επιχειρηθεί αυτόματο recovery.'
        )
        if is_stuck and bool(getattr(settings, 'ingest_auto_recover_enabled', True))
        else (
            f'Η διαδικασία δεν έχει heartbeat πάνω από {stuck_threshold_seconds}s. '
            'Χρησιμοποίησε Recovery.'
            if is_stuck
            else None
        )
    )
    payload['lock_active'] = lock_active
    payload['tenant_id'] = tenant.id
    return JSONResponse(payload)


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
            .where(TenantConnection.is_active.is_(True))
            .order_by(TenantConnection.last_sync_at.desc().nullslast())
        )
    ).all()
    progress_by_tenant: dict[int, dict] = {}
    for _connection, tenant in rows:
        tenant_id_key = int(tenant.id)
        if tenant_id_key not in progress_by_tenant:
            progress_by_tenant[tenant_id_key] = get_ingest_progress(tenant.slug)
    priority_rows = priority_pool_snapshot(limit=100)
    return templates.TemplateResponse(
        'admin/sync_status.html',
        {
            'request': request,
            'rows': rows,
            'progress_by_tenant': progress_by_tenant,
            'priority_rows': priority_rows,
            'active_page': 'sync',
            'title': 'title_sync_status',
        },
    )


@router.post('/admin/sync-status/{tenant_id}/trigger')
async def admin_sync_trigger(
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url='/admin/sync-status', status_code=303)
    conn = (
        await db.execute(
            select(TenantConnection)
            .where(TenantConnection.tenant_id == tenant.id, TenantConnection.is_active.is_(True))
            .order_by(TenantConnection.id.asc())
            .limit(1)
        )
    ).scalar_one_or_none()
    connector_type = getattr(conn, 'connector_type', None) or 'sql_connector'
    return RedirectResponse(
        url=f'/admin/connections?tenant_id={tenant.id}&connector_type={connector_type}',
        status_code=303,
    )


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
            queue='default',
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
            queue='default',
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
        page_description='Διαχείριση τύπων παραστατικών και συμπεριφορών SoftOne, με επίδραση σε έσοδα, κόστος, ποσότητες και υπόλοιπα.',
    )


@router.get('/admin/business-rules/document-type-rules/templates', response_class=HTMLResponse)
async def admin_business_rules_document_types_templates(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenants = (await db.execute(select(Tenant).order_by(Tenant.name.asc()))).scalars().all()
    rulesets = (
        await db.execute(
            select(GlobalRuleSet).order_by(GlobalRuleSet.priority.desc(), GlobalRuleSet.code.asc())
        )
    ).scalars().all()
    return templates.TemplateResponse(
        'admin/business_rules_document_type_templates.html',
        {
            'request': request,
            'active_page': 'business_rules_document_types',
            'title': 'title_business_rules_document_types',
            'tenants': tenants,
            'softone_templates': _softone_document_templates_preview(),
            'softone_doc_type_options': _softone_document_type_options(),
            'softone_doc_options': _softone_document_options(),
            'initial_ruleset_code': (rulesets[0].code if rulesets else 'softone_default_v1'),
            'template_saved': request.query_params.get('template_saved') == '1',
            'wizard_applied': request.query_params.get('wizard_applied') == '1',
            'error_message': request.query_params.get('error') or '',
        },
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
    behavior_code: str = Form(default=''),
    behavior_label: str = Form(default=''),
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

    behavior_code_norm = _normalize_behavior_code(behavior_code)
    if stream in {OperationalStream.sales_documents, OperationalStream.purchase_documents, OperationalStream.inventory_documents} and not behavior_code_norm:
        return RedirectResponse(url=f'{redirect_base}?saved=0&error=Ο+κωδικός+συμπεριφοράς+είναι+υποχρεωτικός+για+το+επιλεγμένο+κύκλωμα', status_code=303)
    behavior_label_norm = str(behavior_label or '').strip()
    doc_type, behavior_label_norm = _softone_canonical_names(
        stream_value=stream.value,
        behavior_code=behavior_code_norm,
        document_type=doc_type,
        behavior_label=behavior_label_norm,
    )
    resolved_rule_key = str(rule_key or '').strip() or _document_rule_key(doc_type, stream.value, behavior_code_norm)
    payload = _build_document_rule_payload(
        behavior_code=behavior_code_norm,
        behavior_label=behavior_label_norm,
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
        await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=stream)
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


@router.post('/admin/business-rules/document-type-rules/delete-global')
async def admin_business_rules_document_types_delete_global(
    ruleset_code: str = Form(default='softone_default_v1'),
    stream_value: str = Form(default=OperationalStream.sales_documents.value),
    rule_key: str = Form(default=''),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    cleaned_rule_key = str(rule_key or '').strip()
    if not cleaned_rule_key:
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Λείπει+rule_key+για+διαγραφή', status_code=303)
    stream = _safe_operational_stream(stream_value)
    ruleset = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == str(ruleset_code or '').strip()))).scalar_one_or_none()
    if ruleset is None:
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Το+ruleset+δεν+βρέθηκε', status_code=303)
    entry = (
        await db.execute(
            select(GlobalRuleEntry).where(
                GlobalRuleEntry.ruleset_id == ruleset.id,
                GlobalRuleEntry.domain == RuleDomain.document_type_rules,
                GlobalRuleEntry.stream == stream,
                GlobalRuleEntry.rule_key == cleaned_rule_key,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Ο+κανόνας+δεν+βρέθηκε', status_code=303)
    await db.delete(entry)
    db.add(
        AuditLog(
            tenant_id=None,
            action='document_rule_delete_global_form',
            entity_type='global_rule_entry',
            entity_id=cleaned_rule_key,
            payload={'stream': stream.value, 'ruleset_code': ruleset.code},
        )
    )
    await db.commit()
    return RedirectResponse(url=f'{redirect_base}?deleted=1', status_code=303)


@router.post('/admin/business-rules/document-type-rules/delete-tenant-override')
async def admin_business_rules_document_types_delete_tenant_override(
    tenant_id: str = Form(default=''),
    stream_value: str = Form(default=OperationalStream.sales_documents.value),
    rule_key: str = Form(default=''),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    tenant_id_text = str(tenant_id or '').strip()
    cleaned_rule_key = str(rule_key or '').strip()
    if not tenant_id_text.isdigit():
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Μη+έγκυρο+tenant+για+διαγραφή', status_code=303)
    if not cleaned_rule_key:
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Λείπει+rule_key+για+διαγραφή', status_code=303)
    tenant_id_int = int(tenant_id_text)
    stream = _safe_operational_stream(stream_value)
    entry = (
        await db.execute(
            select(TenantRuleOverride).where(
                TenantRuleOverride.tenant_id == tenant_id_int,
                TenantRuleOverride.domain == RuleDomain.document_type_rules,
                TenantRuleOverride.stream == stream,
                TenantRuleOverride.rule_key == cleaned_rule_key,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        return RedirectResponse(url=f'{redirect_base}?deleted=0&error=Το+tenant+override+δεν+βρέθηκε', status_code=303)
    await db.delete(entry)
    db.add(
        AuditLog(
            tenant_id=tenant_id_int,
            action='document_rule_delete_tenant_override_form',
            entity_type='tenant_rule_override',
            entity_id=cleaned_rule_key,
            payload={'stream': stream.value, 'tenant_id': tenant_id_int},
        )
    )
    await db.commit()
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=stream)
    return RedirectResponse(url=f'{redirect_base}?deleted=1&tenant_id={tenant_id_int}', status_code=303)


@router.post('/admin/business-rules/document-type-rules/set-tenant-ruleset')
async def admin_business_rules_document_types_set_tenant_ruleset(
    tenant_id: str = Form(default=''),
    ruleset_code: str = Form(default=''),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    tenant_id_text = str(tenant_id or '').strip()
    if not tenant_id_text.isdigit():
        return RedirectResponse(url=f'{redirect_base}?ruleset_saved=0&error=Μη+έγκυρο+tenant', status_code=303)
    tenant_id_int = int(tenant_id_text)
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id_int))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url=f'{redirect_base}?ruleset_saved=0&error=Tenant+δεν+βρέθηκε', status_code=303)

    ruleset_code_clean = str(ruleset_code or '').strip()
    if ruleset_code_clean:
        ruleset = (await db.execute(select(GlobalRuleSet).where(GlobalRuleSet.code == ruleset_code_clean))).scalar_one_or_none()
        if ruleset is None:
            return RedirectResponse(url=f'{redirect_base}?tenant_id={tenant_id_int}&ruleset_saved=0&error=Μη+έγκυρο+ruleset', status_code=303)

    flags = dict(tenant.feature_flags or {})
    if ruleset_code_clean:
        flags['document_type_ruleset_code'] = ruleset_code_clean
    else:
        flags.pop('document_type_ruleset_code', None)
    tenant.feature_flags = flags
    db.add(
        AuditLog(
            tenant_id=tenant_id_int,
            action='document_rules_tenant_ruleset_set',
            entity_type='tenant',
            entity_id=str(tenant_id_int),
            payload={'ruleset_code': ruleset_code_clean},
        )
    )
    await db.commit()
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.sales_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.purchase_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.inventory_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.cash_transactions)
    return RedirectResponse(url=f'{redirect_base}?tenant_id={tenant_id_int}&ruleset_saved=1', status_code=303)


@router.post('/admin/business-rules/document-type-rules/clear-tenant-overrides')
async def admin_business_rules_document_types_clear_tenant_overrides(
    tenant_id: str = Form(default=''),
    redirect_to: str = Form(default='/admin/business-rules/document-type-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules/document-type-rules'
    tenant_id_text = str(tenant_id or '').strip()
    if not tenant_id_text.isdigit():
        return RedirectResponse(url=f'{redirect_base}?tenant_overrides_cleared=0&error=Μη+έγκυρο+tenant', status_code=303)
    tenant_id_int = int(tenant_id_text)
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id_int))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url=f'{redirect_base}?tenant_overrides_cleared=0&error=Tenant+δεν+βρέθηκε', status_code=303)
    previous_ruleset_code = _tenant_document_ruleset_code(tenant)

    rows = (
        await db.execute(
            select(TenantRuleOverride).where(
                TenantRuleOverride.tenant_id == tenant_id_int,
                TenantRuleOverride.domain == RuleDomain.document_type_rules,
            )
        )
    ).scalars().all()
    deleted_count = 0
    for row in rows:
        await db.delete(row)
        deleted_count += 1

    flags = dict(tenant.feature_flags or {})
    if 'document_type_ruleset_code' in flags:
        flags.pop('document_type_ruleset_code', None)
        tenant.feature_flags = flags

    db.add(
        AuditLog(
            tenant_id=tenant_id_int,
            action='document_rules_tenant_overrides_clear',
            entity_type='tenant',
            entity_id=str(tenant_id_int),
            payload={'deleted_count': deleted_count, 'cleared_ruleset_code': previous_ruleset_code},
        )
    )
    await db.commit()
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.sales_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.purchase_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.inventory_documents)
    await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.cash_transactions)
    return RedirectResponse(url=f'{redirect_base}?tenant_id={tenant_id_int}&tenant_overrides_cleared=1', status_code=303)


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
            behavior_code = _normalize_behavior_code(item.get('behavior_code'))
            key = _document_rule_key(doc_type, stream.value, behavior_code)
            payload = _build_document_rule_payload(
                behavior_code=behavior_code,
                behavior_label=str(item.get('behavior_label') or '').strip(),
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
        await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.sales_documents)
        await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.purchase_documents)
        await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.inventory_documents)
        await _schedule_document_rule_refresh(db=db, tenant_ids=[tenant_id_int], stream=OperationalStream.cash_transactions)
        return RedirectResponse(url=f'{redirect_base}?template_saved=1&wizard_applied=1&tenant_id={tenant_id_int}', status_code=303)

    for item in _SOFTONE_DOCUMENT_RULE_TEMPLATES:
        stream = _safe_operational_stream(str(item.get('stream') or OperationalStream.sales_documents.value))
        doc_type = str(item.get('document_type') or '').strip()
        behavior_code = _normalize_behavior_code(item.get('behavior_code'))
        key = _document_rule_key(doc_type, stream.value, behavior_code)
        payload = _build_document_rule_payload(
            behavior_code=behavior_code,
            behavior_label=str(item.get('behavior_label') or '').strip(),
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
        page_description='Ορισμός κανόνων ανάθεσης παραστατικών στα επιχειρησιακά κυκλώματα (sales/purchases/inventory/cash/operating_expenses/supplier_balances/customer_balances).',
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
        page_description='Global defaults και tenant overrides για source query mappings ανά επιχειρησιακό stream (sales/purchases/inventory/cash/operating_expenses/supplier_balances/customer_balances).',
    )


@router.get('/admin/overview/tenant-health', response_class=HTMLResponse)
async def admin_tenant_health(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    now = datetime.utcnow()
    tenants = (
        await db.execute(
            select(Tenant)
            .where(Tenant.status != TenantStatus.terminated)
            .order_by(Tenant.name.asc())
        )
    ).scalars().all()
    tenant_ids = [int(tenant.id) for tenant in tenants]
    connections_by_tenant: dict[int, list[TenantConnection]] = {tenant_id: [] for tenant_id in tenant_ids}
    subscriptions_by_tenant: dict[int, Subscription] = {}
    users_by_tenant: dict[int, int] = {}

    if tenant_ids:
        connections = (
            await db.execute(
                select(TenantConnection)
                .where(TenantConnection.tenant_id.in_(tenant_ids))
                .order_by(
                    TenantConnection.tenant_id.asc(),
                    TenantConnection.is_active.desc(),
                    TenantConnection.last_sync_at.desc().nullslast(),
                    TenantConnection.updated_at.desc(),
                )
            )
        ).scalars().all()
        for connection in connections:
            connections_by_tenant.setdefault(int(connection.tenant_id), []).append(connection)

        subscriptions = (
            await db.execute(
                select(Subscription)
                .where(Subscription.tenant_id.in_(tenant_ids))
                .order_by(Subscription.updated_at.desc())
            )
        ).scalars().all()
        for subscription in subscriptions:
            subscriptions_by_tenant.setdefault(int(subscription.tenant_id), subscription)

        user_counts = (
            await db.execute(
                select(User.tenant_id, func.count(User.id))
                .where(User.tenant_id.in_(tenant_ids), User.is_active.is_(True))
                .group_by(User.tenant_id)
            )
        ).all()
        users_by_tenant = {int(tenant_id): int(count or 0) for tenant_id, count in user_counts if tenant_id is not None}

    priority_rows = priority_pool_snapshot(limit=200)
    priority_by_slug: dict[str, dict[str, Any]] = {}
    for row in priority_rows:
        slug = str(getattr(row, 'tenant_slug', '') or (row.get('tenant_slug') if isinstance(row, dict) else '') or '')
        if not slug:
            continue
        priority_by_slug[slug] = {
            'queue_depth': int(getattr(row, 'queue_depth', 0) or (row.get('queue_depth') if isinstance(row, dict) else 0) or 0),
            'locked': bool(getattr(row, 'locked', False) or (row.get('locked') if isinstance(row, dict) else False)),
            'priority': getattr(row, 'priority', None) if not isinstance(row, dict) else row.get('priority'),
        }

    health_rows: list[dict[str, Any]] = []
    total_active_connections = 0
    stale_sync_count = 0
    failed_sync_count = 0
    no_connection_count = 0
    subscription_risks: list[dict[str, Any]] = []
    sync_risks: list[dict[str, Any]] = []
    action_items: list[dict[str, Any]] = []
    severity_counts = {'healthy': 0, 'warning': 0, 'critical': 0}

    def _enum_value(value: Any) -> str:
        return getattr(value, 'value', value) or ''

    def _dt_age_hours(value: datetime | None) -> float | None:
        if not value:
            return None
        return max((now - value.replace(tzinfo=None)).total_seconds() / 3600, 0)

    def _human_age(hours: float | None) -> str:
        if hours is None:
            return 'ποτέ'
        if hours < 1:
            return 'πριν <1 ώρα'
        if hours < 24:
            return f'πριν {int(hours)} ώρες'
        return f'πριν {int(hours // 24)} ημέρες'

    for tenant in tenants:
        tenant_id = int(tenant.id)
        tenant_connections = connections_by_tenant.get(tenant_id, [])
        active_connections = [connection for connection in tenant_connections if bool(connection.is_active)]
        primary_connection = active_connections[0] if active_connections else (tenant_connections[0] if tenant_connections else None)
        subscription = subscriptions_by_tenant.get(tenant_id)
        plan = _enum_value(subscription.plan if subscription else tenant.plan)
        subscription_status = _enum_value(subscription.status if subscription else tenant.subscription_status)
        tenant_status = _enum_value(tenant.status)
        progress = get_ingest_progress(tenant.slug)
        pool = priority_by_slug.get(tenant.slug, {})
        sync_status = str((primary_connection.sync_status if primary_connection else 'no_connection') or 'never')
        last_sync_at = primary_connection.last_sync_at if primary_connection else None
        sync_age_hours = _dt_age_hours(last_sync_at)
        score = 100
        issues: list[str] = []
        severity = 'healthy'

        if tenant_status != TenantStatus.active.value:
            score -= 40
            issues.append('Ο tenant δεν είναι ενεργός.')
        if subscription_status in {SubscriptionStatus.past_due.value, SubscriptionStatus.suspended.value, SubscriptionStatus.canceled.value}:
            score -= 35
            issues.append(f'Η συνδρομή είναι {subscription_status}.')
        if subscription_status == SubscriptionStatus.trial.value:
            end_at = (subscription.trial_ends_at if subscription else tenant.trial_ends_at) or tenant.current_period_end
            if end_at and end_at.replace(tzinfo=None) < now + timedelta(days=7):
                score -= 10
                issues.append('Το trial λήγει μέσα στις επόμενες 7 ημέρες.')
        if not primary_connection:
            score -= 45
            no_connection_count += 1
            issues.append('Δεν υπάρχει connector.')
        else:
            if primary_connection.is_active:
                total_active_connections += 1
            if sync_status in {'failed', 'error'}:
                score -= 45
                failed_sync_count += 1
                issues.append('Ο τελευταίος συγχρονισμός απέτυχε.')
            if primary_connection.last_test_error:
                score -= 15
                issues.append('Υπάρχει πρόσφατο σφάλμα test connector.')
            if sync_age_hours is None:
                score -= 25
                issues.append('Δεν υπάρχει ολοκληρωμένο sync.')
            elif sync_age_hours > 48:
                score -= 35
                stale_sync_count += 1
                issues.append('Το sync είναι παλιότερο από 48 ώρες.')
            elif sync_age_hours > 24:
                score -= 15
                stale_sync_count += 1
                issues.append('Το sync είναι παλιότερο από 24 ώρες.')

        progress_status = str(progress.get('status') or 'idle')
        progress_pct = int(progress.get('progress_pct') or 0)
        queue_left = int(progress.get('current_queue_depth') or pool.get('queue_depth') or 0)
        if progress_status in {'failed', 'error'}:
            score -= 25
            issues.append('Το ingest progress δείχνει αποτυχία.')
        elif queue_left > 0 or bool(pool.get('locked')):
            score -= 5

        score = max(0, min(100, score))
        if score < 60 or any('απέτυχε' in issue or 'δεν είναι ενεργός' in issue or 'Δεν υπάρχει connector' in issue for issue in issues):
            severity = 'critical'
        elif score < 85 or issues:
            severity = 'warning'
        severity_counts[severity] += 1
        if not issues:
            issues.append('Χωρίς ανοιχτή ένδειξη κινδύνου.')

        row = {
            'tenant': tenant,
            'plan': plan,
            'tenant_status': tenant_status,
            'subscription_status': subscription_status,
            'connector': primary_connection,
            'connector_type': primary_connection.connector_type if primary_connection else '-',
            'sync_status': sync_status,
            'last_sync_at': last_sync_at,
            'sync_age_label': _human_age(sync_age_hours),
            'progress_status': progress_status,
            'progress_pct': progress_pct,
            'queue_left': queue_left,
            'pool_locked': bool(pool.get('locked')),
            'user_count': users_by_tenant.get(tenant_id, 0),
            'issues': issues,
            'score': score,
            'severity': severity,
        }
        health_rows.append(row)

        if severity != 'healthy':
            action_items.append(
                {
                    'severity': severity,
                    'tenant': tenant,
                    'title': issues[0],
                    'detail': ' · '.join(issues[:3]),
                    'href': f'/admin/connections?tenant_id={tenant_id}',
                }
            )
        if subscription_status != SubscriptionStatus.active.value:
            subscription_risks.append(row)
        if sync_status in {'failed', 'error', 'never', 'no_connection'} or sync_age_hours is None or sync_age_hours > 24:
            sync_risks.append(row)

    total_tenants = len(health_rows)
    health_score = round((severity_counts['healthy'] / total_tenants) * 100) if total_tenants else 100
    health_rows.sort(key=lambda row: ({'critical': 0, 'warning': 1, 'healthy': 2}[row['severity']], row['score'], row['tenant'].name))
    action_items.sort(key=lambda item: ({'critical': 0, 'warning': 1, 'healthy': 2}.get(item['severity'], 3), item['tenant'].name))

    return templates.TemplateResponse(
        'admin/tenant_health.html',
        {
            'request': request,
            'active_page': 'tenant_health',
            'title': 'title_tenant_health',
            'health_rows': health_rows,
            'health_score': health_score,
            'severity_counts': severity_counts,
            'total_tenants': total_tenants,
            'total_active_connections': total_active_connections,
            'stale_sync_count': stale_sync_count,
            'failed_sync_count': failed_sync_count,
            'no_connection_count': no_connection_count,
            'sync_risks': sync_risks[:8],
            'subscription_risks': subscription_risks[:8],
            'action_items': action_items[:10],
            'generated_at': now,
        },
    )


def _session_human_delta(start: datetime | None, end: datetime | None = None) -> str:
    if not start:
        return '-'
    end_dt = end or datetime.utcnow()
    start_dt = start.replace(tzinfo=None)
    seconds = max(int((end_dt.replace(tzinfo=None) - start_dt).total_seconds()), 0)
    minutes = seconds // 60
    if minutes < 1:
        return '<1 λεπτό'
    if minutes < 60:
        return f'{minutes} λεπτά'
    hours = minutes // 60
    if hours < 24:
        return f'{hours} ώρες'
    days = hours // 24
    return f'{days} ημέρες'


def _session_status_from_last_seen(last_seen_at: datetime | None, now: datetime) -> tuple[str, str]:
    if not last_seen_at:
        return 'unknown', 'Χωρίς δραστηριότητα'
    minutes = max((now - last_seen_at.replace(tzinfo=None)).total_seconds() / 60, 0)
    if minutes <= 15:
        return 'online', 'Online'
    if minutes <= 60:
        return 'idle', 'Idle'
    return 'stale', 'Ανενεργό'


@router.get('/admin/user-sessions', response_class=HTMLResponse)
async def admin_user_sessions(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    now = datetime.utcnow()
    idle_timeout_minutes = 30
    idle_cutoff = now - timedelta(minutes=idle_timeout_minutes)
    idle_result = await db.execute(
        update(RefreshToken)
        .where(
            RefreshToken.revoked_at.is_(None),
            RefreshToken.expires_at > now,
            func.coalesce(RefreshToken.last_seen_at, RefreshToken.created_at) < idle_cutoff,
        )
        .values(revoked_at=now)
    )
    await db.commit()
    idle_revoked_count = int(getattr(idle_result, 'rowcount', 0) or 0)
    active_rows = (
        await db.execute(
            select(RefreshToken, User, Tenant)
            .join(User, RefreshToken.user_id == User.id)
            .outerjoin(Tenant, User.tenant_id == Tenant.id)
            .where(
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
            )
            .order_by(RefreshToken.created_at.desc())
        )
    ).all()
    expired_unrevoked = (
        await db.execute(
            select(func.count(RefreshToken.id)).where(
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at <= now,
            )
        )
    ).scalar_one() or 0
    user_ids = sorted({int(user.id) for _, user, _ in active_rows})
    latest_log_by_user: dict[int, AuditLog] = {}
    recent_login_by_user: dict[int, AuditLog] = {}
    if user_ids:
        recent_logs = (
            await db.execute(
                select(AuditLog)
                .where(AuditLog.actor_user_id.in_(user_ids))
                .order_by(AuditLog.created_at.desc())
                .limit(1000)
            )
        ).scalars().all()
        for log in recent_logs:
            actor_id = int(log.actor_user_id or 0)
            if actor_id and actor_id not in latest_log_by_user:
                latest_log_by_user[actor_id] = log
            if actor_id and actor_id not in recent_login_by_user and log.action == 'auth_login_success':
                recent_login_by_user[actor_id] = log

    session_count_by_user: dict[int, int] = {}
    for _, user, _ in active_rows:
        session_count_by_user[int(user.id)] = session_count_by_user.get(int(user.id), 0) + 1

    status_counts = {'online': 0, 'idle': 0, 'stale': 0, 'unknown': 0}
    tenant_counts: dict[str, dict[str, Any]] = {}
    sessions: list[dict[str, Any]] = []
    for token_row, user, tenant in active_rows:
        user_id = int(user.id)
        latest_log = latest_log_by_user.get(user_id)
        last_seen_at = token_row.last_seen_at or token_row.created_at
        status_key, status_label = _session_status_from_last_seen(last_seen_at, now)
        status_counts[status_key] += 1
        tenant_name = tenant.name if tenant else 'CloudOn'
        tenant_slug = tenant.slug if tenant else 'global'
        tenant_bucket = tenant_counts.setdefault(tenant_name, {'name': tenant_name, 'slug': tenant_slug, 'count': 0})
        tenant_bucket['count'] += 1
        payload = latest_log.payload if latest_log and isinstance(latest_log.payload, dict) else {}
        login_payload = recent_login_by_user.get(user_id).payload if recent_login_by_user.get(user_id) and isinstance(recent_login_by_user[user_id].payload, dict) else {}
        sessions.append(
            {
                'token': token_row,
                'user': user,
                'tenant': tenant,
                'tenant_name': tenant_name,
                'tenant_slug': tenant_slug,
                'status_key': status_key,
                'status_label': status_label,
                'connected_for': _session_human_delta(token_row.created_at, now),
                'expires_in': _session_human_delta(now, token_row.expires_at),
                'last_seen_at': last_seen_at,
                'last_seen_label': _session_human_delta(last_seen_at, now),
                'last_action': latest_log.action if latest_log else 'session_created',
                'last_path': token_row.last_seen_path or payload.get('path') or payload.get('host') or '-',
                'ip': token_row.last_seen_ip or login_payload.get('ip') or payload.get('ip') or '-',
                'user_agent': token_row.last_seen_user_agent or login_payload.get('user_agent') or payload.get('user_agent') or '-',
                'user_session_count': session_count_by_user.get(user_id, 1),
            }
        )

    sessions.sort(key=lambda row: ({'online': 0, 'idle': 1, 'stale': 2, 'unknown': 3}[row['status_key']], row['tenant_name'], row['user'].email))
    top_tenants = sorted(tenant_counts.values(), key=lambda row: (-row['count'], row['name']))[:8]
    multi_session_users = [row for row in sessions if row['user_session_count'] > 1]
    expiring_soon = [
        row for row in sessions
        if 0 <= (row['token'].expires_at.replace(tzinfo=None) - now).total_seconds() <= 24 * 3600
    ]

    return templates.TemplateResponse(
        'admin/user_sessions.html',
        {
            'request': request,
            'active_page': 'user_sessions',
            'title': 'title_user_sessions',
            'sessions': sessions,
            'status_counts': status_counts,
            'active_sessions': len(sessions),
            'unique_users': len(user_ids),
            'top_tenants': top_tenants,
            'multi_session_users': multi_session_users[:8],
            'expiring_soon': expiring_soon[:8],
            'expired_unrevoked': int(expired_unrevoked),
            'idle_revoked_count': idle_revoked_count,
            'idle_timeout_minutes': idle_timeout_minutes,
            'generated_at': now,
        },
    )


@router.post('/admin/user-sessions/{session_id}/revoke')
async def admin_user_session_revoke(
    request: Request,
    session_id: int,
    admin_user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    token_row = (
        await db.execute(select(RefreshToken).where(RefreshToken.id == session_id).limit(1))
    ).scalar_one_or_none()
    if not token_row:
        return RedirectResponse(url='/admin/user-sessions?revoked=0&reason=session_not_found', status_code=303)
    target_user = (
        await db.execute(select(User).where(User.id == token_row.user_id).limit(1))
    ).scalar_one_or_none()
    now = datetime.utcnow()
    if token_row.revoked_at is None:
        token_row.revoked_at = now
    db.add(
        AuditLog(
            tenant_id=target_user.tenant_id if target_user else None,
            actor_user_id=admin_user.id if admin_user else None,
            action='admin_session_revoke',
            entity_type='auth_session',
            entity_id=token_row.token_jti,
            payload={
                'target_user_id': token_row.user_id,
                'session_id': session_id,
                'ip': _request_client_ip(request),
                'user_agent': _request_user_agent(request),
            },
        )
    )
    await db.commit()
    return RedirectResponse(url='/admin/user-sessions?revoked=1', status_code=303)


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


# ---------------------------------------------------------------------------
# Stream Config — per-tenant series / behavior-code parameterisation
# ---------------------------------------------------------------------------

_STREAM_BEHAVIOR_LABELS: dict[int, str] = {
    101: 'Παραστατικό Πώλησης',
    102: 'Τιμολόγιο Πώλησης',
    103: 'Δελτίο Αποστολής',
    131: 'Απόδειξη Λιανικής (POS)',
    151: 'Πιστωτικό Τιμολόγιο Α',
    152: 'Πιστωτικό Τιμολόγιο Β',
    181: 'Επιστροφή / Πίστωση',
    201: 'Παραγγελία Πώλησης',
    401: 'Τιμολόγιο Αγοράς',
    402: 'Τιμολόγιο Αγοράς (2)',
    411: 'Πιστωτικό Αγοράς',
    431: 'Δελτίο Αποστολής Αγοράς',
    451: 'Παραγγελία Αγοράς',
}


def _stream_behavior_label(code: int | str | None) -> str:
    try:
        normalized = int(code or 0)
    except (TypeError, ValueError):
        normalized = 0
    if normalized <= 0:
        return 'Άγνωστος'
    return _STREAM_BEHAVIOR_LABELS.get(normalized, 'Άγνωστος')


def _infer_behavior_code_from_doc_type(doc_type: str | None) -> int:
    raw = str(doc_type or '').strip()
    if not raw:
        return 0
    match = re.match(r'^(\d+)\s+', raw)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return 0


def _humanize_stream_doc_type(doc_type: str | None, stream_key: str) -> str:
    raw = str(doc_type or '').strip()
    if not raw:
        return '—'

    if raw == 'sales_1351':
        return 'Πωλήσεις ERP (1351)'
    if raw == 'sales_11351':
        return 'Πωλήσεις POS redirect (11351)'

    purchase_match = re.match(r'^(?P<behavior>\d+)\s+purchase\s+(?P<source>\d+)$', raw, re.IGNORECASE)
    if purchase_match:
        behavior_code = int(purchase_match.group('behavior'))
        source_code = purchase_match.group('source')
        return f'Αγορές ERP ({source_code}) · {_stream_behavior_label(behavior_code)}'

    purchase_expense_match = re.match(r'^(?P<behavior>\d+)\s+purchase_expense_(?P<source>\d+)$', raw, re.IGNORECASE)
    if purchase_expense_match:
        behavior_code = int(purchase_expense_match.group('behavior'))
        source_code = purchase_expense_match.group('source')
        return f'Έξοδα Αγορών ({source_code}) · {_stream_behavior_label(behavior_code)}'

    expense_series_match = re.match(r'^softone_series_(?P<series>\d+)$', raw, re.IGNORECASE)
    if expense_series_match:
        return f'Σειρά SoftOne ({expense_series_match.group("series")})'

    if raw.startswith('cash_'):
        return f'Ταμείο {raw.replace("cash_", "").replace("_", " ").strip()}'.strip()
    if raw.startswith('expense_'):
        return f'Έξοδα {raw.replace("expense_", "").replace("_", " ").strip()}'.strip()
    if raw.startswith('operating_expenses_'):
        return f'Λειτουργικά Έξοδα {raw.replace("operating_expenses_", "").replace("_", " ").strip()}'.strip()
    if raw.startswith('purchase_'):
        return f'Αγορές {raw.replace("purchase_", "").replace("_", " ").strip()}'.strip()
    if raw.startswith('sales_'):
        return f'Πωλήσεις {raw.replace("sales_", "").replace("_", " ").strip()}'.strip()

    if stream_key == 'sales':
        return f'Πωλήσεις · {raw.replace("_", " ")}'
    if stream_key == 'purchases':
        return f'Αγορές · {raw.replace("_", " ")}'
    if stream_key == 'cashflows':
        return f'Ταμείο · {raw.replace("_", " ")}'
    if stream_key == 'expenses':
        return f'Έξοδα · {raw.replace("_", " ")}'
    return raw.replace('_', ' ')

_STREAM_DEFS = [
    {
        'key': 'sales',
        'label': 'Πωλήσεις',
        'icon': 'trending-up',
        'source_col_label': 'Πηγή Πώλησης',
        'operational_stream': 'sales_documents',
        'fact_table': 'fact_sales',
        'amount_col': 'net_value',
        'rule_key': 'turnover',
    },
    {
        'key': 'purchases',
        'label': 'Αγορές',
        'icon': 'shopping-cart',
        'source_col_label': 'Πηγή Αγοράς',
        'operational_stream': 'purchase_documents',
        'fact_table': 'fact_purchases',
        'amount_col': 'net_value',
        'rule_key': 'purchase_turnover',
    },
    {
        'key': 'cashflows',
        'label': 'Ταμείο',
        'icon': 'dollar-sign',
        'source_col_label': 'Πηγή Ταμείου',
        'operational_stream': 'cash_transactions',
        'fact_table': 'fact_cashflows',
        'amount_col': 'amount',
        'rule_key': 'cashflow_config',
    },
    {
        'key': 'expenses',
        'label': 'Έξοδα',
        'icon': 'credit-card',
        'source_col_label': 'Πηγή Εξόδου',
        'operational_stream': 'operating_expenses',
        'fact_table': 'fact_expenses',
        'amount_col': 'amount',
        'rule_key': 'expense_config',
    },
]


async def _discover_stream_series(tenant_db: AsyncSession, fact_table: str, amount_col: str, stream_key: str) -> list[dict]:
    has_series = fact_table in {'fact_sales', 'fact_purchases', 'fact_expenses'}
    has_payload = fact_table in {'fact_sales', 'fact_purchases'}
    year_start = date.today().replace(month=1, day=1)

    if has_series and has_payload:
        sql = text(f"""
            SELECT
                COALESCE(document_series, '') AS series,
                COALESCE(document_type, '') AS doc_type,
                COALESCE((source_payload_json->>'source_transaction_type_id')::int, 0) AS behavior_code,
                COUNT(*) AS rows,
                ROUND(COALESCE(SUM({amount_col}), 0)::numeric, 2) AS total_value,
                ROUND(COALESCE(SUM(CASE WHEN doc_date >= :year_start THEN {amount_col} ELSE 0 END), 0)::numeric, 2) AS ytd_value
            FROM {fact_table}
            GROUP BY 1, 2, 3
            ORDER BY ABS(SUM(COALESCE({amount_col}, 0))) DESC
        """)
    elif has_series:
        sql = text(f"""
            SELECT
                COALESCE(document_series, '') AS series,
                COALESCE(document_type, '') AS doc_type,
                0 AS behavior_code,
                COUNT(*) AS rows,
                ROUND(COALESCE(SUM({amount_col}), 0)::numeric, 2) AS total_value,
                ROUND(COALESCE(SUM(CASE WHEN doc_date >= :year_start THEN {amount_col} ELSE 0 END), 0)::numeric, 2) AS ytd_value
            FROM {fact_table}
            GROUP BY 1, 2
            ORDER BY ABS(SUM(COALESCE({amount_col}, 0))) DESC
        """)
    else:
        sql = text(f"""
            SELECT
                '' AS series,
                COALESCE(document_type, '') AS doc_type,
                0 AS behavior_code,
                COUNT(*) AS rows,
                ROUND(COALESCE(SUM({amount_col}), 0)::numeric, 2) AS total_value,
                ROUND(COALESCE(SUM(CASE WHEN transaction_date >= :year_start THEN {amount_col} ELSE 0 END), 0)::numeric, 2) AS ytd_value
            FROM {fact_table}
            GROUP BY 2
            ORDER BY ABS(SUM(COALESCE({amount_col}, 0))) DESC
        """)

    try:
        rows = (await tenant_db.execute(sql, {'year_start': year_start})).fetchall()
    except Exception:
        return []

    result: dict[str, dict] = {}
    for row in rows:
        key = (str(row.series), str(row.doc_type))
        if key not in result:
            result[key] = {
                'series': str(row.series),
                'doc_type': str(row.doc_type),
                'behavior_codes': [],
                'rows': 0,
                'total_value': 0.0,
                'ytd_value': 0.0,
            }
        result[key]['rows'] += int(row.rows)
        result[key]['total_value'] = round(float(result[key]['total_value']) + float(row.total_value), 2)
        result[key]['ytd_value'] = round(float(result[key]['ytd_value']) + float(row.ytd_value), 2)
        code = int(row.behavior_code or 0)
        if code and code not in result[key]['behavior_codes']:
            result[key]['behavior_codes'].append(code)

    for item in result.values():
        if not item['behavior_codes']:
            inferred_code = _infer_behavior_code_from_doc_type(item['doc_type'])
            if inferred_code:
                item['behavior_codes'].append(inferred_code)
        item['behavior_codes'] = sorted(item['behavior_codes'])
        item['doc_type_label'] = _humanize_stream_doc_type(item['doc_type'], stream_key)

    return sorted(result.values(), key=lambda x: abs(x['ytd_value']), reverse=True)


def _excluded_series_set(rule_override: object | None) -> set[str]:
    if rule_override is None:
        return set()
    payload = rule_override.payload_json or {}
    series_rules = payload.get('series_rules') or []
    return {str(r['series']) for r in series_rules if isinstance(r, dict) and not r.get('include_turnover', True)}


@router.get('/admin/tenants/{tenant_id}/stream-config', response_class=HTMLResponse)
async def admin_tenant_stream_config_get(
    request: Request,
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url='/admin/tenants?reason=not_found', status_code=303)

    streams_data = []
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        for sdef in _STREAM_DEFS:
            stream_value = str(sdef.get('operational_stream') or '').strip()
            rule = None
            if stream_value in _CONTROL_OPERATIONAL_STREAM_VALUES:
                rule = (await db.execute(
                    select(TenantRuleOverride).where(
                        TenantRuleOverride.tenant_id == tenant_id,
                        TenantRuleOverride.domain == RuleDomain.kpi_participation_rules,
                        TenantRuleOverride.stream == OperationalStream(stream_value),
                        TenantRuleOverride.rule_key == sdef['rule_key'],
                    )
                )).scalar_one_or_none()

            excluded = _excluded_series_set(rule)
            series_rows = await _discover_stream_series(tenant_db, sdef['fact_table'], sdef['amount_col'], sdef['key'])
            for s in series_rows:
                s['included'] = s['series'] not in excluded

            payload = (rule.payload_json or {}) if rule else {}
            streams_data.append({
                **sdef,
                'series': series_rows,
                'rule_exists': rule is not None,
                'excluded_count': len(excluded),
                'payload': payload,
                'total_rows': sum(s['rows'] for s in series_rows),
                'total_ytd': round(sum(s['ytd_value'] for s in series_rows), 2),
            })
        break

    return templates.TemplateResponse(
        'admin/stream_config.html',
        {
            'request': request,
            'tenant': tenant,
            'streams': streams_data,
            'behavior_labels': _STREAM_BEHAVIOR_LABELS,
            'active_page': 'tenants',
            'title': f'Stream Config — {tenant.name}',
        },
    )


@router.post('/admin/tenants/{tenant_id}/stream-config', response_class=HTMLResponse)
async def admin_tenant_stream_config_post(
    request: Request,
    tenant_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if tenant is None:
        return RedirectResponse(url='/admin/tenants?reason=not_found', status_code=303)

    form = await request.form()
    stream_key = str(form.get('stream_key') or 'sales')
    sdef = next((s for s in _STREAM_DEFS if s['key'] == stream_key), _STREAM_DEFS[0])

    # Collect checked series from form: series_include_{series}_{doctype} = "on"
    all_series_keys = [k for k in form if k.startswith('series_include_')]
    included_series: set[str] = set()
    for k in all_series_keys:
        parts = k[len('series_include_'):].split('__', 1)
        if parts:
            included_series.add(parts[0])

    # All known series for this stream (we need to know which exist)
    excluded_from_form_keys = [k for k in form if k.startswith('series_known_')]
    all_known_series: set[str] = set()
    for k in excluded_from_form_keys:
        parts = k[len('series_known_'):].split('__', 1)
        if parts:
            all_known_series.add(parts[0])

    # Build series_rules: only for explicitly excluded (known but not included)
    excluded_series = all_known_series - included_series
    new_series_rules = [{'series': s, 'include_turnover': False} for s in sorted(excluded_series)]

    # Load existing rule to preserve branch_adjustments and other fields
    rule = (await db.execute(
        select(TenantRuleOverride).where(
            TenantRuleOverride.tenant_id == tenant_id,
            TenantRuleOverride.domain == RuleDomain.kpi_participation_rules,
            TenantRuleOverride.stream == sdef['operational_stream'],
            TenantRuleOverride.rule_key == sdef['rule_key'],
        )
    )).scalar_one_or_none()

    if rule is None:
        rule = TenantRuleOverride(
            tenant_id=tenant_id,
            domain=RuleDomain.kpi_participation_rules,
            stream=sdef['operational_stream'],
            rule_key=sdef['rule_key'],
            override_mode='merge',
            payload_json={},
            is_active=True,
        )
        db.add(rule)

    existing_payload = dict(rule.payload_json or {})
    existing_payload['series_rules'] = new_series_rules
    rule.payload_json = existing_payload
    rule.updated_at = datetime.utcnow()
    await db.commit()

    await invalidate_tenant_cache(str(tenant.id))

    return RedirectResponse(
        url=f'/admin/tenants/{tenant_id}/stream-config?saved=1&stream={stream_key}',
        status_code=303,
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


@router.get('/admin/operational-streams/operating-expenses', response_class=HTMLResponse)
async def admin_operational_stream_operating_expenses(
    request: Request,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
):
    return _render_admin_menu_placeholder(
        request=request,
        active_page='admin_stream_operating_expenses',
        title='title_operating_expenses_dashboard',
        page_title_key='operating_expenses_menu',
        page_description='Admin προβολή για validation λειτουργικών εξόδων (staging -> fact_expenses -> expense aggregates).',
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


@router.get('/admin/settings/mail-server', response_class=HTMLResponse)
async def admin_settings_mail_server(
    request: Request,
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    return templates.TemplateResponse(
        'admin/mail_settings.html',
        {
            'request': request,
            'user': user,
            'active_page': 'settings_mail_server',
            'title': 'title_settings_mail_server',
            'mail_settings': _current_mail_settings(),
            'saved': request.query_params.get('saved') == '1',
            'test_status': request.query_params.get('test') or '',
            'message': request.query_params.get('message') or '',
            'error_message': (
                'Η φόρμα έληξε. Ξαναπάτα Αποστολή Test.'
                if request.query_params.get('csrf_error') == '1'
                else request.query_params.get('error') or ''
            ),
        },
    )


@router.post('/admin/settings/mail-server')
async def admin_settings_mail_server_save(
    request: Request,
    _: User = Depends(require_roles(RoleName.cloudon_admin)),
    smtp_host: str = Form(default=''),
    smtp_port: int = Form(default=587),
    smtp_username: str = Form(default=''),
    smtp_password: str = Form(default=''),
    smtp_from_email: str = Form(default=''),
    smtp_from_name: str = Form(default='CloudOn BI'),
    smtp_use_tls: str | None = Form(default=None),
    app_public_base_url: str = Form(default=''),
):
    existing = _read_control_env()
    clean_host = smtp_host.strip()
    clean_username = smtp_username.strip()
    clean_password = smtp_password.strip() or existing.get('SMTP_PASSWORD', settings.smtp_password)
    clean_from_email = smtp_from_email.strip()
    clean_from_name = smtp_from_name.strip() or 'CloudOn BI'
    clean_base_url = app_public_base_url.strip().rstrip('/')
    clean_port = max(1, min(int(smtp_port or 587), 65535))

    updates = {
        'SMTP_HOST': clean_host,
        'SMTP_PORT': str(clean_port),
        'SMTP_USERNAME': clean_username,
        'SMTP_PASSWORD': clean_password,
        'SMTP_FROM_EMAIL': clean_from_email,
        'SMTP_FROM_NAME': clean_from_name,
        'SMTP_USE_TLS': 'true' if _smtp_bool(smtp_use_tls) else 'false',
        'APP_PUBLIC_BASE_URL': clean_base_url,
    }
    try:
        _write_control_env(updates)
        _apply_runtime_mail_settings(updates)
    except Exception as exc:
        logger.exception('admin_mail_settings_save_failed')
        return RedirectResponse(
            url='/admin/settings/mail-server?' + urlencode({'saved': '0', 'error': str(exc)}),
            status_code=303,
        )

    return RedirectResponse(url='/admin/settings/mail-server?saved=1', status_code=303)


@router.post('/admin/settings/mail-server/test')
async def admin_settings_mail_server_test(
    test_email: str = Form(default=''),
    user: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    target = (test_email or user.email or '').strip()
    if not target:
        return RedirectResponse(
            url='/admin/settings/mail-server?' + urlencode({'test': '0', 'error': 'Δεν υπάρχει email παραλήπτη για δοκιμή.'}),
            status_code=303,
        )

    env_values = _read_control_env()
    if env_values:
        runtime_values = {
            'SMTP_HOST': env_values.get('SMTP_HOST', settings.smtp_host),
            'SMTP_PORT': env_values.get('SMTP_PORT', str(settings.smtp_port or 587)),
            'SMTP_USERNAME': env_values.get('SMTP_USERNAME', settings.smtp_username),
            'SMTP_PASSWORD': env_values.get('SMTP_PASSWORD', settings.smtp_password),
            'SMTP_FROM_EMAIL': env_values.get('SMTP_FROM_EMAIL', settings.smtp_from_email),
            'SMTP_FROM_NAME': env_values.get('SMTP_FROM_NAME', settings.smtp_from_name),
            'SMTP_USE_TLS': env_values.get('SMTP_USE_TLS', 'true' if settings.smtp_use_tls else 'false'),
            'APP_PUBLIC_BASE_URL': env_values.get('APP_PUBLIC_BASE_URL', settings.app_public_base_url),
        }
        _apply_runtime_mail_settings(runtime_values)

    try:
        result = send_email(
            to_email=target,
            subject='BoxVisio BI mail server test',
            text_body='Το test email του κεντρικού BoxVisio BI mail server στάλθηκε επιτυχώς.',
            html_body='<p>Το test email του κεντρικού <strong>BoxVisio BI</strong> mail server στάλθηκε επιτυχώς.</p>',
        )
    except Exception as exc:
        logger.exception('admin_mail_settings_test_failed')
        return RedirectResponse(
            url='/admin/settings/mail-server?' + urlencode({'test': '0', 'error': str(exc)}),
            status_code=303,
        )

    if result.get('status') == 'sent':
        return RedirectResponse(
            url='/admin/settings/mail-server?' + urlencode({'test': '1', 'message': f'Στάλθηκε test email στο {target}.'}),
            status_code=303,
        )
    return RedirectResponse(
        url='/admin/settings/mail-server?' + urlencode({'test': '0', 'error': str(result.get('reason') or 'Το email δεν στάλθηκε.')}),
        status_code=303,
    )


@router.get('/admin/settings/mail-server/test', response_class=HTMLResponse)
async def admin_settings_mail_server_test_get(
    _: User = Depends(require_roles(RoleName.cloudon_admin)),
):
    return RedirectResponse(
        url='/admin/settings/mail-server?' + urlencode({'test': '0', 'error': 'Η φόρμα έληξε. Ξαναπάτα Αποστολή Test.'}),
        status_code=303,
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


@router.post('/admin/business-rules/global-rule/delete')
async def admin_business_rules_global_rule_delete(
    entry_id: int = Form(...),
    redirect_to: str = Form(default='/admin/business-rules'),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    redirect_base = redirect_to if str(redirect_to or '').startswith('/admin/') else '/admin/business-rules'
    entry = (await db.execute(select(GlobalRuleEntry).where(GlobalRuleEntry.id == entry_id))).scalar_one_or_none()
    if entry is None:
        return RedirectResponse(url=f'{redirect_base}?error=Ο+κανόνας+δεν+βρέθηκε', status_code=303)
    db.add(
        AuditLog(
            tenant_id=None,
            action='business_rule_global_delete_ui',
            entity_type='global_rule_entry',
            entity_id=str(entry_id),
            payload={'domain': entry.domain.value, 'stream': entry.stream.value, 'rule_key': entry.rule_key},
        )
    )
    await db.delete(entry)
    await db.commit()
    return RedirectResponse(url=f'{redirect_base}?deleted=1', status_code=303)


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
    tenant_name_map = {t.id: t.name for t in tenants}
    tenant_slug_map = {t.id: t.slug for t in tenants}
    user_counts = {
        'total': len(users),
        'active': sum(1 for u in users if u.is_active),
        'tenant_scoped': sum(1 for u in users if u.tenant_id is not None),
        'cloudon_admin': sum(1 for u in users if u.role == RoleName.cloudon_admin),
    }
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
            'tenant_name_map': tenant_name_map,
            'tenant_slug_map': tenant_slug_map,
            'user_counts': user_counts,
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
    company_id: str | None = Form(default=None),
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
    company_id_value = str(company_id or '').strip() or None
    if selected_role == RoleName.cloudon_admin:
        company_id_value = None
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
        company_id=company_id_value,
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
    company_id: str | None = Form(default=None),
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
    company_id_value = str(company_id or '').strip() or None
    if selected_role == RoleName.cloudon_admin:
        company_id_value = None
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
    user.company_id = company_id_value
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
    await db.execute(delete(RefreshToken).where(RefreshToken.user_id == user_id))
    await db.execute(update(AuditLog).where(AuditLog.actor_user_id == user_id).values(actor_user_id=None))
    await db.delete(user)
    await db.commit()
    return RedirectResponse(url='/admin/users?deleted=1', status_code=303)


@router.post('/admin/users/{user_id}/resend-invite')
async def admin_user_resend_invite(
    user_id: int,
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return RedirectResponse(url='/admin/users?invite=0&reason=not_found', status_code=303)
    # Fresh 48h set-password token (the /invite page resolves it), then email the link.
    token = secrets.token_urlsafe(24)
    user.reset_token = token
    user.reset_token_expires_at = datetime.utcnow() + timedelta(days=2)
    await db.commit()
    slug = ''
    if user.tenant_id:
        tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
        slug = tenant.slug if tenant else ''
    try:
        result = send_user_invite_email(
            full_name=user.full_name or '',
            email=user.email,
            invite_token=token,
            tenant_slug=slug,
        )
    except Exception:  # noqa: BLE001
        logger.exception('admin_user_resend_invite_failed', extra={'user_id': user_id})
        return RedirectResponse(url='/admin/users?invite=0&reason=send_failed', status_code=303)
    status = str(result.get('status') or '')
    if status == 'sent':
        return RedirectResponse(url='/admin/users?invite=1', status_code=303)
    if status == 'skipped':
        return RedirectResponse(url='/admin/users?invite=0&reason=smtp_not_configured', status_code=303)
    return RedirectResponse(url=f'/admin/users?invite=0&reason={result.get("reason") or "error"}', status_code=303)


@router.post('/admin/users/{user_id}/set-password')
async def admin_user_set_password(
    user_id: int,
    password: str = Form(default=''),
    _: object = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    # Admin sets a specific password for a user (the admin then hands it over). Passwords are
    # stored one-way hashed and can never be read back — this is the supported way to give a
    # user a known password. Setting it also activates the account and clears any invite token.
    password = str(password or '')
    if len(password) < 8:
        return RedirectResponse(url='/admin/users?pwset=0&reason=too_short', status_code=303)
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        return RedirectResponse(url='/admin/users?pwset=0&reason=not_found', status_code=303)
    user.password_hash = get_password_hash(password)
    user.is_active = True
    user.reset_token = None
    user.reset_token_expires_at = None
    await db.commit()
    return RedirectResponse(url='/admin/users?pwset=1', status_code=303)


# --- CloudOn admin impersonation: log into any tenant's dashboard without a tenant seat -------
_IMPERSONATION_TTL_SECONDS = 120


def _impersonation_redis():
    try:
        from redis.asyncio import Redis  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    try:
        return Redis.from_url(settings.redis_url, decode_responses=True)
    except Exception:  # noqa: BLE001
        return None


async def _impersonation_put(ott: str, admin_user_id: int, tenant_id: int) -> bool:
    redis = _impersonation_redis()
    if redis is None:
        return False
    try:
        await redis.set(f'impersonate:{ott}', f'{admin_user_id}:{tenant_id}', ex=_IMPERSONATION_TTL_SECONDS)
        return True
    except Exception:  # noqa: BLE001
        return False
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass


async def _impersonation_take(ott: str) -> tuple[int, int] | None:
    """Single-use consume: return (admin_user_id, tenant_id) and delete the token."""
    redis = _impersonation_redis()
    if redis is None or not ott:
        return None
    key = f'impersonate:{ott}'
    try:
        value = await redis.get(key)
        if value:
            await redis.delete(key)
    except Exception:  # noqa: BLE001
        return None
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass
    if not value or ':' not in str(value):
        return None
    admin_part, tenant_part = str(value).split(':', 1)
    try:
        return int(admin_part), int(tenant_part)
    except ValueError:
        return None


@router.get('/admin/tenants/{tenant_id}/impersonate')
async def admin_tenant_impersonate(
    tenant_id: int,
    request: Request,
    admin_user: User = Depends(require_roles(RoleName.cloudon_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    """Start impersonation from the admin panel: mint a single-use handoff token and bounce the
    browser to the tenant portal, which exchanges it for a tenant-scoped session cookie."""
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant:
        return RedirectResponse(url='/admin/tenants?impersonate=0&reason=not_found', status_code=303)
    if tenant.status != TenantStatus.active:
        return RedirectResponse(url='/admin/tenants?impersonate=0&reason=inactive', status_code=303)
    ott = secrets.token_urlsafe(32)
    if not await _impersonation_put(ott, int(admin_user.id), int(tenant_id)):
        return RedirectResponse(url='/admin/tenants?impersonate=0&reason=store_unavailable', status_code=303)
    db.add(
        AuditLog(
            tenant_id=int(tenant_id),
            actor_user_id=int(admin_user.id),
            action='impersonation_start',
            entity_type='tenant',
            entity_id=str(tenant_id),
            payload={'ip': _request_client_ip(request), 'user_agent': _request_user_agent(request)},
        )
    )
    await db.commit()
    return RedirectResponse(url=f'https://{settings.tenant_portal_host}/impersonate/enter?ott={ott}', status_code=303)


@router.get('/impersonate/enter')
async def impersonate_enter(
    request: Request,
    ott: str = Query(default=''),
    db: AsyncSession = Depends(get_control_db),
):
    """Tenant-portal side of the handoff: consume the single-use token and set a tenant-scoped
    session cookie for the CloudOn admin, then land on the tenant dashboard."""
    host = (request.headers.get('host') or '').split(':')[0].lower()
    fail = RedirectResponse(url='/login?impersonate_error=1', status_code=303)
    consumed = await _impersonation_take(ott)
    if not consumed:
        return fail
    admin_id, tenant_id = consumed
    admin = (
        await db.execute(select(User).where(User.id == admin_id, User.is_active.is_(True)))
    ).scalar_one_or_none()
    if not admin or admin.role != RoleName.cloudon_admin:
        return fail
    tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not tenant or tenant.status != TenantStatus.active:
        return fail
    audience = expected_audience_for_host(host) or audience_for_role(admin.role.value)
    token = create_access_token(
        subject=str(admin.id),
        tenant_id=int(tenant_id),
        role=admin.role.value,
        audience=audience,
    )
    db.add(
        AuditLog(
            tenant_id=int(tenant_id),
            actor_user_id=int(admin.id),
            action='impersonation_enter',
            entity_type='tenant',
            entity_id=str(tenant_id),
            payload={'host': host, 'ip': _request_client_ip(request), 'user_agent': _request_user_agent(request)},
        )
    )
    await db.commit()
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    secure_cookie = request.url.scheme == 'https' or forwarded_proto == 'https'
    resp = RedirectResponse(url='/tenant/dashboard', status_code=303)
    resp.set_cookie(
        key='access_token',
        value=token,
        httponly=True,
        secure=secure_cookie,
        samesite='lax',
        max_age=settings.access_token_expire_minutes * 60,
        path='/',
        domain=_cookie_domain_for_host(host),
    )
    return resp


async def _tenant_user_license_context(db: AsyncSession, tenant: Tenant) -> dict[str, object]:
    sub = await get_or_create_subscription(db, tenant)
    plan_row = (await db.execute(select(Plan).where(Plan.code == sub.plan.value))).scalar_one_or_none()
    limit = (
        await db.execute(
            select(SubscriptionLimit).where(
                SubscriptionLimit.subscription_id == sub.id,
                SubscriptionLimit.limit_key == 'max_users',
            )
        )
    ).scalar_one_or_none()
    if limit is None:
        fallback_limit = int(plan_row.max_users) if plan_row else 5
        limit = SubscriptionLimit(
            subscription_id=sub.id,
            limit_key='max_users',
            limit_value=fallback_limit,
            used_value=0,
        )
        db.add(limit)
        await db.flush()
    branch_limit = (
        await db.execute(
            select(SubscriptionLimit).where(
                SubscriptionLimit.subscription_id == sub.id,
                SubscriptionLimit.limit_key == 'max_branches',
            )
        )
    ).scalar_one_or_none()
    if branch_limit is None:
        fallback_branches = int(plan_row.max_branches) if plan_row else 5
        branch_limit = SubscriptionLimit(
            subscription_id=sub.id,
            limit_key='max_branches',
            limit_value=fallback_branches,
            used_value=0,
        )
        db.add(branch_limit)
        await db.flush()

    active_used = (
        await db.execute(
            select(func.count(User.id)).where(
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
                User.is_active.is_(True),
            )
        )
    ).scalar_one()
    total_users = (
        await db.execute(
            select(func.count(User.id)).where(
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
            )
        )
    ).scalar_one()
    limit.used_value = int(active_used or 0)
    branch_limit.used_value = 0
    return {
        'subscription': sub,
        'limit': limit,
        'branch_limit': branch_limit,
        'max_users': int(limit.limit_value or 0),
        'max_branches': int(branch_limit.limit_value or 0),
        'active_users': int(active_used or 0),
        'active_branches': int(branch_limit.used_value or 0),
        'available_users': max(0, int(limit.limit_value or 0) - int(active_used or 0)),
        'total_users': int(total_users or 0),
        'max_concurrent_sessions': await _tenant_max_concurrent_sessions(db, sub),
        'active_sessions': len(await _tenant_active_session_user_ids(db, tenant.id)),
    }


# --- Concurrent-session licensing: the tenant buys N simultaneous connections. It may open many
# user accounts (max_users), but only N distinct users can be logged in at the same time. ---
_CONCURRENT_SESSION_IDLE_MINUTES = 30


async def _tenant_max_concurrent_sessions(db: AsyncSession, sub) -> int:
    """The licensed number of simultaneous connections. Stored as a SubscriptionLimit; if unset,
    seeds from the tenant's max_users so existing tenants keep their current effective behaviour."""
    row = (
        await db.execute(
            select(SubscriptionLimit).where(
                SubscriptionLimit.subscription_id == sub.id,
                SubscriptionLimit.limit_key == 'max_concurrent_sessions',
            )
        )
    ).scalar_one_or_none()
    if row is None:
        seed = (
            await db.execute(
                select(SubscriptionLimit.limit_value).where(
                    SubscriptionLimit.subscription_id == sub.id,
                    SubscriptionLimit.limit_key == 'max_users',
                )
            )
        ).scalar_one_or_none()
        row = SubscriptionLimit(
            subscription_id=sub.id,
            limit_key='max_concurrent_sessions',
            limit_value=int(seed or 5),
            used_value=0,
        )
        db.add(row)
        await db.flush()
    return int(row.limit_value or 0)


async def _tenant_active_session_user_ids(db: AsyncSession, tenant_id: int) -> set[int]:
    """Distinct tenant users currently connected: a non-revoked, non-expired refresh token seen
    within the idle window. Same window the sessions page uses to auto-revoke idle sessions."""
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=_CONCURRENT_SESSION_IDLE_MINUTES)
    rows = (
        await db.execute(
            select(RefreshToken.user_id)
            .join(User, User.id == RefreshToken.user_id)
            .where(
                User.tenant_id == tenant_id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
                func.coalesce(RefreshToken.last_seen_at, RefreshToken.created_at) >= cutoff,
            )
        )
    ).scalars().all()
    return {int(x) for x in rows}


@router.get('/tenant/users', response_class=HTMLResponse)
async def tenant_users_page(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    users = (
        await db.execute(
            select(User)
            .where(
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
            )
            .order_by(User.is_active.desc(), User.full_name.asc().nullslast(), User.email.asc())
        )
    ).scalars().all()
    professional_profiles = await _list_professional_profiles(db)
    license_context = await _tenant_user_license_context(db, tenant)
    await db.commit()
    invite_token = str(request.query_params.get('invite') or '').strip()
    invite_url = f'/invite?token={invite_token}' if invite_token else None
    return templates.TemplateResponse(
        'tenant/users.html',
        {
            'request': request,
            'tenant': tenant,
            'user': user,
            'users': users,
            'professional_profiles': professional_profiles,
            'license_context': license_context,
            'invite_url': invite_url,
            **(await _tenant_navigation_context(tenant)),
            'active_page': 'tenant_users',
            'title': 'Χρήστες & Άδειες',
        },
    )


@router.post('/tenant/users/create')
async def tenant_user_create(
    full_name: str = Form(default=''),
    email: str = Form(...),
    phone: str = Form(default=''),
    role: str = Form(default='tenant_user'),
    professional_profile_code: str | None = Form(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    selected_role = RoleName.tenant_admin if role == RoleName.tenant_admin.value else RoleName.tenant_user
    license_context = await _tenant_user_license_context(db, tenant)
    if int(license_context['active_users']) >= int(license_context['max_users']):
        await db.rollback()
        return RedirectResponse(url='/tenant/users?created=0&reason=no_seats', status_code=303)
    try:
        professional_profile_id = await _resolve_professional_profile_id(
            db,
            selected_role=selected_role,
            requested_profile_code=professional_profile_code,
        )
    except ValueError:
        await db.rollback()
        return RedirectResponse(url='/tenant/users?created=0&reason=bad_profile', status_code=303)
    invite_token = secrets.token_urlsafe(24)
    new_user = User(
        tenant_id=tenant.id,
        professional_profile_id=professional_profile_id,
        full_name=full_name.strip() or None,
        phone=phone.strip() or None,
        email=email.strip().lower(),
        role=selected_role,
        password_hash=get_password_hash(secrets.token_urlsafe(18)),
        is_active=True,
        reset_token=invite_token,
        reset_token_expires_at=datetime.utcnow() + timedelta(days=7),
    )
    db.add(new_user)
    limit_obj = license_context.get('limit')
    if isinstance(limit_obj, SubscriptionLimit):
        limit_obj.used_value = int(license_context['active_users']) + 1
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            actor_user_id=actor.id,
            action='tenant_user_created',
            entity_type='user',
            payload={'email': new_user.email, 'role': selected_role.value},
        )
    )
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return RedirectResponse(url='/tenant/users?created=0&reason=email_exists', status_code=303)
    # Email the invite/set-password link so the new user actually receives access.
    new_email = new_user.email
    new_name = new_user.full_name or ''
    invite_sent = '0'
    try:
        result = await asyncio.to_thread(
            send_user_invite_email,
            full_name=new_name,
            email=new_email,
            invite_token=invite_token,
            tenant_slug=tenant.slug or '',
        )
        if str(result.get('status') or '') == 'sent':
            invite_sent = '1'
        else:
            logger.warning('tenant_user_create_invite_not_sent email=%s status=%s', new_email, result.get('status'))
    except Exception:  # noqa: BLE001
        logger.exception('tenant_user_create_invite_email_failed', extra={'email': new_email})
    return RedirectResponse(url=f'/tenant/users?created=1&invite_sent={invite_sent}&invite={invite_token}', status_code=303)


@router.post('/tenant/users/{user_id}/toggle')
async def tenant_user_toggle(
    user_id: int,
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    target = (
        await db.execute(
            select(User).where(
                User.id == user_id,
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
            )
        )
    ).scalar_one_or_none()
    if not target:
        return RedirectResponse(url='/tenant/users?updated=0&reason=user_not_found', status_code=303)
    if target.id == actor.id and target.is_active:
        return RedirectResponse(url='/tenant/users?updated=0&reason=self_disable', status_code=303)
    license_context = await _tenant_user_license_context(db, tenant)
    if not target.is_active:
        if int(license_context['active_users']) >= int(license_context['max_users']):
            await db.rollback()
            return RedirectResponse(url='/tenant/users?updated=0&reason=no_seats', status_code=303)
    target.is_active = not target.is_active
    limit_obj = license_context.get('limit')
    if isinstance(limit_obj, SubscriptionLimit):
        delta = 1 if target.is_active else -1
        limit_obj.used_value = max(0, int(license_context['active_users']) + delta)
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            actor_user_id=actor.id,
            action='tenant_user_activated' if target.is_active else 'tenant_user_deactivated',
            entity_type='user',
            entity_id=str(target.id),
            payload={'email': target.email},
        )
    )
    await db.commit()
    return RedirectResponse(url='/tenant/users?updated=1', status_code=303)


@router.post('/tenant/users/{user_id}/invite-reset')
async def tenant_user_invite_reset(
    user_id: int,
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    target = (
        await db.execute(
            select(User).where(
                User.id == user_id,
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
            )
        )
    ).scalar_one_or_none()
    if not target:
        return RedirectResponse(url='/tenant/users?invite_reset=0&reason=user_not_found', status_code=303)
    license_context = None
    if not target.is_active:
        license_context = await _tenant_user_license_context(db, tenant)
        if int(license_context['active_users']) >= int(license_context['max_users']):
            await db.rollback()
            return RedirectResponse(url='/tenant/users?invite_reset=0&reason=no_seats', status_code=303)
    token = secrets.token_urlsafe(24)
    target.reset_token = token
    target.reset_token_expires_at = datetime.utcnow() + timedelta(days=7)
    target.is_active = True
    if license_context is not None:
        limit_obj = license_context.get('limit')
        if isinstance(limit_obj, SubscriptionLimit):
            limit_obj.used_value = int(license_context['active_users']) + 1
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            actor_user_id=actor.id,
            action='tenant_user_invite_reset',
            entity_type='user',
            entity_id=str(target.id),
            payload={'email': target.email},
        )
    )
    await db.commit()
    # Actually email the fresh invite/set-password link to the user.
    target_email = target.email
    target_name = target.full_name or ''
    invite_sent = '0'
    try:
        result = await asyncio.to_thread(
            send_user_invite_email,
            full_name=target_name,
            email=target_email,
            invite_token=token,
            tenant_slug=tenant.slug or '',
        )
        if str(result.get('status') or '') == 'sent':
            invite_sent = '1'
        else:
            logger.warning('tenant_user_invite_reset_not_sent email=%s status=%s', target_email, result.get('status'))
    except Exception:  # noqa: BLE001
        logger.exception('tenant_user_invite_reset_email_failed', extra={'email': target_email})
    return RedirectResponse(url=f'/tenant/users?invite_reset=1&invite_sent={invite_sent}&invite={token}', status_code=303)


async def _tenant_managed_user(db: AsyncSession, tenant: Tenant, user_id: int) -> User | None:
    return (
        await db.execute(
            select(User).where(
                User.id == user_id,
                User.tenant_id == tenant.id,
                User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
            )
        )
    ).scalar_one_or_none()


async def _tenant_other_active_admins(db: AsyncSession, tenant_id: int, exclude_user_id: int) -> int:
    return int(
        (
            await db.execute(
                select(func.count(User.id)).where(
                    User.tenant_id == tenant_id,
                    User.role == RoleName.tenant_admin,
                    User.is_active.is_(True),
                    User.id != exclude_user_id,
                )
            )
        ).scalar_one()
        or 0
    )


@router.post('/tenant/users/{user_id}/set-password')
async def tenant_user_set_password(
    user_id: int,
    password: str = Form(default=''),
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    password = str(password or '')
    if len(password) < 8:
        return RedirectResponse(url='/tenant/users?pwset=0&reason=too_short', status_code=303)
    target = await _tenant_managed_user(db, tenant, user_id)
    if not target:
        return RedirectResponse(url='/tenant/users?pwset=0&reason=user_not_found', status_code=303)
    if not target.is_active:
        lc = await _tenant_user_license_context(db, tenant)
        if int(lc['active_users']) >= int(lc['max_users']):
            await db.rollback()
            return RedirectResponse(url='/tenant/users?pwset=0&reason=no_seats', status_code=303)
    target.password_hash = get_password_hash(password)
    target.is_active = True
    target.reset_token = None
    target.reset_token_expires_at = None
    db.add(
        AuditLog(
            tenant_id=tenant.id, actor_user_id=actor.id, action='tenant_user_set_password',
            entity_type='user', entity_id=str(target.id), payload={'email': target.email},
        )
    )
    await db.commit()
    return RedirectResponse(url='/tenant/users?pwset=1', status_code=303)


@router.post('/tenant/users/{user_id}/edit')
async def tenant_user_edit(
    user_id: int,
    full_name: str = Form(default=''),
    phone: str = Form(default=''),
    email: str = Form(default=''),
    role: str = Form(default=''),
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    target = await _tenant_managed_user(db, tenant, user_id)
    if not target:
        return RedirectResponse(url='/tenant/users?updated=0&reason=user_not_found', status_code=303)
    target.full_name = (full_name or '').strip() or target.full_name
    target.phone = (phone or '').strip() or target.phone
    new_email = (email or '').strip().lower()
    if new_email and new_email != (target.email or '').lower():
        if '@' not in new_email or '.' not in new_email.split('@')[-1]:
            await db.rollback()
            return RedirectResponse(url='/tenant/users?updated=0&reason=bad_email', status_code=303)
        clash = (
            await db.execute(select(User.id).where(func.lower(User.email) == new_email, User.id != target.id))
        ).scalar_one_or_none()
        if clash is not None:
            await db.rollback()
            return RedirectResponse(url='/tenant/users?updated=0&reason=email_exists', status_code=303)
        target.email = new_email
    desired_role = RoleName.tenant_admin if role == RoleName.tenant_admin.value else (
        RoleName.tenant_user if role == RoleName.tenant_user.value else None
    )
    if desired_role is not None and desired_role != target.role:
        # Never leave the tenant without an active admin.
        if target.role == RoleName.tenant_admin and desired_role == RoleName.tenant_user:
            if await _tenant_other_active_admins(db, tenant.id, target.id) == 0:
                await db.rollback()
                return RedirectResponse(url='/tenant/users?updated=0&reason=last_admin', status_code=303)
        target.role = desired_role
    db.add(
        AuditLog(
            tenant_id=tenant.id, actor_user_id=actor.id, action='tenant_user_edited',
            entity_type='user', entity_id=str(target.id), payload={'email': target.email},
        )
    )
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return RedirectResponse(url='/tenant/users?updated=0&reason=email_exists', status_code=303)
    return RedirectResponse(url='/tenant/users?updated=1', status_code=303)


@router.post('/tenant/users/{user_id}/delete')
async def tenant_user_delete(
    user_id: int,
    tenant: Tenant = Depends(get_request_tenant),
    actor: User = Depends(require_roles(RoleName.tenant_admin)),
    db: AsyncSession = Depends(get_control_db),
):
    target = await _tenant_managed_user(db, tenant, user_id)
    if not target:
        return RedirectResponse(url='/tenant/users?deleted=0&reason=user_not_found', status_code=303)
    if target.id == actor.id:
        return RedirectResponse(url='/tenant/users?deleted=0&reason=self_delete', status_code=303)
    if target.role == RoleName.tenant_admin and await _tenant_other_active_admins(db, tenant.id, target.id) == 0:
        return RedirectResponse(url='/tenant/users?deleted=0&reason=last_admin', status_code=303)
    email = target.email
    await db.execute(delete(RefreshToken).where(RefreshToken.user_id == target.id))
    db.add(
        AuditLog(
            tenant_id=tenant.id, actor_user_id=actor.id, action='tenant_user_deleted',
            entity_type='user', entity_id=str(target.id), payload={'email': email},
        )
    )
    await db.delete(target)
    await db.commit()
    return RedirectResponse(url='/tenant/users?deleted=1', status_code=303)


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
            'active_page': 'tenant_profile',
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
    db: AsyncSession = Depends(get_control_db),
):
    return templates.TemplateResponse(
        'tenant/settings.html',
        {
            'request': request,
            'tenant': tenant,
            'user': user,
            **(await _tenant_navigation_context(tenant)),
            'era_exploration_data': _tenant_era_exploration_settings(tenant),
            'iqvia_data': _tenant_iqvia_settings(tenant),
            'supplier_orders': _tenant_supplier_order_settings(tenant),
            'call_center_3cx': await _tenant_call_center_settings(db, tenant.id),
            'active_page': 'tenant_settings',
            'title': 'Settings',
        },
    )


@router.post('/tenant/settings/supplier-orders')
async def tenant_settings_supplier_orders(
    lookback_days: int = Form(default=30),
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    if user.role not in {RoleName.tenant_admin, RoleName.cloudon_admin}:
        return RedirectResponse(url='/tenant/settings?supplier_orders_saved=0&reason=permission', status_code=303)
    db_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant.id))).scalar_one_or_none()
    if db_tenant is None:
        return RedirectResponse(url='/tenant/settings?supplier_orders_saved=0&reason=tenant_not_found', status_code=303)
    payload = normalize_supplier_order_settings({'lookback_days': lookback_days})
    flags = dict(db_tenant.feature_flags or {})
    flags['supplier_orders'] = payload
    db_tenant.feature_flags = flags
    db.add(
        AuditLog(
            tenant_id=db_tenant.id,
            action='tenant_supplier_orders_settings_saved',
            entity_type='tenant',
            entity_id=str(db_tenant.id),
            payload=payload,
        )
    )
    await db.commit()
    return RedirectResponse(url='/tenant/settings?supplier_orders_saved=1', status_code=303)


@router.post('/tenant/call-center/sync-now')
async def tenant_call_center_sync_now(
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    if user.role not in {RoleName.tenant_admin, RoleName.cloudon_admin}:
        return RedirectResponse(url='/tenant/call-center?call_center_sync=0&reason=permission', status_code=303)
    result = await _run_3cx_manual_sync_check(db, tenant)
    status = '1' if result.get('ok') else '0'
    reason = urlencode({'message': str(result.get('message') or '')})
    return RedirectResponse(url=f'/tenant/call-center?call_center_sync={status}&{reason}', status_code=303)


@router.post('/tenant/settings/call-center/clear')
async def tenant_settings_call_center_clear(
    return_to: str = Form(default='/tenant/settings'),
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    if user.role not in {RoleName.tenant_admin, RoleName.cloudon_admin}:
        return _tenant_3cx_redirect(return_to, call_center_clear=0, reason='permission')
    conn = await _find_tenant_connection(db, tenant_id=tenant.id, connector_type=_3CX_CONNECTOR_TYPE)
    if conn is None:
        return _tenant_3cx_redirect(return_to, call_center_clear=0, reason='not_found')
    params = dict(conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {})
    previous_import = params.get('manual_import') if isinstance(params.get('manual_import'), dict) else {}
    previous_rows = int(previous_import.get('rows') or 0) if isinstance(previous_import, dict) else 0
    previous_filename = str(previous_import.get('filename') or '') if isinstance(previous_import, dict) else ''
    params.pop('manual_import', None)
    params['import_mode'] = 'replace_snapshot'
    conn.connection_parameters = params
    conn.sync_status = 'cleared'
    conn.last_sync_at = None
    flag_modified(conn, 'connection_parameters')
    db.add(
        AuditLog(
            tenant_id=tenant.id,
            actor_user_id=user.id,
            action='tenant_3cx_call_center_cleared',
            entity_type='tenant_connection',
            entity_id=str(conn.id),
            payload={'previous_rows': previous_rows, 'previous_filename': previous_filename},
        )
    )
    await db.commit()
    return _tenant_3cx_redirect(return_to, call_center_clear=1)


@router.post('/tenant/settings/era-exploration')
async def tenant_settings_era_exploration_upload(
    era_exploration_file: UploadFile | None = File(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if user.role not in {RoleName.tenant_admin, RoleName.cloudon_admin}:
        return RedirectResponse(url='/tenant/settings?era_upload=0&reason=permission', status_code=303)
    if era_exploration_file is None or not era_exploration_file.filename:
        return RedirectResponse(url='/tenant/settings?era_upload=0&reason=missing_file', status_code=303)
    db_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant.id))).scalar_one_or_none()
    if db_tenant is None:
        return RedirectResponse(url='/tenant/settings?era_upload=0&reason=tenant_not_found', status_code=303)
    payload, error = _save_era_exploration_upload(
        db_tenant,
        era_exploration_file,
        str(user.email or user.full_name or user.id),
    )
    if error:
        return RedirectResponse(url=f'/tenant/settings?era_upload=0&reason={error}', status_code=303)
    try:
        imported = await import_era_exploration_file(
            tenant_db,
            Path(str((payload or {}).get('archive_path') or (payload or {}).get('file_path'))),
            source_filename=str((payload or {}).get('filename') or ''),
            source_sha256=str((payload or {}).get('source_sha256') or ''),
            imported_by=str(user.email or user.full_name or user.id),
        )
        payload = {**(payload or {}), **imported}
    except EraDuplicateMarketImportError:
        logger.info('tenant_era_exploration_duplicate_upload', extra={'tenant_id': db_tenant.id, 'filename': (payload or {}).get('filename')})
        return RedirectResponse(url='/tenant/settings?era_upload=0&reason=era_duplicate_file', status_code=303)
    except Exception:
        logger.exception('tenant_era_exploration_db_import_failed', extra={'tenant_id': db_tenant.id})
        return RedirectResponse(url='/tenant/settings?era_upload=0&reason=era_db_import_failed', status_code=303)
    flags = dict(db_tenant.feature_flags or {})
    flags['era_exploration_data_config'] = payload or {}
    db_tenant.feature_flags = flags
    db.add(
        AuditLog(
            tenant_id=db_tenant.id,
            action='tenant_era_exploration_uploaded',
            entity_type='tenant',
            entity_id=str(db_tenant.id),
            payload=payload or {},
        )
    )
    await db.commit()
    return RedirectResponse(url='/tenant/settings?era_upload=1', status_code=303)


@router.post('/tenant/settings/iqvia')
async def tenant_settings_iqvia_upload(
    iqvia_file: UploadFile | None = File(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if user.role not in {RoleName.tenant_admin, RoleName.cloudon_admin}:
        return RedirectResponse(url='/tenant/settings?iqvia_upload=0&reason=permission', status_code=303)
    if iqvia_file is None or not iqvia_file.filename:
        return RedirectResponse(url='/tenant/settings?iqvia_upload=0&reason=missing_file', status_code=303)
    db_tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant.id))).scalar_one_or_none()
    if db_tenant is None:
        return RedirectResponse(url='/tenant/settings?iqvia_upload=0&reason=tenant_not_found', status_code=303)
    payload, error = _save_iqvia_upload(
        db_tenant,
        iqvia_file,
        str(user.email or user.full_name or user.id),
    )
    if error:
        return RedirectResponse(url=f'/tenant/settings?iqvia_upload=0&reason={error}', status_code=303)
    try:
        imported = await import_iqvia_file(
            tenant_db,
            Path(str((payload or {}).get('archive_path') or (payload or {}).get('file_path'))),
            source_filename=str((payload or {}).get('filename') or ''),
            source_sha256=str((payload or {}).get('source_sha256') or ''),
            imported_by=str(user.email or user.full_name or user.id),
        )
        payload = {**(payload or {}), **imported}
    except IqviaDuplicateMarketImportError:
        logger.info('tenant_iqvia_duplicate_upload', extra={'tenant_id': db_tenant.id, 'filename': (payload or {}).get('filename')})
        return RedirectResponse(url='/tenant/settings?iqvia_upload=0&reason=iqvia_duplicate_file', status_code=303)
    except Exception:
        logger.exception('tenant_iqvia_db_import_failed', extra={'tenant_id': db_tenant.id})
        return RedirectResponse(url='/tenant/settings?iqvia_upload=0&reason=iqvia_db_import_failed', status_code=303)
    flags = dict(db_tenant.feature_flags or {})
    flags['iqvia_config'] = payload or {}
    db_tenant.feature_flags = flags
    db.add(
        AuditLog(
            tenant_id=db_tenant.id,
            action='tenant_iqvia_uploaded',
            entity_type='tenant',
            entity_id=str(db_tenant.id),
            payload=payload or {},
        )
    )
    await db.commit()
    return RedirectResponse(url='/tenant/settings?iqvia_upload=1', status_code=303)


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
            'active_page': 'tenant_messages',
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


@router.get('/tenant/e-shop-analysis', response_class=HTMLResponse)
async def tenant_eshop_analysis_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/eshop_analysis_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'eshop_analysis',
            'title': 'E-Shop analysis',
        },
    )


@router.get('/tenant/sales-documents', response_class=HTMLResponse)
async def tenant_sales_documents_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=365)
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


@router.get('/tenant/pos', response_class=HTMLResponse)
async def tenant_pos_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/pos_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'pos',
            'title': 'Φυσικό Σημείο Πώλησης',
        },
    )


@router.get('/tenant/call-center', response_class=HTMLResponse)
async def tenant_call_center_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    today = date.today()

    def _parse_call_center_filter_date(raw: str | None, fallback: date | None) -> date | None:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    period = str(request.query_params.get('period') or 'month').strip().lower()
    if period == 'today':
        default_from: date | None = today
        default_to: date | None = today
    elif period == 'yesterday':
        default_from = today - timedelta(days=1)
        default_to = default_from
    elif period == 'week':
        default_to = today
        default_from = today - timedelta(days=6)
    elif period == 'all':
        default_from = None
        default_to = None
    else:
        default_to = today
        default_from = today - timedelta(days=30)
    from_date = _parse_call_center_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_call_center_filter_date(request.query_params.get('to'), default_to)
    if from_date is not None and to_date is not None and from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_call_center_scope = (
        request.query_params.get('queues')
        or request.query_params.get('queue')
        or request.query_params.get('q')
        or request.query_params.get('did')
        or request.query_params.get('caller')
        or request.query_params.get('source')
        or request.query_params.get('agent')
        or request.query_params.get('extension')
        or request.query_params.get('ext')
        or request.query_params.get('direction')
        or request.query_params.get('status')
        or ''
    )
    return templates.TemplateResponse(
        'tenant/call_center_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'call_center_3cx': await _tenant_call_center_settings(
                db,
                tenant.id,
                from_date=from_date,
                to_date=to_date,
                queues=selected_call_center_scope,
            ),
            'call_center_selected_scope': selected_call_center_scope,
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'call_center',
            'title': 'Τηλεφωνικό Κέντρο',
        },
    )


@router.get('/tenant/call-center/report/export.xlsx')
async def tenant_call_center_report_export_xlsx(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    today = date.today()

    def _parse_call_center_filter_date(raw: str | None, fallback: date | None) -> date | None:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    period = str(request.query_params.get('period') or 'month').strip().lower()
    if period == 'today':
        default_from: date | None = today
        default_to: date | None = today
    elif period == 'yesterday':
        default_from = today - timedelta(days=1)
        default_to = default_from
    elif period == 'week':
        default_to = today
        default_from = today - timedelta(days=6)
    elif period == 'all':
        default_from = None
        default_to = None
    else:
        default_to = today
        default_from = today - timedelta(days=30)
    from_date = _parse_call_center_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_call_center_filter_date(request.query_params.get('to'), default_to)
    if from_date is not None and to_date is not None and from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_call_center_scope = (
        request.query_params.get('queues')
        or request.query_params.get('queue')
        or request.query_params.get('q')
        or request.query_params.get('did')
        or request.query_params.get('caller')
        or request.query_params.get('source')
        or request.query_params.get('agent')
        or request.query_params.get('extension')
        or request.query_params.get('ext')
        or request.query_params.get('direction')
        or request.query_params.get('status')
        or ''
    )
    call_center = await _tenant_call_center_settings(
        db,
        tenant.id,
        from_date=from_date,
        to_date=to_date,
        queues=selected_call_center_scope,
    )
    headers = [
        'Called Number',
        'Week',
        'Week Start',
        'Week End',
        'Inbound Calls',
        'Answer Rate %',
        'Answered',
        'Missed Calls',
        'Avg Wait (sec)',
        'Avg Talk (sec)',
        'Abandonment %',
        'Repeat Rate %',
        'Redirected %',
        'Channel / Queue',
        'Unique Callers',
    ]
    export_rows: list[list[object]] = []
    for row in call_center.get('did_weekly_rows') or []:
        if not isinstance(row, dict):
            continue
        export_rows.append(
            [
                row.get('did') or '',
                row.get('week') or '',
                row.get('week_start') or '',
                row.get('week_end') or '',
                int(row.get('inbound_calls') or 0),
                float(row.get('answer_rate_pct') or 0),
                int(row.get('answered') or 0),
                int(row.get('missed_calls') or 0),
                int(row.get('avg_waiting_time_seconds') or 0),
                int(row.get('avg_talking_time_seconds') or 0),
                float(row.get('abandonment_rate_pct') or 0),
                float(row.get('call_repeat_rate_pct') or 0),
                float(row.get('call_redirected_pct') or 0),
                row.get('top_queue') or '',
                int(row.get('unique_sources') or 0),
            ]
        )
    content = _build_xlsx_bytes(
        sheet_name='Call Center Report',
        headers=headers,
        rows=export_rows,
        column_widths=[16, 12, 14, 14, 15, 15, 12, 13, 15, 15, 16, 14, 14, 28, 15],
    )
    filename_from = from_date.isoformat() if from_date else 'all'
    filename_to = to_date.isoformat() if to_date else today.isoformat()
    filename = f'call_center_report_{filename_from}_{filename_to}.xlsx'
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/tenant/call-center/traffic/export.xlsx')
async def tenant_call_center_traffic_export_xlsx(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    today = date.today()

    def _parse_call_center_filter_date(raw: str | None, fallback: date | None) -> date | None:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    period = str(request.query_params.get('period') or 'month').strip().lower()
    if period == 'today':
        default_from: date | None = today
        default_to: date | None = today
    elif period == 'yesterday':
        default_from = today - timedelta(days=1)
        default_to = default_from
    elif period == 'week':
        default_to = today
        default_from = today - timedelta(days=6)
    elif period == 'all':
        default_from = None
        default_to = None
    else:
        default_to = today
        default_from = today - timedelta(days=30)
    from_date = _parse_call_center_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_call_center_filter_date(request.query_params.get('to'), default_to)
    if from_date is not None and to_date is not None and from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_call_center_scope = (
        request.query_params.get('queues')
        or request.query_params.get('queue')
        or request.query_params.get('q')
        or request.query_params.get('did')
        or request.query_params.get('caller')
        or request.query_params.get('source')
        or request.query_params.get('agent')
        or request.query_params.get('extension')
        or request.query_params.get('ext')
        or request.query_params.get('direction')
        or request.query_params.get('status')
        or ''
    )
    call_center = await _tenant_call_center_settings(
        db,
        tenant.id,
        from_date=from_date,
        to_date=to_date,
        queues=selected_call_center_scope,
    )
    headers = ['Date', 'Calls', 'Inbound', 'Outbound', 'Answered', 'Missed']
    export_rows: list[list[object]] = []
    for row in call_center.get('daily_rows') or []:
        if not isinstance(row, dict):
            continue
        calls = int(row.get('calls') or 0)
        outbound = int(row.get('outbound') or 0)
        inbound = calls - outbound if calls >= outbound else calls
        export_rows.append(
            [
                row.get('date') or '',
                calls,
                inbound,
                outbound,
                int(row.get('answered') or 0),
                int(row.get('missed') or 0),
            ]
        )
    content = _build_xlsx_bytes(
        sheet_name='Call Traffic',
        headers=headers,
        rows=export_rows,
        column_widths=[14, 12, 12, 12, 12, 12],
    )
    filename_from = from_date.isoformat() if from_date else 'all'
    filename_to = to_date.isoformat() if to_date else today.isoformat()
    filename = f'call_traffic_{filename_from}_{filename_to}.xlsx'
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/tenant/call-center/inbound/export.xlsx')
async def tenant_call_center_inbound_export_xlsx(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    today = date.today()

    def _parse_call_center_filter_date(raw: str | None, fallback: date | None) -> date | None:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    period = str(request.query_params.get('period') or 'month').strip().lower()
    if period == 'today':
        default_from: date | None = today
        default_to: date | None = today
    elif period == 'yesterday':
        default_from = today - timedelta(days=1)
        default_to = default_from
    elif period == 'week':
        default_to = today
        default_from = today - timedelta(days=6)
    elif period == 'all':
        default_from = None
        default_to = None
    else:
        default_to = today
        default_from = today - timedelta(days=30)
    from_date = _parse_call_center_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_call_center_filter_date(request.query_params.get('to'), default_to)
    if from_date is not None and to_date is not None and from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_call_center_scope = (
        request.query_params.get('queues')
        or request.query_params.get('queue')
        or request.query_params.get('q')
        or request.query_params.get('did')
        or request.query_params.get('caller')
        or request.query_params.get('source')
        or request.query_params.get('agent')
        or request.query_params.get('extension')
        or request.query_params.get('ext')
        or request.query_params.get('direction')
        or request.query_params.get('status')
        or ''
    )
    call_center = await _tenant_call_center_settings(
        db,
        tenant.id,
        from_date=from_date,
        to_date=to_date,
        queues=selected_call_center_scope,
    )
    headers = ['Agent', 'Extension', 'Answer Rate (%)', 'Inbound', 'Inbound Answered', 'Inbound Missed', 'Outbound']
    export_rows: list[list[object]] = []
    for row in call_center.get('agent_rows') or []:
        if not isinstance(row, dict):
            continue
        agent_total = int(row.get('calls') or (int(row.get('inbound') or 0) + int(row.get('outbound') or 0)))
        outbound = int(row.get('outbound') or 0)
        inbound = int(row.get('inbound') if row.get('inbound') is not None else (agent_total - outbound if agent_total >= outbound else 0))
        inbound_answered = int(row.get('inbound_answered') if row.get('inbound_answered') is not None else row.get('answered') or 0)
        inbound_missed = int(row.get('inbound_missed') if row.get('inbound_missed') is not None else row.get('missed') or 0)
        answer_rate = round((inbound_answered / inbound) * 100, 1) if inbound else 0
        export_rows.append(
            [
                row.get('agent') or '',
                row.get('extension') or '',
                answer_rate,
                inbound,
                inbound_answered,
                inbound_missed,
                outbound,
            ]
        )
    content = _build_xlsx_bytes(
        sheet_name='Inbound Calls',
        headers=headers,
        rows=export_rows,
        column_widths=[28, 12, 16, 12, 18, 16, 12],
    )
    filename_from = from_date.isoformat() if from_date else 'all'
    filename_to = to_date.isoformat() if to_date else today.isoformat()
    filename = f'inbound_calls_{filename_from}_{filename_to}.xlsx'
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/tenant/call-center/outbound/export.xlsx')
async def tenant_call_center_outbound_export_xlsx(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
):
    today = date.today()

    def _parse_call_center_filter_date(raw: str | None, fallback: date | None) -> date | None:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    period = str(request.query_params.get('period') or 'month').strip().lower()
    if period == 'today':
        default_from: date | None = today
        default_to: date | None = today
    elif period == 'yesterday':
        default_from = today - timedelta(days=1)
        default_to = default_from
    elif period == 'week':
        default_to = today
        default_from = today - timedelta(days=6)
    elif period == 'all':
        default_from = None
        default_to = None
    else:
        default_to = today
        default_from = today - timedelta(days=30)
    from_date = _parse_call_center_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_call_center_filter_date(request.query_params.get('to'), default_to)
    if from_date is not None and to_date is not None and from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_call_center_scope = (
        request.query_params.get('queues')
        or request.query_params.get('queue')
        or request.query_params.get('q')
        or request.query_params.get('did')
        or request.query_params.get('caller')
        or request.query_params.get('source')
        or request.query_params.get('agent')
        or request.query_params.get('extension')
        or request.query_params.get('ext')
        or request.query_params.get('direction')
        or request.query_params.get('status')
        or ''
    )
    call_center = await _tenant_call_center_settings(
        db,
        tenant.id,
        from_date=from_date,
        to_date=to_date,
        queues=selected_call_center_scope,
    )
    headers = [
        'Agent',
        'Extension',
        'Answer Rate (%)',
        'Inbound',
        'Outbound',
        'Inbound Answered',
        'Outbound Answered',
        'Missed',
        'Total',
    ]
    export_rows: list[list[object]] = []
    for row in call_center.get('agent_rows') or []:
        if not isinstance(row, dict):
            continue
        agent_total = int(row.get('calls') or (int(row.get('inbound') or 0) + int(row.get('outbound') or 0)))
        outbound = int(row.get('outbound') or 0)
        inbound = int(row.get('inbound') if row.get('inbound') is not None else (agent_total - outbound if agent_total >= outbound else 0))
        outbound_answered = int(row.get('outbound_answered') or 0)
        outbound_answer_rate = round((outbound_answered / outbound) * 100, 1) if outbound else 0
        export_rows.append(
            [
                row.get('agent') or '',
                row.get('extension') or '',
                outbound_answer_rate,
                inbound,
                outbound,
                int(row.get('inbound_answered') if row.get('inbound_answered') is not None else row.get('answered') or 0),
                outbound_answered,
                int(row.get('missed') or 0),
                agent_total,
            ]
        )
    content = _build_xlsx_bytes(
        sheet_name='Outbound Calls',
        headers=headers,
        rows=export_rows,
        column_widths=[28, 12, 16, 12, 12, 18, 18, 12, 12],
    )
    filename_from = from_date.isoformat() if from_date else 'all'
    filename_to = to_date.isoformat() if to_date else today.isoformat()
    filename = f'outbound_calls_{filename_from}_{filename_to}.xlsx'
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
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
    from_date = to_date - timedelta(days=365)
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
    from_date = to_date - timedelta(days=365)
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


@router.get('/tenant/operating-expenses', response_class=HTMLResponse)
async def tenant_operating_expenses_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/operating_expenses_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'operating_expenses',
            'title': 'title_operating_expenses_dashboard',
        },
    )


@router.get('/tenant/expense-documents', response_class=HTMLResponse)
async def tenant_expense_documents_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=365)
    return templates.TemplateResponse(
        'tenant/expense_documents_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'active_page': 'expense_documents',
            'title': 'title_expense_documents_dashboard',
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


@router.get('/tenant/replenishment', response_class=HTMLResponse)
async def tenant_replenishment_dashboard(
    request: Request,
    branch: str | None = Query(default=None),
    status: str | None = Query(default=None),
    category: str | None = Query(default=None),
    vendor: str | None = Query(default=None),
    abc: str | None = Query(default=None),
    search: str | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    feature_locked = not feature_flags['replenishment_enabled']
    top_need_rows = []
    top_overstock_rows = []
    issue_rows = []
    latest_snapshot = None
    summary = {}
    availability = {}
    try:
        if feature_locked:
            raise PermissionError('replenishment_feature_locked')
        _iic = _tenant_inventory_item_classification_settings(tenant)
        _scope_sold_days = (
            int(_iic.get('inventory_scope_sold_days') or 0)
            if _iic.get('status_source') == 'active_available'
            else None
        )
        availability, _availability_cache_hit = await get_or_set_cache(
            namespace='dashboard:replenishment_availability',
            tenant_key=str(tenant.id),
            params={
                'version': 'availability-searchable-abc-filters-v2',
                'branch': branch or '',
                'status': status or '',
                'category': category or '',
                'vendor': vendor or '',
                'abc': abc or '',
                'search': search or '',
                'scope_sold_days': _scope_sold_days or 0,
            },
            ttl_seconds=1800,
            producer=lambda: build_availability_foundation(
                tenant_db,
                branch=branch,
                status=status,
                category=category,
                vendor=vendor,
                abc=abc,
                search=search,
                scope_sold_days=_scope_sold_days,
            ),
        )
        latest_snapshot = (
            await tenant_db.execute(
                select(ReplenishmentSnapshot).order_by(ReplenishmentSnapshot.imported_at.desc()).limit(1)
            )
        ).scalar_one_or_none()
        summary = latest_snapshot.summary_json if latest_snapshot and isinstance(latest_snapshot.summary_json, dict) else {}
        if latest_snapshot is not None:
            top_need_rows = (
                (
                    await tenant_db.execute(
                        select(ReplenishmentLine)
                        .where(ReplenishmentLine.snapshot_id == latest_snapshot.id)
                        .order_by(ReplenishmentLine.supplier_order_qty.desc())
                        .limit(10)
                    )
                )
                .scalars()
                .all()
            )
            top_overstock_rows = (
                (
                    await tenant_db.execute(
                        select(ReplenishmentLine)
                        .where(ReplenishmentLine.snapshot_id == latest_snapshot.id)
                        .order_by(ReplenishmentLine.total_overstock_qty.asc())
                        .limit(10)
                    )
                )
                .scalars()
                .all()
            )
            issue_rows = (
                (
                    await tenant_db.execute(
                        select(ReplenishmentDataQualityIssue)
                        .where(ReplenishmentDataQualityIssue.snapshot_id == latest_snapshot.id)
                        .order_by(
                            ReplenishmentDataQualityIssue.severity.asc(),
                            ReplenishmentDataQualityIssue.source_row.asc(),
                        )
                        .limit(20)
                    )
                )
                .scalars()
                .all()
            )
        else:
            facts_replenishment, _facts_replenishment_cache_hit = await get_or_set_cache(
                namespace='dashboard:replenishment_facts',
                tenant_key=str(tenant.id),
                params={'version': 'facts-replenishment-v1'},
                ttl_seconds=1800,
                producer=lambda: build_replenishment_from_facts(tenant_db),
            )
            summary = dict(facts_replenishment['summary'])
            # The production Availability view is built from BI facts and already excludes
            # non-stock SoftOne rows. The legacy fallback quality list can still flag old
            # service rows as missing purchase price, so keep data-quality clean unless an
            # imported FnR snapshot provides explicit workbook issues.
            summary['issue_count'] = 0
            top_need_rows = facts_replenishment['top_need_rows']
            top_overstock_rows = facts_replenishment['top_overstock_rows']
            issue_rows = []
    except PermissionError:
        latest_snapshot = None
        summary = {}
        availability = {}
        top_need_rows = []
        top_overstock_rows = []
        issue_rows = []
    except Exception:
        logger.exception('tenant_replenishment_snapshot_load_failed', extra={'tenant_id': tenant.id})
        await tenant_db.rollback()
        latest_snapshot = None
        summary = {}
        availability = {}
        top_need_rows = []
        top_overstock_rows = []
        issue_rows = []
    return templates.TemplateResponse(
        'tenant/replenishment_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': feature_locked,
            'active_page': 'replenishment',
            'title': 'Replenishment / Availability',
            'latest_snapshot': latest_snapshot,
            'summary': summary,
            'availability': availability,
            'top_need_rows': top_need_rows,
            'top_overstock_rows': top_overstock_rows,
            'issue_rows': issue_rows,
            'hide_page_filters': True,
        },
    )


def _fnr_query_list(value: str | None) -> list[str]:
    return [part.strip() for part in str(value or '').replace('\n', ',').split(',') if part.strip()]


def _fnr_float(value: object, default: float) -> float:
    try:
        return float(str(value or '').replace(',', '.'))
    except (TypeError, ValueError):
        return default


def _fnr_int(value: object, default: int) -> int:
    try:
        return max(int(float(str(value or '').replace(',', '.'))), 1)
    except (TypeError, ValueError):
        return default


def _fnr_export_row(row: dict[str, object]) -> list[object]:
    stores = row.get('stores') if isinstance(row.get('stores'), dict) else {}
    stock_weeks = row.get('stock_weeks') if isinstance(row.get('stock_weeks'), dict) else {}
    target_stock = row.get('target_stock') if isinstance(row.get('target_stock'), dict) else {}
    need_qty = row.get('need_qty') if isinstance(row.get('need_qty'), dict) else {}
    overstock_qty = row.get('overstock_qty') if isinstance(row.get('overstock_qty'), dict) else {}

    def store_value(code: str, field: str) -> object:
        metric = stores.get(code) if isinstance(stores.get(code), dict) else {}
        return metric.get(field, 0) if isinstance(metric, dict) else 0

    values: list[object] = [
        row.get('item_code', ''),
        row.get('item_name', ''),
        row.get('category_1', ''),
        row.get('category_2', ''),
        row.get('category_3', ''),
        row.get('status_1', ''),
        row.get('status_2', ''),
        row.get('min_stock', 0),
        row.get('repl_moq', 0),
        row.get('vendor_moq', 0),
    ]
    for field in ('sales_avg_1', 'sales_avg_2', 'stock_qty'):
        values.extend(store_value(code, field) for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend(stock_weeks.get(code, 0) for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend(store_value(code, 'expected_qty') for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend(target_stock.get(code, 0) for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend(need_qty.get(code, 0) for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend(overstock_qty.get(code, 0) for code in ('KAS', 'AGD', 'PER', 'ELL', 'SPA', 'LOGICA'))
    values.extend([
        row.get('supplier_order_qty', 0),
        row.get('weeks_of_stock_total', 0),
        row.get('purchase_price', 0),
        row.get('supplier_order_value', 0),
    ])
    return values


def _xlsx_column_letter(index: int) -> str:
    value = max(int(index), 1)
    out = ''
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def _xlsx_cell(row_no: int, col_no: int, value: object, style: int = 0) -> str:
    ref = f'{_xlsx_column_letter(col_no)}{row_no}'
    style_attr = f' s="{style}"' if style else ''
    if value is None:
        return f'<c r="{ref}"{style_attr}/>'
    if isinstance(value, bool):
        return f'<c r="{ref}"{style_attr} t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{style_attr}><v>{float(value):.6f}</v></c>'
    text_value = xml_escape(str(value), {'"': '&quot;'})
    return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{text_value}</t></is></c>'


def _xlsx_sheet_xml(
    rows: list[list[tuple[object, int] | object]],
    *,
    widths: list[float] | None = None,
    freeze_row: int | None = None,
    auto_filter_row: int | None = None,
) -> str:
    max_cols = max((len(row) for row in rows), default=1)
    max_rows = max(len(rows), 1)
    dimension = f'A1:{_xlsx_column_letter(max_cols)}{max_rows}'
    col_xml = ''
    if widths:
        col_parts = []
        for idx, width in enumerate(widths, start=1):
            col_parts.append(f'<col min="{idx}" max="{idx}" width="{float(width):.2f}" customWidth="1"/>')
        col_xml = f'<cols>{"".join(col_parts)}</cols>'
    pane_xml = ''
    if freeze_row and freeze_row > 1:
        pane_xml = (
            '<sheetViews><sheetView workbookViewId="0">'
            f'<pane ySplit="{freeze_row - 1}" topLeftCell="A{freeze_row}" activePane="bottomLeft" state="frozen"/>'
            '</sheetView></sheetViews>'
        )
    else:
        pane_xml = '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
    row_parts: list[str] = []
    for row_no, row in enumerate(rows, start=1):
        cells = []
        for col_no, raw_cell in enumerate(row, start=1):
            if isinstance(raw_cell, tuple):
                value, style = raw_cell
            else:
                value, style = raw_cell, 0
            cells.append(_xlsx_cell(row_no, col_no, value, style))
        row_parts.append(f'<row r="{row_no}">{"".join(cells)}</row>')
    auto_filter_xml = ''
    if auto_filter_row and auto_filter_row <= max_rows:
        auto_filter_xml = f'<autoFilter ref="A{auto_filter_row}:{_xlsx_column_letter(max_cols)}{max_rows}"/>'
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<dimension ref="{dimension}"/>'
        f'{pane_xml}'
        f'{col_xml}'
        f'<sheetData>{"".join(row_parts)}</sheetData>'
        f'{auto_filter_xml}'
        '</worksheet>'
    )


def _build_xlsx_workbook(sheets: list[dict[str, object]]) -> bytes:
    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2"><numFmt numFmtId="164" formatCode="#,##0.00"/><numFmt numFmtId="165" formatCode="#,##0.00 €"/></numFmts>
  <fonts count="6">
    <font><sz val="11"/><color rgb="FF111827"/><name val="Calibri"/></font>
    <font><b/><sz val="18"/><color rgb="FF111827"/><name val="Calibri"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>
    <font><b/><sz val="11"/><color rgb="FF1E3A8A"/><name val="Calibri"/></font>
    <font><b/><sz val="11"/><color rgb="FF92400E"/><name val="Calibri"/></font>
    <font><b/><sz val="11"/><color rgb="FF065F46"/><name val="Calibri"/></font>
  </fonts>
  <fills count="8">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F2937"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAFE"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF7ED"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFECFDF5"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFEF3C7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFEFF6FF"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border><left style="thin"><color rgb="FFE2E8F0"/></left><right style="thin"><color rgb="FFE2E8F0"/></right><top style="thin"><color rgb="FFE2E8F0"/></top><bottom style="thin"><color rgb="FFE2E8F0"/></bottom><diagonal/></border>
  </borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="10">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="1" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="3" fillId="3" borderId="1" xfId="0" applyFont="1" applyFill="1"/>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>
    <xf numFmtId="164" fontId="4" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyNumberFormat="1"><alignment horizontal="right"/></xf>
    <xf numFmtId="0" fontId="5" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1"/>
    <xf numFmtId="165" fontId="5" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyNumberFormat="1"><alignment horizontal="right"/></xf>
    <xf numFmtId="0" fontId="3" fillId="7" borderId="1" xfId="0" applyFont="1" applyFill="1"/>
  </cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>"""
    content_types = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
    ]
    workbook_sheets = []
    workbook_rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
    ]
    for idx, sheet in enumerate(sheets, start=1):
        content_types.append(f'<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')
        name = xml_escape(str(sheet.get('name') or f'Sheet{idx}'), {'"': '&quot;'})
        workbook_sheets.append(f'<sheet name="{name}" sheetId="{idx}" r:id="rId{idx}"/>')
        workbook_rels.append(f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>')
    content_types.append('</Types>')
    workbook_rels.append(f'<Relationship Id="rId{len(sheets) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>')
    workbook_rels.append('</Relationships>')
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets>{"".join(workbook_sheets)}</sheets></workbook>'
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml', ''.join(content_types))
        archive.writestr('_rels/.rels', root_rels)
        archive.writestr('xl/workbook.xml', workbook_xml)
        archive.writestr('xl/_rels/workbook.xml.rels', ''.join(workbook_rels))
        archive.writestr('xl/styles.xml', styles_xml)
        for idx, sheet in enumerate(sheets, start=1):
            archive.writestr(f'xl/worksheets/sheet{idx}.xml', str(sheet['xml']))
    return output.getvalue()


def _build_fnr_order_xlsx(fnr: dict[str, object], *, tenant_name: str = '', include_no_order: bool = False) -> bytes:
    summary = fnr.get('summary') if isinstance(fnr.get('summary'), dict) else {}
    filters = fnr.get('filters') if isinstance(fnr.get('filters'), dict) else {}
    parameters = fnr.get('parameters') if isinstance(fnr.get('parameters'), dict) else {}
    order_rows = [row for row in (fnr.get('order_rows') or []) if isinstance(row, dict)]
    # When the "include items without an order" toggle is on, the detail sheet covers every
    # item (overstock-first); otherwise it stays scoped to the supplier-order lines.
    detail_rows = [row for row in (fnr.get('rows') or []) if isinstance(row, dict)] if include_no_order else order_rows
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    def joined(key: str) -> str:
        value = filters.get(key)
        if isinstance(value, list):
            return ', '.join(str(item) for item in value if str(item).strip()) or 'Όλα'
        return str(value or 'Όλα')

    order_sheet: list[list[tuple[object, int] | object]] = [
        [('FNR Παραγγελία Προμηθευτή', 1)],
        [(tenant_name or 'Tenant', 3), (f'Ημερομηνία δεδομένων: {summary.get("as_of", "-")}', 3), (f'Παραγωγή: {generated_at}', 3)],
        [],
        [('Σύνοψη', 7), ('Γραμμές παραγγελίας', 9), (summary.get('order_rows_count', 0), 4), ('Ποσότητα', 9), (summary.get('total_supplier_order_qty', 0), 4), ('Αξία', 9), (summary.get('total_supplier_order_value', 0), 8)],
        [('Φίλτρα', 7), ('Φαρμακεία', 9), joined('pharmacies'), ('Κατηγ. 1', 9), joined('category_1'), ('Προμηθευτές', 9), joined('suppliers')],
        [('Παράμετροι', 7), ('Target weeks', 9), parameters.get('target_stock_weeks', ''), ('Overstock weeks', 9), parameters.get('overstock_weeks', ''), ('MO1/MO2 weeks', 9), f"{parameters.get('sales_avg_period_1_weeks', '')}/{parameters.get('sales_avg_period_2_weeks', '')}"],
        [],
    ]
    order_header = [
        'Προμηθευτής', 'Κωδικός', 'Περιγραφή', 'Κατηγορία 1', 'Κατηγορία 2', 'Κατηγορία 3',
        'Status', 'Ποσότητα Παραγγελίας', 'Τιμή Αγοράς', 'Αξία Παραγγελίας', 'Need Σύνολο',
        'Stock Logica', 'Expected Logica', 'Weeks Total',
        'Need ΚΑΣ', 'Need ΑΓΔ', 'Need ΠΕΡ', 'Need ΕΛΛ', 'Need ΣΠΑ', 'Need Logica',
    ]
    order_sheet.append([(header, 2) for header in order_header])
    sorted_rows = sorted(order_rows, key=lambda row: (str(row.get('supplier') or 'Χωρίς προμηθευτή'), str(row.get('item_name') or '')))
    current_supplier = None
    supplier_qty = 0.0
    supplier_value = 0.0

    def subtotal_row(label: str, qty: float, value: float) -> list[tuple[object, int] | object]:
        return [(f'Σύνολο {label}', 7), '', '', '', '', '', '', (qty, 6), '', (value, 8)]

    for row in sorted_rows:
        supplier = str(row.get('supplier') or 'Χωρίς προμηθευτή')
        if current_supplier is not None and supplier != current_supplier:
            order_sheet.append(subtotal_row(current_supplier, supplier_qty, supplier_value))
            supplier_qty = 0.0
            supplier_value = 0.0
        current_supplier = supplier
        stores = row.get('stores') if isinstance(row.get('stores'), dict) else {}
        logica = stores.get('LOGICA') if isinstance(stores.get('LOGICA'), dict) else {}
        need = row.get('need_qty') if isinstance(row.get('need_qty'), dict) else {}
        qty = float(row.get('supplier_order_qty') or 0)
        value = float(row.get('supplier_order_value') or 0)
        supplier_qty += qty
        supplier_value += value
        order_sheet.append([
            supplier,
            row.get('item_code', ''),
            row.get('item_name', ''),
            row.get('category_1', ''),
            row.get('category_2', ''),
            row.get('category_3', ''),
            row.get('status_1', ''),
            (qty, 6),
            (row.get('purchase_price', 0), 4),
            (value, 5),
            (row.get('total_need_qty', 0), 4),
            (logica.get('stock_qty', 0), 4),
            (logica.get('expected_qty', 0), 4),
            (row.get('weeks_of_stock_total', 0), 4),
            (need.get('KAS', 0), 4),
            (need.get('AGD', 0), 4),
            (need.get('PER', 0), 4),
            (need.get('ELL', 0), 4),
            (need.get('SPA', 0), 4),
            (need.get('LOGICA', 0), 4),
        ])
    if current_supplier is not None:
        order_sheet.append(subtotal_row(current_supplier, supplier_qty, supplier_value))

    detail_title = (
        f'Όλα τα είδη (overstock πρώτα) - {generated_at}'
        if include_no_order
        else f'Πλήρης ανάλυση γραμμών παραγγελίας - {generated_at}'
    )
    # FNR Detail: insert Προμηθευτής (B) + Ομάδα (C) right after the item code, shifting the rest right.
    detail_header = [FNR_OUTPUT_COLUMNS[0], 'Προμηθευτής', 'Ομάδα', *FNR_OUTPUT_COLUMNS[1:]]
    detail_sheet: list[list[tuple[object, int] | object]] = [
        [('FNR Detail', 1)],
        [(detail_title, 3)],
        [],
        [(header, 2) for header in detail_header],
    ]
    for row in detail_rows:
        cells = _fnr_export_row(row)
        cells = [cells[0], row.get('supplier', ''), row.get('group', ''), *cells[1:]]
        detail_sheet.append([(value, 4 if isinstance(value, (int, float)) else 0) for value in cells])

    sheets = [
        {
            'name': 'Παραγγελία',
            'xml': _xlsx_sheet_xml(
                order_sheet,
                widths=[22, 14, 42, 18, 18, 18, 16, 18, 14, 18, 14, 14, 14, 14, 12, 12, 12, 12, 12, 14],
                freeze_row=8,
                auto_filter_row=8,
            ),
        },
        {
            'name': 'FNR Detail',
            'xml': _xlsx_sheet_xml(
                detail_sheet,
                widths=[14, 22, 18, 42, 18, 18, 18, 14, 18, 11, 11, 11] + [12] * (len(FNR_OUTPUT_COLUMNS) - 10),
                freeze_row=4,
                auto_filter_row=4,
            ),
        },
    ]
    return _build_xlsx_workbook(sheets)


def _tenant_fnr_store_map(tenant) -> dict[str, object] | None:
    """Per-tenant FnR warehouse->store map from feature_flags (None = branch-level)."""
    flags = getattr(tenant, 'feature_flags', None)
    raw = flags.get('fnr_store_warehouses') if isinstance(flags, dict) else None
    return raw if isinstance(raw, dict) and raw else None


async def _build_fnr_context(
    tenant_db: AsyncSession,
    *,
    pharmacies: str | None = None,
    group: str | None = None,
    category_1: str | None = None,
    category_2: str | None = None,
    category_3: str | None = None,
    suppliers: str | None = None,
    search: str | None = None,
    target_stock_weeks: str | None = None,
    overstock_weeks: str | None = None,
    sales_avg_period_1_weeks: str | None = None,
    sales_avg_period_2_weeks: str | None = None,
    limit: int = 5000,
    scope_sold_days: int | None = None,
    store_warehouse_map: dict[str, object] | None = None,
    include_no_order: bool = False,
) -> dict[str, object]:
    return await build_fnr_excel_from_facts(
        tenant_db,
        pharmacies=_fnr_query_list(pharmacies),
        groups=_fnr_query_list(group),
        category_1=_fnr_query_list(category_1),
        category_2=_fnr_query_list(category_2),
        category_3=_fnr_query_list(category_3),
        suppliers=_fnr_query_list(suppliers),
        search=search or '',
        target_stock_weeks=_fnr_float(target_stock_weeks, 4.0),
        overstock_weeks=_fnr_float(overstock_weeks, 12.0),
        sales_avg_period_1_weeks=_fnr_int(sales_avg_period_1_weeks, 4),
        sales_avg_period_2_weeks=_fnr_int(sales_avg_period_2_weeks, 12),
        limit=limit,
        scope_sold_days=scope_sold_days,
        store_warehouse_map=store_warehouse_map,
        include_no_order=include_no_order,
    )


def _fnr_scope_sold_days(tenant: Tenant | None) -> int | None:
    """FnR item universe follows the tenant's active-items window only when the
    'active_available' status source is selected; otherwise keep the legacy scope."""
    iic = _tenant_inventory_item_classification_settings(tenant)
    if iic.get('status_source') != 'active_available':
        return None
    return int(iic.get('inventory_scope_sold_days') or 0) or None


def _worksheet_cache_params(**values: object) -> dict[str, object]:
    return {key: '' if value is None else str(value) for key, value in sorted(values.items())}


async def _enqueue_fnr_expected_orders_sync(tenant: Tenant) -> dict[str, object]:
    stream = 'supplier_orders'
    job = {
        'connector': 'sql_connector',
        'stream': stream,
        'entity': STREAM_TO_ENTITY[stream],
        'tenant_slug': tenant.slug,
        'payload': {
            'limit': 5000,
            'bulk_upsert': True,
            'ignore_sync_state': True,
            'ensure_complete': True,
            'live_priority': True,
            'front_of_queue': True,
            'reason': 'fnr_expected_orders_refresh',
        },
        'attempt': 0,
        'max_retries': settings.ingest_job_max_retries,
        'priority': 'critical',
        'live_priority': True,
        'front_of_queue': True,
    }
    queued = enqueue_tenant_job(tenant.slug, job)
    current_depth = queue_depth(tenant.slug)
    begin_ingest_progress(
        tenant_slug=tenant.slug,
        operation='fnr_expected_orders',
        status='queued',
        total_jobs=max(1, int(queued or 1)),
        start_queue_depth=current_depth,
        target_queue_depth=max(0, current_depth - max(1, int(queued or 1))),
    )
    task_id = ''
    try:
        task = celery_client.send_task('worker.tasks.drain_tenant_ingest_queue', kwargs={'tenant_slug': tenant.slug, 'max_jobs': 3})
        task_id = str(getattr(task, 'id', '') or '')
    except Exception:
        logger.exception('fnr_expected_orders_drain_enqueue_failed', extra={'tenant_id': tenant.id, 'tenant_slug': tenant.slug})
    try:
        await invalidate_tenant_cache(str(tenant.id), namespace_prefix='tenant:worksheet:fnr')
    except Exception:
        logger.exception('fnr_expected_orders_cache_invalidation_failed', extra={'tenant_id': tenant.id, 'tenant_slug': tenant.slug})
    return {'status': 'queued', 'queued': queued, 'task_id': task_id, 'stream': stream}


def _availability_date(value: object, default: date) -> date:
    return _parse_date_or_none(str(value or '')) or default


async def _build_availability_context(
    tenant_db: AsyncSession,
    *,
    pharmacies: str | None = None,
    category_1: str | None = None,
    category_2: str | None = None,
    category_3: str | None = None,
    suppliers: str | None = None,
    group: str | None = None,
    status_abcd: str | None = None,
    commercial_status: str | None = None,
    period_from: str | None = None,
    period_to: str | None = None,
    stock_date_1: str | None = None,
    stock_date_2: str | None = None,
    step: str | None = None,
) -> dict[str, object]:
    today = date.today()
    default_from = date(today.year, max(today.month - 3, 1), 1)
    to_date = _availability_date(period_to, today)
    return await build_availability_brief_from_facts(
        tenant_db,
        pharmacies=_fnr_query_list(pharmacies),
        category_1=_fnr_query_list(category_1),
        category_2=_fnr_query_list(category_2),
        category_3=_fnr_query_list(category_3),
        suppliers=_fnr_query_list(suppliers),
        group=_fnr_query_list(group),
        status_abcd=_fnr_query_list(status_abcd),
        commercial_status=_fnr_query_list(commercial_status),
        period_from=_availability_date(period_from, default_from),
        period_to=to_date,
        stock_date_1=_availability_date(stock_date_1, to_date),
        stock_date_2=_availability_date(stock_date_2, date(to_date.year - 1, 12, 31)),
        step=str(step or 'month').strip().lower(),
    )


def _availability_table_export_row(row: dict[str, object], store_codes: tuple[str, ...] = ('LOGICA', 'AGD', 'KAS', 'ELL', 'SPA', 'PER')) -> list[object]:
    availability = row.get('availability') if isinstance(row.get('availability'), dict) else {}
    return [
        row.get('status_abcd', ''),
        row.get('commercial_status', ''),
        row.get('sku_count', 0),
        row.get('sku_live_online', 0),
        row.get('web_availability', 0),
        row.get('sales_units', 0),
        row.get('sales_value', 0),
        row.get('margin_pct', 0),
        row.get('purchase_units', 0),
        row.get('purchase_value', 0),
        row.get('stock_units_1', 0),
        row.get('stock_value_1', 0),
        row.get('stock_units_2', 0),
        row.get('stock_value_2', 0),
        row.get('dio', ''),
        *[availability.get(code, 0) for code in store_codes],
    ]


async def _build_destocking_context(tenant_db: AsyncSession, **kwargs: object) -> dict[str, object]:
    today = date.today()
    period_to = _availability_date(kwargs.get('period_to'), today)
    default_from = date(period_to.year, max(period_to.month - 3, 1), 1)
    return await build_destocking_brief_from_facts(
        tenant_db,
        pharmacies=_fnr_query_list(kwargs.get('pharmacies')),
        category_1=_fnr_query_list(kwargs.get('category_1')),
        category_2=_fnr_query_list(kwargs.get('category_2')),
        category_3=_fnr_query_list(kwargs.get('category_3')),
        suppliers=_fnr_query_list(kwargs.get('suppliers')),
        group=_fnr_query_list(kwargs.get('group')),
        status_abcd=_fnr_query_list(kwargs.get('status_abcd')),
        commercial_status=_fnr_query_list(kwargs.get('commercial_status')),
        period_from=_availability_date(kwargs.get('period_from'), default_from),
        period_to=period_to,
        stock_date_1=_availability_date(kwargs.get('stock_date_1'), period_to),
        stock_date_2=_availability_date(kwargs.get('stock_date_2'), date(period_to.year - 1, 12, 31)),
        threshold_weeks=_fnr_float(kwargs.get('threshold_weeks'), 8.0),
        step=str(kwargs.get('step') or 'month').strip().lower(),
    )


def _build_destocking_xlsx(destocking: dict[str, object], *, tenant_name: str = '') -> bytes:
    summary = destocking.get('summary') if isinstance(destocking.get('summary'), dict) else {}
    params = destocking.get('parameters') if isinstance(destocking.get('parameters'), dict) else {}
    table_rows = [row for row in (destocking.get('table_rows') or []) if isinstance(row, dict)]
    trends = destocking.get('trends') if isinstance(destocking.get('trends'), dict) else {}
    correlation = destocking.get('correlation') if isinstance(destocking.get('correlation'), dict) else {}
    recs = [row for row in (destocking.get('recommendations') or []) if isinstance(row, dict)]
    periods = trends.get('periods') if isinstance(trends.get('periods'), list) else []
    table_sheet: list[list[tuple[object, int] | object]] = [
        [('BoxVisio Destocking Brief', 1)],
        [(tenant_name or 'Tenant', 3), (f'Period: {params.get("period_from", "-")} - {params.get("period_to", "-")}', 3), (f'Threshold: {params.get("threshold_weeks", "-")} weeks', 3)],
        [],
        [('Total Overstock', 7), (summary.get('total_overstock', 0), 5), ('Stock Value Date 1', 7), (summary.get('stock_value_1', 0), 5), ('Recommendations', 7), (summary.get('recommendations_count', 0), 4)],
        [],
        [(header, 2) for header in ['ABCD Status', 'Εμπορικό Status', 'SKU Count', 'Units on Date 1', 'Value (€) on Date 1', 'Units on Date 2', 'Value (€) on Date 2', 'DIO on Date 1 vs Period 1']],
    ]
    for row in table_rows:
        table_sheet.append([row.get('status_abcd', ''), row.get('commercial_status', ''), (row.get('sku_count', 0), 4), (row.get('stock_units_1', 0), 4), (row.get('stock_value_1', 0), 5), (row.get('stock_units_2', 0), 4), (row.get('stock_value_2', 0), 5), row.get('dio', '')])
    trend_sheet: list[list[tuple[object, int] | object]] = [[('Trends', 1)], [('Line chart source data: stock / overstock ανά location.', 3)], []]
    for series in trends.get('series') or []:
        if isinstance(series, dict):
            trend_sheet.append([(series.get('name', ''), 7)])
            trend_sheet.append([('', 2), *[(period, 2) for period in periods]])
            for line in series.get('lines') or []:
                if isinstance(line, dict):
                    trend_sheet.append([line.get('name', ''), *[(value, 5) for value in (line.get('values') or [])]])
            trend_sheet.append([])
    corr_sheet: list[list[tuple[object, int] | object]] = [[('Correlation', 1)], [('Dual line source data: margin % σε σχέση με overstock.', 3)], []]
    for series in correlation.get('series') or []:
        if isinstance(series, dict):
            corr_sheet.append([(series.get('name', ''), 7)])
            corr_sheet.append([('', 2), *[(period, 2) for period in (correlation.get('periods') or [])]])
            corr_sheet.append(['overstock', *[(value, 5) for value in (series.get('overstock') or [])]])
            corr_sheet.append(['D3', *[(value, 5) for value in (series.get('d3_overstock') or [])]])
            corr_sheet.append(['margin', *[(value, 4) for value in (series.get('margin') or [])]])
            corr_sheet.append([])
    rec_sheet = [[('Recommendations for Destocking', 1)], [], [('', 2), ('Action', 2), ('Status ABCD', 2), ('Status Εμπορικό', 2), ('Vendor', 2), ('Location', 2), ('Cur Overstock', 2), ('Target Overstock', 2), ('Destocking potential', 2), ('Show SKU', 2)]]
    for idx, row in enumerate(recs, start=1):
        rec_sheet.append([idx, row.get('action', ''), row.get('status_abcd', ''), row.get('commercial_status', ''), row.get('vendor', ''), row.get('location', ''), (row.get('cur_overstock', 0), 5), (row.get('target_overstock', 0), 5), (row.get('destocking_potential', 0), 5), row.get('show_sku', '')])
    return _build_xlsx_workbook([
        {'name': 'Table', 'xml': _xlsx_sheet_xml(table_sheet, widths=[16, 22, 12, 16, 18, 16, 18, 18], freeze_row=6, auto_filter_row=6)},
        {'name': 'Trends', 'xml': _xlsx_sheet_xml(trend_sheet, widths=[24] + [12] * max(len(periods), 1), freeze_row=4)},
        {'name': 'Correlation', 'xml': _xlsx_sheet_xml(corr_sheet, widths=[24] + [12] * max(len(periods), 1), freeze_row=4)},
        {'name': 'Recommendations', 'xml': _xlsx_sheet_xml(rec_sheet, widths=[8, 24, 14, 22, 28, 18, 16, 16, 20, 48], freeze_row=3, auto_filter_row=3)},
    ])


def _build_availability_xlsx(availability: dict[str, object], *, tenant_name: str = '') -> bytes:
    summary = availability.get('summary') if isinstance(availability.get('summary'), dict) else {}
    parameters = availability.get('parameters') if isinstance(availability.get('parameters'), dict) else {}
    table_rows = [row for row in (availability.get('table_rows') or []) if isinstance(row, dict)]
    recommendations = [row for row in (availability.get('recommendations') or []) if isinstance(row, dict)]
    trends = availability.get('trends') if isinstance(availability.get('trends'), dict) else {}
    correlation = availability.get('correlation') if isinstance(availability.get('correlation'), dict) else {}
    store_codes = ('LOGICA', 'AGD', 'KAS', 'ELL', 'SPA', 'PER')
    headers = [
        'ABCD Status', 'Εμπορικό Status', 'SKU Count', 'SKU Live Online', 'Web Availability',
        'Sales (units) over Period 1', 'Sales (value) over Period 1', 'Margin % over Period 1',
        'Purchases (units) in Period 1', 'Purchases (value) in Period 1',
        'Units on Date 1', 'Value (€) on Date 1', 'Units on Date 2', 'Value (€) on Date 2',
        'DIO on Date 1 vs Period 1', 'Availability Logica', 'Availability Αγ. Δημ.',
        'Availability Κασσαβέτη', 'Availability Ελληνικού', 'Availability Σπατών', 'Availability Περιστερίου',
    ]
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    table_sheet: list[list[tuple[object, int] | object]] = [
        [('BoxVisio Availability Brief', 1)],
        [(tenant_name or 'Tenant', 3), (f'Period: {parameters.get("period_from", "-")} - {parameters.get("period_to", "-")}', 3), (f'Generated: {generated_at}', 3)],
        [(f'Stock Date 1: {parameters.get("stock_date_1", "-")}', 3), (f'Stock Date 2: {parameters.get("stock_date_2", "-")}', 3), (f'Step: {parameters.get("step", "-")}', 3)],
        [],
        [('SKU Count', 7), (summary.get('sku_count', 0), 4), ('SKU Live Online', 7), (summary.get('sku_live_online', 0), 4), ('Web Availability', 7), (summary.get('web_availability', 0), 4), ('Sales Value', 7), (summary.get('sales_value', 0), 5)],
        [],
        [(header, 2) for header in headers],
    ]
    for row in table_rows:
        table_sheet.append([(value, 4 if isinstance(value, (int, float)) else 0) for value in _availability_table_export_row(row, store_codes)])
    periods = trends.get('periods') if isinstance(trends.get('periods'), list) else []
    trend_sheet: list[list[tuple[object, int] | object]] = [
        [('Trends', 1)],
        [('Line chart source data: availability % ανά φαρμακείο, Logica και συνολικά.', 3)],
        [],
        [('', 2), *[(period, 2) for period in periods]],
    ]
    for series in trends.get('series') or []:
        if isinstance(series, dict):
            trend_sheet.append([series.get('name', ''), *[(value, 4) for value in (series.get('values') or [])]])
    corr_sheet: list[list[tuple[object, int] | object]] = [
        [('Correlation', 1)],
        [('Dual line chart source data: availability % σε σχέση με μεταβολή πωλήσεων από πέρσι.', 3)],
        [],
    ]
    corr_periods = correlation.get('periods') if isinstance(correlation.get('periods'), list) else []
    for series in correlation.get('series') or []:
        if isinstance(series, dict):
            corr_sheet.append([(series.get('name', ''), 7)])
            corr_sheet.append([('', 2), *[(period, 2) for period in corr_periods]])
            corr_sheet.append(['availability', *[(value, 4) for value in (series.get('availability') or [])]])
            corr_sheet.append(['sales vs PY', *[(value, 4) for value in (series.get('sales_vs_py') or [])]])
            corr_sheet.append([])
    rec_sheet: list[list[tuple[object, int] | object]] = [
        [('Recommendations for Sales Growth', 1)],
        [],
        [('', 2), ('Action', 2), ('Status ABCD', 2), ('Status Εμπορικό', 2), ('Vendor', 2), ('Location', 2), ('Cur Availability', 2), ('Target Availability', 2), ('Monthly revenue potential', 2)],
    ]
    for idx, row in enumerate(recommendations, start=1):
        rec_sheet.append([
            idx,
            row.get('action', ''),
            row.get('status_abcd', ''),
            row.get('commercial_status', ''),
            row.get('vendor', ''),
            row.get('location', ''),
            (row.get('cur_availability', 0), 4),
            (row.get('target_availability', 0), 4),
            (row.get('monthly_revenue_potential', 0), 5),
        ])
    return _build_xlsx_workbook([
        {'name': 'Table', 'xml': _xlsx_sheet_xml(table_sheet, widths=[16, 22, 12, 16, 16, 18, 18, 14, 18, 18, 14, 16, 14, 16, 18, 16, 16, 18, 18, 16, 18], freeze_row=7, auto_filter_row=7)},
        {'name': 'Trends', 'xml': _xlsx_sheet_xml(trend_sheet, widths=[24] + [12] * max(len(periods), 1), freeze_row=4, auto_filter_row=4)},
        {'name': 'Correlation', 'xml': _xlsx_sheet_xml(corr_sheet, widths=[24] + [12] * max(len(periods), 1), freeze_row=4, auto_filter_row=4)},
        {'name': 'Recommendations', 'xml': _xlsx_sheet_xml(rec_sheet, widths=[8, 24, 14, 22, 28, 18, 16, 16, 22], freeze_row=3, auto_filter_row=3)},
    ])


@router.get('/tenant/destocking', response_class=HTMLResponse)
async def tenant_destocking_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    feature_locked = not feature_flags['replenishment_enabled']
    destocking = {'summary': {}, 'table_rows': [], 'recommendations': [], 'options': {}, 'parameters': {}}
    if not feature_locked:
        try:
            destocking, _cache_hit = await get_or_set_cache(
                namespace='tenant:worksheet:destocking',
                tenant_key=str(tenant.id),
                params=_worksheet_cache_params(version='destocking-d3-period-v3', **dict(request.query_params)),
                ttl_seconds=300,
                producer=lambda: _build_destocking_context(tenant_db, **dict(request.query_params)),
            )
        except Exception:
            logger.exception('tenant_destocking_load_failed', extra={'tenant_id': tenant.id})
            await tenant_db.rollback()
    query_values = {key: request.query_params.get(key, '') for key in ['pharmacies', 'category_1', 'category_2', 'category_3', 'suppliers', 'group', 'status_abcd', 'commercial_status', 'period_from', 'period_to', 'stock_date_1', 'stock_date_2', 'step']}
    query_values['threshold_weeks'] = request.query_params.get('threshold_weeks', '8')
    return templates.TemplateResponse(
        'tenant/destocking_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': feature_locked,
            'active_page': 'destocking',
            'title': 'Destocking',
            'destocking': destocking,
            'hide_page_filters': True,
            'query_values': query_values,
        },
    )


@router.get('/tenant/destocking/export')
async def tenant_destocking_export(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    if not feature_flags.get('replenishment_enabled', False):
        return Response('Destocking feature is locked', status_code=403, media_type='text/plain')
    destocking, _cache_hit = await get_or_set_cache(
        namespace='tenant:worksheet:destocking',
        tenant_key=str(tenant.id),
        params=_worksheet_cache_params(version='destocking-d3-period-v3', **dict(request.query_params)),
        ttl_seconds=300,
        producer=lambda: _build_destocking_context(tenant_db, **dict(request.query_params)),
    )
    content = _build_destocking_xlsx(destocking, tenant_name=str(tenant.name or tenant.slug or 'Tenant'))
    filename = f"Destocking_brief_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    return Response(content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'})


@router.get('/tenant/availability', response_class=HTMLResponse)
async def tenant_availability_dashboard(
    request: Request,
    pharmacies: str | None = Query(default=''),
    category_1: str | None = Query(default=''),
    category_2: str | None = Query(default=''),
    category_3: str | None = Query(default=''),
    suppliers: str | None = Query(default=''),
    group: str | None = Query(default=''),
    status_abcd: str | None = Query(default=''),
    commercial_status: str | None = Query(default=''),
    period_from: str | None = Query(default=''),
    period_to: str | None = Query(default=''),
    stock_date_1: str | None = Query(default=''),
    stock_date_2: str | None = Query(default=''),
    step: str | None = Query(default='month'),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    feature_locked = not feature_flags['replenishment_enabled']
    availability = {'summary': {}, 'table_rows': [], 'recommendations': [], 'options': {}, 'parameters': {}}
    if not feature_locked:
        try:
            availability, _cache_hit = await get_or_set_cache(
                namespace='tenant:worksheet:availability',
                tenant_key=str(tenant.id),
                params=_worksheet_cache_params(
                    version='availability-worksheet-v3',
                    pharmacies=pharmacies,
                    category_1=category_1,
                    category_2=category_2,
                    category_3=category_3,
                    suppliers=suppliers,
                    group=group,
                    status_abcd=status_abcd,
                    commercial_status=commercial_status,
                    period_from=period_from,
                    period_to=period_to,
                    stock_date_1=stock_date_1,
                    stock_date_2=stock_date_2,
                    step=step,
                ),
                ttl_seconds=300,
                producer=lambda: _build_availability_context(
                    tenant_db,
                    pharmacies=pharmacies,
                    category_1=category_1,
                    category_2=category_2,
                    category_3=category_3,
                    suppliers=suppliers,
                    group=group,
                    status_abcd=status_abcd,
                    commercial_status=commercial_status,
                    period_from=period_from,
                    period_to=period_to,
                    stock_date_1=stock_date_1,
                    stock_date_2=stock_date_2,
                    step=step,
                ),
            )
        except Exception:
            logger.exception('tenant_availability_load_failed', extra={'tenant_id': tenant.id})
            await tenant_db.rollback()
    return templates.TemplateResponse(
        'tenant/availability_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': feature_locked,
            'active_page': 'availability',
            'title': 'Availability',
            'availability': availability,
            'hide_page_filters': True,
            'query_values': {
                'pharmacies': pharmacies or '',
                'category_1': category_1 or '',
                'category_2': category_2 or '',
                'category_3': category_3 or '',
                'suppliers': suppliers or '',
                'group': group or '',
                'status_abcd': status_abcd or '',
                'commercial_status': commercial_status or '',
                'period_from': period_from or '',
                'period_to': period_to or '',
                'stock_date_1': stock_date_1 or '',
                'stock_date_2': stock_date_2 or '',
                'step': step or 'month',
            },
        },
    )


@router.get('/tenant/availability/export')
async def tenant_availability_export(
    request: Request,
    pharmacies: str | None = Query(default=''),
    category_1: str | None = Query(default=''),
    category_2: str | None = Query(default=''),
    category_3: str | None = Query(default=''),
    suppliers: str | None = Query(default=''),
    group: str | None = Query(default=''),
    status_abcd: str | None = Query(default=''),
    commercial_status: str | None = Query(default=''),
    period_from: str | None = Query(default=''),
    period_to: str | None = Query(default=''),
    stock_date_1: str | None = Query(default=''),
    stock_date_2: str | None = Query(default=''),
    step: str | None = Query(default='month'),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    if not feature_flags.get('replenishment_enabled', False):
        return Response('Availability feature is locked', status_code=403, media_type='text/plain')
    availability, _cache_hit = await get_or_set_cache(
        namespace='tenant:worksheet:availability',
        tenant_key=str(tenant.id),
        params=_worksheet_cache_params(
            version='availability-worksheet-v3',
            pharmacies=pharmacies,
            category_1=category_1,
            category_2=category_2,
            category_3=category_3,
            suppliers=suppliers,
            group=group,
            status_abcd=status_abcd,
            commercial_status=commercial_status,
            period_from=period_from,
            period_to=period_to,
            stock_date_1=stock_date_1,
            stock_date_2=stock_date_2,
            step=step,
        ),
        ttl_seconds=300,
        producer=lambda: _build_availability_context(
            tenant_db,
            pharmacies=pharmacies,
            category_1=category_1,
            category_2=category_2,
            category_3=category_3,
            suppliers=suppliers,
            group=group,
            status_abcd=status_abcd,
            commercial_status=commercial_status,
            period_from=period_from,
            period_to=period_to,
            stock_date_1=stock_date_1,
            stock_date_2=stock_date_2,
            step=step,
        ),
    )
    content = _build_availability_xlsx(availability, tenant_name=str(tenant.name or tenant.slug or 'Tenant'))
    filename = f"Availability_brief_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    return Response(
        content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'},
    )


@router.get('/tenant/fnr', response_class=HTMLResponse)
async def tenant_fnr_dashboard(
    request: Request,
    pharmacies: str | None = Query(default=''),
    group: str | None = Query(default=''),
    category_1: str | None = Query(default=''),
    category_2: str | None = Query(default=''),
    category_3: str | None = Query(default=''),
    suppliers: str | None = Query(default=''),
    search: str | None = Query(default=''),
    target_stock_weeks: str | None = Query(default='4'),
    overstock_weeks: str | None = Query(default='12'),
    sales_avg_period_1_weeks: str | None = Query(default='4'),
    sales_avg_period_2_weeks: str | None = Query(default='12'),
    include_no_order: str | None = Query(default=''),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    feature_locked = not feature_flags['replenishment_enabled']
    fnr_include_no_order = str(include_no_order or '').strip().lower() in {'1', 'true', 'on', 'yes'}
    fnr = {'summary': {}, 'rows': [], 'order_rows': [], 'options': {}, 'parameters': {}}
    if not feature_locked:
        try:
            fnr, _cache_hit = await get_or_set_cache(
                namespace='tenant:worksheet:fnr',
                tenant_key=str(tenant.id),
                params=_worksheet_cache_params(
                    version='fnr-worksheet-v11',
                    pharmacies=pharmacies,
                    group=group,
                    category_1=category_1,
                    category_2=category_2,
                    category_3=category_3,
                    suppliers=suppliers,
                    search=search,
                    target_stock_weeks=target_stock_weeks,
                    overstock_weeks=overstock_weeks,
                    sales_avg_period_1_weeks=sales_avg_period_1_weeks,
                    sales_avg_period_2_weeks=sales_avg_period_2_weeks,
                    include_no_order=fnr_include_no_order,
                    scope_sold_days=_fnr_scope_sold_days(tenant) or 0,
                    limit=5000,
                ),
                ttl_seconds=300,
                producer=lambda: _build_fnr_context(
                    tenant_db,
                    pharmacies=pharmacies,
                    group=group,
                    category_1=category_1,
                    category_2=category_2,
                    category_3=category_3,
                    suppliers=suppliers,
                    search=search,
                    target_stock_weeks=target_stock_weeks,
                    overstock_weeks=overstock_weeks,
                    sales_avg_period_1_weeks=sales_avg_period_1_weeks,
                    sales_avg_period_2_weeks=sales_avg_period_2_weeks,
                    limit=5000,
                    scope_sold_days=_fnr_scope_sold_days(tenant),
                    store_warehouse_map=_tenant_fnr_store_map(tenant),
                    include_no_order=fnr_include_no_order,
                ),
            )
        except Exception:
            logger.exception('tenant_fnr_load_failed', extra={'tenant_id': tenant.id})
            await tenant_db.rollback()
    fnr_query_pairs = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key not in {'view', 'expected_sync'}
    ]
    fnr_query_no_view = urlencode(fnr_query_pairs)
    return templates.TemplateResponse(
        'tenant/fnr_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': feature_locked,
            'active_page': 'replenishment',
            'manual_help_anchor': 'fnr',
            'title': 'FNR',
            'fnr': fnr,
            'columns': FNR_OUTPUT_COLUMNS,
            'hide_page_filters': True,
            'fnr_query_no_view': fnr_query_no_view,
            'query_values': {
                'pharmacies': pharmacies or '',
                'group': group or '',
                'category_1': category_1 or '',
                'category_2': category_2 or '',
                'category_3': category_3 or '',
                'suppliers': suppliers or '',
                'search': search or '',
                'target_stock_weeks': target_stock_weeks or '4',
                'overstock_weeks': overstock_weeks or '12',
                'sales_avg_period_1_weeks': sales_avg_period_1_weeks or '4',
                'sales_avg_period_2_weeks': sales_avg_period_2_weeks or '12',
                'include_no_order': '1' if fnr_include_no_order else '',
            },
        },
    )


@router.post('/tenant/fnr/sync-expected')
async def tenant_fnr_sync_expected_orders(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    await _enqueue_fnr_expected_orders_sync(tenant)
    query = dict(request.query_params)
    query['expected_sync'] = 'queued'
    redirect_url = '/tenant/fnr'
    if query:
        redirect_url = f'{redirect_url}?{urlencode(query)}'
    return RedirectResponse(url=redirect_url, status_code=303)


@router.post('/tenant/fnr/refresh-stock')
async def tenant_fnr_refresh_stock(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    """Pull fresh SoftOne net stock into today's snapshot, then bust the worksheet caches so
    the FnR is generated against current inventory (not a stale balance)."""
    from app.services.inventory_snapshot import refresh_inventory_snapshot

    result: dict = {'status': 'error'}
    try:
        async with ControlSessionLocal() as control_db:
            result = await refresh_inventory_snapshot(control_db, tenant_db, tenant_id=int(tenant.id))
        if result.get('status') == 'ok':
            # FnR reads fact_inventory directly (already fresh); rebuild the inventory aggregates
            # on the worker so the agg-based circuits (Αξία/Availability/Destocking/Είδη) follow.
            today_str = date.today().isoformat()
            celery_client.send_task(
                'worker.tasks.refresh_aggregates_for_entity',
                kwargs={'tenant_slug': tenant.slug, 'entity': 'inventory', 'from_date_str': today_str, 'to_date_str': today_str},
                queue='ingest',
            )
        await invalidate_tenant_cache(str(tenant.id))
    except Exception:
        logger.exception('fnr_refresh_stock_failed', extra={'tenant_id': tenant.id})

    query = dict(request.query_params)
    query['stock_refreshed'] = '1' if result.get('status') == 'ok' else 'err'
    if result.get('items'):
        query['stock_items'] = str(result.get('items'))
    redirect_url = '/tenant/fnr'
    if query:
        redirect_url = f'{redirect_url}?{urlencode(query)}'
    return RedirectResponse(url=redirect_url, status_code=303)


@router.get('/tenant/fnr/sync-expected/progress')
async def tenant_fnr_sync_expected_orders_progress(
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    payload = get_ingest_progress(tenant.slug)
    status = str(payload.get('status') or 'idle')
    queue_left = int(payload.get('current_queue_depth') or 0)
    pct = float(payload.get('progress_pct') or 0)
    if str(payload.get('operation') or '') != 'fnr_expected_orders':
        status = 'idle'
        pct = 0.0
        queue_left = 0
    return JSONResponse(
        {
            'status': status,
            'progress_pct': pct,
            'queue_left': queue_left,
            'current_stream': payload.get('current_stream'),
            'current_entity': payload.get('current_entity'),
            'updated_at': payload.get('updated_at'),
            'last_error': payload.get('last_error'),
        }
    )


@router.get('/tenant/fnr/export')
async def tenant_fnr_export(
    request: Request,
    pharmacies: str | None = Query(default=''),
    group: str | None = Query(default=''),
    category_1: str | None = Query(default=''),
    category_2: str | None = Query(default=''),
    category_3: str | None = Query(default=''),
    suppliers: str | None = Query(default=''),
    search: str | None = Query(default=''),
    target_stock_weeks: str | None = Query(default='4'),
    overstock_weeks: str | None = Query(default='12'),
    sales_avg_period_1_weeks: str | None = Query(default='4'),
    sales_avg_period_2_weeks: str | None = Query(default='12'),
    include_no_order: str | None = Query(default=''),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    if not feature_flags.get('replenishment_enabled', False):
        return Response('FNR feature is locked', status_code=403, media_type='text/plain')
    fnr_include_no_order = str(include_no_order or '').strip().lower() in {'1', 'true', 'on', 'yes'}
    fnr, _cache_hit = await get_or_set_cache(
        namespace='tenant:worksheet:fnr',
        tenant_key=str(tenant.id),
        params=_worksheet_cache_params(
            version='fnr-worksheet-v11',
            pharmacies=pharmacies,
            group=group,
            category_1=category_1,
            category_2=category_2,
            category_3=category_3,
            suppliers=suppliers,
            search=search,
            target_stock_weeks=target_stock_weeks,
            overstock_weeks=overstock_weeks,
            sales_avg_period_1_weeks=sales_avg_period_1_weeks,
            sales_avg_period_2_weeks=sales_avg_period_2_weeks,
            include_no_order=fnr_include_no_order,
            scope_sold_days=_fnr_scope_sold_days(tenant) or 0,
            limit=20000,
        ),
        ttl_seconds=300,
        producer=lambda: _build_fnr_context(
            tenant_db,
            pharmacies=pharmacies,
            group=group,
            category_1=category_1,
            category_2=category_2,
            category_3=category_3,
            suppliers=suppliers,
            search=search,
            target_stock_weeks=target_stock_weeks,
            overstock_weeks=overstock_weeks,
            sales_avg_period_1_weeks=sales_avg_period_1_weeks,
            sales_avg_period_2_weeks=sales_avg_period_2_weeks,
            limit=20000,
            scope_sold_days=_fnr_scope_sold_days(tenant),
            store_warehouse_map=_tenant_fnr_store_map(tenant),
            include_no_order=fnr_include_no_order,
        ),
    )
    content = _build_fnr_order_xlsx(fnr, tenant_name=str(tenant.name or tenant.slug or 'Tenant'), include_no_order=fnr_include_no_order)
    filename = f"FNR_order_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    return Response(
        content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'},
    )


@router.get('/v1/kpi/replenishment/availability-drilldown')
async def replenishment_availability_drilldown(
    request: Request,
    dimension: str = Query(default='store'),
    value: str = Query(default=''),
    kind: str = Query(default='all'),
    branch: str | None = Query(default=None),
    status: str | None = Query(default=None),
    category: str | None = Query(default=None),
    vendor: str | None = Query(default=None),
    abc: str | None = Query(default=None),
    search: str | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    if not feature_flags.get('replenishment_enabled', False):
        return JSONResponse(
            status_code=403,
            content={'detail': 'Το Replenishment / Availability δεν είναι διαθέσιμο στη συνδρομή σας.'},
        )
    allowed_dimensions = {'store', 'status', 'category', 'vendor'}
    allowed_kinds = {'all', 'shortage', 'stockout', 'overstock'}
    safe_dimension = dimension if dimension in allowed_dimensions else 'store'
    safe_kind = kind if kind in allowed_kinds else 'all'
    _drill_iic = _tenant_inventory_item_classification_settings(tenant)
    _drill_scope_sold_days = (
        int(_drill_iic.get('inventory_scope_sold_days') or 0)
        if _drill_iic.get('status_source') == 'active_available'
        else None
    )
    data, cache_hit = await get_or_set_cache(
        namespace='dashboard:replenishment_availability_drilldown',
        tenant_key=str(tenant.id),
        params={
            'version': 'availability-searchable-abc-filters-v2',
            'branch': branch or '',
            'status': status or '',
            'category': category or '',
            'vendor': vendor or '',
            'abc': abc or '',
            'search': search or '',
            'dimension': safe_dimension,
            'value': value or '',
            'kind': safe_kind,
            'scope_sold_days': _drill_scope_sold_days or 0,
        },
        ttl_seconds=1800,
        producer=lambda: build_availability_foundation(
            tenant_db,
            branch=branch,
            status=status,
            category=category,
            vendor=vendor,
            abc=abc,
            search=search,
            scope_sold_days=_drill_scope_sold_days,
            include_detail_rows=True,
            detail_kind=safe_kind,
            detail_dimension=safe_dimension,
            detail_value=value,
            detail_limit=150,
        ),
    )
    response = JSONResponse(
        {
            'summary': data.get('summary') or {},
            'dimension': safe_dimension,
            'value': value,
            'kind': safe_kind,
            'rows': data.get('detail_rows') or [],
        }
    )
    response.headers['X-KPI-Cache'] = 'HIT' if cache_hit else 'MISS'
    return response


@router.get('/tenant/supplier-orders', response_class=HTMLResponse)
async def tenant_supplier_orders_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    control_db: AsyncSession = Depends(get_control_db),
    _user=Depends(get_current_user),
):
    feature_flags = await _tenant_navigation_context(tenant)
    settings_payload = await supplier_order_settings_for_tenant(tenant)
    feature_locked = not feature_flags.get('supplier_orders_enabled', False)
    today = date.today()
    lookback_days = int(settings_payload.get('lookback_days') or 30)

    def _parse_filter_date(raw: str | None, fallback: date) -> date:
        text_value = str(raw or '').strip()
        if not text_value:
            return fallback
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(text_value, fmt).date()
            except ValueError:
                continue
        return fallback

    default_from = today - timedelta(days=max(1, lookback_days))
    from_date = _parse_filter_date(request.query_params.get('from'), default_from)
    to_date = _parse_filter_date(request.query_params.get('to'), today)
    if from_date > to_date:
        from_date, to_date = to_date, from_date
    only_open = str(request.query_params.get('only_open', '1')).strip().lower() not in {'0', 'false', 'no'}
    supplier = str(request.query_params.get('supplier') or '').strip()
    filters = SupplierOrdersFilters(
        from_date=from_date,
        to_date=to_date,
        supplier=supplier,
        only_open=only_open,
        limit=500,
    )
    if feature_locked:
        dashboard = {
            'filters': filters,
            'summary': {'documents': 0, 'open_documents': 0, 'closed_documents': 0, 'lines': 0, 'open_qty': 0.0, 'open_value': 0.0, 'suppliers': 0},
            'supplier_rows': [],
            'document_rows': [],
            'line_rows': [],
            'error': None,
        }
    else:
        dashboard = await build_supplier_orders_dashboard(control_db, tenant, filters)
    return templates.TemplateResponse(
        'tenant/supplier_orders_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **feature_flags,
            'feature_locked': feature_locked,
            'active_page': 'supplier_orders',
            'title': 'Παραγγελίες Προμηθευτών',
            'dashboard': dashboard,
            'supplier_order_settings': settings_payload,
            'hide_page_filters': True,
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


# --- Help section ----------------------------------------------------------
# The manual used to be a single long page at /tenant/manual reachable only from
# a small button in each page header. It is now its own sidebar section with a
# task-oriented entry point, because people arrive with a question ("who owes me
# money?") rather than with a page name.

_HELP_SHOT_DIR = Path(__file__).resolve().parents[1] / 'static' / 'docs' / 'manual'


def _help_shot_exists(shot: str | None) -> bool:
    """Screenshots are generated out of band, so a page must render fine when
    one is missing rather than showing a broken image."""
    if not shot:
        return False
    try:
        return (_HELP_SHOT_DIR / f'{shot}.jpg').is_file()
    except OSError:
        return False


def _help_shot_or_none(shot: str) -> str | None:
    return help_shot_url(shot) if _help_shot_exists(shot) else None


def _help_lang(request: Request) -> str:
    return 'en' if str(request.cookies.get('lang', 'el')).lower().startswith('en') else 'el'


#  Page titles are the only Help strings not held in help_content, because the
#  shell renders them before the content module is consulted.
_HELP_TITLES = {
    'help_home': ('Βοήθεια', 'Help'),
    'help_find': ('Πώς θα βρω…', 'How do I find…'),
    'help_circuits': ('Οι σελίδες βήμα-βήμα', 'Every screen, step by step'),
    'help_kpis': ('Λεξικό KPI', 'KPI dictionary'),
    'help_faq': ('Συχνές απορίες', 'Common questions'),
}


async def _help_context(
    request: Request,
    tenant: Tenant,
    active_page: str,
    title: str | None = None,
) -> dict[str, Any]:
    lang = _help_lang(request)
    titles = _HELP_TITLES.get(active_page)
    resolved_title = title or (titles[1] if lang == 'en' and titles else (titles[0] if titles else 'Help'))
    return {
        'request': request,
        'tenant': tenant,
        **(await _tenant_navigation_context(tenant)),
        'hide_page_filters': True,
        'active_page': active_page,
        'title': resolved_title,
        'lang': lang,
        'circuits': help_circuits(lang),
        'circuits_by_id': help_circuits_by_id(lang),
        'circuit_groups': help_circuit_groups(lang),
        'task_groups': help_task_groups(lang),
        'faq': help_faq(lang),
        'kpi_count': len(kpi_catalog_for_lang(lang)),
        'kpis_for_circuit': help_kpis_for_circuit,
        'kpis_own': lambda circuit_id, lg='el': kpi_catalog_by_circuit(lg).get(circuit_id, []),
        'shot_url': help_shot_url,
        'shot_exists': _help_shot_exists,
        'shots': {
            'ui_tour': _help_shot_or_none('ui-tour-full'),
            'kpi_help': _help_shot_or_none('ui-kpi-help'),
            'date_modal': _help_shot_or_none('ui-date-modal'),
            'filters': _help_shot_or_none('ui-filters'),
            'sidebar': _help_shot_or_none('ui-sidebar'),
        },
    }


@router.get('/tenant/help', response_class=HTMLResponse)
async def tenant_help_home(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/help/index.html',
        await _help_context(request, tenant, 'help_home'),
    )


@router.get('/tenant/help/find', response_class=HTMLResponse)
async def tenant_help_find(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/help/find.html',
        await _help_context(request, tenant, 'help_find'),
    )


@router.get('/tenant/help/circuits', response_class=HTMLResponse)
async def tenant_help_circuits(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/help/circuits.html',
        await _help_context(request, tenant, 'help_circuits'),
    )


@router.get('/tenant/help/kpis', response_class=HTMLResponse)
async def tenant_help_kpis(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/help/kpis.html',
        await _help_context(request, tenant, 'help_kpis'),
    )


@router.get('/tenant/help/faq', response_class=HTMLResponse)
async def tenant_help_faq(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/help/faq.html',
        await _help_context(request, tenant, 'help_faq'),
    )


@router.get('/tenant/help/circuits/{circuit_id}', response_class=HTMLResponse)
async def tenant_help_circuit(
    circuit_id: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    """Help for one circuit on its own page — linkable, printable, shareable.

    The combined page stays available; this is what the in-page help button and
    every "open the full guide" link point at.
    """
    lang = _help_lang(request)
    circuit = help_circuit_for_lang(circuit_id, lang)
    if circuit is None:
        return RedirectResponse(url='/tenant/help/circuits', status_code=302)
    context = await _help_context(request, tenant, 'help_circuits', title=circuit['title'])
    context['circuit'] = circuit
    context['circuit_kpis'] = help_kpis_for_circuit(circuit_id, lang)
    return templates.TemplateResponse('tenant/help/circuit.html', context)


@router.get('/tenant/help/circuits/{circuit_id}/panel', response_class=HTMLResponse)
async def tenant_help_circuit_panel(
    circuit_id: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    """The same circuit help as a bare fragment, for the in-page slide-over.

    Contextual help must not cost the user their filters, so the panel is loaded
    into the current page instead of navigating away from it.
    """
    lang = _help_lang(request)
    circuit = help_circuit_for_lang(circuit_id, lang)
    if circuit is None:
        return HTMLResponse('', status_code=404)
    return templates.TemplateResponse(
        'tenant/help/_circuit_panel.html',
        {
            'request': request,
            'tenant': tenant,
            'lang': lang,
            'circuit': circuit,
            'circuit_kpis': help_kpis_for_circuit(circuit_id, lang),
            'circuits_by_id': help_circuits_by_id(lang),
            'shot_url': help_shot_url,
            'shot_exists': _help_shot_exists,
        },
    )


@router.get('/tenant/help/kpi-catalog.json')
async def tenant_help_kpi_catalog(
    request: Request,
    _tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    """Feeds the KPI help popup. Served separately (and cached) rather than
    inlined into every page, since the catalog is ~50 KB of static text."""
    lang = 'en' if str(request.cookies.get('lang', 'el')).lower().startswith('en') else 'el'
    return JSONResponse(
        {'lang': lang, 'entries': kpi_catalog_for_lang(lang), 'fallback': kpi_default_help(lang)},
        headers={'Cache-Control': 'private, max-age=3600'},
    )


@router.get('/tenant/manual')
async def tenant_user_manual(request: Request):
    """Kept so existing per-page help links (and bookmarks) still resolve.

    The old anchors are circuit ids, which the new circuits page reuses verbatim,
    so /tenant/manual#price-control lands exactly where it used to.
    """
    return RedirectResponse(url='/tenant/help/circuits', status_code=301)


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


@router.get('/tenant/era-exploration-data', response_class=HTMLResponse)
async def tenant_era_exploration_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/era_exploration_data.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'active_page': 'era_exploration_data',
            'title': 'eRA Exploration Data',
        },
    )


@router.get('/tenant/iqvia', response_class=HTMLResponse)
async def tenant_iqvia_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/iqvia.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'active_page': 'iqvia',
            'title': 'IQVIA Market Data',
        },
    )


@router.get('/tenant/business-advisor', response_class=HTMLResponse)
async def tenant_business_advisor_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    return templates.TemplateResponse(
        'tenant/business_advisor.html',
        {
            'request': request,
            'tenant': tenant,
            **(await _tenant_navigation_context(tenant)),
            'default_from': from_date,
            'default_to': to_date,
            'default_target_margin': 35.0,
            'active_page': 'business_advisor',
            'title': 'Σύμβουλος Επιχείρησης',
            'manual_help_anchor': 'business-advisor',
        },
    )


async def _render_tenant_cashflow_dashboard(
    request: Request,
    tenant: Tenant,
    raw_category: str | None,
):
    feature_flags = await _tenant_navigation_context(tenant)
    to_date = date.today()
    normalized_category = _normalize_cashflow_category(raw_category)
    is_known_category = normalized_category in _CASHFLOW_CATEGORY_LABEL_KEY_MAP
    is_accounts_mode = normalized_category == 'financial_accounts'
    is_documents_mode = is_known_category and not is_accounts_mode
    from_date = date(to_date.year, 1, 1) if is_documents_mode else (to_date - timedelta(days=30))
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
        queue='default',
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
async def tenant_compare_period_redirect(
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
    return RedirectResponse(url='/tenant/finance-dashboard', status_code=302)


# Shared filter definition for the Εξαγωγές circuit (Αναφορές + CSV/Excel). Keep
# both pages driven by this one list so they stay in parity by construction.
_EXPORT_FILTER_FIELDS = [
    ('branches', 'Υποκατάστημα', 'Όλα τα υποκαταστήματα'),
    ('warehouses', 'Αποθηκευτικός χώρος', 'Όλοι οι αποθηκευτικοί χώροι'),
    ('brands', 'Brands', 'Όλα τα brands'),
    ('group', 'Ομάδα Ειδών', 'Όλες οι ομάδες'),
    ('category_1', 'Κατηγορία 1', 'Όλες οι κατηγορίες 1'),
    ('category_2', 'Κατηγορία 2', 'Όλες οι κατηγορίες 2'),
    ('category_3', 'Κατηγορία 3', 'Όλες οι κατηγορίες 3'),
]

# Dimensions the flexible "Ανάλυση" report can group by, and the two modes.
_EXPORT_PIVOT_DIMENSIONS = [
    ('item', 'Είδος'),
    ('channel', 'Κανάλι πώλησης'),
    ('group', 'Ομάδα Ειδών'),
    ('brand', 'Brand'),
    ('category_1', 'Κατηγορία 1'),
    ('category_2', 'Κατηγορία 2'),
    ('category_3', 'Κατηγορία 3'),
    ('branch', 'Υποκατάστημα'),
    ('warehouse', 'Αποθηκευτικός χώρος'),
]
_EXPORT_PIVOT_DIM_LABELS = dict(_EXPORT_PIVOT_DIMENSIONS)
_EXPORT_PIVOT_MODES = [('analysis', 'Ανάλυση περιόδου'), ('comparison', 'Σύγκριση Α / Β')]

# Item-attribute columns — only meaningful when grouping by Είδος (item).
_EXPORT_PIVOT_ITEM_ATTRS = [
    ('a_barcode', 'Barcode', 'text'),
    ('a_brand', 'Brand', 'text'),
    ('a_group', 'Ομάδα', 'text'),
    ('a_cat1', 'Κατηγορία 1', 'text'),
    ('a_cat2', 'Κατηγορία 2', 'text'),
    ('a_cat3', 'Κατηγορία 3', 'text'),
]
_EXPORT_PIVOT_ATTR_KEYS = {key for key, _l, _k in _EXPORT_PIVOT_ITEM_ATTRS}

# Metrics the user can add as columns to the flexible "Ανάλυση" report (analysis mode).
_EXPORT_PIVOT_METRICS = [
    ('net_value', 'Καθαρή Αξία', 'money'),
    ('qty', 'Τεμάχια', 'int'),
    ('contribution_pct', 'Contribution %', 'pct'),
    ('margin_pct', 'Margin %', 'pct'),
    ('cost', 'Κόστος', 'money'),
    ('profit', 'Μικτό Κέρδος', 'money'),
    ('gross_value', 'Μικτή Αξία', 'money'),
    ('doc_count', 'Παραστατικά', 'int'),
    ('item_count', 'Είδη (SKU)', 'int'),
    ('avg_per_doc', 'Μέση αξία/παραστατικό', 'money'),
    ('avg_per_item', 'Μέση αξία/είδος', 'money'),
    ('vat', 'ΦΠΑ', 'money'),
    ('discount', 'Έκπτωση', 'money'),
]
_EXPORT_PIVOT_METRIC_MAP = {
    key: {'label': label, 'kind': kind}
    for key, label, kind in (_EXPORT_PIVOT_METRICS + _EXPORT_PIVOT_ITEM_ATTRS)
}
_EXPORT_PIVOT_DEFAULT_METRICS = ['net_value', 'qty', 'contribution_pct', 'margin_pct']


def _export_selected_metrics(request: Request, group_by: str = 'channel') -> list[str]:
    """Metrics/columns chosen for the report, in the exact order the user arranged
    them (the query submits `metric` params in column order), validated + defaulted.
    Item-attribute columns are only kept when grouping by Είδος."""
    ordered: list[str] = []
    for m in request.query_params.getlist('metric'):
        if m in _EXPORT_PIVOT_METRIC_MAP and m not in ordered:
            ordered.append(m)
    if group_by != 'item':
        ordered = [m for m in ordered if m not in _EXPORT_PIVOT_ATTR_KEYS]
    return ordered or list(_EXPORT_PIVOT_DEFAULT_METRICS)


_EXPORT_DOWNLOAD_HEADERS = [
    'Όνομα είδους', 'Barcode', 'Brand', 'Κατηγορία 1', 'Κατηγορία 2', 'Κατηγορία 3',
    'Ποσότητα (τεμ.)', 'Αξία',
]


def _export_clean_date(value: object) -> str:
    text = str(value or '').strip()
    try:
        datetime.strptime(text, '%Y-%m-%d')
        return text
    except ValueError:
        return ''


async def _export_query(
    request: Request,
    tenant: Tenant,
    *,
    limit: int,
    compute: bool,
    report_kind: str = 'items',
) -> tuple[dict, dict, dict, list[dict], dict]:
    """Shared data path for the Εξαγωγές pages and downloads: open the tenant DB,
    load filter options, parse the request filters, and (when compute) run the
    per-item sales aggregation. Returns (options, selected, period, rows, totals)."""
    period = {
        'from': _export_clean_date(request.query_params.get('period_from')),
        'to': _export_clean_date(request.query_params.get('period_to')),
        'b_from': _export_clean_date(request.query_params.get('period_b_from')),
        'b_to': _export_clean_date(request.query_params.get('period_b_to')),
    }
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        options = await export_filter_options(tenant_db)
        selected: dict[str, list[str]] = {}
        for key, _label, _ph in _EXPORT_FILTER_FIELDS:
            raw = str(request.query_params.get(key) or '').strip()
            valid = {opt['value'] for opt in options.get(key, [])}
            selected[key] = [v for v in (p.strip() for p in raw.split(',')) if v and v in valid]

        rows: list[dict] = []
        totals: dict = {'count': 0, 'qty': 0.0, 'value': 0.0}
        if compute:
            filter_kwargs = dict(
                brands=selected.get('brands'),
                category_1=selected.get('category_1'),
                category_2=selected.get('category_2'),
                category_3=selected.get('category_3'),
                groups=selected.get('group'),
                branches=selected.get('branches'),
                warehouses=selected.get('warehouses'),
                period_from=(datetime.strptime(period['from'], '%Y-%m-%d').date() if period['from'] else None),
                period_to=(datetime.strptime(period['to'], '%Y-%m-%d').date() if period['to'] else None),
            )
            # Sold quantity/value must sign returns exactly like the dashboard,
            # which depends on the tenant's KPI-participation config in context.
            async with ControlSessionLocal() as control_db:
                sales_kpi_config = await _resolve_rule_payload(
                    control_db,
                    tenant_id=int(tenant.id),
                    domain=RuleDomain.kpi_participation_rules,
                    stream=OperationalStream.sales_documents,
                    rule_key='sales_kpi_config',
                    fallback_payload={},
                )
            scope_token = set_current_sales_kpi_participation_config(sales_kpi_config)
            try:
                if report_kind == 'analysis':
                    a_group_by = str(request.query_params.get('group_by') or 'channel').strip()
                    a_mode = str(request.query_params.get('mode') or 'analysis').strip()
                    dim_kwargs = {k: v for k, v in filter_kwargs.items() if k not in {'period_from', 'period_to'}}
                    rows, totals = await sales_pivot(
                        tenant_db,
                        group_by=a_group_by,
                        mode=a_mode,
                        period_from=filter_kwargs.get('period_from'),
                        period_to=filter_kwargs.get('period_to'),
                        period_b_from=(datetime.strptime(period['b_from'], '%Y-%m-%d').date() if period['b_from'] else None),
                        period_b_to=(datetime.strptime(period['b_to'], '%Y-%m-%d').date() if period['b_to'] else None),
                        **dim_kwargs,
                    )
                elif report_kind == 'channels':
                    rows, totals = await sales_by_channel(tenant_db, **filter_kwargs)
                elif report_kind == 'group_comparison':
                    dim_kwargs = {k: v for k, v in filter_kwargs.items() if k not in {'period_from', 'period_to'}}
                    rows, totals = await sales_comparison_by_group(
                        tenant_db,
                        period_a_from=(datetime.strptime(period['from'], '%Y-%m-%d').date() if period['from'] else None),
                        period_a_to=(datetime.strptime(period['to'], '%Y-%m-%d').date() if period['to'] else None),
                        period_b_from=(datetime.strptime(period['b_from'], '%Y-%m-%d').date() if period['b_from'] else None),
                        period_b_to=(datetime.strptime(period['b_to'], '%Y-%m-%d').date() if period['b_to'] else None),
                        **dim_kwargs,
                    )
                else:
                    rows = await export_item_rows(tenant_db, limit=limit, **filter_kwargs)
                    totals = await export_item_totals(tenant_db, **filter_kwargs)
            finally:
                reset_current_sales_kpi_participation_config(scope_token)
        return options, selected, period, rows, totals

    empty = {key: [] for key, _label, _ph in _EXPORT_FILTER_FIELDS}
    return empty, {key: [] for key, _label, _ph in _EXPORT_FILTER_FIELDS}, period, [], {'count': 0, 'qty': 0.0, 'value': 0.0}


async def _render_exports_workbench(
    *,
    request: Request,
    tenant: Tenant,
    variant: str,
) -> HTMLResponse:
    """Render an Εξαγωγές page (Reports or CSV/Excel). Both share the same filter
    bar and option loader; only titling/target differ."""
    variants = {
        'reports': {
            'active_page': 'exports_reports',
            'form_action': '/tenant/exports/reports',
            'heading': 'Αναφορές',
            'description': 'Κεντρική λίστα αναφορών με φίλτρα ανά υποκατάστημα, αποθηκευτικό χώρο, brand και τις τρεις κατηγορίες είδους.',
            'title': 'Αναφορές',
            'report_kind': 'items',
        },
        'csv_excel': {
            'active_page': 'exports_csv_excel',
            'form_action': '/tenant/exports/csv-excel',
            'heading': 'Εξαγωγές - CSV / Excel',
            'description': 'Εξαγωγές CSV/Excel με φίλτρα ανά υποκατάστημα, αποθηκευτικό χώρο, brand και τις τρεις κατηγορίες είδους.',
            'title': 'Εξαγωγές - CSV / Excel',
            'report_kind': 'items',
        },
        'channels': {
            'active_page': 'exports_channels',
            'form_action': '/tenant/exports/channels',
            'heading': 'Συνεισφορά ανά Κανάλι Πώλησης',
            'description': 'Καθαρή αξία, συνεισφορά % και margin % ανά κανάλι πώλησης, με τα ίδια φίλτρα (περίοδος/κατάστημα/κατηγορία).',
            'title': 'Ανά Κανάλι Πώλησης',
            'report_kind': 'channels',
        },
        'group_comparison': {
            'active_page': 'exports_group_comparison',
            'form_action': '/tenant/exports/group-comparison',
            'heading': 'Σύγκριση Πωλήσεων ανά Ομάδα',
            'description': 'Τζίρος, κόστος και κέρδος ανά ομάδα ειδών, για δύο περιόδους (Α = τρέχων μήνας, Β = περσινός αντίστοιχος μήνας — ή ελεύθερη περίοδος).',
            'title': 'Σύγκριση Ομάδων',
            'report_kind': 'group_comparison',
        },
        'analysis': {
            'active_page': 'exports_analysis',
            'form_action': '/tenant/exports/analysis',
            'heading': 'Report Builder',
            'description': 'Φτιάξε το δικό σου report: ομαδοποίησε κατά είδος, κανάλι, ομάδα, brand, κατηγορία, κατάστημα ή αποθήκη — διάλεξε στήλες και σειρά — σε ανάλυση περιόδου ή σύγκριση Α/Β.',
            'title': 'Report Builder',
            'report_kind': 'analysis',
        },
    }
    cfg = variants[variant]
    report_kind = cfg['report_kind']

    # The report is computed on demand: entering the page shows an empty result
    # and only pressing "Υπολογισμός" (which submits calc=1) runs the aggregation,
    # so we never scan all-time sales just because someone opened the page.
    calculated = str(request.query_params.get('calc') or '').strip() in {'1', 'true', 'yes'}

    # Flexible "Ανάλυση" report: group-by dimension + mode (analysis | comparison).
    pivot_group_by = str(request.query_params.get('group_by') or 'channel').strip()
    if pivot_group_by not in _EXPORT_PIVOT_DIM_LABELS:
        pivot_group_by = 'channel'
    pivot_mode = str(request.query_params.get('mode') or 'analysis').strip()
    if pivot_mode not in {'analysis', 'comparison'}:
        pivot_mode = 'analysis'

    _ROW_LIMIT = 1000
    options, selected, period, rows, totals = await _export_query(
        request, tenant, limit=_ROW_LIMIT, compute=calculated, report_kind=report_kind
    )

    # Pre-fill sensible period defaults so the user can hit "Υπολογισμός" at once.
    def _shift_year(d, years):
        try:
            return d.replace(year=d.year - years)
        except ValueError:  # 29 Feb -> 28 Feb on a non-leap year
            return d.replace(year=d.year - years, day=28)
    today = datetime.utcnow().date()
    a_from = today.replace(day=1)
    wants_comparison = report_kind == 'group_comparison' or (report_kind == 'analysis' and pivot_mode == 'comparison')
    if wants_comparison:
        defaults = {
            'from': a_from.isoformat(), 'to': today.isoformat(),
            'b_from': _shift_year(a_from, 1).isoformat(), 'b_to': _shift_year(today, 1).isoformat(),
        }
    elif report_kind == 'analysis':
        defaults = {'from': a_from.isoformat(), 'to': today.isoformat()}
    else:
        defaults = {}
    for key, value in defaults.items():
        if not period.get(key):
            period[key] = value

    label_maps: dict[str, dict[str, str]] = {}
    for key, _label, _ph in _EXPORT_FILTER_FIELDS:
        label_maps[key] = {opt['value']: opt['label'] for opt in options.get(key, [])}

    return templates.TemplateResponse(
        'tenant/exports_workbench.html',
        {
            'request': request,
            'tenant': tenant,
            **await _tenant_navigation_context(tenant),
            'active_page': cfg['active_page'],
            'title': cfg['title'],
            'page_title_key': cfg['title'],
            'page_heading': cfg['heading'],
            'page_description': cfg['description'],
            'form_action': cfg['form_action'],
            'filter_fields': _EXPORT_FILTER_FIELDS,
            'options': options,
            'selected': selected,
            'label_maps': label_maps,
            'period': period,
            'category_hierarchy': options.get('category_hierarchy', []),
            'rows': rows,
            'row_limit': _ROW_LIMIT,
            'calculated': calculated,
            'totals': totals,
            'report_kind': report_kind,
            'pivot_dimensions': _EXPORT_PIVOT_DIMENSIONS,
            'pivot_modes': _EXPORT_PIVOT_MODES,
            'pivot_group_by': pivot_group_by,
            'pivot_mode': pivot_mode,
            'pivot_group_label': _EXPORT_PIVOT_DIM_LABELS.get(pivot_group_by, 'Ομάδα'),
            'pivot_metrics_catalog': _EXPORT_PIVOT_METRICS,
            'pivot_item_attrs': _EXPORT_PIVOT_ITEM_ATTRS,
            'pivot_metrics_selected': _export_selected_metrics(request, pivot_group_by),
            'pivot_metric_map': _EXPORT_PIVOT_METRIC_MAP,
        },
    )


@router.get('/tenant/exports/reports', response_class=HTMLResponse)
async def tenant_exports_reports(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_workbench(request=request, tenant=tenant, variant='reports')


@router.get('/tenant/exports/sellout', response_class=HTMLResponse)
async def tenant_exports_sellout(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return templates.TemplateResponse(
        'tenant/sellout_report.html',
        {
            'request': request,
            'tenant': tenant,
            **await _tenant_navigation_context(tenant),
            'active_page': 'exports_sellout',
            'title': 'Sell Out',
            'page_title_key': 'Sell Out',
        },
    )


@router.get('/tenant/exports/csv-excel', response_class=HTMLResponse)
async def tenant_exports_csv_excel(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_workbench(request=request, tenant=tenant, variant='csv_excel')


_EXPORT_DOWNLOAD_LIMIT = 100000


async def _render_exports_download(
    *,
    request: Request,
    tenant: Tenant,
    variant: str,
    fmt: str,
) -> Response:
    """Produce the actual CSV or Excel file for the current filter selection.
    Shares the exact query/aggregation with the on-screen report (parity)."""
    fmt = (fmt or '').strip().lower()
    if fmt not in {'csv', 'xlsx'}:
        return Response('Μη έγκυρη μορφή εξαγωγής.', status_code=400, media_type='text/plain; charset=utf-8')

    report_kind = {'channels': 'channels', 'group_comparison': 'group_comparison', 'analysis': 'analysis'}.get(variant, 'items')
    _options, _selected, period, rows, totals = await _export_query(
        request, tenant, limit=_EXPORT_DOWNLOAD_LIMIT, compute=True, report_kind=report_kind
    )

    if report_kind == 'analysis':
        a_group_by = str(request.query_params.get('group_by') or 'channel').strip()
        dim_label = _EXPORT_PIVOT_DIM_LABELS.get(a_group_by, 'Ομάδα')
        a_mode = str(request.query_params.get('mode') or 'analysis').strip()
        if a_mode == 'comparison':
            headers = [dim_label, 'Τζίρος Α', 'Κόστος Α', 'Κέρδος Α', 'Τζίρος Β', 'Κόστος Β', 'Κέρδος Β']
            widths = [26, 15, 15, 15, 15, 15, 15]
            keys = ['turnover_a', 'cost_a', 'profit_a', 'turnover_b', 'cost_b', 'profit_b']
            data_rows = [[r['label']] + [round(float(r.get(k) or 0), 2) for k in keys] for r in rows]
            if data_rows:
                data_rows.append(['ΣΥΝΟΛΟ'] + [round(float(totals.get(k) or 0), 2) for k in keys])
        else:
            metrics = _export_selected_metrics(request, a_group_by)
            headers = [dim_label] + [_EXPORT_PIVOT_METRIC_MAP[m]['label'] for m in metrics]
            widths = [26] + [15] * len(metrics)

            def _mval(src, m):
                kind = _EXPORT_PIVOT_METRIC_MAP[m]['kind']
                if kind == 'text':
                    return str(src.get(m) or '')
                return round(float(src.get(m) or 0), 0 if kind == 'int' else 2)
            data_rows = [[r['label']] + [_mval(r, m) for m in metrics] for r in rows]
            if data_rows:
                data_rows.append(['ΣΥΝΟΛΟ'] + [_mval(totals, m) for m in metrics])
        base = 'analysi'
    elif report_kind == 'group_comparison':
        headers = ['Ομάδα', 'Τζίρος Α', 'Κόστος Α', 'Κέρδος Α', 'Τζίρος Β', 'Κόστος Β', 'Κέρδος Β']
        widths = [26, 15, 15, 15, 15, 15, 15]
        data_rows = [
            [
                r['group'],
                round(float(r['turnover_a'] or 0), 2), round(float(r['cost_a'] or 0), 2), round(float(r['profit_a'] or 0), 2),
                round(float(r['turnover_b'] or 0), 2), round(float(r['cost_b'] or 0), 2), round(float(r['profit_b'] or 0), 2),
            ]
            for r in rows
        ]
        if data_rows:
            data_rows.append([
                'ΣΥΝΟΛΟ',
                round(float(totals.get('turnover_a') or 0), 2), round(float(totals.get('cost_a') or 0), 2), round(float(totals.get('profit_a') or 0), 2),
                round(float(totals.get('turnover_b') or 0), 2), round(float(totals.get('cost_b') or 0), 2), round(float(totals.get('profit_b') or 0), 2),
            ])
        base = 'sygrisi_omadon'
    elif report_kind == 'channels':
        headers = ['Κανάλι', 'Καθαρή Αξία', 'Τεμάχια', 'Contribution %', 'Margin %']
        widths = [32, 16, 12, 16, 14]
        data_rows = [
            [
                r['channel'], round(float(r['net_value'] or 0), 2), round(float(r['qty'] or 0), 0),
                round(float(r['contribution_pct'] or 0), 2), round(float(r['margin_pct'] or 0), 2),
            ]
            for r in rows
        ]
        if data_rows:
            data_rows.append([
                'ΣΥΝΟΛΟ', round(float(totals.get('net_value') or 0), 2), round(float(totals.get('qty') or 0), 0),
                round(float(totals.get('contribution_pct') or 0), 2), round(float(totals.get('margin_pct') or 0), 2),
            ])
        base = 'kanali_polisis'
    else:
        headers = _EXPORT_DOWNLOAD_HEADERS
        widths = [38, 16, 20, 22, 22, 22, 15, 15]
        data_rows = [
            [
                r['name'], r['barcode'], r['brand'],
                r['category_1'], r['category_2'], r['category_3'],
                round(float(r['sold_qty'] or 0), 3), round(float(r['sold_value'] or 0), 2),
            ]
            for r in rows
        ]
        if data_rows:
            data_rows.append([
                'ΣΥΝΟΛΟ', '', '', '', '', '',
                round(float(totals.get('qty') or 0), 3), round(float(totals.get('value') or 0), 2),
            ])
        base = 'anafores' if variant == 'reports' else 'export'

    span = ''
    if period.get('from') or period.get('to'):
        span = f"_{period.get('from') or 'start'}_{period.get('to') or 'today'}"
    stamp = datetime.utcnow().strftime('%Y%m%d')
    filename = f'{base}{span}_{stamp}.{fmt}'

    if fmt == 'csv':
        buffer = io.StringIO()
        buffer.write('﻿')  # UTF-8 BOM so Excel renders Greek correctly
        writer = csv.writer(buffer, delimiter=';')
        writer.writerow(headers)
        writer.writerows(data_rows)
        content = buffer.getvalue().encode('utf-8')
        return Response(
            content=content,
            media_type='text/csv; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )

    content = _build_xlsx_bytes(
        sheet_name='Εξαγωγή',
        headers=headers,
        rows=data_rows,
        column_widths=widths,
    )
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/tenant/exports/reports/download/{fmt}')
async def tenant_exports_reports_download(
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_download(request=request, tenant=tenant, variant='reports', fmt=fmt)


@router.get('/tenant/exports/csv-excel/download/{fmt}')
async def tenant_exports_csv_excel_download(
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_download(request=request, tenant=tenant, variant='csv_excel', fmt=fmt)


@router.get('/tenant/exports/channels', response_class=HTMLResponse)
async def tenant_exports_channels(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_workbench(request=request, tenant=tenant, variant='channels')


@router.get('/tenant/exports/channels/download/{fmt}')
async def tenant_exports_channels_download(
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_download(request=request, tenant=tenant, variant='channels', fmt=fmt)


@router.get('/tenant/exports/group-comparison', response_class=HTMLResponse)
async def tenant_exports_group_comparison(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_workbench(request=request, tenant=tenant, variant='group_comparison')


@router.get('/tenant/exports/group-comparison/download/{fmt}')
async def tenant_exports_group_comparison_download(
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_download(request=request, tenant=tenant, variant='group_comparison', fmt=fmt)


@router.get('/tenant/exports/analysis', response_class=HTMLResponse)
async def tenant_exports_analysis(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_workbench(request=request, tenant=tenant, variant='analysis')


@router.get('/tenant/store-dashboard', response_class=HTMLResponse)
async def tenant_store_dashboard(
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    from app.services.kpi_queries import store_dashboard, store_transfer_suggestions
    today = datetime.utcnow().date()
    period = {
        'from': _export_clean_date(request.query_params.get('period_from')) or today.replace(day=1).isoformat(),
        'to': _export_clean_date(request.query_params.get('period_to')) or today.isoformat(),
    }
    date_from = datetime.strptime(period['from'], '%Y-%m-%d').date()
    date_to = datetime.strptime(period['to'], '%Y-%m-%d').date()

    async with ControlSessionLocal() as control_db:
        sales_kpi_config = await _resolve_rule_payload(
            control_db, tenant_id=int(tenant.id),
            domain=RuleDomain.kpi_participation_rules, stream=OperationalStream.sales_documents,
            rule_key='sales_kpi_config', fallback_payload={},
        )

    branches: list[dict] = []
    data: dict = {}
    branch_ext = str(request.query_params.get('branch') or '').strip()
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id), db_name=tenant.db_name,
        db_user=tenant.db_user, db_password=tenant.db_password,
    ):
        rows = (await tenant_db.execute(text(
            "SELECT b.external_id, b.name FROM dim_branches b "
            "WHERE b.external_id IN (SELECT DISTINCT branch_ext_id FROM fact_sales WHERE branch_ext_id IS NOT NULL) "
            "ORDER BY lower(coalesce(b.name, b.external_id))"
        ))).all()
        branches = [{'value': str(r[0]), 'label': str(r[1] or r[0])} for r in rows]
        valid = {b['value'] for b in branches}
        if branch_ext not in valid:
            branch_ext = branches[0]['value'] if branches else ''
        if branch_ext:
            scope_token = set_current_sales_kpi_participation_config(sales_kpi_config)
            try:
                data = await store_dashboard(tenant_db, branch_ext=branch_ext, date_from=date_from, date_to=date_to)
                transfers = await store_transfer_suggestions(
                    tenant_db, branch_ext=branch_ext, date_from=date_from, date_to=date_to,
                    target_days=(date_to - date_from).days + 1,
                )
                data['transfers'] = transfers.get('transfers', [])
                data['transfer_value'] = transfers.get('transfer_value', 0.0)
                data['target_days'] = transfers.get('target_days', 14)
            finally:
                reset_current_sales_kpi_participation_config(scope_token)
        break

    branch_label = next((b['label'] for b in branches if b['value'] == branch_ext), branch_ext)
    return templates.TemplateResponse(
        'tenant/store_dashboard.html',
        {
            'request': request,
            'tenant': tenant,
            **await _tenant_navigation_context(tenant),
            'active_page': 'store_dashboard',
            'title': 'Κατάστημα',
            'page_title_key': 'Κατάστημα',
            'branches': branches,
            'branch_ext': branch_ext,
            'branch_label': branch_label,
            'period': period,
            'data': data,
        },
    )


@router.get('/tenant/store-dashboard/download/{card}/{fmt}')
async def tenant_store_dashboard_download(
    card: str,
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    from app.services.kpi_queries import store_dashboard, store_transfer_suggestions
    fmt = (fmt or '').strip().lower()
    card = (card or '').strip().lower()
    if fmt not in {'csv', 'xlsx'} or card not in {'lost', 'dead', 'transfer'}:
        return Response('Μη έγκυρη εξαγωγή.', status_code=400, media_type='text/plain; charset=utf-8')

    today = datetime.utcnow().date()
    dfrom = _export_clean_date(request.query_params.get('period_from')) or today.replace(day=1).isoformat()
    dto = _export_clean_date(request.query_params.get('period_to')) or today.isoformat()
    date_from = datetime.strptime(dfrom, '%Y-%m-%d').date()
    date_to = datetime.strptime(dto, '%Y-%m-%d').date()
    branch_ext = str(request.query_params.get('branch') or '').strip()
    if not branch_ext:
        return Response('Δεν επιλέχθηκε κατάστημα.', status_code=400, media_type='text/plain; charset=utf-8')

    async with ControlSessionLocal() as control_db:
        sales_kpi_config = await _resolve_rule_payload(
            control_db, tenant_id=int(tenant.id),
            domain=RuleDomain.kpi_participation_rules, stream=OperationalStream.sales_documents,
            rule_key='sales_kpi_config', fallback_payload={},
        )
    data: dict = {}
    branch_label = branch_ext
    async for tenant_db in get_tenant_db_session(
        tenant_key=str(tenant.id), db_name=tenant.db_name,
        db_user=tenant.db_user, db_password=tenant.db_password,
    ):
        branch_label = (await tenant_db.execute(
            text("SELECT name FROM dim_branches WHERE external_id = :e"), {'e': branch_ext}
        )).scalar() or branch_ext
        scope_token = set_current_sales_kpi_participation_config(sales_kpi_config)
        try:
            if card == 'transfer':
                data = await store_transfer_suggestions(
                    tenant_db, branch_ext=branch_ext, date_from=date_from, date_to=date_to,
                    target_days=(date_to - date_from).days + 1, top_n=100000,
                )
            else:
                data = await store_dashboard(tenant_db, branch_ext=branch_ext, date_from=date_from, date_to=date_to, top_n=100000)
        finally:
            reset_current_sales_kpi_participation_config(scope_token)
        break

    _period_gr = f"{dfrom[8:10]}/{dfrom[5:7]}/{dfrom[0:4]} έως {dto[8:10]}/{dto[5:7]}/{dto[0:4]}"
    if card == 'transfer':
        headers = ['Είδος', 'Barcode', 'Από κατάστημα', 'Προς κατάστημα', 'Ποσότητα', 'Αξία μεταφοράς', 'Χαμ. τζίρος περιόδου']
        widths = [46, 16, 22, 22, 12, 16, 18]
        data_rows = [[r['name'], r['barcode'], r['from_branch'], branch_label,
                      round(float(r['qty'] or 0), 0), round(float(r['value'] or 0), 2), round(float(r.get('lost_value') or 0), 2)]
                     for r in data.get('transfers', [])]
        base, sheet = 'metafores', 'Προτεινόμενες μεταφορές'
    elif card == 'lost':
        headers = ['Είδος', 'Barcode', 'Πωλήσεις περιόδου', 'Τεμάχια', 'Ημερήσιο (~)']
        widths = [46, 16, 18, 12, 14]
        data_rows = [[r['name'], r['barcode'], round(float(r['sold_value'] or 0), 2),
                      round(float(r['sold_qty'] or 0), 0), round(float(r['lost_daily'] or 0), 2)]
                     for r in data.get('lost_sales', [])]
        base, sheet = 'xamenes_polisis', 'Χαμένες πωλήσεις'
    else:
        headers = ['Είδος', 'Barcode', 'Τεμ. στοκ', 'Δεσμευμένο €']
        widths = [46, 16, 12, 16]
        data_rows = [[r['name'], r['barcode'], round(float(r['stock_qty'] or 0), 0), round(float(r['tied_value'] or 0), 2)]
                     for r in data.get('dead_stock', [])]
        base, sheet = 'valtomeno_apothema', 'Βαλτωμένο απόθεμα'

    title = f'{sheet} — {branch_label} — {_period_gr}'
    stamp = datetime.utcnow().strftime('%Y%m%d')
    filename = f'{base}_{branch_ext.replace(":", "-")}_{dfrom}_{dto}_{stamp}.{fmt}'
    if fmt == 'csv':
        buffer = io.StringIO()
        buffer.write('﻿')
        writer = csv.writer(buffer, delimiter=';')
        writer.writerow([title])
        writer.writerow([])
        writer.writerow(headers)
        writer.writerows(data_rows)
        return Response(
            content=buffer.getvalue().encode('utf-8'),
            media_type='text/csv; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    content = _build_xlsx_bytes(sheet_name=sheet, headers=headers, rows=data_rows, column_widths=widths, title=title)
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/tenant/exports/analysis/download/{fmt}')
async def tenant_exports_analysis_download(
    fmt: str,
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    return await _render_exports_download(request=request, tenant=tenant, variant='analysis', fmt=fmt)
