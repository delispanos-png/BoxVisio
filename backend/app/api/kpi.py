import logging
import csv
import html
from datetime import date, timedelta
from io import StringIO
import re
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_request_tenant, get_tenant_db
from app.models.control import PlanName, Tenant
from app.services.supplier_targets import (
    clone_supplier_target,
    create_supplier_target,
    delete_supplier_target,
    list_supplier_targets,
    supplier_target_filter_options,
    update_supplier_target,
)
from app.services.kpi_queries import (
    cashflow_by_entry_type,
    cashflow_account_detail,
    cashflow_accounts_overview,
    customer_detail,
    customers_overview,
    suppliers_overview,
    cashflow_document_detail,
    cashflow_documents_overview,
    cashflow_monthly_trend,
    cashflow_summary,
    inventory_document_detail,
    inventory_documents_overview,
    inventory_by_brand,
    inventory_by_commercial_category,
    inventory_by_manufacturer,
    inventory_filter_options,
    inventory_item_detail,
    inventory_items_overview,
    inventory_snapshot,
    purchases_filter_options,
    sellout_filter_options,
    sellout_report,
    purchase_document_detail,
    purchases_documents_overview,
    expenses_filter_options,
    expense_document_detail,
    expenses_documents_overview,
    purchases_decision_pack,
    purchases_by_supplier,
    purchases_summary,
    price_control_filter_options,
    price_control_items,
    sales_filter_options,
    executive_dashboard_cards_summary,
    executive_dashboard_summary,
    finance_dashboard_summary,
    stream_sales_summary,
    stream_purchases_summary,
    stream_inventory_summary,
    stream_expenses_summary,
    stream_cash_summary,
    stream_balances_summary,
    sales_entity_ranking,
    sales_decision_pack,
    sales_document_detail,
    sales_documents_overview,
    e_shop_analysis_summary,
    normalize_document_series_labels_config,
    normalize_eshop_fulfillment_config,
    sales_by_branch,
    sales_by_brand,
    sales_by_category,
    sales_monthly_trend_from_monthly_agg,
    sales_summary,
    expenses_summary,
    receivables_aging,
    receivables_collection_trend,
    receivables_summary,
    receivables_top_customers,
    stock_aging,
    normalize_inventory_item_classification_config,
    pos_summary,
    pos_branches,
    pos_daily_trend,
    pos_by_payment_method,
    pos_by_category,
    sales_intraday,
    sales_daily_by_branch,
    sales_monthly_by_branch,
    normalize_price_margin_targets_config,
)
from app.services.kpi_cache import get_or_set_cache
from app.services.business_advisor import business_advisor_report
from app.services.era_exploration import era_exploration_report
from app.services.iqvia import iqvia_report
from app.services.intelligence_service import acknowledge_insight, list_insights
from app.core.config import settings
from app.core.celery_sender import make_celery_sender

router = APIRouter(tags=['kpi'])
logger = logging.getLogger(__name__)
celery_client = make_celery_sender('kpi_sender')
_DASHBOARD_CACHE_TTL_SECONDS = 300


class SupplierTargetCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    supplier_ext_id: str = Field(min_length=1, max_length=120)
    supplier_name: str | None = Field(default=None, max_length=160)
    target_year: int = Field(ge=2000, le=2100)
    target_amount: float = Field(ge=0)
    rebate_percent: float = Field(ge=0, le=100)
    rebate_amount: float = Field(default=0, ge=0)
    item_external_ids: list[str] = Field(default_factory=list)
    agreement_notes: str | None = Field(default=None, max_length=4000)
    notes: str | None = Field(default=None, max_length=500)


class SupplierTargetUpdateIn(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    supplier_ext_id: str | None = Field(default=None, min_length=1, max_length=120)
    supplier_name: str | None = Field(default=None, max_length=160)
    target_year: int | None = Field(default=None, ge=2000, le=2100)
    target_amount: float | None = Field(default=None, ge=0)
    rebate_percent: float | None = Field(default=None, ge=0, le=100)
    rebate_amount: float | None = Field(default=None, ge=0)
    item_external_ids: list[str] | None = None
    agreement_notes: str | None = Field(default=None, max_length=4000)
    notes: str | None = Field(default=None, max_length=500)
    is_active: bool | None = None


class SupplierTargetCloneIn(BaseModel):
    target_year: int | None = Field(default=None, ge=2000, le=2100)
    name: str | None = Field(default=None, min_length=1, max_length=120)


def _default_from() -> date:
    return date.today() - timedelta(days=30)


def _default_from_ytd() -> date:
    today = date.today()
    return date(today.year, 1, 1)


def _default_to() -> date:
    return date.today()


def _tenant_inventory_item_classification_from_request(request: Request) -> dict[str, int]:
    tenant = getattr(request.state, 'tenant', None)
    flags = getattr(tenant, 'feature_flags', None)
    raw = {}
    if isinstance(flags, dict):
        cfg = flags.get('inventory_item_classification')
        if isinstance(cfg, dict):
            raw = cfg
    return normalize_inventory_item_classification_config(raw)


def _tenant_eshop_fulfillment_from_request(request: Request) -> dict[str, object]:
    tenant = getattr(request.state, 'tenant', None)
    flags = getattr(tenant, 'feature_flags', None)
    raw = {'use_defaults': False}
    if isinstance(flags, dict):
        cfg = flags.get('eshop_fulfillment')
        if isinstance(cfg, dict):
            raw = cfg
    return normalize_eshop_fulfillment_config(raw)


def _tenant_price_margin_targets_from_request(request: Request) -> dict[str, object]:
    tenant = getattr(request.state, 'tenant', None)
    flags = getattr(tenant, 'feature_flags', None)
    raw = None
    if isinstance(flags, dict):
        cfg = flags.get('price_margin_targets')
        if isinstance(cfg, dict):
            raw = cfg
    return normalize_price_margin_targets_config(raw)


def _tenant_business_advisor_targets_from_request(request: Request) -> dict[str, object]:
    tenant = getattr(request.state, 'tenant', None)
    flags = getattr(tenant, 'feature_flags', None)
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


def _mask_executive_summary_for_subscription(request: Request, payload: dict) -> dict:
    """Remove mixed-dashboard values that belong to locked commercial modules."""
    out = dict(payload or {})
    features = getattr(request.state, 'subscription_features', None) or {}
    cards = dict(out.get('cards') or {})
    if not bool(features.get('purchases')):
        for key in ('purchases_period', 'purchases_year'):
            cards[key] = {}
    out['cards'] = cards
    return out


def _tenant_document_series_labels_from_request(request: Request) -> dict[str, str]:
    tenant = getattr(request.state, 'tenant', None)
    flags = getattr(tenant, 'feature_flags', None)
    raw = {}
    if isinstance(flags, dict):
        cfg = flags.get('document_series_labels')
        if isinstance(cfg, dict):
            raw = cfg
    return normalize_document_series_labels_config(raw)


_PROFILE_INSIGHT_PRIORITY: dict[str, list[str]] = {
    'FINANCE': ['receivables', 'cashflow', 'purchases'],
    'INVENTORY': ['inventory'],
    'SALES': ['sales'],
}

_PROFILE_KPI_EMPHASIS: dict[str, dict[str, list[str]]] = {
    'OWNER': {
        'executive': ['turnover', 'profit', 'margin_pct', 'trend'],
        'finance': ['net_cash', 'receivables_total', 'supplier_open_balance'],
    },
    'MANAGER': {
        'executive': ['turnover', 'qty', 'profit', 'by_branch'],
        'finance': ['net_cash', 'receivables_overdue', 'supplier_open_balance'],
    },
    'FINANCE': {
        'executive': ['net_cash', 'receivables_total', 'payables_total'],
        'finance': ['receivables_overdue', 'cash_in', 'cash_out', 'net_cash', 'aging'],
    },
    'INVENTORY': {
        'executive': ['stock_value', 'stock_turnover', 'dead_stock'],
        'finance': ['stock_value', 'cash_out'],
    },
    'SALES': {
        'executive': ['turnover', 'qty', 'margin_pct', 'sales_growth'],
        'finance': ['collections', 'net_cash'],
    },
}

_STREAM_DEFAULT_KPI_EMPHASIS: dict[str, list[str]] = {
    'sales': ['turnover', 'qty', 'margin_pct', 'top_products'],
    'purchases': ['purchases_total', 'supplier_dependency', 'cost_trend'],
    'inventory': ['stock_on_hand', 'stock_value', 'stock_aging'],
    'expenses': ['total_expenses', 'expense_ratio_to_revenue_pct', 'by_category'],
    'cash': ['cash_in', 'cash_out', 'net_cash', 'by_type'],
    'balances': ['supplier_open_balance', 'customer_open_balance', 'aging'],
}


def _prioritize_insights_for_profile(items: list[dict], profile_code: str | None) -> list[dict]:
    code = (profile_code or '').strip().upper()
    prioritized_categories = _PROFILE_INSIGHT_PRIORITY.get(code)
    if not prioritized_categories:
        return items
    order_map = {category: idx for idx, category in enumerate(prioritized_categories)}
    return sorted(items, key=lambda row: order_map.get(str(row.get('category') or '').lower(), 999))


def _kpi_emphasis_payload(request: Request, surface: str) -> dict:
    profile_code = str(getattr(request.state, 'professional_profile_code', '') or '').upper() or 'MANAGER'
    profile_map = _PROFILE_KPI_EMPHASIS.get(profile_code, _PROFILE_KPI_EMPHASIS['MANAGER'])
    return {
        'profile_code': profile_code,
        'surface': surface,
        'priorities': list(
            profile_map.get(surface, _STREAM_DEFAULT_KPI_EMPHASIS.get(surface, []))
        ),
    }


def _parse_flexible_date(
    raw: str | None,
    *,
    fallback: date | None = None,
    field_name: str = 'date',
    strict: bool = True,
) -> date | None:
    value = str(raw or '').strip()
    if not value:
        return fallback

    # Accept ISO date-time payloads by slicing the date prefix.
    if len(value) >= 10 and re.match(r'^\d{4}-\d{2}-\d{2}', value):
        value = value[:10]

    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    patterns: list[tuple[str, str]] = [
        (r'^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$', 'dmy'),
        (r'^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2})$', 'dmy2'),
        (r'^(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})$', 'ymd'),
        (r'^(\d{4})(\d{2})(\d{2})$', 'ymd_compact'),
    ]
    for pattern, mode in patterns:
        m = re.match(pattern, value)
        if not m:
            continue
        try:
            if mode == 'dmy':
                day = int(m.group(1))
                month = int(m.group(2))
                year = int(m.group(3))
            elif mode == 'dmy2':
                day = int(m.group(1))
                month = int(m.group(2))
                yy = int(m.group(3))
                year = 2000 + yy if yy < 70 else 1900 + yy
            elif mode == 'ymd':
                year = int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
            else:
                year = int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
            return date(year, month, day)
        except ValueError:
            if strict:
                raise HTTPException(status_code=422, detail=f'Invalid {field_name} date')
            return fallback

    if strict:
        raise HTTPException(status_code=422, detail=f'Invalid {field_name} date format')
    return fallback


def _parse_int_param(
    raw: str | None,
    *,
    default: int,
    min_value: int,
    max_value: int,
) -> int:
    value = str(raw or '').strip()
    if not value:
        return default
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return default
    return max(min_value, min(parsed, max_value))


def _csv_response(filename: str, header: list[str], rows: list[list[str]]) -> StreamingResponse:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


def _excel_html_response(filename: str, title: str, header: list[str], rows: list[list[object]]) -> Response:
    def cell(value: object, tag: str = 'td') -> str:
        return f'<{tag}>{html.escape(str(value if value is not None else ""))}</{tag}>'

    table_header = ''.join(cell(col, 'th') for col in header)
    table_rows = ''.join('<tr>' + ''.join(cell(value) for value in row) + '</tr>' for row in rows)
    body = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    table {{ border-collapse: collapse; font-family: Arial, sans-serif; font-size: 12px; }}
    th {{ background: #eef2ff; font-weight: 700; }}
    th, td {{ border: 1px solid #cbd5e1; padding: 6px 8px; }}
  </style>
</head>
<body>
  <h3>{html.escape(title)}</h3>
  <table><thead><tr>{table_header}</tr></thead><tbody>{table_rows}</tbody></table>
</body>
</html>"""
    return Response(
        content=body.encode('utf-8-sig'),
        media_type='application/vnd.ms-excel',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


def _tenant_cache_key(request: Request) -> str:
    tenant = getattr(request.state, 'tenant', None)
    tenant_id = getattr(tenant, 'id', None)
    return str(tenant_id or 'unknown')


def _normalize_list(values: list[str] | None) -> tuple[str, ...]:
    if not values:
        return tuple()
    return tuple(sorted({str(value) for value in values if str(value).strip()}))


def _clean_query_list(values: list[str] | None) -> list[str] | None:
    cleaned = [str(value).strip() for value in (values or []) if str(value or '').strip()]
    return cleaned or None


def _kpi_filter_cache_params(
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None,
    warehouses: list[str] | None,
    brands: list[str] | None,
    categories: list[str] | None,
    groups: list[str] | None,
) -> dict:
    return {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'brands': _normalize_list(brands),
        'categories': _normalize_list(categories),
        'groups': _normalize_list(groups),
    }


async def _cached_kpi_response(
    *,
    request: Request,
    response: Response,
    namespace: str,
    params: dict,
    producer,
    ttl_seconds: int = _DASHBOARD_CACHE_TTL_SECONDS,
):
    data, hit = await get_or_set_cache(
        namespace=namespace,
        tenant_key=_tenant_cache_key(request),
        params=params,
        ttl_seconds=ttl_seconds,
        producer=producer,
    )
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['X-KPI-Cache'] = 'HIT' if hit else 'MISS'
    return data


class ExecutiveSummaryOut(BaseModel):
    period: dict
    anchors: dict
    cards: dict
    comparisons: dict | None = None
    branch_breakdown: dict
    trend: dict
    key_alerts: list[dict]
    kpi_emphasis: dict | None = None


class FinanceSummaryOut(BaseModel):
    period: dict
    receivables_summary: dict
    receivables_aging: dict
    receivables_trend: dict
    suppliers: dict
    cash_summary: dict
    cash_types: list[dict]
    cash_accounts: dict
    kpi_emphasis: dict | None = None


class StreamSummaryOut(BaseModel):
    summary: dict | None = None
    by_branch: list[dict] | None = None
    by_category: list[dict] | None = None
    by_supplier: list[dict] | None = None
    trend: list[dict] | dict | None = None
    snapshot: dict | None = None
    aging: dict | None = None
    by_brand: list[dict] | None = None
    by_commercial_category: list[dict] | None = None
    by_manufacturer: list[dict] | None = None
    intelligence: dict | None = None
    by_type: list[dict] | None = None
    receivables: dict | None = None
    supplier_balances: dict | None = None
    top_suppliers: list[dict] | None = None
    kpi_emphasis: dict | None = None


@router.get('/v1/dashboard/executive-summary', response_model=ExecutiveSummaryOut)
@router.get('/api/dashboard/executive-summary', response_model=ExecutiveSummaryOut)
@router.get('/dashboard/executive-summary', response_model=ExecutiveSummaryOut)
async def get_dashboard_executive_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    fast: bool = Query(default=False),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    if fast:
        data = await _cached_kpi_response(
            request=request,
            response=response,
            namespace='dashboard:executive_cards',
            params=params,
            producer=lambda: executive_dashboard_cards_summary(
                tenant_db,
                date_from=date_from,
                date_to=date_to,
                branches=branches,
                warehouses=warehouses,
                brands=brands,
                categories=categories,
                groups=groups,
            ),
        )
        out = _mask_executive_summary_for_subscription(request, dict(data or {}))
        out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'executive')
        return out
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:executive_summary',
        params=params,
        producer=lambda: executive_dashboard_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )
    try:
        cards = (data or {}).get('cards') or {}
        logger.info(
            'executive_summary_cards_debug',
            extra={
                'tenant_cache_key': _tenant_cache_key(request),
                'date_from': str(date_from),
                'date_to': str(date_to),
                'week_net_value': cards.get('week', {}).get('net_value'),
                'month_net_value': cards.get('month', {}).get('net_value'),
                'year_net_value': cards.get('year', {}).get('net_value'),
                'period_sales_net_value': cards.get('period_sales', {}).get('net_value'),
                'purchases_period_net_value': cards.get('purchases_period', {}).get('net_value'),
                'cache_status': response.headers.get('X-KPI-Cache'),
            },
        )
    except Exception:
        logger.exception('executive_summary_cards_debug_failed')
    out = _mask_executive_summary_for_subscription(request, dict(data or {}))
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'executive')
    return out


@router.get('/v1/dashboard/finance-summary', response_model=FinanceSummaryOut)
@router.get('/api/dashboard/finance-summary', response_model=FinanceSummaryOut)
@router.get('/dashboard/finance-summary', response_model=FinanceSummaryOut)
async def get_dashboard_finance_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    supplier_limit: int = Query(default=50, ge=1, le=250),
    account_limit: int = Query(default=50, ge=1, le=250),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'supplier_limit': int(supplier_limit),
        'account_limit': int(account_limit),
    }
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:finance_summary',
        params=params,
        producer=lambda: finance_dashboard_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            supplier_limit=supplier_limit,
            account_limit=account_limit,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'finance')
    return out


@router.get('/v1/streams/sales/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/sales/summary', response_model=StreamSummaryOut)
@router.get('/streams/sales/summary', response_model=StreamSummaryOut)
async def get_stream_sales_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:sales_summary',
        params=params,
        producer=lambda: stream_sales_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'sales')
    return out


@router.get('/v1/streams/purchases/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/purchases/summary', response_model=StreamSummaryOut)
@router.get('/streams/purchases/summary', response_model=StreamSummaryOut)
async def get_stream_purchases_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:purchases_summary',
        params=params,
        producer=lambda: stream_purchases_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'purchases')
    return out


@router.get('/v1/streams/inventory/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/inventory/summary', response_model=StreamSummaryOut)
@router.get('/streams/inventory/summary', response_model=StreamSummaryOut)
async def get_stream_inventory_summary(
    request: Request,
    response: Response,
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {
        'as_of': str(as_of),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'brands': _normalize_list(brands),
        'categories': _normalize_list(categories),
        'groups': _normalize_list(groups),
    }
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:inventory_summary',
        params=params,
        producer=lambda: stream_inventory_summary(
            tenant_db,
            as_of=as_of,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'inventory')
    return out


@router.get('/v1/streams/expenses/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/expenses/summary', response_model=StreamSummaryOut)
@router.get('/streams/expenses/summary', response_model=StreamSummaryOut)
async def get_stream_expenses_summary(
    request: Request,
    response: Response,
    date_from_raw: str | None = Query(default=None, alias='from'),
    date_to_raw: str | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    date_from = _parse_flexible_date(
        date_from_raw,
        fallback=_default_from(),
        field_name='from',
        strict=False,
    )
    date_to = _parse_flexible_date(
        date_to_raw,
        fallback=_default_to(),
        field_name='to',
        strict=False,
    )
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'categories': _normalize_list(categories),
    }
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:expenses_summary',
        params=params,
        producer=lambda: stream_expenses_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            categories=categories,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'expenses')
    return out


@router.get('/v1/streams/cash/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/cash/summary', response_model=StreamSummaryOut)
@router.get('/streams/cash/summary', response_model=StreamSummaryOut)
async def get_stream_cash_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)}
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:cash_summary',
        params=params,
        producer=lambda: stream_cash_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'cash')
    return out


@router.get('/v1/streams/balances/summary', response_model=StreamSummaryOut)
@router.get('/api/streams/balances/summary', response_model=StreamSummaryOut)
@router.get('/streams/balances/summary', response_model=StreamSummaryOut)
async def get_stream_balances_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)}
    data = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='stream:balances_summary',
        params=params,
        producer=lambda: stream_balances_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )
    out = dict(data or {})
    out['kpi_emphasis'] = _kpi_emphasis_payload(request, 'balances')
    return out


@router.get('/v1/kpi/expenses/summary')
@router.get('/kpi/expenses/summary')
async def get_expenses_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'categories': _normalize_list(categories),
    }
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:expenses_summary',
        params=params,
        producer=lambda: expenses_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            categories=categories,
        ),
    )


@router.get('/v1/kpi/sales/summary')
@router.get('/kpi/sales/summary')
async def get_sales_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sales_summary',
        params=params,
        producer=lambda: sales_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/sales/decision-pack')
async def get_sales_decision_pack(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    top_n: int = Query(default=10, ge=1, le=50),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        params = _kpi_filter_cache_params(
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        params['top_n'] = int(top_n)
        return await _cached_kpi_response(
            request=request,
            response=response,
            namespace='dashboard:sales_decision_pack',
            params=params,
            producer=lambda: sales_decision_pack(
                tenant_db,
                date_from=date_from,
                date_to=date_to,
                branches=branches,
                warehouses=warehouses,
                brands=brands,
                categories=categories,
                groups=groups,
                top_n=top_n,
            ),
        )
    except Exception:
        days = max(1, (date_to - date_from).days + 1)
        prev_to = date_from.fromordinal(date_from.toordinal() - 1)
        prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
        return {
            'period': {
                'from': str(date_from),
                'to': str(date_to),
                'prev_from': str(prev_from),
                'prev_to': str(prev_to),
            },
            'cards': {
                'turnover': 0.0,
                'gross_profit': 0.0,
                'margin_pct': 0.0,
                'qty_sold': 0.0,
                'avg_basket_value': 0.0,
                'growth_pct': None,
                'avg_sale_per_day': 0.0,
                'avg_margin_per_branch': 0.0,
            },
            'current_summary': {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0},
            'previous_summary': {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0},
            'trend_monthly': [],
            'by_branch': [],
            'by_brand': [],
            'by_category': [],
            'by_group': [],
            'top_products': [],
            'movers': {'fast': [], 'slow': [], 'purchases': []},
            'seasonality': [],
            'new_codes': [],
            'best_entities': {'product': None, 'brand': None, 'category': None, 'group': None, 'branch': None},
            'insights': ['Η απόδοση πωλήσεων είναι σταθερή στο επιλεγμένο διάστημα.'],
            'margin_alerts': ['Δεν εντοπίστηκε σημαντική διάβρωση περιθωρίου στο επιλεγμένο διάστημα.'],
            'insight_records': [],
        }


@router.get('/v1/kpi/sales/ranking')
async def get_sales_entity_ranking(
    entity: str = Query(...),
    metric: str = Query(default='profitability'),
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=30, ge=1, le=200),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await sales_entity_ranking(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            entity=entity,
            metric=metric,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/v1/kpi/sales/filter-options')
async def get_sales_filter_options(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sales_filter_options',
        params=params,
        producer=lambda: sales_filter_options(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/pos/summary')
async def get_pos_summary(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await pos_summary(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )


@router.get('/v1/kpi/pos/branches')
async def get_pos_branches(
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await pos_branches(tenant_db)


@router.get('/v1/kpi/pos/daily-trend')
async def get_pos_daily_trend(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    rows = await pos_daily_trend(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )
    return [
        {
            'date': row.get('date'),
            'receipts': row.get('receipts', 0),
            'gross_value': row.get('value', 0),
        }
        for row in rows
    ]


@router.get('/v1/kpi/pos/by-payment-method')
async def get_pos_by_payment_method(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    rows = await pos_by_payment_method(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )
    return [
        {
            'payment_method': row.get('name', 'Μετρητά'),
            'receipts': row.get('receipts', 0),
            'gross_value': row.get('value', 0),
            'pct': row.get('pct', 0),
        }
        for row in rows
    ]


@router.get('/v1/kpi/pos/by-category')
async def get_pos_by_category(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    rows = await pos_by_category(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )
    return [
        {
            'category': row.get('name', '—'),
            'receipts': row.get('receipts', 0),
            'gross_value': row.get('value', 0),
            'pct': row.get('pct', 0),
        }
        for row in rows
    ]


@router.get('/v1/kpi/sales/intraday')
async def get_sales_intraday(
    day: date = Query(...),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_intraday(tenant_db, day=day, branches=branches)


@router.get('/v1/kpi/sales/weekly')
async def get_sales_weekly(
    week_from: date = Query(...),
    week_to: date = Query(...),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_daily_by_branch(tenant_db, date_from=week_from, date_to=week_to, branches=branches)


@router.get('/v1/kpi/sales/ytd-monthly')
async def get_sales_ytd_monthly(
    ytd_from: date = Query(...),
    ytd_to: date = Query(...),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_monthly_by_branch(tenant_db, date_from=ytd_from, date_to=ytd_to, branches=branches)


@router.get('/v1/kpi/sales/documents')
async def get_sales_documents(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    channels: list[str] | None = Query(default=None),
    status: str = Query(default='all'),
    series: str | None = Query(default=None),
    document_no: str | None = Query(default=None),
    eshop_code: str | None = Query(default=None),
    customer: str | None = Query(default=None),
    from_ref: str | None = Query(default=None),
    to_ref: str | None = Query(default=None),
    gross_min: float | None = Query(default=None),
    gross_max: float | None = Query(default=None),
    q: str | None = Query(default=None),
    behaviors: list[int] | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    fulfillment_config = _tenant_eshop_fulfillment_from_request(request)
    document_series_labels = _tenant_document_series_labels_from_request(request)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    params.update(
        {
            'channels': _normalize_list(channels),
            'status': status or 'all',
            'series': series or '',
            'document_no': document_no or '',
            'eshop_code': eshop_code or '',
            'customer': customer or '',
            'from_ref': from_ref or '',
            'to_ref': to_ref or '',
            'gross_min': gross_min,
            'gross_max': gross_max,
            'q': q or '',
            'behaviors': _normalize_list(behaviors),
            'limit': limit,
            'offset': offset,
            'fulfillment': fulfillment_config,
            'series_labels': document_series_labels,
        }
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sales_documents',
        params=params,
        producer=lambda: sales_documents_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            channels=channels,
            status=status,
            series=series,
            document_no=document_no,
            eshop_code=eshop_code,
            customer=customer,
            from_ref=from_ref,
            to_ref=to_ref,
            gross_min=gross_min,
            gross_max=gross_max,
            q=q,
            behaviors=behaviors,
            limit=limit,
            offset=offset,
            fulfillment_config=fulfillment_config,
            document_series_labels=document_series_labels,
        ),
    )


@router.get('/v1/kpi/sales/documents/{document_id}/detail')
async def get_sales_document_detail(
    request: Request,
    document_id: str,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    behaviors: list[int] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await sales_document_detail(
            tenant_db,
            document_id=document_id,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            behaviors=behaviors,
            fulfillment_config=_tenant_eshop_fulfillment_from_request(request),
            document_series_labels=_tenant_document_series_labels_from_request(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/eshop/analysis')
async def get_eshop_analysis(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=5, le=200),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    fulfillment_config = _tenant_eshop_fulfillment_from_request(request)
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'page': int(page),
        'page_size': int(page_size),
        'fulfillment': fulfillment_config,
    }
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:eshop_analysis',
        params=params,
        producer=lambda: e_shop_analysis_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            page=page,
            page_size=page_size,
            fulfillment_config=fulfillment_config,
        ),
    )


@router.get('/v1/kpi/purchases/documents')
async def get_purchases_documents(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    series: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    document_series_labels = _tenant_document_series_labels_from_request(request)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    params.update(
        {
            'series': series or '',
            'q': q or '',
            'limit': limit,
            'offset': offset,
            'series_labels': document_series_labels,
        }
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:purchases_documents',
        params=params,
        producer=lambda: purchases_documents_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            series=series,
            q=q,
            limit=limit,
            offset=offset,
            document_series_labels=document_series_labels,
        ),
    )


@router.get('/v1/kpi/purchases/documents/{document_id}/detail')
async def get_purchase_document_detail(
    request: Request,
    document_id: str,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await purchase_document_detail(
            tenant_db,
            document_id=document_id,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            document_series_labels=_tenant_document_series_labels_from_request(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/expenses/documents')
async def get_expenses_documents(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    series: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:expenses_documents',
        params={
            'from': str(date_from),
            'to': str(date_to),
            'branches': _normalize_list(branches),
            'categories': _normalize_list(categories),
            'series': series or '',
            'q': q or '',
            'limit': limit,
            'offset': offset,
        },
        producer=lambda: expenses_documents_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            categories=categories,
            series=series,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/kpi/expenses/documents/{document_id}/detail')
async def get_expense_document_detail(
    document_id: str,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await expense_document_detail(
            tenant_db,
            document_id=document_id,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            categories=categories,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/intelligence/insights')
async def get_recent_intelligence_insights(
    request: Request,
    limit: int = Query(default=20, ge=1, le=500),
    category: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    insights = await list_insights(
        tenant_db,
        category=category,
        severity=severity,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )
    profile_code = getattr(request.state, 'professional_profile_code', None)
    return _prioritize_insights_for_profile(insights, profile_code)


@router.post('/v1/intelligence/insights/{insight_id}/acknowledge')
async def acknowledge_intelligence_insight(
    insight_id: UUID,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    ok = await acknowledge_insight(tenant_db, insight_id=insight_id, user_id=None)
    if not ok:
        raise HTTPException(status_code=404, detail='Insight not found')
    return {'status': 'acknowledged', 'insight_id': str(insight_id)}


@router.post('/v1/intelligence/run-now')
async def run_intelligence_now(
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    task = celery_client.send_task(
        'worker.tasks.generate_insights_for_tenant',
        kwargs={'tenant_slug': tenant.slug},
        queue='default',
    )
    return {'status': 'queued', 'tenant': tenant.slug, 'task_id': task.id}


@router.get('/v1/kpi/sales/by-branch')
@router.get('/kpi/sales/by-branch')
async def get_sales_by_branch(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sales_by_branch',
        params=params,
        producer=lambda: sales_by_branch(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/sales/trend-monthly')
@router.get('/kpi/sales/trend-monthly')
async def get_sales_trend_monthly(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sales_trend_monthly',
        params=params,
        producer=lambda: sales_monthly_trend_from_monthly_agg(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/sales/by-brand')
@router.get('/kpi/sales/by-brand')
async def get_sales_by_brand(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_by_brand(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/by-category')
@router.get('/kpi/sales/by-category')
async def get_sales_by_category(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_by_category(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/compare')
@router.get('/kpi/sales/compare')
async def get_sales_compare(
    a_from: date = Query(alias='A_from'),
    a_to: date = Query(alias='A_to'),
    b_from: date = Query(alias='B_from'),
    b_to: date = Query(alias='B_to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return {
        'A': await sales_summary(
            tenant_db,
            date_from=a_from,
            date_to=a_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'B': await sales_summary(
            tenant_db,
            date_from=b_from,
            date_to=b_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    }


@router.get('/v1/kpi/purchases/summary')
@router.get('/kpi/purchases/summary')
async def get_purchases_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:purchases_summary',
        params=params,
        producer=lambda: purchases_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/purchases/by-supplier')
@router.get('/kpi/purchases/by-supplier')
async def get_purchases_by_supplier(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    return await purchases_by_supplier(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/purchases/compare')
@router.get('/kpi/purchases/compare')
async def get_purchases_compare(
    a_from: date = Query(alias='A_from'),
    a_to: date = Query(alias='A_to'),
    b_from: date = Query(alias='B_from'),
    b_to: date = Query(alias='B_to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    return {
        'A': await purchases_summary(
            tenant_db,
            date_from=a_from,
            date_to=a_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'B': await purchases_summary(
            tenant_db,
            date_from=b_from,
            date_to=b_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    }


@router.get('/v1/kpi/purchases/decision-pack')
async def get_purchases_decision_pack(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:purchases_decision_pack',
        params=params,
        producer=lambda: purchases_decision_pack(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/purchases/filter-options')
async def get_purchases_filter_options(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:purchases_filter_options',
        params=params,
        producer=lambda: purchases_filter_options(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/reports/sellout/filter-options')
async def get_sellout_filter_options(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
    }
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sellout_filter_options',
        params=params,
        producer=lambda: sellout_filter_options(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
        ),
    )


@router.get('/v1/reports/sellout')
async def get_sellout_report(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    suppliers: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    suppliers = _clean_query_list(suppliers)
    params = {
        'from': str(date_from),
        'to': str(date_to),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'brands': _normalize_list(brands),
        'categories': _normalize_list(categories),
        'groups': _normalize_list(groups),
        'suppliers': _normalize_list(suppliers),
        'q': str(q or ''),
        'limit': int(limit),
        'offset': int(offset),
    }
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:sellout_report',
        params=params,
        producer=lambda: sellout_report(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            suppliers=suppliers,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/reports/sellout/export.csv')
async def export_sellout_report_csv(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    suppliers: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    suppliers = _clean_query_list(suppliers)
    data = await sellout_report(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        suppliers=suppliers,
        q=q,
        limit=1000,
        offset=0,
    )
    csv_rows = [
        [
            str(r.get('item_code', '')),
            str(r.get('product', '')),
            str(r.get('barcode', '')),
            str(r.get('color', '')),
            str(r.get('size', '')),
            str(r.get('sales_value', 0)),
            str(r.get('sales_qty', 0)),
            str(r.get('gross_profit_pct', 0)),
            str(r.get('gross_profit_value', 0)),
            str(r.get('stock_qty', 0)),
            str(r.get('supplier_name', '')),
            str(r.get('brand', '')),
            str(r.get('category', '')),
            str(r.get('action', '')),
        ]
        for r in data.get('rows', [])
    ]
    return _csv_response(
        'sellout_report.csv',
        [
            'ΚΩΔΙΚΟΣ ΕΙΔΟΥΣ',
            'ΠΡΟΙΟΝ',
            'BARCODE',
            'ΧΡΩΜΑ',
            'ΜΕΓΕΘΟΣ',
            'ΠΩΛΗΣΕΙΣ ΣΕ ΑΞΙΑ',
            'ΠΩΛΗΣΕΙΣ ΣΕ ΠΟΣΟΤΗΤΑ',
            'ΜΙΚΤΟ ΚΕΡΔΟΣ %',
            'ΜΙΚΤΟ ΚΕΡΔΟΣ ΑΞΙΑ',
            'ΑΠΟΘΕΜΑ',
            'ΠΡΟΜΗΘΕΥΤΗΣ',
            'BRAND',
            'ΚΑΤΗΓΟΡΙΑ',
            'ΠΡΟΤΑΣΗ',
        ],
        csv_rows,
    )


@router.get('/v1/reports/sellout/actions/export.xls')
async def export_sellout_action_xls(
    action: str = Query(default='stockout'),
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    suppliers: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    action_map = {
        'reorder': ('reorder_candidates', 'Παραγγελία - διαθεσιμότητα'),
        'stockout': ('stockout_risk', 'Κίνδυνος έλλειψης'),
        'lostsales': ('lost_sales', 'Πιθανή χαμένη ζήτηση'),
        'overstock': ('overstock_risk', 'Υπεραπόθεμα - μετακίνηση'),
        'margin': ('low_margin', 'Χαμηλό περιθώριο'),
        'gmroi': ('top_gmroi', 'Top αποδοτικά είδη'),
        'transfer': ('transfer_candidates', 'Πιθανές ανακατανομές'),
    }
    key, title = action_map.get(str(action or '').strip().lower(), action_map['stockout'])
    branches = _clean_query_list(branches)
    warehouses = _clean_query_list(warehouses)
    brands = _clean_query_list(brands)
    categories = _clean_query_list(categories)
    groups = _clean_query_list(groups)
    suppliers = _clean_query_list(suppliers)
    data = await sellout_report(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        suppliers=suppliers,
        q=q,
        limit=20000,
        offset=0,
        action_limit=20000,
    )
    rows = data.get('actions', {}).get(key, [])
    excel_rows = [
        [
            r.get('item_code', ''),
            r.get('product', ''),
            r.get('barcode', ''),
            r.get('brand', ''),
            r.get('supplier_name', ''),
            r.get('category', ''),
            r.get('sales_value', 0),
            r.get('sales_qty', 0),
            r.get('stock_qty', 0),
            r.get('stock_value', 0),
            r.get('days_of_supply', ''),
            r.get('sell_through_pct', 0),
            r.get('gross_profit_pct', 0),
            r.get('gross_profit_value', 0),
            r.get('gmroi', 0),
            r.get('reorder_qty', 0),
            r.get('lost_sales_value', 0),
            r.get('action', ''),
        ]
        for r in rows
    ]
    return _excel_html_response(
        f'sellout_{str(action or "analysis").strip().lower()}.xls',
        f'Sell Out - {title}',
        [
            'ΚΩΔΙΚΟΣ ΕΙΔΟΥΣ',
            'ΠΡΟΙΟΝ',
            'BARCODE',
            'BRAND',
            'ΠΡΟΜΗΘΕΥΤΗΣ',
            'ΚΑΤΗΓΟΡΙΑ',
            'ΠΩΛΗΣΕΙΣ ΑΞΙΑ',
            'ΠΩΛΗΣΕΙΣ ΠΟΣΟΤΗΤΑ',
            'STOCK',
            'ΑΞΙΑ STOCK',
            'ΗΜΕΡΕΣ ΚΑΛΥΨΗΣ',
            'SELL-THROUGH %',
            'ΜΙΚΤΟ %',
            'ΜΙΚΤΟ ΚΕΡΔΟΣ',
            'GMROI',
            'ΠΡΟΤ. ΠΑΡΑΓΓΕΛΙΑ',
            'ΠΙΘΑΝΗ ΧΑΜΕΝΗ ΖΗΤΗΣΗ',
            'ΠΡΟΤΑΣΗ',
        ],
        excel_rows,
    )


@router.get('/v1/kpi/expenses/filter-options')
async def get_expenses_filter_options(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await expenses_filter_options(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        categories=categories,
    )


@router.get('/v1/kpi/inventory/documents')
async def get_inventory_documents(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    series: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    document_series_labels = _tenant_document_series_labels_from_request(request)
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    params.update({'series': series or '', 'q': q or '', 'limit': limit, 'offset': offset, 'series_labels': document_series_labels})
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:inventory_documents',
        params=params,
        producer=lambda: inventory_documents_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            series=series,
            q=q,
            limit=limit,
            offset=offset,
            document_series_labels=document_series_labels,
        ),
    )


@router.get('/v1/kpi/inventory/documents/{document_id}/detail')
async def get_inventory_document_detail(
    request: Request,
    document_id: str,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await inventory_document_detail(
            tenant_db,
            document_id=document_id,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            document_series_labels=_tenant_document_series_labels_from_request(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/inventory/snapshot')
@router.get('/kpi/inventory/snapshot')
async def get_inventory_snapshot(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_snapshot(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/stock-aging')
@router.get('/kpi/inventory/stock-aging')
async def get_stock_aging(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await stock_aging(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/by-brand')
@router.get('/kpi/inventory/by-brand')
async def get_inventory_by_brand(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_brand(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/by-commercial-category')
@router.get('/kpi/inventory/by-commercial-category')
async def get_inventory_by_commercial_category(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_commercial_category(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/by-manufacturer')
@router.get('/kpi/inventory/by-manufacturer')
async def get_inventory_by_manufacturer(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    lookback_days: int = Query(default=365, ge=30, le=1825),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_manufacturer(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        lookback_days=lookback_days,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/filter-options')
@router.get('/kpi/inventory/filter-options')
async def get_inventory_filter_options(
    request: Request,
    response: Response,
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = {
        'as_of': str(as_of),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'brands': _normalize_list(brands),
        'categories': _normalize_list(categories),
        'groups': _normalize_list(groups),
    }
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:inventory_filter_options',
        params=params,
        producer=lambda: inventory_filter_options(
            tenant_db,
            as_of=as_of,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    )


@router.get('/v1/kpi/inventory/items')
@router.get('/kpi/inventory/items')
async def get_inventory_items(
    request: Request,
    response: Response,
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    abc_categories: list[str] | None = Query(default=None),
    commercial_statuses: list[str] | None = Query(default=None),
    status: str = Query(default='all'),
    movement: str = Query(default='all'),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    classification_config = _tenant_inventory_item_classification_from_request(request)
    params = {
        'as_of': str(as_of),
        'branches': _normalize_list(branches),
        'warehouses': _normalize_list(warehouses),
        'brands': _normalize_list(brands),
        'categories': _normalize_list(categories),
        'groups': _normalize_list(groups),
        'abc_categories': _normalize_list(abc_categories),
        'commercial_statuses': _normalize_list(commercial_statuses),
        'status': str(status or 'all'),
        'movement': str(movement or 'all'),
        'q': str(q or ''),
        'limit': int(limit),
        'offset': int(offset),
        'classification': classification_config,
    }
    result = await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:inventory_items',
        params=params,
        producer=lambda: inventory_items_overview(
            tenant_db,
            as_of=as_of,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            abc_categories=abc_categories,
            commercial_statuses=commercial_statuses,
            status=status,
            movement=movement,
            q=q,
            limit=limit,
            offset=offset,
            classification_config=classification_config,
        ),
    )
    result = dict(result or {})
    allowed_scope = getattr(request.state, 'allowed_branch_scope', None)
    result['scope'] = {
        'company_id': getattr(request.state, 'scoped_company_id', None),
        'allowed_branches': list(allowed_scope or []),
    }
    return result


@router.get('/v1/kpi/inventory/items/{item_code}/detail')
@router.get('/kpi/inventory/items/{item_code}/detail')
async def get_inventory_item_detail(
    request: Request,
    item_code: str,
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    classification_config = _tenant_inventory_item_classification_from_request(request)
    try:
        return await inventory_item_detail(
            tenant_db,
            item_code=item_code,
            as_of=as_of,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            classification_config=classification_config,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/customers')
@router.get('/kpi/customers')
async def get_customers(
    request: Request,
    response: Response,
    date_from_raw: str | None = Query(default=None, alias='from'),
    date_to_raw: str | None = Query(default=None, alias='to'),
    q: str | None = Query(default=None),
    limit_raw: str | None = Query(default=None, alias='limit'),
    offset_raw: str | None = Query(default=None, alias='offset'),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    date_from = _parse_flexible_date(
        date_from_raw,
        fallback=date.today() - timedelta(days=365),
        field_name='from',
        strict=False,
    )
    date_to = _parse_flexible_date(
        date_to_raw,
        fallback=_default_to(),
        field_name='to',
        strict=False,
    )
    limit = _parse_int_param(limit_raw, default=250, min_value=1, max_value=500)
    offset = _parse_int_param(offset_raw, default=0, min_value=0, max_value=1_000_000)
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:customers',
        params={'from': str(date_from), 'to': str(date_to), 'q': q or '', 'limit': limit, 'offset': offset},
        producer=lambda: customers_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            q=q,
            limit=limit,
            offset=offset,
            include_profile=False,
            balance_only=True,
        ),
    )


@router.get('/v1/kpi/customers/{customer_id}/detail')
@router.get('/kpi/customers/{customer_id}/detail')
async def get_customer_detail(
    customer_id: str,
    date_from_raw: str | None = Query(default=None, alias='from'),
    date_to_raw: str | None = Query(default=None, alias='to'),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    date_from = _parse_flexible_date(date_from_raw, fallback=None, field_name='from')
    date_to = _parse_flexible_date(date_to_raw, fallback=None, field_name='to')
    try:
        return await customer_detail(
            tenant_db,
            customer_id=customer_id,
            date_from=date_from,
            date_to=date_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/receivables/summary')
@router.get('/kpi/receivables/summary')
async def get_receivables_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:receivables_summary',
        params={'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: receivables_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/receivables/aging')
@router.get('/kpi/receivables/aging')
async def get_receivables_aging(
    request: Request,
    response: Response,
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:receivables_aging',
        params={'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: receivables_aging(
            tenant_db,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/receivables/top-customers')
@router.get('/kpi/receivables/top-customers')
async def get_receivables_top_customers(
    request: Request,
    response: Response,
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:receivables_top_customers',
        params={
            'to': str(date_to),
            'branches': _normalize_list(branches),
            'q': q or '',
            'limit': limit,
            'offset': offset,
        },
        producer=lambda: receivables_top_customers(
            tenant_db,
            date_to=date_to,
            branches=branches,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/kpi/receivables/collection-trend')
@router.get('/kpi/receivables/collection-trend')
async def get_receivables_collection_trend(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:receivables_collection_trend',
        params={'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: receivables_collection_trend(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/suppliers')
@router.get('/kpi/suppliers')
async def get_suppliers(
    request: Request,
    response: Response,
    date_from_raw: str | None = Query(default=None, alias='from'),
    date_to_raw: str | None = Query(default=None, alias='to'),
    branches: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    limit_raw: str | None = Query(default=None, alias='limit'),
    offset_raw: str | None = Query(default=None, alias='offset'),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    date_from = _parse_flexible_date(
        date_from_raw,
        fallback=date.today() - timedelta(days=365),
        field_name='from',
        strict=False,
    )
    date_to = _parse_flexible_date(
        date_to_raw,
        fallback=_default_to(),
        field_name='to',
        strict=False,
    )
    limit = _parse_int_param(limit_raw, default=250, min_value=1, max_value=500)
    offset = _parse_int_param(offset_raw, default=0, min_value=0, max_value=1_000_000)
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:suppliers',
        params={
            'from': str(date_from),
            'to': str(date_to),
            'branches': _normalize_list(branches),
            'q': q or '',
            'limit': limit,
            'offset': offset,
        },
        producer=lambda: suppliers_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/kpi/cashflow/summary')
@router.get('/kpi/cashflow/summary')
@router.get('/v1/kpi/cashflows/summary')
@router.get('/kpi/cashflows/summary')
async def get_cashflow_summary(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:cashflow_summary',
        params={'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: cashflow_summary(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/cashflow/by-type')
@router.get('/kpi/cashflow/by-type')
@router.get('/v1/kpi/cashflows/by-type')
@router.get('/kpi/cashflows/by-type')
async def get_cashflow_by_type(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:cashflow_by_type',
        params={'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: cashflow_by_entry_type(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/cashflow/trend-monthly')
@router.get('/kpi/cashflow/trend-monthly')
@router.get('/v1/kpi/cashflows/trend-monthly')
@router.get('/kpi/cashflows/trend-monthly')
async def get_cashflow_trend_monthly(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:cashflow_trend_monthly',
        params={'from': str(date_from), 'to': str(date_to), 'branches': _normalize_list(branches)},
        producer=lambda: cashflow_monthly_trend(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
        ),
    )


@router.get('/v1/kpi/cashflow/documents')
async def get_cashflow_documents(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from_ytd, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    category: str | None = Query(default=None),
    branches: list[str] | None = Query(default=None),
    series: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:cashflow_documents',
        params={
            'from': str(date_from),
            'to': str(date_to),
            'category': category or '',
            'branches': _normalize_list(branches),
            'series': series or '',
            'q': q or '',
            'limit': limit,
            'offset': offset,
        },
        producer=lambda: cashflow_documents_overview(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            category=category,
            branches=branches,
            series=series,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/kpi/cashflow/documents/{document_id}/detail')
async def get_cashflow_document_detail(
    document_id: str,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    category: str | None = Query(default=None),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await cashflow_document_detail(
            tenant_db,
            document_id=document_id,
            date_from=date_from,
            date_to=date_to,
            category=category,
            branches=branches,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/cashflow/accounts')
async def get_cashflow_accounts(
    request: Request,
    response: Response,
    as_of: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:cashflow_accounts',
        params={
            'to': str(as_of),
            'branches': _normalize_list(branches),
            'q': q or '',
            'limit': limit,
            'offset': offset,
        },
        producer=lambda: cashflow_accounts_overview(
            tenant_db,
            as_of=as_of,
            branches=branches,
            q=q,
            limit=limit,
            offset=offset,
        ),
    )


@router.get('/v1/kpi/cashflow/accounts/{account_id}/detail')
async def get_cashflow_account_detail(
    account_id: str,
    as_of: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await cashflow_account_detail(
            tenant_db,
            account_id=account_id,
            as_of=as_of,
            branches=branches,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/sales/by-branch/export.csv')
async def export_sales_by_branch_csv(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    rows = await sales_by_branch(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    csv_rows = [[r['branch'], str(r['net_value']), str(r['gross_value'])] for r in rows]
    return _csv_response('sales_by_branch.csv', ['branch', 'net_value', 'gross_value'], csv_rows)


@router.get('/v1/kpi/purchases/by-supplier/export.csv')
async def export_purchases_by_supplier_csv(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    rows = await purchases_by_supplier(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    csv_rows = [[r['supplier'], str(r['net_value']), str(r['cost_amount'])] for r in rows]
    return _csv_response('purchases_by_supplier.csv', ['supplier', 'net_value', 'cost_amount'], csv_rows)


@router.get('/v1/kpi/supplier-targets/filter-options')
async def get_supplier_targets_filter_options(
    supplier_ext_id: str | None = Query(default=None),
    q: str | None = Query(default=None),
    max_items: int = Query(default=1500, ge=50, le=5000),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await supplier_target_filter_options(
        tenant_db,
        supplier_ext_id=supplier_ext_id,
        item_query=q,
        max_items=max_items,
    )


@router.get('/v1/kpi/supplier-targets')
async def get_supplier_targets(
    year: int = Query(default_factory=lambda: date.today().year, ge=2000, le=2100),
    as_of: date | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return {
        'year': year,
        'as_of': as_of.isoformat() if as_of else None,
        'targets': await list_supplier_targets(tenant_db, year=year, as_of=as_of),
    }


@router.post('/v1/kpi/supplier-targets')
async def post_supplier_target(
    payload: SupplierTargetCreateIn,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        created = await create_supplier_target(
            tenant_db,
            name=payload.name,
            supplier_ext_id=payload.supplier_ext_id,
            supplier_name=payload.supplier_name,
            target_year=payload.target_year,
            target_amount=payload.target_amount,
            rebate_percent=payload.rebate_percent,
            rebate_amount=payload.rebate_amount,
            item_external_ids=payload.item_external_ids,
            agreement_notes=payload.agreement_notes,
            notes=payload.notes,
        )
    except ValueError as exc:
        if str(exc) == 'duplicate_target':
            raise HTTPException(
                status_code=409,
                detail='Υπάρχει ήδη στόχος με ίδιο όνομα για τον ίδιο προμηθευτή και έτος.',
            ) from exc
        raise
    return {'status': 'created', **created}


@router.patch('/v1/kpi/supplier-targets/{target_id}')
async def patch_supplier_target(
    target_id: UUID,
    payload: SupplierTargetUpdateIn,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        ok = await update_supplier_target(
            tenant_db,
            target_id=target_id,
            name=payload.name,
            supplier_ext_id=payload.supplier_ext_id,
            supplier_name=payload.supplier_name,
            target_year=payload.target_year,
            target_amount=payload.target_amount,
            rebate_percent=payload.rebate_percent,
            rebate_amount=payload.rebate_amount,
            item_external_ids=payload.item_external_ids,
            agreement_notes=payload.agreement_notes,
            notes=payload.notes,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        if str(exc) == 'duplicate_target':
            raise HTTPException(
                status_code=409,
                detail='Υπάρχει ήδη στόχος με ίδιο όνομα για τον ίδιο προμηθευτή και έτος.',
            ) from exc
        if str(exc) == 'target_locked':
            raise HTTPException(
                status_code=409,
                detail='Η συμφωνία είναι ανενεργή λόγω λήξης περιόδου και δεν επιτρέπεται επεξεργασία.',
            ) from exc
        raise
    if not ok:
        raise HTTPException(status_code=404, detail='Supplier target not found')
    return {'status': 'updated', 'id': str(target_id)}


@router.post('/v1/kpi/supplier-targets/{target_id}/clone')
async def post_supplier_target_clone(
    target_id: UUID,
    payload: SupplierTargetCloneIn,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        cloned = await clone_supplier_target(
            tenant_db,
            target_id=target_id,
            target_year=payload.target_year,
            name=payload.name,
        )
    except ValueError as exc:
        if str(exc) == 'duplicate_target':
            raise HTTPException(
                status_code=409,
                detail='Υπάρχει ήδη στόχος με ίδιο όνομα για τον ίδιο προμηθευτή και έτος.',
            ) from exc
        raise
    if not cloned:
        raise HTTPException(status_code=404, detail='Supplier target not found')
    return {'status': 'cloned', **cloned}


@router.delete('/v1/kpi/supplier-targets/{target_id}')
async def delete_supplier_target_endpoint(
    target_id: UUID,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    ok = await delete_supplier_target(
        tenant_db,
        target_id=target_id,
    )
    if not ok:
        raise HTTPException(status_code=404, detail='Supplier target not found')
    return {'status': 'deleted', 'id': str(target_id)}


@router.get('/v1/kpi/price-control/filter-options')
async def get_price_control_filter_options(
    supplier_ext_id: str | None = Query(default=None),
    target_id: str | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await price_control_filter_options(
        tenant_db,
        supplier_ext_id=supplier_ext_id,
        target_id=target_id,
    )


@router.get('/v1/kpi/price-control/items')
async def get_price_control_items(
    request: Request,
    date_from: date = Query(default_factory=lambda: date(date.today().year, 1, 1), alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    supplier_ext_id: str | None = Query(default=None),
    target_id: str | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    item_codes: list[str] | None = Query(default=None),
    target_margin_pct: float = Query(default=35.0, ge=0, le=95),
    discount_pct: float = Query(default=0.0, ge=0, le=99),
    price_position: str | None = Query(default=None, pattern='^(above|below|)$'),
    limit: int = Query(default=500, ge=1, le=2000),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await price_control_items(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        supplier_ext_id=supplier_ext_id,
        target_id=target_id,
        categories=categories,
        groups=groups,
        item_codes=item_codes,
        target_margin_pct=target_margin_pct,
        discount_pct=discount_pct,
        price_position=price_position,
        limit=limit,
        price_margin_targets=_tenant_price_margin_targets_from_request(request),
    )


@router.get('/v1/kpi/era-exploration')
async def get_era_exploration_report(
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    q: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    category: str | None = Query(default=None),
    assortment_status: str | None = Query(default=None),
    price_position: str | None = Query(default=None),
    sort: str = Query(default='market_sales'),
    direction: str = Query(default='desc', pattern='^(asc|desc)$'),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=10, le=500),
):
    try:
        return await era_exploration_report(
            tenant,
            tenant_db,
            q=q,
            brand=brand,
            category=category,
            assortment_status=assortment_status,
            price_position=price_position,
            sort=sort,
            direction=direction,
            page=page,
            page_size=page_size,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/iqvia')
async def get_iqvia_report(
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
    q: str | None = Query(default=None),
    category: str | None = Query(default=None),
    manufacturer: str | None = Query(default=None),
    territory: str | None = Query(default=None),
    atc3: str | None = Query(default=None),
    otc3: str | None = Query(default=None),
    period: str | None = Query(default=None),
    sort: str = Query(default='values'),
    direction: str = Query(default='desc', pattern='^(asc|desc)$'),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=10, le=500),
):
    try:
        return await iqvia_report(
            tenant,
            tenant_db,
            q=q,
            category=category,
            manufacturer=manufacturer,
            territory=territory,
            atc3=atc3,
            otc3=otc3,
            period=period,
            sort=sort,
            direction=direction,
            page=page,
            page_size=page_size,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/business-advisor')
async def get_business_advisor_report(
    request: Request,
    response: Response,
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    target_margin_pct: float = Query(default=35.0, ge=0, le=95),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    params = _kpi_filter_cache_params(
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    params['target_margin_pct'] = round(float(target_margin_pct), 2)
    params['drilldown_version'] = 2
    params['price_margin_targets'] = _tenant_price_margin_targets_from_request(request)
    params['business_advisor_targets'] = _tenant_business_advisor_targets_from_request(request)
    params['fulfillment'] = _tenant_eshop_fulfillment_from_request(request)
    return await _cached_kpi_response(
        request=request,
        response=response,
        namespace='dashboard:business_advisor',
        params=params,
        ttl_seconds=1800,
        producer=lambda: business_advisor_report(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            branches=_clean_query_list(branches),
            warehouses=_clean_query_list(warehouses),
            brands=_clean_query_list(brands),
            categories=_clean_query_list(categories),
            groups=_clean_query_list(groups),
            target_margin_pct=target_margin_pct,
            price_margin_targets=_tenant_price_margin_targets_from_request(request),
            inventory_coverage_target_days=int(
                _tenant_business_advisor_targets_from_request(request).get('inventory_coverage_days') or 60
            ),
            fulfillment_config=_tenant_eshop_fulfillment_from_request(request),
        ),
    )
