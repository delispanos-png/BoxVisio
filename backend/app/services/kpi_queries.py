from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from uuid import UUID

from sqlalchemy import Date, Integer, String, and_, case, cast, func, literal, literal_column, not_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import over

from app.services.intelligence_service import list_recent_insights
from app.services.kpi_participation_scope import get_current_sales_kpi_participation_config
from app.services.request_scope import get_allowed_branch_scope
from app.models.tenant import (
    AggPurchasesDaily,
    AggPurchasesMonthly,
    AggSalesDaily,
    AggSalesDailyCompany,
    AggSalesDailyBranch,
    AggSalesMonthly,
    AggInventorySnapshotDaily,
    AggStockAging,
    AggCashDaily,
    AggCashByType,
    AggCashAccounts,
    AggExpensesDaily,
    AggExpensesMonthly,
    AggExpensesByCategoryDaily,
    AggExpensesByBranchDaily,
    AggCustomerBalancesDaily,
    DimBranch,
    DimBrand,
    DimAccount,
    DimCategory,
    DimCustomer,
    DimExpenseCategory,
    DimGroup,
    DimItem,
    DimSupplier,
    DimWarehouse,
    AggSupplierBalancesDaily,
    FactCashflow,
    FactCustomerBalance,
    FactExpense,
    FactInventory,
    FactPurchases,
    FactSales,
    FactSupplierBalance,
    SupplierTarget,
    SupplierTargetItem,
)

_DEFAULT_INVENTORY_ITEM_CLASSIFICATION = {
    'status_source': 'sales_window',
    'active_last_sale_days': 60,
    'fast_sales_qty_30d_min': 50,
    'slow_sales_qty_30d_max': 5,
}


def normalize_inventory_item_classification_config(raw: dict | None) -> dict[str, object]:
    source = raw if isinstance(raw, dict) else {}
    status_source_raw = str(source.get('status_source') or '').strip().lower()
    status_source = 'softone' if status_source_raw in {'softone', 'source', 'source_flag'} else 'sales_window'

    def _int_value(key: str, default: int, min_value: int, max_value: int) -> int:
        val = source.get(key, default)
        try:
            parsed = int(str(val).strip())
        except Exception:
            parsed = int(default)
        return max(min_value, min(max_value, parsed))

    active_days = _int_value(
        'active_last_sale_days',
        _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['active_last_sale_days'],
        1,
        3650,
    )
    fast_min = _int_value(
        'fast_sales_qty_30d_min',
        _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['fast_sales_qty_30d_min'],
        1,
        1_000_000,
    )
    slow_max = _int_value(
        'slow_sales_qty_30d_max',
        _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['slow_sales_qty_30d_max'],
        0,
        1_000_000,
    )
    if slow_max >= fast_min:
        slow_max = max(0, fast_min - 1)

    return {
        'status_source': status_source,
        'active_last_sale_days': active_days,
        'fast_sales_qty_30d_min': fast_min,
        'slow_sales_qty_30d_max': slow_max,
    }


def _classify_inventory_item(
    *,
    as_of: date,
    last_sale_date: date | None,
    sales_qty_30: float,
    config: dict[str, object],
    is_active_source: bool | None = None,
) -> tuple[str, str]:
    status_source = str(config.get('status_source') or 'sales_window').strip().lower()
    active_days = int(config.get('active_last_sale_days') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['active_last_sale_days'])
    fast_min = int(config.get('fast_sales_qty_30d_min') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['fast_sales_qty_30d_min'])
    slow_max = int(config.get('slow_sales_qty_30d_max') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['slow_sales_qty_30d_max'])

    if status_source == 'softone' and is_active_source is not None:
        is_active = bool(is_active_source)
    else:
        is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=active_days)))
    movement = 'fast' if sales_qty_30 >= fast_min else ('slow' if sales_qty_30 <= slow_max else 'normal')
    return ('active' if is_active else 'inactive'), movement


def _date_range(column, date_from: date, date_to: date):
    return column >= date_from, column <= date_to


def _clean_item_name(name: str | None, fallback: str | None = None) -> str:
    raw = str(name or fallback or '').strip()
    if not raw:
        return 'N/A'
    cleaned = raw
    if cleaned.endswith(')') and '(ITM' in cleaned.upper():
        left = cleaned.rfind('(ITM')
        if left >= 0:
            cleaned = cleaned[:left].strip()
    parts = cleaned.split()
    if parts and parts[-1].upper().startswith('ITM') and parts[-1][3:].isdigit():
        cleaned = ' '.join(parts[:-1]).strip()
    return cleaned or raw


def _raw_scalar(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return value


def _append_raw_field(raw_fields, key: str, value, label: str | None = None):
    raw_fields.append({'key': key, 'label': label or key, 'value': _raw_scalar(value)})


def _append_model_raw_fields(raw_fields, prefix: str, model_instance):
    if model_instance is None:
        return
    for column in model_instance.__table__.columns:
        _append_raw_field(raw_fields, f'{prefix}.{column.name}', getattr(model_instance, column.name))


def _normalize_payload_key(value: str) -> str:
    return ''.join(ch for ch in str(value or '').lower() if ch.isalnum())


def _payload_value(payload: dict | None, *aliases: str):
    if not isinstance(payload, dict):
        return None
    normalized = {_normalize_payload_key(k): v for k, v in payload.items()}
    for alias in aliases:
        val = normalized.get(_normalize_payload_key(alias))
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        return val
    return None


def _payload_text(payload: dict | None, *aliases: str, fallback: str = '') -> str:
    val = _payload_value(payload, *aliases)
    if val is None:
        return fallback
    txt = str(val).strip()
    return txt or fallback


def _payload_float(payload: dict | None, *aliases: str) -> float | None:
    val = _payload_value(payload, *aliases)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _payload_bool(payload: dict | None, *aliases: str) -> bool | None:
    val = _payload_value(payload, *aliases)
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    txt = str(val).strip().lower()
    if txt in {'1', 'true', 'yes', 'y', 'on', 'ναι'}:
        return True
    if txt in {'0', 'false', 'no', 'n', 'off', 'οχι', 'όχι'}:
        return False
    return None


def _split_softone_codes(raw: str | None) -> list[str]:
    txt = str(raw or '').strip()
    if not txt:
        return []
    values: list[str] = []
    seen: set[str] = set()
    for token in txt.replace('\n', ',').replace(';', ',').split(','):
        cleaned = str(token or '').strip()
        if not cleaned or cleaned in {'-', '0', 'N/A', 'n/a', 'null', 'NULL'}:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(cleaned)
    return values


def _resolve_line_discount(
    payload: dict | None,
    *,
    net_value: float,
    fallback_pct: float | None = None,
    fallback_amount: float | None = None,
) -> tuple[float, float]:
    pct_from_payload = _payload_float(
        payload,
        'discount_pct',
        'discount_percent',
        'disc_pct',
        'line_discount_pct',
        'discprc',
        'disc1prc',
        'disc2prc',
        'disc3prc',
        'disc4prc',
        'ekpt_pct',
        'ekptosi_pct',
    )
    amount_from_payload = _payload_float(
        payload,
        'discount_amount',
        'discount_value',
        'disc_amount',
        'discamnt',
        'disc1val',
        'disc2val',
        'disc3val',
        'disc4val',
        'line_discount',
        'line_discount_amount',
        'ekpt_value',
        'ekptosi_value',
    )
    _ = net_value
    discount_pct = pct_from_payload
    discount_amount = amount_from_payload

    if discount_pct is None and fallback_pct is not None:
        discount_pct = float(fallback_pct)
    if discount_amount is None and fallback_amount is not None:
        discount_amount = float(fallback_amount)

    discount_pct = max(0.0, min(100.0, float(discount_pct or 0.0)))
    discount_amount = max(0.0, float(discount_amount or 0.0))
    return discount_pct, discount_amount


_GREEK_TONOS_SRC = 'άέήίϊΐόύϋΰώΆΈΉΊΪΌΎΫΏ'
_GREEK_TONOS_DST = 'αεηιιιουυυωαεηιιουυω'


def _normalize_search_term(value: str | None) -> str:
    raw = str(value or '').strip().lower()
    if not raw:
        return ''
    return raw.translate(str.maketrans(_GREEK_TONOS_SRC, _GREEK_TONOS_DST))


def _sql_normalized_text(expr):
    return func.translate(func.lower(cast(func.coalesce(expr, literal('')), String)), _GREEK_TONOS_SRC, _GREEK_TONOS_DST)


def _payload_code_name(
    payload: dict | None,
    code_aliases: list[str],
    name_aliases: list[str],
    fallback: str = '',
) -> str:
    code = _payload_text(payload, *code_aliases, fallback='')
    name = _payload_text(payload, *name_aliases, fallback='')
    if code and name and code.lower() != name.lower():
        return f'{code} {name}'
    return name or code or fallback


def _effective_branch_filter(branches: list[str] | tuple[str, ...] | None):
    allowed_scope = get_allowed_branch_scope()
    requested: list[str] | None
    if branches is None:
        requested = None
    else:
        requested = []
        seen: set[str] = set()
        for raw in branches:
            value = str(raw or '').strip()
            if not value or value in seen:
                continue
            seen.add(value)
            requested.append(value)

    if allowed_scope is None:
        if requested is not None and len(requested) == 0:
            return None
        return requested

    allowed = list(allowed_scope)
    allowed_set = set(allowed)
    if not requested:
        return allowed

    intersection = [code for code in requested if code in allowed_set]
    return intersection


def _apply_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggSalesDaily.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggSalesDaily.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggSalesDaily.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggSalesDaily.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggSalesDaily.group_ext_id.in_(groups))
    return stmt


def _apply_purchase_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggPurchasesDaily.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggPurchasesDaily.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggPurchasesDaily.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggPurchasesDaily.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggPurchasesDaily.group_ext_id.in_(groups))
    return stmt


def _apply_fact_purchase_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        brand_item_codes = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(or_(DimBrand.external_id.in_(brands), DimBrand.name.in_(brands)))
        )
        inventory_brand_item_codes = (
            select(FactInventory.item_code)
            .where(FactInventory.item_code.is_not(None))
            .where(
                or_(
                    FactInventory.source_payload_json['brand_external_id'].astext.in_(brands),
                    FactInventory.source_payload_json['brand_name'].astext.in_(brands),
                )
            )
        )
        stmt = stmt.where(
            or_(
                FactPurchases.brand_ext_id.in_(brands),
                FactPurchases.item_code.in_(brand_item_codes),
                FactPurchases.item_code.in_(inventory_brand_item_codes),
            )
        )
    if categories:
        stmt = stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactPurchases.group_ext_id.in_(groups))
    return stmt


def _fact_purchase_signed_discount_expr():
    discount_total = (
        func.coalesce(FactPurchases.discount_amount, 0)
        + func.coalesce(FactPurchases.discount1_amount, 0)
        + func.coalesce(FactPurchases.discount2_amount, 0)
        + func.coalesce(FactPurchases.discount3_amount, 0)
    )
    abs_discount = func.abs(discount_total)
    return case((func.coalesce(FactPurchases.net_value, 0) < 0, -abs_discount), else_=abs_discount)


def _apply_sales_monthly_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggSalesMonthly.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggSalesMonthly.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggSalesMonthly.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggSalesMonthly.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggSalesMonthly.group_ext_id.in_(groups))
    return stmt


def _apply_purchase_monthly_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggPurchasesMonthly.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggPurchasesMonthly.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggPurchasesMonthly.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggPurchasesMonthly.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggPurchasesMonthly.group_ext_id.in_(groups))
    return stmt


def _apply_expense_filters(stmt, branches=None, categories=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggExpensesDaily.branch_ext_id.in_(branches))
    if categories:
        stmt = stmt.where(AggExpensesDaily.expense_category_code.in_(categories))
    return stmt


def _apply_fact_expense_filters(stmt, branches=None, categories=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactExpense.branch_ext_id.in_(branches))
    if categories:
        stmt = stmt.where(FactExpense.expense_category_code.in_(categories))
    return stmt


def _softone_clean_dimension_text(column):
    text_col = func.btrim(cast(column, String))
    return func.nullif(func.nullif(func.nullif(text_col, ''), '0'), '-')


def _item_category_path_expr():
    c1 = func.coalesce(_softone_clean_dimension_text(DimItem.category_1), literal('N/A'))
    c2 = func.coalesce(_softone_clean_dimension_text(DimItem.category_2), c1)
    c3 = func.coalesce(_softone_clean_dimension_text(DimItem.category_3), c2)
    return func.concat(c1, literal(' > '), c2, literal(' > '), c3)


def _apply_fact_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None, channels=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactSales.group_ext_id.in_(groups))
    if channels:
        stmt = stmt.where(FactSales.channel_ext_id.in_(channels))
    return stmt


def _parse_rule_date(value: object) -> date | None:
    txt = str(value or '').strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


def _normalize_sales_turnover_rule(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    series = str(raw.get('series') or raw.get('document_series') or '').strip()
    if not series:
        return None
    include_turnover_raw = raw.get('include_turnover')
    exclude_raw = raw.get('exclude')
    enabled_raw = raw.get('enabled')
    branch_ext_ids_raw = raw.get('branch_ext_ids') or raw.get('branches') or []
    branch_ext_ids = []
    if isinstance(branch_ext_ids_raw, list):
        seen: set[str] = set()
        for item in branch_ext_ids_raw:
            code = str(item or '').strip()
            if not code or code in seen:
                continue
            seen.add(code)
            branch_ext_ids.append(code)
    include_turnover = True
    if include_turnover_raw is not None:
        include_turnover = bool(include_turnover_raw)
    elif exclude_raw is not None:
        include_turnover = not bool(exclude_raw)
    enabled = bool(enabled_raw) if enabled_raw is not None else True
    return {
        'series': series,
        'include_turnover': include_turnover,
        'enabled': enabled,
        'date_from': _parse_rule_date(raw.get('date_from')),
        'date_to': _parse_rule_date(raw.get('date_to')),
        'branch_ext_ids': branch_ext_ids,
    }


def _sales_turnover_rules() -> list[dict]:
    raw = get_current_sales_kpi_participation_config()
    raw_rules = raw.get('series_rules') if isinstance(raw, dict) else []
    if not isinstance(raw_rules, list):
        return []
    normalized: list[dict] = []
    for item in raw_rules:
        row = _normalize_sales_turnover_rule(item)
        if row is not None:
            normalized.append(row)
    return normalized


def _has_sales_turnover_series_rules() -> bool:
    return bool(_sales_turnover_rules())


def _sales_behavior_codes() -> list[int]:
    raw = get_current_sales_kpi_participation_config()
    raw_codes = raw.get('include_behavior_codes') if isinstance(raw, dict) else []
    if not isinstance(raw_codes, list):
        return []
    out: list[int] = []
    seen: set[int] = set()
    for item in raw_codes:
        try:
            code = int(str(item).strip())
        except (TypeError, ValueError):
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _sales_behavior_sign_map(map_key: str) -> dict[int, float]:
    raw = get_current_sales_kpi_participation_config()
    raw_map = raw.get(map_key) if isinstance(raw, dict) else {}
    out: dict[int, float] = {}
    if not isinstance(raw_map, dict):
        return out
    for k, v in raw_map.items():
        try:
            code = int(str(k).strip())
            sign = float(v)
        except (TypeError, ValueError):
            continue
        if sign == 0:
            continue
        out[code] = sign
    return out


def _fact_sales_behavior_code_expr():
    return cast(FactSales.source_payload_json['source_transaction_type_id'].astext, Integer)


def _fact_sales_behavior_sign_expr(*, quantity: bool):
    sign_map = _sales_behavior_sign_map('quantity_sign_by_behavior' if quantity else 'amount_sign_by_behavior')
    if not sign_map:
        return literal(1.0)
    behavior_code = _fact_sales_behavior_code_expr()
    whens = [(behavior_code == int(code), float(sign)) for code, sign in sign_map.items()]
    return case(*whens, else_=literal(1.0))


def _has_sales_behavior_rules() -> bool:
    return bool(
        _sales_behavior_codes()
        or _sales_behavior_sign_map('amount_sign_by_behavior')
        or _sales_behavior_sign_map('quantity_sign_by_behavior')
    )


def _apply_fact_sales_behavior_rules(stmt):
    codes = _sales_behavior_codes()
    if not codes:
        return stmt
    behavior_code = _fact_sales_behavior_code_expr()
    return stmt.where(behavior_code.in_(codes))


def _normalize_sales_branch_adjustment(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    branch_ext_id = str(raw.get('branch_ext_id') or raw.get('branch') or '').strip()
    if not branch_ext_id:
        return None
    try:
        delta_net_value = float(raw.get('delta_net_value') or raw.get('net_delta') or 0.0)
    except (TypeError, ValueError):
        return None
    return {
        'branch_ext_id': branch_ext_id,
        'delta_net_value': delta_net_value,
        'date_from': _parse_rule_date(raw.get('date_from')),
        'date_to': _parse_rule_date(raw.get('date_to')),
    }


def _sales_branch_adjustments_for_range(date_from: date, date_to: date) -> list[dict]:
    raw = get_current_sales_kpi_participation_config()
    raw_adjustments = raw.get('branch_adjustments') if isinstance(raw, dict) else []
    if not isinstance(raw_adjustments, list):
        return []
    normalized: list[dict] = []
    for item in raw_adjustments:
        row = _normalize_sales_branch_adjustment(item)
        if row is None:
            continue
        adj_from = row.get('date_from')
        adj_to = row.get('date_to')
        if isinstance(adj_from, date) and adj_from != date_from:
            continue
        if isinstance(adj_to, date) and adj_to != date_to:
            continue
        normalized.append(row)
    return normalized


def _apply_fact_sales_turnover_rules(stmt):
    exclusion_predicates = []
    for rule in _sales_turnover_rules():
        if not bool(rule.get('enabled', True)):
            continue
        if bool(rule.get('include_turnover', True)):
            continue
        predicate = FactSales.document_series == str(rule.get('series') or '').strip()
        date_from = rule.get('date_from')
        date_to = rule.get('date_to')
        branch_ext_ids = rule.get('branch_ext_ids') or []
        if isinstance(date_from, date):
            predicate = and_(predicate, FactSales.doc_date >= date_from)
        if isinstance(date_to, date):
            predicate = and_(predicate, FactSales.doc_date <= date_to)
        if branch_ext_ids:
            predicate = and_(predicate, FactSales.branch_ext_id.in_(branch_ext_ids))
        exclusion_predicates.append(predicate)
    if exclusion_predicates:
        stmt = stmt.where(not_(or_(*exclusion_predicates)))
    return stmt


def _apply_fact_purchases_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        dim_brand_item_codes = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(or_(DimBrand.external_id.in_(brands), DimBrand.name.in_(brands)))
        )
        brand_item_codes = (
            select(FactInventory.item_code)
            .where(FactInventory.item_code.is_not(None))
            .where(
                or_(
                    FactInventory.source_payload_json['brand_external_id'].astext.in_(brands),
                    FactInventory.source_payload_json['brand_name'].astext.in_(brands),
                )
            )
        )
        stmt = stmt.where(
            or_(
                FactPurchases.brand_ext_id.in_(brands),
                FactPurchases.item_code.in_(dim_brand_item_codes),
                FactPurchases.item_code.in_(brand_item_codes),
            )
        )
    if categories:
        category_path = _item_category_path_expr()
        category_item_codes = (
            select(DimItem.external_id)
            .where(DimItem.external_id.is_not(None))
            .where(category_path.in_(categories))
        )
        stmt = stmt.where(or_(FactPurchases.category_ext_id.in_(categories), FactPurchases.item_code.in_(category_item_codes)))
    if groups:
        group_item_codes = (
            select(FactInventory.item_code)
            .where(FactInventory.item_code.is_not(None))
            .where(FactInventory.source_payload_json['group_external_id'].astext.in_(groups))
        )
        stmt = stmt.where(or_(FactPurchases.group_ext_id.in_(groups), FactPurchases.item_code.in_(group_item_codes)))
    return stmt


def _fact_purchases_document_key_expr():
    explicit_doc_id = func.nullif(func.btrim(cast(func.coalesce(FactPurchases.document_id, literal('')), String)), '')

    # Try to infer a document key from line-level external/event ids first.
    # Fallback is a coarse grouping key to avoid one-document-per-line behavior.
    ext_token = func.nullif(
        func.substring(
            cast(func.coalesce(FactPurchases.external_id, literal('')), String),
            '^(.*?)(?:[_-]ITM[[:alnum:]-]+(?:[_-]EV)?$)',
        ),
        '',
    )
    event_token = func.nullif(
        func.substring(
            cast(func.coalesce(FactPurchases.event_id, literal('')), String),
            '^(.*?)(?:[_-]ITM[[:alnum:]-]+(?:[_-]EV)?$)',
        ),
        '',
    )
    coarse_key = func.concat(
        cast(FactPurchases.doc_date, String),
        '|',
        func.coalesce(FactPurchases.branch_ext_id, literal('')),
        '|',
        func.coalesce(FactPurchases.warehouse_ext_id, literal('')),
        '|',
        func.coalesce(FactPurchases.supplier_ext_id, literal('')),
    )
    return func.coalesce(explicit_doc_id, ext_token, event_token, coarse_key)


def _fact_purchases_document_no_expr(doc_key_expr):
    explicit_no = func.nullif(func.btrim(cast(func.coalesce(FactPurchases.document_no, literal('')), String)), '')
    explicit_doc_id = func.nullif(func.btrim(cast(func.coalesce(FactPurchases.document_id, literal('')), String)), '')

    # Prefer explicit document number encoded by imports/connectors.
    event_doc_no = func.nullif(
        func.substring(
            cast(func.coalesce(FactPurchases.event_id, literal('')), String),
            '^PURDOC\\|([^|]+)\\|.*$',
        ),
        '',
    )
    external_doc_no = func.nullif(
        func.substring(
            cast(func.coalesce(FactPurchases.external_id, literal('')), String),
            '^PURDOC\\|([^|]+)\\|.*$',
        ),
        '',
    )
    item_doc_no = func.nullif(func.btrim(cast(func.coalesce(FactPurchases.item_code, literal('')), String)), '')
    return func.coalesce(explicit_no, explicit_doc_id, event_doc_no, external_doc_no, item_doc_no, doc_key_expr)


def _purchase_document_no_from_fact(fact: FactPurchases, doc_id: str) -> str:
    explicit_no = str(fact.document_no or '').strip()
    if explicit_no:
        return explicit_no
    explicit_id = str(fact.document_id or '').strip()
    if explicit_id:
        return explicit_id

    for raw in (fact.event_id, fact.external_id):
        txt = str(raw or '').strip()
        if not txt:
            continue
        m = re.match(r'^PURDOC\|([^|]+)\|', txt)
        if m and str(m.group(1) or '').strip():
            return str(m.group(1)).strip()

    item_code = str(fact.item_code or '').strip()
    if item_code:
        return item_code

    # Hide technical composite keys from UI when no clean doc number exists.
    if '|BRX_' in doc_id or '|WHX_' in doc_id or '|SUPX_' in doc_id:
        return '-'
    return doc_id or '-'


def _fact_inventory_document_key_expr():
    explicit_doc_id = func.nullif(func.btrim(cast(func.coalesce(FactInventory.document_id, literal('')), String)), '')

    # Inventory rows are usually line-level. Extract a document token when external_id
    # contains item suffix, otherwise fallback to a coarse doc grouping key.
    ext_token = func.nullif(
        func.substring(
            cast(func.coalesce(FactInventory.external_id, literal('')), String),
            '^(.*?)(?:[_-]ITM[[:alnum:]_:-]+)$',
        ),
        '',
    )
    coarse_key = func.concat(
        cast(FactInventory.doc_date, String),
        '|',
        func.coalesce(FactInventory.branch_ext_id, literal('')),
        '|',
        func.coalesce(FactInventory.warehouse_ext_id, literal('')),
        '|',
        func.coalesce(FactInventory.document_type, literal('')),
    )
    return func.coalesce(explicit_doc_id, ext_token, coarse_key)


def _sales_customer_key_expr():
    customer_code = func.nullif(func.btrim(cast(func.coalesce(FactSales.customer_code, literal('')), String)), '')
    customer_name = func.nullif(func.btrim(cast(func.coalesce(FactSales.customer_name, literal('')), String)), '')
    return func.coalesce(customer_code, customer_name, cast(FactSales.external_id, String))


def _customer_balance_key_expr():
    customer_ext = func.nullif(func.btrim(cast(func.coalesce(FactCustomerBalance.customer_ext_id, literal('')), String)), '')
    customer_name = func.nullif(func.btrim(cast(func.coalesce(FactCustomerBalance.customer_name, literal('')), String)), '')
    return func.coalesce(customer_ext, customer_name, cast(FactCustomerBalance.external_id, String))


def _customer_profile_from_fact(fact: FactSales | None) -> dict:
    if fact is None:
        return {
            'customer_code': '',
            'customer_name': 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ',
            'afm': '',
            'amka': '',
            'profession': '',
            'vat_status': '',
            'payment_method': '',
            'address': '',
            'city': '',
            'area': '',
            'zip': '',
            'phone_1': '',
            'phone_2': '',
            'email': '',
            'carrier_name': '',
            'reason': '',
            'notes_1': '',
            'notes_2': '',
            'is_active': True,
            'balance': 0.0,
            'updated_at': None,
        }

    payload = fact.source_payload_json if isinstance(fact.source_payload_json, dict) else {}
    payment_method = _payload_code_name(
        payload,
        ['payment_code', 'payment_method_code', 'payment_type_code'],
        ['payment_name', 'payment_method', 'payment_type', 'payment_mode'],
        fallback=str(fact.payment_method or ''),
    )
    vat_status = _payload_text(
        payload,
        'vat_status',
        'fpa_status',
        'tax_status',
        'vat_category',
        'fpa_category',
        fallback='',
    )
    active_raw = _payload_text(payload, 'is_active', 'active', 'enabled', fallback='')
    if active_raw:
        normalized = active_raw.strip().lower()
        is_active = normalized in {'1', 'true', 'yes', 'y', 'ναι', 'nai'}
    else:
        is_active = True

    balance = _payload_float(
        payload,
        'customer_balance',
        'balance',
        'remaining_balance',
        'outstanding',
        'ypoloipo',
    )

    return {
        'customer_code': str(fact.customer_code or ''),
        'customer_name': str(fact.customer_name or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
        'afm': _payload_text(payload, 'customer_afm', 'afm', 'vat_no', 'vat_number', fallback=''),
        'amka': _payload_text(payload, 'customer_amka', 'amka', fallback=''),
        'profession': _payload_text(payload, 'customer_profession', 'profession', 'occupation', fallback=''),
        'vat_status': vat_status,
        'payment_method': payment_method,
        'address': _payload_text(payload, 'customer_address', 'address', 'delivery_address', fallback=str(fact.delivery_address or '')),
        'city': _payload_text(payload, 'customer_city', 'city', 'delivery_city', fallback=str(fact.delivery_city or '')),
        'area': _payload_text(payload, 'customer_area', 'area', 'delivery_area', fallback=str(fact.delivery_area or '')),
        'zip': _payload_text(payload, 'customer_zip', 'zip', 'postal_code', fallback=str(fact.delivery_zip or '')),
        'phone_1': _payload_text(payload, 'customer_phone', 'phone', 'telephone1', 'phone1', fallback=''),
        'phone_2': _payload_text(payload, 'customer_phone2', 'telephone2', 'phone2', 'mobile', fallback=''),
        'email': _payload_text(payload, 'customer_email', 'email', fallback=''),
        'carrier_name': _payload_text(payload, 'carrier_name', 'transport_company', fallback=str(fact.carrier_name or '')),
        'reason': str(fact.reason or ''),
        'notes_1': str(fact.notes or ''),
        'notes_2': str(fact.notes_2 or ''),
        'is_active': is_active,
        'balance': float(balance or 0.0),
        'updated_at': _raw_scalar(fact.source_updated_at or fact.updated_at),
    }


async def _latest_customer_balances_map(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
    customer_ids: list[str] | None = None,
    aggregate_only: bool = False,
) -> dict[str, dict[str, object]]:
    if customer_ids is not None and len(customer_ids) == 0:
        return {}

    agg_has_rows = (await db.execute(select(AggCustomerBalancesDaily.balance_date).limit(1))).first() is not None
    if agg_has_rows:
        agg_key_expr = func.nullif(
            func.btrim(cast(func.coalesce(AggCustomerBalancesDaily.customer_ext_id, literal('')), String)),
            '',
        )
        agg_snapshots_stmt = (
            select(
                agg_key_expr.label('customer_id'),
                func.coalesce(func.max(AggCustomerBalancesDaily.customer_ext_id), literal('')).label('customer_code'),
                func.coalesce(func.max(AggCustomerBalancesDaily.customer_ext_id), literal('')).label('customer_name'),
                AggCustomerBalancesDaily.balance_date.label('balance_date'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
                literal(None).label('last_collection_date'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
                func.max(AggCustomerBalancesDaily.updated_at).label('updated_at'),
            )
            .select_from(AggCustomerBalancesDaily)
            .where(AggCustomerBalancesDaily.balance_date <= as_of)
            .where(agg_key_expr.is_not(None))
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            agg_snapshots_stmt = agg_snapshots_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
        if customer_ids:
            agg_snapshots_stmt = agg_snapshots_stmt.where(agg_key_expr.in_(customer_ids))

        agg_snapshots = agg_snapshots_stmt.group_by(agg_key_expr, AggCustomerBalancesDaily.balance_date).subquery(
            'customer_agg_balances_by_day'
        )
        agg_ranked = (
            select(
                agg_snapshots.c.customer_id,
                agg_snapshots.c.customer_code,
                agg_snapshots.c.customer_name,
                agg_snapshots.c.balance_date,
                agg_snapshots.c.open_balance,
                agg_snapshots.c.overdue_balance,
                agg_snapshots.c.aging_bucket_0_30,
                agg_snapshots.c.aging_bucket_31_60,
                agg_snapshots.c.aging_bucket_61_90,
                agg_snapshots.c.aging_bucket_90_plus,
                agg_snapshots.c.last_collection_date,
                agg_snapshots.c.trend_vs_previous,
                agg_snapshots.c.updated_at,
                over(
                    func.row_number(),
                    partition_by=agg_snapshots.c.customer_id,
                    order_by=agg_snapshots.c.balance_date.desc(),
                ).label('rn'),
            )
        ).subquery('customer_agg_balances_ranked')

        agg_latest_rows = (await db.execute(select(agg_ranked).where(agg_ranked.c.rn == 1))).mappings().all()
        if agg_latest_rows:
            if aggregate_only:
                # caller only needs balance totals — skip name-resolution queries entirely
                return {
                    str(row.get('customer_id') or '').strip(): dict(row)
                    for row in agg_latest_rows
                    if str(row.get('customer_id') or '').strip()
                }
            customer_ids = list(
                dict.fromkeys(
                    str(row.get('customer_id') or '').strip()
                    for row in agg_latest_rows
                    if str(row.get('customer_id') or '').strip()
                )
            )
            dim_customer_map: dict[str, dict[str, str]] = {}
            if customer_ids:
                dim_rows = []
                for idx in range(0, len(customer_ids), 5000):
                    batch = customer_ids[idx : idx + 5000]
                    dim_rows.extend(
                        (
                            await db.execute(
                                select(
                                    DimCustomer.external_id.label('customer_id'),
                                    DimCustomer.customer_code.label('customer_code'),
                                    DimCustomer.name.label('customer_name'),
                                ).where(DimCustomer.external_id.in_(batch))
                            )
                        ).mappings().all()
                    )
                dim_customer_map = {
                    str(r.get('customer_id') or '').strip(): {
                        'customer_code': str(r.get('customer_code') or '').strip(),
                        'customer_name': str(r.get('customer_name') or '').strip(),
                    }
                    for r in dim_rows
                    if str(r.get('customer_id') or '').strip()
                }

            sales_profile_map: dict[str, dict[str, str]] = {}
            unresolved_ids = [cid for cid in customer_ids if cid and cid not in dim_customer_map]
            if unresolved_ids:
                sales_key_expr = _sales_customer_key_expr()
                from_sales_by_key = []
                for idx in range(0, len(unresolved_ids), 5000):
                    batch = unresolved_ids[idx : idx + 5000]
                    from_sales_by_key.extend(
                        (
                            await db.execute(
                                select(
                                    sales_key_expr.label('customer_id'),
                                    func.coalesce(func.max(FactSales.customer_code), literal('')).label('customer_code'),
                                    func.coalesce(func.max(FactSales.customer_name), literal('')).label('customer_name'),
                                )
                                .where(sales_key_expr.in_(batch))
                                .group_by(sales_key_expr)
                            )
                        ).mappings().all()
                    )
                for row in from_sales_by_key:
                    cid = str(row.get('customer_id') or '').strip()
                    if not cid:
                        continue
                    sales_profile_map[cid] = {
                        'customer_code': str(row.get('customer_code') or '').strip(),
                        'customer_name': str(row.get('customer_name') or '').strip(),
                    }

                unresolved_codes = [cid for cid in unresolved_ids if cid and cid not in sales_profile_map]
                if unresolved_codes:
                    from_sales_by_code = []
                    customer_code_expr = cast(func.coalesce(FactSales.customer_code, literal('')), String)
                    for idx in range(0, len(unresolved_codes), 5000):
                        batch = unresolved_codes[idx : idx + 5000]
                        from_sales_by_code.extend(
                            (
                                await db.execute(
                                    select(
                                        customer_code_expr.label('customer_id'),
                                        func.coalesce(func.max(FactSales.customer_code), literal('')).label('customer_code'),
                                        func.coalesce(func.max(FactSales.customer_name), literal('')).label('customer_name'),
                                    )
                                    .where(customer_code_expr.in_(batch))
                                    .group_by(customer_code_expr)
                                )
                            ).mappings().all()
                        )
                    for row in from_sales_by_code:
                        cid = str(row.get('customer_id') or '').strip()
                        if not cid:
                            continue
                        sales_profile_map[cid] = {
                            'customer_code': str(row.get('customer_code') or '').strip(),
                            'customer_name': str(row.get('customer_name') or '').strip(),
                        }

            out: dict[str, dict[str, object]] = {}
            for row in agg_latest_rows:
                customer_id = str(row.get('customer_id') or '').strip()
                if not customer_id:
                    continue
                snap = dict(row)
                profile = dim_customer_map.get(customer_id) or sales_profile_map.get(customer_id) or {}
                customer_code = str(
                    profile.get('customer_code') or snap.get('customer_code') or customer_id
                ).strip()
                customer_name = str(
                    profile.get('customer_name') or snap.get('customer_name') or customer_code or customer_id
                ).strip()
                snap['customer_code'] = customer_code
                snap['customer_name'] = customer_name
                out[customer_id] = snap
            return out

    # When aggregate snapshots are not built yet, callers that prefer aggregate-only
    # data should gracefully fall back to raw fact balances instead of returning an
    # empty customer list while facts already exist.
    if aggregate_only and agg_has_rows:
        return {}

    key_expr = _customer_balance_key_expr()
    snapshots_stmt = (
        select(
            key_expr.label('customer_id'),
            func.coalesce(func.max(FactCustomerBalance.customer_ext_id), literal('')).label('customer_code'),
            func.coalesce(func.max(FactCustomerBalance.customer_name), literal('')).label('customer_name'),
            FactCustomerBalance.balance_date.label('balance_date'),
            func.coalesce(func.sum(FactCustomerBalance.open_balance), 0).label('open_balance'),
            func.coalesce(func.sum(FactCustomerBalance.overdue_balance), 0).label('overdue_balance'),
            func.coalesce(func.sum(FactCustomerBalance.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
            func.coalesce(func.sum(FactCustomerBalance.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
            func.coalesce(func.sum(FactCustomerBalance.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
            func.coalesce(func.sum(FactCustomerBalance.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
            func.max(FactCustomerBalance.last_collection_date).label('last_collection_date'),
            func.coalesce(func.sum(FactCustomerBalance.trend_vs_previous), 0).label('trend_vs_previous'),
            func.max(FactCustomerBalance.updated_at).label('updated_at'),
        )
        .select_from(FactCustomerBalance)
        .where(FactCustomerBalance.balance_date <= as_of)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        snapshots_stmt = snapshots_stmt.where(FactCustomerBalance.branch_ext_id.in_(branches))
    if customer_ids:
        snapshots_stmt = snapshots_stmt.where(key_expr.in_(customer_ids))

    snapshots = snapshots_stmt.group_by(key_expr, FactCustomerBalance.balance_date).subquery('customer_balances_by_day')
    ranked = (
        select(
            snapshots.c.customer_id,
            snapshots.c.customer_code,
            snapshots.c.customer_name,
            snapshots.c.balance_date,
            snapshots.c.open_balance,
            snapshots.c.overdue_balance,
            snapshots.c.aging_bucket_0_30,
            snapshots.c.aging_bucket_31_60,
            snapshots.c.aging_bucket_61_90,
            snapshots.c.aging_bucket_90_plus,
            snapshots.c.last_collection_date,
            snapshots.c.trend_vs_previous,
            snapshots.c.updated_at,
            over(
                func.row_number(),
                partition_by=snapshots.c.customer_id,
                order_by=snapshots.c.balance_date.desc(),
            ).label('rn'),
        )
    ).subquery('customer_balances_ranked')

    latest_rows = (await db.execute(select(ranked).where(ranked.c.rn == 1))).mappings().all()
    out: dict[str, dict[str, object]] = {}
    for row in latest_rows:
        customer_id = str(row.get('customer_id') or '').strip()
        if not customer_id:
            continue
        out[customer_id] = {
            'customer_code': str(row.get('customer_code') or customer_id).strip(),
            'customer_name': str(row.get('customer_name') or customer_id).strip(),
            'balance_date': row.get('balance_date'),
            'open_balance': float(row.get('open_balance') or 0),
            'overdue_balance': float(row.get('overdue_balance') or 0),
            'aging_bucket_0_30': float(row.get('aging_bucket_0_30') or 0),
            'aging_bucket_31_60': float(row.get('aging_bucket_31_60') or 0),
            'aging_bucket_61_90': float(row.get('aging_bucket_61_90') or 0),
            'aging_bucket_90_plus': float(row.get('aging_bucket_90_plus') or 0),
            'last_collection_date': row.get('last_collection_date'),
            'trend_vs_previous': float(row.get('trend_vs_previous') or 0),
            'updated_at': row.get('updated_at'),
        }
    return out


def _season_case(date_col):
    month_expr = cast(func.extract('month', date_col), Integer)
    return case(
        (month_expr.in_([12, 1, 2]), 'winter'),
        (month_expr.in_([3, 4, 5]), 'spring'),
        (month_expr.in_([6, 7, 8]), 'summer'),
        else_='autumn',
    )


def _inventory_base_stmt():
    return (
        select(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
    )


def _inventory_item_scope_predicate():
    return or_(DimItem.softone_sotype.is_(None), DimItem.softone_sotype == 51)


def _apply_inventory_filters(
    stmt,
    branches=None,
    warehouses=None,
    brands=None,
    categories=None,
    groups=None,
    branch_ext_col=None,
):
    stmt = stmt.where(_inventory_item_scope_predicate())
    branches = _effective_branch_filter(branches)
    if branches is not None:
        if branch_ext_col is not None:
            stmt = stmt.where(or_(DimBranch.external_id.in_(branches), branch_ext_col.in_(branches)))
        else:
            stmt = stmt.where(DimBranch.external_id.in_(branches))
    if warehouses:
        stmt = stmt.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if categories:
        category_path_expr = func.concat_ws(
            ' > ',
            func.nullif(func.btrim(DimItem.category_1), ''),
            func.nullif(func.btrim(DimItem.category_2), ''),
            func.nullif(func.btrim(DimItem.category_3), ''),
        )
        stmt = stmt.where(
            or_(
                DimCategory.external_id.in_(categories),
                category_path_expr.in_(categories),
            )
        )
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    return stmt


def _month_floor(value: date) -> date:
    return value.replace(day=1)


def _month_sequence(date_from: date, date_to: date) -> list[date]:
    current = _month_floor(date_from)
    end = _month_floor(date_to)
    months: list[date] = []
    while current <= end:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _month_label(value: date) -> str:
    labels = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαϊ', 'Ιουν', 'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']
    return labels[value.month - 1]


def _start_of_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _start_of_month(value: date) -> date:
    return value.replace(day=1)


def _start_of_year(value: date) -> date:
    return value.replace(month=1, day=1)


def _safe_same_day(year: int, month: int, day: int) -> date:
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)
    return month_start.replace(day=min(day, month_end.day))


def _window_bounds(windows: dict[str, tuple[date, date]]) -> tuple[date, date]:
    starts = [rng[0] for rng in windows.values()]
    ends = [rng[1] for rng in windows.values()]
    return min(starts), max(ends)


async def _should_use_fact_sales_source(db: AsyncSession, *, date_from: date, date_to: date) -> bool:
    """Fallback to FactSales when daily aggregates are missing/incomplete for the requested range."""
    fact_stmt = (
        select(
            func.min(FactSales.doc_date),
            func.max(FactSales.doc_date),
            func.count(func.distinct(FactSales.doc_date)),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    fact_min, fact_max, fact_days_raw = (await db.execute(fact_stmt)).one()
    if fact_max is None:
        return False

    agg_stmt = (
        select(
            func.min(AggSalesDaily.doc_date),
            func.max(AggSalesDaily.doc_date),
            func.count(func.distinct(AggSalesDaily.doc_date)),
        )
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    agg_min, agg_max, agg_days_raw = (await db.execute(agg_stmt)).one()
    if agg_max is None:
        return True

    fact_days = int(fact_days_raw or 0)
    agg_days = int(agg_days_raw or 0)
    if agg_min > fact_min or agg_max < fact_max:
        return True
    if agg_days < fact_days:
        return True
    return False


async def _sales_summaries_by_windows(
    db: AsyncSession,
    *,
    windows: dict[str, tuple[date, date]],
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict[str, dict]:
    if not windows:
        return {}

    global_from, global_to = _window_bounds(windows)
    use_fact_source = _has_sales_turnover_series_rules() or _has_sales_behavior_rules() or await _should_use_fact_sales_source(
        db, date_from=global_from, date_to=global_to
    )
    if use_fact_source:
        qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
        net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        gross_expr = func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        cols = []
        for key, (window_from, window_to) in windows.items():
            cond = FactSales.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    func.count(FactSales.id).filter(cond).label(f'{key}_records'),
                    func.coalesce(func.sum(qty_expr).filter(cond), 0).label(f'{key}_qty'),
                    func.coalesce(func.sum(net_expr).filter(cond), 0).label(f'{key}_net_value'),
                    func.coalesce(func.sum(gross_expr).filter(cond), 0).label(f'{key}_gross_value'),
                ]
            )

        stmt = select(*cols).where(*_date_range(FactSales.doc_date, global_from, global_to))
        stmt = _apply_fact_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        stmt = _apply_fact_sales_behavior_rules(stmt)
        stmt = _apply_fact_sales_turnover_rules(stmt)
        row = (await db.execute(stmt)).mappings().one()

        out: dict[str, dict] = {}
        for key in windows:
            out[key] = {
                'records': int(row.get(f'{key}_records') or 0),
                'qty': float(row.get(f'{key}_qty') or 0),
                'net_value': float(row.get(f'{key}_net_value') or 0),
                'gross_value': float(row.get(f'{key}_gross_value') or 0),
            }
        return out

    cols = []
    for key, (window_from, window_to) in windows.items():
        cond = AggSalesDaily.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.count(AggSalesDaily.id).filter(cond).label(f'{key}_records'),
                func.coalesce(func.sum(AggSalesDaily.qty).filter(cond), 0).label(f'{key}_qty'),
                func.coalesce(func.sum(AggSalesDaily.net_value).filter(cond), 0).label(f'{key}_net_value'),
                func.coalesce(func.sum(AggSalesDaily.gross_value).filter(cond), 0).label(f'{key}_gross_value'),
            ]
        )

    stmt = select(*cols).where(*_date_range(AggSalesDaily.doc_date, global_from, global_to))
    stmt = _apply_sales_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    row = (await db.execute(stmt)).mappings().one()

    out: dict[str, dict] = {}
    for key in windows:
        out[key] = {
            'records': int(row.get(f'{key}_records') or 0),
            'qty': float(row.get(f'{key}_qty') or 0),
            'net_value': float(row.get(f'{key}_net_value') or 0),
            'gross_value': float(row.get(f'{key}_gross_value') or 0),
        }
    return out


async def _purchases_summaries_by_windows(
    db: AsyncSession,
    *,
    windows: dict[str, tuple[date, date]],
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict[str, dict]:
    if not windows:
        return {}

    global_from, global_to = _window_bounds(windows)
    net_expr = func.coalesce(FactPurchases.net_value, 0)
    cost_expr = func.coalesce(FactPurchases.cost_amount, 0)
    discount_expr = _fact_purchase_signed_discount_expr()
    before_discount_expr = net_expr + discount_expr
    cols = []
    for key, (window_from, window_to) in windows.items():
        cond = FactPurchases.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.count(FactPurchases.id).filter(cond).label(f'{key}_records'),
                func.coalesce(func.sum(FactPurchases.qty).filter(cond), 0).label(f'{key}_qty'),
                func.coalesce(func.sum(net_expr).filter(cond), 0).label(f'{key}_net_value'),
                func.coalesce(func.sum(cost_expr).filter(cond), 0).label(f'{key}_cost_amount'),
                func.coalesce(func.sum(before_discount_expr).filter(cond), 0).label(f'{key}_gross_value'),
                func.coalesce(func.sum(discount_expr).filter(cond), 0).label(f'{key}_discount_amount'),
            ]
        )

    stmt = select(*cols).where(*_date_range(FactPurchases.doc_date, global_from, global_to))
    stmt = _apply_fact_purchase_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    row = (await db.execute(stmt)).mappings().one()

    out: dict[str, dict] = {}
    for key in windows:
        out[key] = {
            'records': int(row.get(f'{key}_records') or 0),
            'qty': float(row.get(f'{key}_qty') or 0),
            'net_value': float(row.get(f'{key}_net_value') or 0),
            'cost_amount': float(row.get(f'{key}_cost_amount') or 0),
            'gross_value': float(row.get(f'{key}_gross_value') or 0),
            'discount_amount': float(row.get(f'{key}_discount_amount') or 0),
        }
    return out


def _map_branch_window_rows(
    raw_rows: list[dict],
    *,
    key_prefix: str,
) -> list[dict]:
    prepped_by_branch: dict[str, dict] = {}
    for row in raw_rows:
        net_value = float(row.get(f'{key_prefix}_net') or 0)
        gross_value = float(row.get(f'{key_prefix}_gross') or 0)
        cost_amount = float(row.get(f'{key_prefix}_cost') or 0)
        if net_value == 0 and gross_value == 0 and cost_amount == 0:
            continue
        branch = row.get('branch_name') or row.get('branch_ext_id') or 'N/A'
        branch_code = row.get('branch_ext_id') or 'N/A'
        bucket = prepped_by_branch.setdefault(
            branch,
            {
                'branch': branch,
                'branch_code': branch_code,
                'net_value': 0.0,
                'gross_value': 0.0,
                'cost_amount': 0.0,
            },
        )
        if branch_code not in str(bucket.get('branch_code') or '').split(', '):
            bucket['branch_code'] = f"{bucket['branch_code']}, {branch_code}"
        bucket['net_value'] = float(bucket['net_value']) + net_value
        bucket['gross_value'] = float(bucket['gross_value']) + gross_value
        bucket['cost_amount'] = float(bucket['cost_amount']) + cost_amount

    prepped = list(prepped_by_branch.values())
    total_net = sum(float(item['net_value']) for item in prepped)
    avg_net = (total_net / len(prepped)) if prepped else 0.0
    out: list[dict] = []
    for item in prepped:
        net_value = float(item['net_value'])
        cost_amount = float(item['cost_amount'])
        out.append(
            {
                **item,
                'contribution_pct': (net_value / total_net * 100.0) if total_net > 0 else 0.0,
                'margin_pct': ((net_value - cost_amount) / net_value * 100.0) if net_value > 0 and cost_amount > 0 else 0.0,
                'performance_index_pct': (net_value / avg_net * 100.0) if avg_net > 0 else 0.0,
            }
        )
    out.sort(key=lambda x: float(x.get('net_value') or 0), reverse=True)
    return out


def _apply_sales_branch_adjustments_to_rows(
    rows: list[dict],
    *,
    date_from: date,
    date_to: date,
) -> list[dict]:
    branch_adjustments = _sales_branch_adjustments_for_range(date_from, date_to)
    if not branch_adjustments:
        return rows

    branch_delta_map = {
        str(item.get('branch_ext_id') or '').strip(): float(item.get('delta_net_value') or 0.0)
        for item in branch_adjustments
        if str(item.get('branch_ext_id') or '').strip()
    }
    if not branch_delta_map:
        return rows

    row_map: dict[str, dict] = {}
    for row in rows:
        branch_code = str(row.get('branch_code') or '').split(',')[0].strip()
        if not branch_code:
            continue
        row_map[branch_code] = {
            'branch': row.get('branch') or branch_code,
            'branch_code': branch_code,
            'net_value': float(row.get('net_value') or 0),
            'gross_value': float(row.get('gross_value') or 0),
            'cost_amount': float(row.get('cost_amount') or 0),
        }

    for branch_code in branch_delta_map:
        if branch_code not in row_map:
            row_map[branch_code] = {
                'branch': branch_code,
                'branch_code': branch_code,
                'net_value': 0.0,
                'gross_value': 0.0,
                'cost_amount': 0.0,
            }

    adjusted_rows = list(row_map.values())
    total_net = sum(float(item['net_value']) + float(branch_delta_map.get(item['branch_code'], 0.0)) for item in adjusted_rows)
    avg_net = (total_net / len(adjusted_rows)) if adjusted_rows else 0.0

    out: list[dict] = []
    for item in adjusted_rows:
        branch_code = item['branch_code']
        net_value = float(item['net_value']) + float(branch_delta_map.get(branch_code, 0.0))
        cost_amount = float(item['cost_amount'])
        out.append(
            {
                'branch': item['branch'],
                'branch_code': branch_code,
                'net_value': net_value,
                'gross_value': float(item['gross_value']),
                'cost_amount': cost_amount,
                'contribution_pct': (net_value / total_net * 100.0) if total_net > 0 else 0.0,
                'margin_pct': ((net_value - cost_amount) / net_value * 100.0) if net_value > 0 and cost_amount > 0 else 0.0,
                'performance_index_pct': (net_value / avg_net * 100.0) if avg_net > 0 else 0.0,
            }
        )

    out.sort(key=lambda item: float(item.get('net_value') or 0.0), reverse=True)
    return out


async def _sales_by_branch_windows(
    db: AsyncSession,
    *,
    windows: dict[str, tuple[date, date]],
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict[str, list[dict]]:
    if not windows:
        return {}

    global_from, global_to = _window_bounds(windows)
    use_fact_source = _has_sales_turnover_series_rules() or _has_sales_behavior_rules() or await _should_use_fact_sales_source(
        db, date_from=global_from, date_to=global_to
    )
    if use_fact_source:
        net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        gross_expr = func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        cost_expr = func.coalesce(FactSales.cost_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        cols = [
            FactSales.branch_ext_id.label('branch_ext_id'),
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
        ]
        for key, (window_from, window_to) in windows.items():
            cond = FactSales.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    func.coalesce(func.sum(net_expr).filter(cond), 0).label(f'{key}_net'),
                    func.coalesce(func.sum(gross_expr).filter(cond), 0).label(f'{key}_gross'),
                    func.coalesce(func.sum(cost_expr).filter(cond), 0).label(f'{key}_cost'),
                ]
            )

        stmt = (
            select(*cols)
            .select_from(FactSales)
            .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
            .where(*_date_range(FactSales.doc_date, global_from, global_to))
        )
        stmt = _apply_fact_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        stmt = _apply_fact_sales_behavior_rules(stmt)
        stmt = _apply_fact_sales_turnover_rules(stmt)
        rows = (await db.execute(stmt.group_by(FactSales.branch_ext_id))).mappings().all()
        out: dict[str, list[dict]] = {}
        for key, (window_from, window_to) in windows.items():
            mapped = _map_branch_window_rows(rows, key_prefix=key)
            out[key] = _apply_sales_branch_adjustments_to_rows(
                mapped,
                date_from=window_from,
                date_to=window_to,
            )
        return out

    use_branch_daily = not any([warehouses, brands, categories, groups])
    if use_branch_daily:
        source_branch_col = AggSalesDailyBranch.branch_ext_id
        source_date_col = AggSalesDailyBranch.doc_date
        source_net_col = AggSalesDailyBranch.net_value
        source_gross_col = AggSalesDailyBranch.gross_value
        source_cost_col = AggSalesDailyBranch.cost_amount
    else:
        source_branch_col = AggSalesDaily.branch_ext_id
        source_date_col = AggSalesDaily.doc_date
        source_net_col = AggSalesDaily.net_value
        source_gross_col = AggSalesDaily.gross_value
        source_cost_col = None

    cols = [
        source_branch_col.label('branch_ext_id'),
        func.coalesce(func.max(DimBranch.name), source_branch_col).label('branch_name'),
    ]
    for key, (window_from, window_to) in windows.items():
        cond = source_date_col.between(window_from, window_to)
        cols.extend(
            [
                func.coalesce(func.sum(source_net_col).filter(cond), 0).label(f'{key}_net'),
                func.coalesce(func.sum(source_gross_col).filter(cond), 0).label(f'{key}_gross'),
                (
                    func.coalesce(func.sum(source_cost_col).filter(cond), 0)
                    if source_cost_col is not None
                    else literal(0)
                ).label(f'{key}_cost'),
            ]
        )

    normalized_branches = _effective_branch_filter(branches)

    if use_branch_daily:
        stmt = (
            select(*cols)
            .select_from(AggSalesDailyBranch)
            .join(DimBranch, DimBranch.external_id == AggSalesDailyBranch.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDailyBranch.doc_date, global_from, global_to))
        )
        if normalized_branches is not None:
            stmt = stmt.where(AggSalesDailyBranch.branch_ext_id.in_(normalized_branches))
        rows = (await db.execute(stmt.group_by(AggSalesDailyBranch.branch_ext_id))).mappings().all()
        if not rows:
            fallback_cols = [
                AggSalesDaily.branch_ext_id.label('branch_ext_id'),
                func.coalesce(func.max(DimBranch.name), AggSalesDaily.branch_ext_id).label('branch_name'),
            ]
            for key, (window_from, window_to) in windows.items():
                cond = AggSalesDaily.doc_date.between(window_from, window_to)
                fallback_cols.extend(
                    [
                        func.coalesce(func.sum(AggSalesDaily.net_value).filter(cond), 0).label(f'{key}_net'),
                        func.coalesce(func.sum(AggSalesDaily.gross_value).filter(cond), 0).label(f'{key}_gross'),
                        literal(0).label(f'{key}_cost'),
                    ]
                )

            fallback_stmt = (
                select(*fallback_cols)
                .select_from(AggSalesDaily)
                .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
                .where(*_date_range(AggSalesDaily.doc_date, global_from, global_to))
            )
            fallback_stmt = _apply_sales_filters(
                fallback_stmt,
                branches=normalized_branches,
                warehouses=warehouses,
                brands=brands,
                categories=categories,
                groups=groups,
            )
            rows = (await db.execute(fallback_stmt.group_by(AggSalesDaily.branch_ext_id))).mappings().all()
    else:
        stmt = (
            select(*cols)
            .select_from(AggSalesDaily)
            .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDaily.doc_date, global_from, global_to))
        )
        stmt = _apply_sales_filters(
            stmt,
            branches=normalized_branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        rows = (await db.execute(stmt.group_by(AggSalesDaily.branch_ext_id))).mappings().all()

    out: dict[str, list[dict]] = {}
    for key, (window_from, window_to) in windows.items():
        mapped = _map_branch_window_rows(rows, key_prefix=key)
        out[key] = _apply_sales_branch_adjustments_to_rows(
            mapped,
            date_from=window_from,
            date_to=window_to,
        )
    return out


async def sales_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    if _has_sales_turnover_series_rules() or _has_sales_behavior_rules():
        qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
        net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        gross_expr = func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        stmt = (
            select(
                func.count(FactSales.id),
                func.coalesce(func.sum(qty_expr), 0),
                func.coalesce(func.sum(net_expr), 0),
                func.coalesce(func.sum(gross_expr), 0),
            )
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        stmt = _apply_fact_sales_filters(
            stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        stmt = _apply_fact_sales_behavior_rules(stmt)
        stmt = _apply_fact_sales_turnover_rules(stmt)
        row = (await db.execute(stmt)).one()
        return {
            'records': int(row[0] or 0),
            'qty': float(row[1] or 0),
            'net_value': float(row[2] or 0),
            'gross_value': float(row[3] or 0),
        }

    stmt = (
        select(
            func.count(AggSalesDaily.id),
            func.coalesce(func.sum(AggSalesDaily.qty), 0),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
        )
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    row = (await db.execute(stmt)).one()
    return {
        'records': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'net_value': float(row[2] or 0),
        'gross_value': float(row[3] or 0),
    }


async def sales_by_branch(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    if _has_sales_turnover_series_rules() or _has_sales_behavior_rules():
        net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        gross_expr = func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        cost_expr = func.coalesce(FactSales.cost_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
        stmt = (
            select(
                FactSales.branch_ext_id,
                func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
                func.coalesce(func.sum(net_expr), 0).label('net_value'),
                func.coalesce(func.sum(gross_expr), 0).label('gross_value'),
                func.coalesce(func.sum(cost_expr), 0).label('cost_amount'),
            )
            .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        stmt = _apply_fact_sales_filters(
            stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        stmt = _apply_fact_sales_behavior_rules(stmt)
        stmt = _apply_fact_sales_turnover_rules(stmt)
        rows = (
            await db.execute(
                stmt.group_by(FactSales.branch_ext_id).order_by(func.sum(net_expr).desc())
            )
        ).all()
    else:
        use_branch_level_agg = not any([warehouses, brands, categories, groups])
        rows = []

        if use_branch_level_agg:
            try:
                stmt = (
                    select(
                        AggSalesDailyBranch.branch_ext_id,
                        func.coalesce(func.max(DimBranch.name), AggSalesDailyBranch.branch_ext_id).label('branch_name'),
                        func.coalesce(func.sum(AggSalesDailyBranch.net_value), 0).label('net_value'),
                        func.coalesce(func.sum(AggSalesDailyBranch.gross_value), 0).label('gross_value'),
                        func.coalesce(func.sum(AggSalesDailyBranch.cost_amount), 0).label('cost_amount'),
                    )
                    .join(DimBranch, DimBranch.external_id == AggSalesDailyBranch.branch_ext_id, isouter=True)
                    .where(*_date_range(AggSalesDailyBranch.doc_date, date_from, date_to))
                )
                branches = _effective_branch_filter(branches)
                if branches is not None:
                    stmt = stmt.where(AggSalesDailyBranch.branch_ext_id.in_(branches))
                stmt = stmt.group_by(AggSalesDailyBranch.branch_ext_id).order_by(func.sum(AggSalesDailyBranch.net_value).desc())
                rows = (await db.execute(stmt)).all()
            except Exception:
                rows = []

        if not rows:
            stmt = (
                select(
                    AggSalesDaily.branch_ext_id,
                    func.coalesce(func.max(DimBranch.name), AggSalesDaily.branch_ext_id).label('branch_name'),
                    func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
                    func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
                    literal(0).label('cost_amount'),
                )
                .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
                .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
            )
            stmt = _apply_sales_filters(
                stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
            )
            stmt = stmt.group_by(AggSalesDaily.branch_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
            rows = (await db.execute(stmt)).all()

    branch_adjustments = _sales_branch_adjustments_for_range(date_from, date_to)
    branch_delta_map = {
        str(item.get('branch_ext_id') or '').strip(): float(item.get('delta_net_value') or 0.0)
        for item in branch_adjustments
        if str(item.get('branch_ext_id') or '').strip()
    }
    row_map: dict[str, tuple] = {str(r[0] or '').strip(): r for r in rows}
    for branch_ext_id in branch_delta_map:
        if branch_ext_id not in row_map:
            row_map[branch_ext_id] = (branch_ext_id, branch_ext_id, 0.0, 0.0, 0.0)
    adjusted_rows = list(row_map.values())

    total_net = sum(
        float(r[2] or 0) + float(branch_delta_map.get(str(r[0] or '').strip(), 0.0))
        for r in adjusted_rows
    )
    avg_net = (total_net / len(adjusted_rows)) if adjusted_rows else 0.0
    out = []
    for r in adjusted_rows:
        branch_key = str(r[0] or '').strip()
        net_value = float(r[2] or 0) + float(branch_delta_map.get(branch_key, 0.0))
        gross_value = float(r[3] or 0)
        cost_amount = float(r[4] or 0)
        contribution_pct = (net_value / total_net * 100.0) if total_net > 0 else 0.0
        margin_pct = ((net_value - cost_amount) / net_value * 100.0) if net_value > 0 and cost_amount > 0 else 0.0
        performance_index_pct = (net_value / avg_net * 100.0) if avg_net > 0 else 0.0
        out.append(
            {
                'branch': r[1] or r[0] or 'N/A',
                'branch_code': r[0] or 'N/A',
                'net_value': net_value,
                'gross_value': gross_value,
                'cost_amount': cost_amount,
                'contribution_pct': contribution_pct,
                'margin_pct': margin_pct,
                'performance_index_pct': performance_index_pct,
            }
        )
    out.sort(key=lambda item: float(item.get('net_value') or 0.0), reverse=True)
    return out


async def _latest_sales_anchor_date(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> date | None:
    """Return latest available sales date inside the selected range.

    We use this to keep day/week/month widgets meaningful when the chosen
    period extends beyond the latest loaded tenant sales data.
    """
    if _has_sales_turnover_series_rules():
        stmt = select(func.max(FactSales.doc_date)).where(*_date_range(FactSales.doc_date, date_from, date_to))
        stmt = _apply_fact_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        stmt = _apply_fact_sales_turnover_rules(stmt)
    else:
        stmt = select(func.max(AggSalesDaily.doc_date)).where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
        stmt = _apply_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    latest = (await db.execute(stmt)).scalar_one_or_none()
    if latest is None:
        return None
    return latest.date() if hasattr(latest, 'date') else latest


async def sales_by_brand(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.brand_ext_id,
            func.coalesce(func.max(DimBrand.name), AggSalesDaily.brand_ext_id).label('brand_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimBrand, DimBrand.external_id == AggSalesDaily.brand_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.brand_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'brand': r[1] or r[0] or 'N/A',
            'brand_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_by_category(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.category_ext_id,
            func.coalesce(func.max(DimCategory.name), AggSalesDaily.category_ext_id).label('category_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimCategory, DimCategory.external_id == AggSalesDaily.category_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.category_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'category': r[1] or r[0] or 'N/A',
            'category_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_by_group(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.group_ext_id,
            func.coalesce(func.max(DimGroup.name), AggSalesDaily.group_ext_id).label('group_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimGroup, DimGroup.external_id == AggSalesDaily.group_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.group_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'group': r[1] or r[0] or 'N/A',
            'group_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    net_expr = func.coalesce(FactPurchases.net_value, 0)
    cost_expr = func.coalesce(FactPurchases.cost_amount, 0)
    discount_expr = _fact_purchase_signed_discount_expr()
    before_discount_expr = net_expr + discount_expr
    stmt = (
        select(
            func.count(FactPurchases.id),
            func.coalesce(func.sum(FactPurchases.qty), 0),
            func.coalesce(func.sum(net_expr), 0),
            func.coalesce(func.sum(cost_expr), 0),
            func.coalesce(func.sum(before_discount_expr), 0),
            func.coalesce(func.sum(discount_expr), 0),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    row = (await db.execute(stmt)).one()
    net_value = float(row[2] or 0)
    gross_value = float(row[4] or 0)
    discount_amount = float(row[5] or 0)
    return {
        'records': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'net_value': net_value,
        'cost_amount': float(row[3] or 0),
        'gross_value': gross_value,
        'discount_amount': discount_amount,
        'before_discount_value': gross_value,
        'after_discount_value': net_value,
    }


async def purchases_by_supplier(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            FactPurchases.supplier_ext_id,
            func.coalesce(func.max(DimSupplier.name), FactPurchases.supplier_ext_id).label('supplier_name'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactPurchases.discount_amount), 0).label('discount_amount'),
        )
        .select_from(FactPurchases)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_purchases_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(FactPurchases.supplier_ext_id).order_by(func.sum(FactPurchases.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'supplier': r[1] or r[0] or 'N/A',
            'supplier_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'cost_amount': float(r[3] or 0),
            'discount_amount': float(r[4] or 0),
        }
        for r in rows
    ]


async def purchases_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactPurchases.doc_date), Date)
    stmt = (
        select(
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_purchases_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'cost_amount': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_monthly_trend_from_monthly_agg(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    if _has_sales_turnover_series_rules():
        month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
        stmt = (
            select(
                month_start_expr.label('month_start'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            )
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        stmt = _apply_fact_sales_filters(
            stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        stmt = _apply_fact_sales_turnover_rules(stmt)
        stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
        rows = (await db.execute(stmt)).all()
        return [
            {
                'month_start': str(r[0]),
                'net_value': float(r[1] or 0),
                'gross_value': float(r[2] or 0),
                'qty': float(r[3] or 0),
            }
            for r in rows
        ]

    stmt = (
        select(
            AggSalesMonthly.month_start.label('month_start'),
            func.coalesce(func.sum(AggSalesMonthly.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesMonthly.gross_value), 0).label('gross_value'),
            func.coalesce(func.sum(AggSalesMonthly.qty), 0).label('qty'),
        )
        .where(*_date_range(AggSalesMonthly.month_start, _month_floor(date_from), _month_floor(date_to)))
    )
    stmt = _apply_sales_monthly_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesMonthly.month_start).order_by(AggSalesMonthly.month_start)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'gross_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_monthly_trend_from_monthly_agg(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggPurchasesMonthly.month_start.label('month_start'),
            func.coalesce(func.sum(AggPurchasesMonthly.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggPurchasesMonthly.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(AggPurchasesMonthly.qty), 0).label('qty'),
        )
        .where(*_date_range(AggPurchasesMonthly.month_start, _month_floor(date_from), _month_floor(date_to)))
    )
    stmt = _apply_purchase_monthly_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggPurchasesMonthly.month_start).order_by(AggPurchasesMonthly.month_start)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'cost_amount': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_margin_by_supplier(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    rows = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    enriched = []
    for r in rows:
        # net_value = χονδρική τιμή τιμολογίου (NETLINEVAL από SoftOne)
        # discount_amount = εκπτώσεις (PRICE*QTY - NETLINEVAL)
        # gross_value = χονδρική τιμή προ εκπτώσεων = net_value + discount_amount
        # cost_amount = actual net (fallback αν δεν υπάρχει discount_amount)
        net_value = float(r['net_value'])
        discount_amount = float(r.get('discount_amount') or 0)
        if discount_amount > 0:
            # Έχουμε εκπτώσεις από bridge: gross = net + discount
            gross_value = net_value + discount_amount
            cost_value = net_value
        else:
            # Fallback: παλιά λογική (cost_amount από bridge)
            cost_amount = float(r['cost_amount'])
            gross_value = net_value
            cost_value = cost_amount
        margin_value = gross_value - cost_value
        margin_pct = (margin_value / gross_value * 100.0) if gross_value > 0 else 0.0
        enriched.append(
            {
                'supplier': r['supplier'],
                'net_value': gross_value,    # Καθαρή Αξία = χονδρική προ εκπτώσεων
                'cost_amount': cost_value,   # Κόστος = χονδρική μετά εκπτώσεων
                'margin_value': margin_value,
                'margin_pct': margin_pct,
            }
        )
    return enriched


async def purchases_cost_change_detection(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    current = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    previous = await purchases_by_supplier(
        db,
        date_from=prev_from,
        date_to=prev_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    prev_map = {str(x['supplier']): float(x['cost_amount']) for x in previous}
    changes = []
    for row in current:
        supplier = str(row['supplier'])
        cur_cost = float(row['cost_amount'])
        prev_cost = prev_map.get(supplier, 0.0)
        if prev_cost > 0:
            delta_pct = ((cur_cost - prev_cost) / prev_cost) * 100.0
        else:
            delta_pct = None
        if delta_pct is not None and abs(delta_pct) >= 10:
            changes.append(
                {
                    'supplier': supplier,
                    'current_cost': cur_cost,
                    'previous_cost': prev_cost,
                    'delta_pct': delta_pct,
                }
            )
    changes.sort(key=lambda x: abs(float(x['delta_pct'] or 0)), reverse=True)
    return {
        'period': {
            'from': str(date_from),
            'to': str(date_to),
            'prev_from': str(prev_from),
            'prev_to': str(prev_to),
        },
        'alerts': changes[:10],
    }


def _same_day_previous_year(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        # 29/02 -> 28/02 for PYTD comparisons.
        return value.replace(year=value.year - 1, day=28)


async def purchases_items_ytd_breakdown(
    db: AsyncSession,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 250,
):
    def aggregate_stmt(window_from: date, window_to: date):
        discount_amount = func.coalesce(FactPurchases.discount_amount, 0)
        stmt = (
            select(
                FactPurchases.item_code.label('item_code'),
                func.coalesce(func.sum(FactPurchases.net_value), 0).label('value'),
                func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
                func.coalesce(func.sum(func.abs(discount_amount)), 0).label('discount_amount'),
                func.coalesce(
                    func.sum(func.abs(FactPurchases.net_value) + func.abs(discount_amount)),
                    0,
                ).label('discount_base'),
            )
            .where(*_date_range(FactPurchases.doc_date, window_from, window_to))
            .where(FactPurchases.item_code.is_not(None))
            .where(FactPurchases.item_code != '')
        )
        stmt = _apply_fact_purchases_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        return stmt.group_by(FactPurchases.item_code)

    ytd_from = date(date_to.year, 1, 1)
    pytd_to = _same_day_previous_year(date_to)
    pytd_from = date(pytd_to.year, 1, 1)

    ytd_rows = (await db.execute(aggregate_stmt(ytd_from, date_to))).all()
    pytd_rows = (await db.execute(aggregate_stmt(pytd_from, pytd_to))).all()

    by_code: dict[str, dict[str, float | str]] = {}

    def ensure(code: str) -> dict[str, float | str]:
        row = by_code.setdefault(
            code,
            {
                'item_code': code,
                'barcode': '',
                'item_name': code,
                'ytd_value': 0.0,
                'ytd_qty': 0.0,
                'ytd_discount_pct': 0.0,
                'pytd_value': 0.0,
                'pytd_qty': 0.0,
                'pytd_discount_pct': 0.0,
                '_ytd_discount_amount': 0.0,
                '_ytd_discount_base': 0.0,
                '_pytd_discount_amount': 0.0,
                '_pytd_discount_base': 0.0,
            },
        )
        return row

    for code, value, qty, discount_amount, discount_base in ytd_rows:
        item = ensure(str(code))
        item['ytd_value'] = float(value or 0)
        item['ytd_qty'] = float(qty or 0)
        item['_ytd_discount_amount'] = float(discount_amount or 0)
        item['_ytd_discount_base'] = float(discount_base or 0)

    for code, value, qty, discount_amount, discount_base in pytd_rows:
        item = ensure(str(code))
        item['pytd_value'] = float(value or 0)
        item['pytd_qty'] = float(qty or 0)
        item['_pytd_discount_amount'] = float(discount_amount or 0)
        item['_pytd_discount_base'] = float(discount_base or 0)

    codes = list(by_code.keys())
    if codes:
        dim_rows = (
            await db.execute(
                select(DimItem.external_id, DimItem.barcode, DimItem.name).where(DimItem.external_id.in_(codes))
            )
        ).all()
        for ext, barcode, name in dim_rows:
            item = by_code.get(str(ext))
            if not item:
                continue
            item['barcode'] = str(barcode or '')
            item['item_name'] = _clean_item_name(str(name or ''), fallback=str(ext))

    rows = []
    for item in by_code.values():
        ytd_base = float(item.pop('_ytd_discount_base') or 0)
        ytd_disc = float(item.pop('_ytd_discount_amount') or 0)
        pytd_base = float(item.pop('_pytd_discount_base') or 0)
        pytd_disc = float(item.pop('_pytd_discount_amount') or 0)
        item['ytd_discount_pct'] = (ytd_disc / ytd_base * 100.0) if ytd_base else 0.0
        item['pytd_discount_pct'] = (pytd_disc / pytd_base * 100.0) if pytd_base else 0.0
        rows.append(item)

    rows.sort(key=lambda r: abs(float(r.get('ytd_value') or 0)), reverse=True)
    return {
        'periods': {
            'ytd_from': str(ytd_from),
            'ytd_to': str(date_to),
            'pytd_from': str(pytd_from),
            'pytd_to': str(pytd_to),
        },
        'rows': rows[: max(1, min(limit, 1000))],
    }


async def purchases_decision_pack(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    summary = await purchases_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    trend = await purchases_monthly_trend(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    supplier_distribution = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    margin_by_supplier = await purchases_margin_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    cost_change = await purchases_cost_change_detection(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    seasonal = await purchases_seasonality(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    new_codes = await new_item_codes_activity(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=10,
    )
    item_breakdown = await purchases_items_ytd_breakdown(
        db,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=250,
    )
    return {
        'summary': summary,
        'purchase_trend': trend,
        'supplier_distribution': supplier_distribution,
        'margin_by_supplier': margin_by_supplier,
        'cost_change_detection': cost_change,
        'seasonality': seasonal,
        'new_codes': new_codes,
        'item_breakdown': item_breakdown,
    }


async def _distinct_dimension_values(
    db: AsyncSession,
    source_date_column,
    source_dim_column,
    date_from: date,
    date_to: date,
    filters_applier,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(source_dim_column)
        .where(*_date_range(source_date_column, date_from, date_to))
        .where(source_dim_column.is_not(None))
        .where(source_dim_column != '')
    )
    stmt = filters_applier(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.distinct().order_by(source_dim_column)
    rows = (await db.execute(stmt)).scalars().all()
    return [str(v) for v in rows if v]


async def _distinct_purchase_dimension_values(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    agg_column,
    fact_column,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    values = await _distinct_dimension_values(
        db,
        AggPurchasesDaily.doc_date,
        agg_column,
        date_from,
        date_to,
        _apply_purchase_filters,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    if values:
        return values

    stmt = (
        select(fact_column)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        .where(fact_column.is_not(None))
        .where(fact_column != '')
    )
    stmt = _apply_fact_purchases_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.distinct().order_by(fact_column)
    rows = (await db.execute(stmt)).scalars().all()
    return [str(v) for v in rows if v]


async def _purchase_item_dimension_options(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    dimension: str,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    purchase_items = (
        select(FactPurchases.item_code)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        .where(FactPurchases.item_code.is_not(None))
    )
    purchase_items = _apply_fact_purchases_filters(
        purchase_items,
        branches=branches,
        warehouses=warehouses,
        brands=None if dimension == 'brands' else brands,
        categories=None if dimension == 'categories' else categories,
        groups=None if dimension == 'groups' else groups,
    ).distinct()

    if dimension == 'categories':
        c1 = func.coalesce(_softone_clean_dimension_text(DimItem.category_1), literal('N/A'))
        c2 = func.coalesce(_softone_clean_dimension_text(DimItem.category_2), c1)
        c3 = func.coalesce(_softone_clean_dimension_text(DimItem.category_3), c2)
        path = func.concat(c1, literal(' > '), c2, literal(' > '), c3).label('path')
        rows = (
            await db.execute(
                select(path)
                .where(DimItem.external_id.in_(purchase_items))
                .where(
                    or_(
                        _softone_clean_dimension_text(DimItem.category_1).is_not(None),
                        _softone_clean_dimension_text(DimItem.category_2).is_not(None),
                        _softone_clean_dimension_text(DimItem.category_3).is_not(None),
                    )
                )
                .distinct()
                .order_by(path)
            )
        ).scalars().all()
        values = [str(v) for v in rows if v]
        return values, {v: v for v in values}

    if dimension == 'brands':
        value_expr = FactInventory.source_payload_json['brand_external_id'].astext
        label_expr = FactInventory.source_payload_json['brand_name'].astext
    elif dimension == 'groups':
        value_expr = FactInventory.source_payload_json['group_external_id'].astext
        label_expr = FactInventory.source_payload_json['group_name'].astext
    else:
        return [], {}

    rows = (
        await db.execute(
            select(value_expr.label('value'), func.max(label_expr).label('label'))
            .where(FactInventory.item_code.in_(purchase_items))
            .where(value_expr.is_not(None))
            .where(value_expr != '')
            .group_by(value_expr)
            .order_by(func.max(label_expr), value_expr)
        )
    ).all()
    values: list[str] = []
    labels: dict[str, str] = {}
    for value, label in rows:
        if not value:
            continue
        key = str(value)
        values.append(key)
        labels[key] = str(label or value)
    return values, labels


async def _dimension_label_map(db: AsyncSession, model) -> dict[str, str]:
    if model is DimCategory:
        parent = aliased(DimCategory)
        grand_parent = aliased(DimCategory)
        rows = (
            await db.execute(
                select(
                    DimCategory.external_id,
                    DimCategory.name,
                    parent.name.label('parent_name'),
                    grand_parent.name.label('grand_parent_name'),
                )
                .select_from(DimCategory)
                .join(parent, DimCategory.parent_id == parent.id, isouter=True)
                .join(grand_parent, parent.parent_id == grand_parent.id, isouter=True)
                .where(DimCategory.external_id.is_not(None))
            )
        ).all()
        label_map: dict[str, str] = {}
        for ext, name, parent_name, grand_parent_name in rows:
            if not ext:
                continue
            path_parts = [str(x).strip() for x in [grand_parent_name, parent_name, name] if x and str(x).strip()]
            if path_parts:
                label_map[str(ext)] = " > ".join(path_parts)
            else:
                label_map[str(ext)] = str(ext)
        return label_map

    if model is DimWarehouse:
        rows = (
            await db.execute(
                select(DimWarehouse.external_id, DimWarehouse.name).where(DimWarehouse.external_id.is_not(None))
            )
        ).all()
        label_map: dict[str, str] = {}
        for ext, name in rows:
            if not ext:
                continue
            ext_txt = str(ext).strip()
            name_txt = str(name or "").strip()
            if name_txt and name_txt.lower() != ext_txt.lower():
                label_map[ext_txt] = f"{ext_txt} - {name_txt}"
            else:
                label_map[ext_txt] = ext_txt
        return label_map

    rows = (await db.execute(select(model.external_id, model.name).where(model.external_id.is_not(None)))).all()
    return {str(ext): str(name or ext) for ext, name in rows if ext}


async def _expense_category_label_map(db: AsyncSession) -> dict[str, str]:
    rows = (
        await db.execute(
            select(DimExpenseCategory.category_code, DimExpenseCategory.category_name).where(
                DimExpenseCategory.category_code.is_not(None)
            )
        )
    ).all()
    return {str(code): str(name or code) for code, name in rows if code}


async def sales_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    async def _build_options(
        source_date_column,
        branch_col,
        warehouse_col,
        brand_col,
        category_col,
        group_col,
        filters_applier,
        from_date: date,
        to_date: date,
    ) -> dict[str, list[str]]:
        return {
            'branches': await _distinct_dimension_values(
                db,
                source_date_column,
                branch_col,
                from_date,
                to_date,
                filters_applier,
                branches=None,
                warehouses=warehouses,
                brands=brands,
                categories=categories,
                groups=groups,
            ),
            'warehouses': await _distinct_dimension_values(
                db,
                source_date_column,
                warehouse_col,
                from_date,
                to_date,
                filters_applier,
                branches=branches,
                warehouses=None,
                brands=brands,
                categories=categories,
                groups=groups,
            ),
            'brands': await _distinct_dimension_values(
                db,
                source_date_column,
                brand_col,
                from_date,
                to_date,
                filters_applier,
                branches=branches,
                warehouses=warehouses,
                brands=None,
                categories=categories,
                groups=groups,
            ),
            'categories': await _distinct_dimension_values(
                db,
                source_date_column,
                category_col,
                from_date,
                to_date,
                filters_applier,
                branches=branches,
                warehouses=warehouses,
                brands=brands,
                categories=None,
                groups=groups,
            ),
            'groups': await _distinct_dimension_values(
                db,
                source_date_column,
                group_col,
                from_date,
                to_date,
                filters_applier,
                branches=branches,
                warehouses=warehouses,
                brands=brands,
                categories=categories,
                groups=None,
            ),
        }

    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    agg_has_rows = (await db.execute(select(AggSalesDaily.doc_date).limit(1))).first() is not None
    if agg_has_rows:
        source_date_col = AggSalesDaily.doc_date
        source_branch_col = AggSalesDaily.branch_ext_id
        source_warehouse_col = AggSalesDaily.warehouse_ext_id
        source_brand_col = AggSalesDaily.brand_ext_id
        source_category_col = AggSalesDaily.category_ext_id
        source_group_col = AggSalesDaily.group_ext_id
        source_filters = _apply_sales_filters
    else:
        source_date_col = FactSales.doc_date
        source_branch_col = FactSales.branch_ext_id
        source_warehouse_col = FactSales.warehouse_ext_id
        source_brand_col = FactSales.brand_ext_id
        source_category_col = FactSales.category_ext_id
        source_group_col = FactSales.group_ext_id
        source_filters = _apply_fact_sales_filters

    options = await _build_options(
        source_date_col,
        source_branch_col,
        source_warehouse_col,
        source_brand_col,
        source_category_col,
        source_group_col,
        source_filters,
        date_from,
        date_to,
    )

    if not any(options[key] for key in ('branches', 'warehouses', 'brands', 'categories', 'groups')):
        bounds = (
            await db.execute(
                select(func.min(source_date_col).label('min_date'), func.max(source_date_col).label('max_date'))
                .where(source_date_col.is_not(None))
            )
        ).mappings().one()
        min_date = bounds.get('min_date')
        max_date = bounds.get('max_date')
        if isinstance(min_date, date) and isinstance(max_date, date):
            options = await _build_options(
                source_date_col,
                source_branch_col,
                source_warehouse_col,
                source_brand_col,
                source_category_col,
                source_group_col,
                source_filters,
                min_date,
                max_date,
            )

    channels_rows = (
        await db.execute(
            select(FactSales.channel_ext_id, FactSales.channel_name)
            .where(FactSales.doc_date >= date_from)
            .where(FactSales.doc_date <= date_to)
            .where(FactSales.channel_ext_id.is_not(None))
            .where(FactSales.channel_ext_id != '')
            .distinct()
            .order_by(FactSales.channel_name)
        )
    ).all()
    channel_ids = [str(r[0]) for r in channels_rows]
    labels['channels'] = {str(r[0]): str(r[1] or r[0]) for r in channels_rows}

    return {**options, 'channels': channel_ids, 'labels': labels}


async def sales_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    channels: list[str] | None = None,
    status: str = 'all',
    series: str | None = None,
    document_no: str | None = None,
    eshop_code: str | None = None,
    customer: str | None = None,
    from_ref: str | None = None,
    to_ref: str | None = None,
    gross_min: float | None = None,
    gross_max: float | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    doc_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    base = (
        select(
            doc_key.label('document_id'),
            func.max(FactSales.document_no).label('document_no'),
            func.max(FactSales.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactSales.warehouse_ext_id), literal('N/A')).label(
                'warehouse_name'
            ),
            func.coalesce(func.max(FactSales.document_series), func.max(FactSales.document_type), literal('N/A')).label(
                'series_label'
            ),
            func.coalesce(func.max(FactSales.document_status), literal('N/A')).label('status_label'),
            func.coalesce(func.max(FactSales.document_type), literal('N/A')).label('document_type'),
            func.coalesce(func.max(FactSales.eshop_code), literal('')).label('eshop_code'),
            func.coalesce(
                func.max(FactSales.customer_name),
                func.max(FactSales.customer_code),
                literal('ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
            ).label('customer_name'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty_total'),
            func.coalesce(func.sum(func.coalesce(FactSales.qty_executed, FactSales.qty)), 0).label('qty_exec_total'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(func.coalesce(FactSales.vat_amount, 0)), 0).label('vat_value'),
            (func.coalesce(func.sum(FactSales.net_value), 0) + func.coalesce(func.sum(func.coalesce(FactSales.vat_amount, 0)), 0)).label('gross_value'),
            func.count(FactSales.id).label('line_count'),
            func.coalesce(func.max(FactSales.origin_ref), literal('')).label('origin_ref'),
            func.coalesce(func.max(FactSales.destination_ref), literal('')).label('destination_ref'),
            func.coalesce(func.max(FactSales.delivery_address), literal('')).label('delivery_address'),
            func.coalesce(func.max(FactSales.delivery_city), literal('')).label('delivery_city'),
            func.coalesce(func.max(FactSales.notes), literal('')).label('notes_1'),
            func.coalesce(func.max(FactSales.notes_2), literal('')).label('notes_2'),
            func.max(func.coalesce(FactSales.source_updated_at, FactSales.updated_at)).label('last_update'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    base = _apply_fact_sales_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        channels=channels,
    )

    status_clean = str(status or 'all').strip().lower()
    if status_clean not in {'', 'all'}:
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.document_status, literal('')), String)).like(f'%{status_clean}%')
        )

    series_clean = str(series or '').strip().lower()
    if series_clean:
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.document_series, FactSales.document_type, literal('')), String)).like(
                f'%{series_clean}%'
            )
        )

    document_no_clean = str(document_no or '').strip().lower()
    if document_no_clean:
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.document_no, FactSales.document_id, FactSales.external_id), String)).like(
                f'%{document_no_clean}%'
            )
        )

    eshop_code_clean = str(eshop_code or '').strip().lower()
    if eshop_code_clean:
        base = base.where(func.lower(cast(func.coalesce(FactSales.eshop_code, literal('')), String)).like(f'%{eshop_code_clean}%'))

    customer_clean = str(customer or '').strip().lower()
    if customer_clean:
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.customer_name, FactSales.customer_code, literal('')), String)).like(
                f'%{customer_clean}%'
            )
        )

    from_ref_clean = str(from_ref or '').strip().lower()
    if from_ref_clean:
        base = base.where(func.lower(cast(func.coalesce(FactSales.origin_ref, literal('')), String)).like(f'%{from_ref_clean}%'))

    to_ref_clean = str(to_ref or '').strip().lower()
    if to_ref_clean:
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.destination_ref, literal('')), String)).like(f'%{to_ref_clean}%')
        )

    gross_total_expr = func.coalesce(func.sum(FactSales.net_value), 0) + func.coalesce(func.sum(func.coalesce(FactSales.vat_amount, 0)), 0)
    if gross_min is not None:
        base = base.having(gross_total_expr >= float(gross_min))
    if gross_max is not None:
        base = base.having(gross_total_expr <= float(gross_max))

    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            func.lower(cast(func.coalesce(FactSales.document_no, FactSales.document_id, FactSales.external_id), String)).like(
                like
            )
            | func.lower(cast(func.coalesce(FactSales.customer_name, FactSales.customer_code, literal('')), String)).like(
                like
            )
            | func.lower(cast(func.coalesce(FactSales.eshop_code, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactSales.document_series, FactSales.document_type, literal('')), String)).like(
                like
            )
            | func.lower(cast(func.coalesce(FactSales.delivery_address, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactSales.delivery_city, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactSales.notes, FactSales.notes_2, literal('')), String)).like(like)
        )

    docs_sub = base.group_by(doc_key).subquery('sales_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(docs_sub.c.net_value), 0).label('net_value'),
                func.coalesce(func.sum(docs_sub.c.vat_value), 0).label('vat_value'),
                func.coalesce(func.sum(docs_sub.c.qty_total), 0).label('qty_total'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        delivery_address = str(r.get('delivery_address') or '').strip()
        delivery_city = str(r.get('delivery_city') or '').strip()
        delivery_parts = [part for part in [delivery_address, delivery_city] if part]
        notes_1 = str(r.get('notes_1') or '').strip()
        notes_2 = str(r.get('notes_2') or '').strip()
        notes_preview = notes_1 or notes_2
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': str(r.get('branch_name') or 'N/A'),
                'warehouse': str(r.get('warehouse_name') or 'N/A'),
                'series': str(r.get('series_label') or 'N/A'),
                'document_type': str(r.get('document_type') or 'N/A'),
                'status': str(r.get('status_label') or 'N/A'),
                'eshop_code': str(r.get('eshop_code') or ''),
                'customer': str(r.get('customer_name') or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
                'total_qty': float(r.get('qty_total') or 0),
                'total_qty_executed': float(r.get('qty_exec_total') or 0),
                'total_net_value': float(r.get('net_value') or 0),
                'total_vat_value': float(r.get('vat_value') or 0),
                'total_gross_value': float(r.get('gross_value') or 0),
                'line_count': int(r.get('line_count') or 0),
                'from_ref': str(r.get('origin_ref') or ''),
                'to_ref': str(r.get('destination_ref') or ''),
                'delivery_info': ' | '.join(delivery_parts),
                'comments_info': notes_preview[:220],
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'gross_value': float(totals_row['gross_value'] or 0),
            'net_value': float(totals_row['net_value'] or 0),
            'vat_value': float(totals_row['vat_value'] or 0),
            'qty_total': float(totals_row['qty_total'] or 0),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def sales_document_detail(
    db: AsyncSession,
    document_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    doc_id = str(document_id or '').strip()
    if not doc_id:
        raise ValueError('Missing document id')

    doc_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    stmt = (
        select(
            FactSales,
            DimItem.name.label('item_name'),
            DimBranch.name.label('branch_name'),
            DimWarehouse.name.label('warehouse_name'),
        )
        .select_from(FactSales)
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
        .where(doc_key == doc_id)
    )
    if date_from is not None:
        stmt = stmt.where(FactSales.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactSales.doc_date <= date_to)
    stmt = _apply_fact_sales_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    rows = (
        await db.execute(
            stmt.order_by(
                FactSales.doc_date.desc(),
                FactSales.line_no.asc().nulls_last(),
                FactSales.external_id.asc(),
            )
        )
    ).all()
    if not rows:
        raise ValueError('Sales document not found')

    first_fact: FactSales = rows[0][0]
    branch_name = str(rows[0][2] or first_fact.branch_ext_id or 'N/A')
    warehouse_name = str(rows[0][3] or first_fact.warehouse_ext_id or 'N/A')
    source_payload = first_fact.source_payload_json if isinstance(first_fact.source_payload_json, dict) else {}

    line_rows = []
    total_qty = 0.0
    total_exec = 0.0
    total_net = 0.0
    total_vat = 0.0
    total_gross = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactSales = row[0]
        source_payload_row = fact.source_payload_json if isinstance(fact.source_payload_json, dict) else {}
        import_tag = str(source_payload_row.get('import_tag') or '')
        payload_item_code = _payload_text(
            source_payload_row,
            'item_code',
            'item_id',
            'sku',
            'barcode',
            'product_code',
            'product_id',
            fallback='',
        )
        payload_item_name = _payload_text(
            source_payload_row,
            'item_name',
            'item_description',
            'item_desc',
            'item_descr',
            'description',
            'product_name',
            'product_description',
            'mtrl_name',
            'mtrl_descr',
            'mtrl_description',
            'name',
            'title',
            'descr',
            fallback='',
        )
        item_code = str(payload_item_code or fact.item_code or '').strip()
        dim_item_name_raw = str(row[1] or '').strip()
        prefer_payload_name = bool(payload_item_name) and (
            not dim_item_name_raw or (item_code and dim_item_name_raw.lower() == item_code.lower())
        )
        item_name_source = payload_item_name if prefer_payload_name else (row[1] or payload_item_name or '')
        item_name = _clean_item_name(item_name_source, None)
        if item_name == 'N/A':
            item_name = ''

        qty = float(fact.qty or 0)
        qty_exec = float(fact.qty_executed if fact.qty_executed is not None else qty)
        net_value = float(fact.net_value or 0)
        gross_value = float(fact.gross_value or 0)
        vat_value = float(fact.vat_amount if fact.vat_amount is not None else (gross_value - net_value))
        unit_price = float(fact.unit_price) if fact.unit_price is not None else (net_value / qty if qty else 0.0)
        fact_discount_pct = float(fact.discount_pct) if fact.discount_pct is not None else None
        fact_discount_amount = float(fact.discount_amount) if fact.discount_amount is not None else None
        payload_discount_pct, payload_discount_amount = _resolve_line_discount(
            source_payload_row,
            net_value=net_value,
            fallback_pct=fact_discount_pct,
            fallback_amount=fact_discount_amount,
        )
        discount_pct = (
            fact_discount_pct
            if fact_discount_pct is not None and abs(fact_discount_pct) > 0.0001
            else payload_discount_pct
        )
        discount_amount = (
            fact_discount_amount
            if fact_discount_amount is not None and abs(fact_discount_amount) > 0.0001
            else payload_discount_amount
        )

        total_qty += qty
        total_exec += qty_exec
        total_net += net_value
        total_vat += vat_value
        total_gross += net_value + vat_value

        line_rows.append(
            {
                'row_no': idx,
                'line_no': int(fact.line_no) if fact.line_no is not None else idx,
                'item_code': item_code,
                'item_name': item_name,
                'qty': qty,
                'qty_executed': qty_exec,
                'unit_price': unit_price,
                'discount_pct': discount_pct,
                'discount_amount': discount_amount,
                'vat_amount': vat_value,
                'line_total': gross_value,
                'line_net': net_value,
                'line_external_id': str(fact.external_id or ''),
            }
        )

    doc_no = str(first_fact.document_no or first_fact.document_id or first_fact.external_id or '')
    doc_key_value = str(first_fact.document_id or first_fact.document_no or first_fact.external_id or '')
    expenses_value = _payload_float(
        source_payload,
        'expenses_value',
        'expense_value',
        'expenses_amount',
        'expense_amount',
        'total_expenses',
        'expenses_total',
        'other_charges',
        'charges_amount',
        'shipping_cost',
        'fees_amount',
        'value_expenses',
        'axia_exodon',
    )
    if expenses_value is None:
        residual = total_gross - total_net - total_vat
        expenses_value = float(residual) if abs(residual) > 0.0001 else 0.0

    header_series = _payload_code_name(
        source_payload,
        ['series_code', 'series_id', 'series_no'],
        ['series_name', 'series_description', 'document_series'],
        fallback=str(first_fact.document_series or ''),
    )
    header_type = _payload_code_name(
        source_payload,
        ['document_type_code', 'doc_type_code', 'type_code'],
        ['document_type_name', 'doc_type_name', 'document_type', 'type_name'],
        fallback=str(first_fact.document_type or ''),
    )
    header_status = _payload_code_name(
        source_payload,
        ['status_code', 'document_status_code'],
        ['status_name', 'document_status', 'status'],
        fallback=str(first_fact.document_status or ''),
    )
    header_payment = _payload_code_name(
        source_payload,
        ['payment_code', 'payment_method_code', 'payment_type_code'],
        ['payment_name', 'payment_method', 'payment_type', 'payment_mode'],
        fallback=str(first_fact.payment_method or ''),
    )
    header_shipping = _payload_code_name(
        source_payload,
        ['shipping_code', 'shipment_code', 'dispatch_code'],
        ['shipping_name', 'shipping_method', 'shipment_method', 'dispatch_method'],
        fallback=str(first_fact.shipping_method or ''),
    )
    movement_text = _payload_code_name(
        source_payload,
        ['movement_code', 'movement_type_code', 'dispatch_movement_code'],
        ['movement_name', 'movement_type', 'dispatch_movement', 'delivery_movement'],
        fallback=str(first_fact.movement_type or ''),
    )
    customer_branch = _payload_text(
        source_payload,
        'customer_branch',
        'customer_branch_name',
        'customer_store',
        'subcustomer_branch',
        fallback='',
    )

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'fact_sales.header', first_fact)
    _append_raw_field(raw_fields, 'dim_branches.name', branch_name)
    _append_raw_field(raw_fields, 'dim_warehouses.name', warehouse_name)
    if isinstance(first_fact.source_payload_json, dict):
        for key, value in first_fact.source_payload_json.items():
            _append_raw_field(raw_fields, f'source.header.{key}', value)

    for idx, row in enumerate(rows, start=1):
        fact: FactSales = row[0]
        if isinstance(fact.source_payload_json, dict):
            for key, value in fact.source_payload_json.items():
                _append_raw_field(raw_fields, f'source.line[{idx}].{key}', value)

    return {
        'document_id': doc_key_value,
        'document_no': doc_no,
        'document_date': first_fact.doc_date.isoformat() if first_fact.doc_date else '',
        'header': {
            'branch_code': str(first_fact.branch_ext_id or ''),
            'branch_name': branch_name,
            'warehouse_code': str(first_fact.warehouse_ext_id or ''),
            'warehouse_name': warehouse_name,
            'series': header_series,
            'document_type': header_type,
            'status': header_status,
            'eshop_code': str(first_fact.eshop_code or ''),
            'customer_code': str(first_fact.customer_code or ''),
            'customer_name': str(first_fact.customer_name or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
            'payment_method': header_payment,
            'shipping_method': header_shipping,
            'reason': str(first_fact.reason or ''),
            'from_ref': str(first_fact.origin_ref or ''),
            'to_ref': str(first_fact.destination_ref or ''),
            'channel_name': str(getattr(first_fact, 'channel_name', '') or ''),
        },
        'delivery': {
            'customer_branch': customer_branch,
            'address': _payload_text(
                source_payload,
                'delivery_address',
                'ship_address',
                'address_delivery',
                fallback=str(first_fact.delivery_address or ''),
            ),
            'zip': _payload_text(
                source_payload,
                'delivery_zip',
                'delivery_postal_code',
                'postal_code',
                fallback=str(first_fact.delivery_zip or ''),
            ),
            'city': _payload_text(
                source_payload,
                'delivery_city',
                'ship_city',
                'city',
                fallback=str(first_fact.delivery_city or ''),
            ),
            'area': _payload_text(
                source_payload,
                'delivery_area',
                'region',
                fallback=str(first_fact.delivery_area or ''),
            ),
            'movement_type': movement_text,
            'carrier_name': _payload_text(
                source_payload,
                'carrier_name',
                'carrier',
                'transport_company',
                fallback=str(first_fact.carrier_name or ''),
            ),
            'transport_medium': _payload_text(
                source_payload,
                'transport_medium',
                'transport_means',
                'vehicle_type',
                fallback=str(first_fact.transport_medium or ''),
            ),
            'transport_no': _payload_text(
                source_payload,
                'transport_no',
                'vehicle_no',
                'transport_number',
                fallback=str(first_fact.transport_no or ''),
            ),
            'route_name': _payload_text(
                source_payload,
                'route_name',
                'route',
                'itinerary',
                fallback=str(first_fact.route_name or ''),
            ),
            'loading_date': _payload_text(
                source_payload,
                'loading_date',
                'load_date',
                'shipping_date',
                fallback=(first_fact.loading_date.isoformat() if first_fact.loading_date else ''),
            ),
            'delivery_date': _payload_text(
                source_payload,
                'delivery_date',
                'ship_date',
                'eta_date',
                fallback=(first_fact.delivery_date.isoformat() if first_fact.delivery_date else ''),
            ),
        },
        'notes': {
            'notes_1': _payload_text(
                source_payload,
                'notes',
                'remarks',
                'comment',
                'observation',
                fallback=str(first_fact.notes or ''),
            ),
            'notes_2': _payload_text(
                source_payload,
                'notes_2',
                'comments2',
                'remarks_2',
                'aitiologia2',
                fallback=str(first_fact.notes_2 or ''),
            ),
        },
        'audit': {
            'created_at': _raw_scalar(first_fact.source_created_at),
            'created_by': str(first_fact.source_created_by or ''),
            'updated_at': _raw_scalar(first_fact.source_updated_at or first_fact.updated_at),
            'updated_by': str(first_fact.source_updated_by or ''),
        },
        'totals': {
            'gross_value': total_gross,
            'net_value': total_net,
            'vat_value': total_vat,
            'expenses_value': expenses_value,
            'qty_total': total_qty,
            'qty_exec_total': total_exec,
            'line_count': len(line_rows),
        },
        'lines': line_rows,
        'lines_note': '',
        'raw_fields': raw_fields,
    }


async def purchases_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    doc_key = _fact_purchases_document_key_expr()
    doc_no_expr = _fact_purchases_document_no_expr(doc_key)
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(doc_no_expr), literal('')).label('document_no'),
            func.max(FactPurchases.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactPurchases.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactPurchases.warehouse_ext_id), literal('N/A')).label(
                'warehouse_name'
            ),
            func.coalesce(func.max(FactPurchases.document_series), func.max(FactPurchases.document_type), literal('Αγορές')).label(
                'series_label'
            ),
            literal('').label('status_label'),
            func.coalesce(func.max(FactPurchases.document_type), literal('Παραστατικό Αγορών')).label('document_type'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactPurchases.supplier_ext_id), literal('N/A')).label(
                'supplier_name'
            ),
            literal('').label('reason'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('qty_total'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('cost_value'),
            func.count(FactPurchases.id).label('line_count'),
            func.max(FactPurchases.updated_at).label('last_update'),
        )
        .select_from(FactPurchases)
        .join(DimBranch, DimBranch.external_id == FactPurchases.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactPurchases.warehouse_ext_id, isouter=True)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    base = _apply_fact_purchases_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )

    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            func.lower(cast(doc_key, String)).like(like)
            | func.lower(cast(func.coalesce(doc_no_expr, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimSupplier.name, FactPurchases.supplier_ext_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.supplier_ext_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.item_code, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.document_series, FactPurchases.document_type, literal('')), String)).like(
                like
            )
        )

    docs_sub = base.group_by(doc_key).subquery('purchase_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.net_value), 0).label('net_value'),
                func.coalesce(func.sum(docs_sub.c.cost_value), 0).label('cost_value'),
                func.coalesce(func.sum(docs_sub.c.qty_total), 0).label('qty_total'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': str(r.get('branch_name') or 'N/A'),
                'warehouse': str(r.get('warehouse_name') or 'N/A'),
                'series': str(r.get('series_label') or r.get('document_type') or 'Αγορές'),
                'document_type': str(r.get('document_type') or 'Παραστατικό Αγορών'),
                'status': str(r.get('status_label') or ''),
                'supplier': str(r.get('supplier_name') or 'N/A'),
                'reason': str(r.get('reason') or ''),
                'total_qty': float(r.get('qty_total') or 0),
                'total_net_value': float(r.get('net_value') or 0),
                'total_cost_value': float(r.get('cost_value') or 0),
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'net_value': float(totals_row['net_value'] or 0),
            'cost_value': float(totals_row['cost_value'] or 0),
            'qty_total': float(totals_row['qty_total'] or 0),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def purchase_document_detail(
    db: AsyncSession,
    document_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    doc_id = str(document_id or '').strip()
    if not doc_id:
        raise ValueError('Missing document id')

    doc_key = _fact_purchases_document_key_expr()
    stmt = (
        select(
            FactPurchases,
            DimItem.name.label('item_name'),
            DimBranch.name.label('branch_name'),
            DimWarehouse.name.label('warehouse_name'),
            DimSupplier.name.label('supplier_name'),
        )
        .select_from(FactPurchases)
        .join(DimItem, DimItem.external_id == FactPurchases.item_code, isouter=True)
        .join(DimBranch, DimBranch.external_id == FactPurchases.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactPurchases.warehouse_ext_id, isouter=True)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(doc_key == doc_id)
    )
    if date_from is not None:
        stmt = stmt.where(FactPurchases.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactPurchases.doc_date <= date_to)
    stmt = _apply_fact_purchases_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )

    rows = (
        await db.execute(
            stmt.order_by(
                FactPurchases.doc_date.desc(),
                FactPurchases.external_id.asc(),
            )
        )
    ).all()
    if not rows:
        raise ValueError('Purchase document not found')

    first_fact: FactPurchases = rows[0][0]
    branch_name = str(rows[0][2] or first_fact.branch_ext_id or 'N/A')
    warehouse_name = str(rows[0][3] or first_fact.warehouse_ext_id or 'N/A')
    supplier_name = str(rows[0][4] or first_fact.supplier_ext_id or 'N/A')
    document_no = _purchase_document_no_from_fact(first_fact, doc_id)

    line_rows = []
    total_qty = 0.0
    total_net = 0.0
    total_vat = 0.0
    total_gross = 0.0
    total_cost = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactPurchases = row[0]
        payload = fact.source_payload_json if isinstance(fact.source_payload_json, dict) else {}
        payload_item_name = _payload_text(
            payload,
            'item_name',
            'item_description',
            'item_desc',
            'item_descr',
            'description',
            'product_name',
            'product_description',
            'mtrl_name',
            'mtrl_descr',
            'mtrl_description',
            'name',
            'title',
            'descr',
            fallback='',
        )
        item_code = str(fact.item_code or '').strip()
        dim_item_name_raw = str(row[1] or '').strip()
        prefer_payload_name = bool(payload_item_name) and (
            not dim_item_name_raw or (item_code and dim_item_name_raw.lower() == item_code.lower())
        )
        item_name = _clean_item_name(payload_item_name if prefer_payload_name else row[1], fact.item_code)
        qty = float(fact.qty or 0)
        net_value = float(fact.net_value or 0)
        cost_value = float(fact.cost_amount or 0)
        vat_value = _payload_float(
            payload,
            'vat_amount',
            'tax_amount',
            'fpa_amount',
            'line_vat',
            'line_tax',
            'vatvalue',
            'taxvalue',
        )
        gross_value = _payload_float(
            payload,
            'gross_value',
            'gross_amount',
            'line_total',
            'total_value',
            'value_total',
        )
        if vat_value is None and gross_value is not None:
            vat_value = gross_value - net_value
        if vat_value is None:
            vat_value = 0.0
        if gross_value is None:
            gross_value = net_value + vat_value

        vat_value = max(0.0, float(vat_value))
        gross_value = float(gross_value)
        unit_price = net_value / qty if qty else 0.0

        total_qty += qty
        total_net += net_value
        total_vat += vat_value
        total_gross += gross_value
        total_cost += cost_value
        fact_discount_pct = float(fact.discount_pct) if getattr(fact, 'discount_pct', None) is not None else None
        fact_discount_amount = (
            float(fact.discount_amount) if getattr(fact, 'discount_amount', None) is not None else None
        )
        discount_pct, discount_amount = _resolve_line_discount(
            payload,
            net_value=net_value,
            fallback_pct=fact_discount_pct,
            fallback_amount=fact_discount_amount,
        )
        discount1_pct = (
            float(fact.discount1_pct)
            if getattr(fact, 'discount1_pct', None) is not None
            else _payload_float(payload, 'discount1_pct', 'disc1prc', 'discount_1_pct')
        )
        discount2_pct = (
            float(fact.discount2_pct)
            if getattr(fact, 'discount2_pct', None) is not None
            else _payload_float(payload, 'discount2_pct', 'disc2prc', 'discount_2_pct')
        )
        discount3_pct = (
            float(fact.discount3_pct)
            if getattr(fact, 'discount3_pct', None) is not None
            else _payload_float(payload, 'discount3_pct', 'disc3prc', 'discount_3_pct')
        )
        discount1_amount = (
            float(fact.discount1_amount)
            if getattr(fact, 'discount1_amount', None) is not None
            else _payload_float(payload, 'discount1_amount', 'disc1val', 'discount_1_amount')
        )
        discount2_amount = (
            float(fact.discount2_amount)
            if getattr(fact, 'discount2_amount', None) is not None
            else _payload_float(payload, 'discount2_amount', 'disc2val', 'discount_2_amount')
        )
        discount3_amount = (
            float(fact.discount3_amount)
            if getattr(fact, 'discount3_amount', None) is not None
            else _payload_float(payload, 'discount3_amount', 'disc3val', 'discount_3_amount')
        )

        line_rows.append(
            {
                'row_no': idx,
                'line_no': idx,
                'item_code': str(fact.item_code or ''),
                'item_name': item_name,
                'qty': qty,
                'qty_executed': qty,
                'unit_price': unit_price,
                'discount1_pct': float(discount1_pct or 0.0),
                'discount2_pct': float(discount2_pct or 0.0),
                'discount3_pct': float(discount3_pct or 0.0),
                'discount1_amount': float(discount1_amount or 0.0),
                'discount2_amount': float(discount2_amount or 0.0),
                'discount3_amount': float(discount3_amount or 0.0),
                'discount_pct': discount_pct,
                'discount_amount': discount_amount,
                'vat_amount': vat_value,
                'line_total': gross_value,
                'line_net': net_value,
                'line_external_id': str(fact.external_id or ''),
                'channel_name': str(getattr(fact, 'channel_name', '') or ''),
            }
        )

    expenses_value = total_gross - total_net - total_vat
    if abs(expenses_value) <= 0.0001:
        expenses_value = 0.0

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'fact_purchases.header', first_fact)
    _append_raw_field(raw_fields, 'dim_branches.name', branch_name)
    _append_raw_field(raw_fields, 'dim_warehouses.name', warehouse_name)
    _append_raw_field(raw_fields, 'dim_suppliers.name', supplier_name)
    if isinstance(first_fact.source_payload_json, dict):
        for key, value in first_fact.source_payload_json.items():
            _append_raw_field(raw_fields, f'source.header.{key}', value)
    for idx, row in enumerate(rows, start=1):
        fact: FactPurchases = row[0]
        if isinstance(fact.source_payload_json, dict):
            for key, value in fact.source_payload_json.items():
                _append_raw_field(raw_fields, f'source.line[{idx}].{key}', value)

    return {
        'document_id': doc_id,
        'document_no': document_no,
        'document_date': first_fact.doc_date.isoformat() if first_fact.doc_date else '',
        'header': {
            'branch_code': str(first_fact.branch_ext_id or ''),
            'branch_name': branch_name,
            'warehouse_code': str(first_fact.warehouse_ext_id or ''),
            'warehouse_name': warehouse_name,
            'series': str(first_fact.document_series or first_fact.document_type or 'Αγορές'),
            'document_type': str(first_fact.document_type or 'Παραστατικό Αγορών'),
            'status': '',
            'supplier_code': str(first_fact.supplier_ext_id or ''),
            'supplier_name': supplier_name,
            'payment_method': '',
            'reason': '',
        },
        'notes': {
            'notes_1': '',
            'notes_2': '',
        },
        'audit': {
            'created_at': _raw_scalar(first_fact.updated_at),
            'created_by': '',
            'updated_at': _raw_scalar(first_fact.updated_at),
            'updated_by': '',
        },
        'totals': {
            'gross_value': total_gross,
            'net_value': total_net,
            'vat_value': total_vat,
            'expenses_value': expenses_value,
            'cost_value': total_cost,
            'qty_total': total_qty,
            'line_count': len(line_rows),
        },
        'lines': line_rows,
        'raw_fields': raw_fields,
    }


async def inventory_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    doc_key = _fact_inventory_document_key_expr()
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactInventory.document_no), func.max(doc_key), literal('')).label('document_no'),
            func.max(FactInventory.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactInventory.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(FactInventory.branch_ext_id), literal('')).label('branch_code'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactInventory.warehouse_ext_id), literal('N/A')).label(
                'warehouse_name'
            ),
            func.coalesce(func.max(FactInventory.warehouse_ext_id), literal('')).label('warehouse_code'),
            func.coalesce(
                func.max(FactInventory.document_series),
                func.max(FactInventory.document_type),
                literal('Κίνηση Αποθήκης'),
            ).label('series_label'),
            func.coalesce(func.max(FactInventory.document_type), literal('Κίνηση Αποθήκης')).label('document_type'),
            literal('').label('status_label'),
            literal('').label('reason'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_total'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_total'),
            func.coalesce(func.sum(FactInventory.cost_amount), 0).label('cost_total'),
            func.count(FactInventory.id).label('line_count'),
            func.max(FactInventory.updated_at).label('last_update'),
        )
        .select_from(FactInventory)
        .join(DimBranch, DimBranch.id == FactInventory.branch_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.id == FactInventory.warehouse_id, isouter=True)
        .join(DimItem, DimItem.id == FactInventory.item_id, isouter=True)
        .join(DimBrand, DimBrand.id == DimItem.brand_id, isouter=True)
        .join(DimCategory, DimCategory.id == DimItem.category_id, isouter=True)
        .join(DimGroup, DimGroup.id == DimItem.group_id, isouter=True)
        .where(*_date_range(FactInventory.doc_date, date_from, date_to))
        # Inventory snapshots belong to inventory balance views, not ERP warehouse documents.
        .where(func.coalesce(FactInventory.document_series, literal('')) != literal('SNAPSHOT'))
        .where(_inventory_item_scope_predicate())
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        base = base.where(DimBranch.external_id.in_(branches))
    if warehouses:
        base = base.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        base = base.where(DimBrand.external_id.in_(brands))
    if categories:
        base = base.where(DimCategory.external_id.in_(categories))
    if groups:
        base = base.where(DimGroup.external_id.in_(groups))

    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            func.lower(cast(doc_key, String)).like(like)
            | func.lower(cast(func.coalesce(FactInventory.document_no, FactInventory.document_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactInventory.document_series, FactInventory.document_type, literal('')), String)).like(
                like
            )
            | func.lower(cast(func.coalesce(DimBranch.name, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimWarehouse.name, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimItem.external_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimItem.name, literal('')), String)).like(like)
        )

    docs_sub = base.group_by(doc_key).subquery('inventory_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.value_total), 0).label('value_total'),
                func.coalesce(func.sum(docs_sub.c.cost_total), 0).label('cost_total'),
                func.coalesce(func.sum(docs_sub.c.qty_total), 0).label('qty_total'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        branch_name = str(r.get('branch_name') or 'N/A')
        warehouse_name = str(r.get('warehouse_name') or 'N/A')
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': branch_name,
                'branch_code': str(r.get('branch_code') or ''),
                'warehouse': warehouse_name,
                'warehouse_code': str(r.get('warehouse_code') or ''),
                'branch_2': branch_name,
                'warehouse_2': warehouse_name,
                'series': str(r.get('series_label') or r.get('document_type') or 'Κίνηση Αποθήκης'),
                'document_type': str(r.get('document_type') or 'Κίνηση Αποθήκης'),
                'status': str(r.get('status_label') or ''),
                'reason': str(r.get('reason') or ''),
                'total_qty': float(r.get('qty_total') or 0),
                'total_value': float(r.get('value_total') or 0),
                'total_cost': float(r.get('cost_total') or 0),
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'value_total': float(totals_row['value_total'] or 0),
            'cost_total': float(totals_row['cost_total'] or 0),
            'qty_total': float(totals_row['qty_total'] or 0),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def inventory_document_detail(
    db: AsyncSession,
    document_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    doc_id = str(document_id or '').strip()
    if not doc_id:
        raise ValueError('Missing document id')

    doc_key = _fact_inventory_document_key_expr()
    stmt = (
        select(
            FactInventory,
            DimItem.external_id.label('item_code'),
            DimItem.name.label('item_name'),
            DimBranch.external_id.label('branch_code'),
            DimBranch.name.label('branch_name'),
            DimWarehouse.external_id.label('warehouse_code'),
            DimWarehouse.name.label('warehouse_name'),
            DimBrand.external_id.label('brand_code'),
            DimCategory.external_id.label('category_code'),
            DimGroup.external_id.label('group_code'),
        )
        .select_from(FactInventory)
        .join(DimItem, DimItem.id == FactInventory.item_id, isouter=True)
        .join(DimBranch, DimBranch.id == FactInventory.branch_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.id == FactInventory.warehouse_id, isouter=True)
        .join(DimBrand, DimBrand.id == DimItem.brand_id, isouter=True)
        .join(DimCategory, DimCategory.id == DimItem.category_id, isouter=True)
        .join(DimGroup, DimGroup.id == DimItem.group_id, isouter=True)
        .where(doc_key == doc_id)
        .where(func.coalesce(FactInventory.document_series, literal('')) != literal('SNAPSHOT'))
        .where(_inventory_item_scope_predicate())
    )
    if date_from is not None:
        stmt = stmt.where(FactInventory.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactInventory.doc_date <= date_to)
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    if warehouses:
        stmt = stmt.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if categories:
        stmt = stmt.where(DimCategory.external_id.in_(categories))
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))

    rows = (
        await db.execute(
            stmt.order_by(
                FactInventory.doc_date.desc(),
                FactInventory.external_id.asc(),
            )
        )
    ).all()
    if not rows:
        raise ValueError('Inventory document not found')

    first_fact: FactInventory = rows[0][0]
    branch_code = str(rows[0][3] or first_fact.branch_ext_id or '')
    branch_name = str(rows[0][4] or first_fact.branch_ext_id or 'N/A')
    warehouse_code = str(rows[0][5] or first_fact.warehouse_ext_id or '')
    warehouse_name = str(rows[0][6] or first_fact.warehouse_ext_id or 'N/A')

    line_rows = []
    total_qty = 0.0
    total_value = 0.0
    total_cost = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactInventory = row[0]
        item_code = str(row[1] or fact.item_code or '')
        payload = fact.source_payload_json if isinstance(fact.source_payload_json, dict) else {}
        payload_item_name = _payload_text(
            payload,
            'item_name',
            'item_description',
            'item_desc',
            'item_descr',
            'description',
            'product_name',
            'product_description',
            'mtrl_name',
            'mtrl_descr',
            'mtrl_description',
            'name',
            'title',
            'descr',
            fallback='',
        )
        dim_item_name_raw = str(row[2] or '').strip()
        prefer_payload_name = bool(payload_item_name) and (
            not dim_item_name_raw or (item_code and dim_item_name_raw.lower() == item_code.lower())
        )
        item_name = _clean_item_name(payload_item_name if prefer_payload_name else row[2], item_code)
        qty = float(fact.qty_on_hand or 0)
        value_amount = float(fact.value_amount or 0)
        cost_amount = float(fact.cost_amount or 0)
        unit_price = value_amount / qty if qty else 0.0

        total_qty += qty
        total_value += value_amount
        total_cost += cost_amount
        discount_pct, discount_amount = _resolve_line_discount(payload, net_value=value_amount)

        line_rows.append(
            {
                'row_no': idx,
                'line_no': idx,
                'item_code': item_code,
                'item_name': item_name,
                'qty': qty,
                'qty_executed': 0.0,
                'unit_price': unit_price,
                'discount_pct': discount_pct,
                'discount_amount': discount_amount,
                'vat_amount': 0.0,
                'line_total': value_amount,
                'line_net': value_amount,
                'line_external_id': str(fact.external_id or ''),
            }
        )

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'fact_inventory.header', first_fact)
    _append_raw_field(raw_fields, 'dim_branches.external_id', branch_code)
    _append_raw_field(raw_fields, 'dim_branches.name', branch_name)
    _append_raw_field(raw_fields, 'dim_warehouses.external_id', warehouse_code)
    _append_raw_field(raw_fields, 'dim_warehouses.name', warehouse_name)
    if isinstance(first_fact.source_payload_json, dict):
        for key, value in first_fact.source_payload_json.items():
            _append_raw_field(raw_fields, f'source.header.{key}', value)
    for idx, row in enumerate(rows, start=1):
        fact: FactInventory = row[0]
        if isinstance(fact.source_payload_json, dict):
            for key, value in fact.source_payload_json.items():
                _append_raw_field(raw_fields, f'source.line[{idx}].{key}', value)

    return {
        'document_id': doc_id,
        'document_no': str(first_fact.document_no or doc_id),
        'document_date': first_fact.doc_date.isoformat() if first_fact.doc_date else '',
        'header': {
            'branch_code': branch_code,
            'branch_name': branch_name,
            'warehouse_code': warehouse_code,
            'warehouse_name': warehouse_name,
            'branch_code_2': branch_code,
            'branch_name_2': branch_name,
            'warehouse_code_2': warehouse_code,
            'warehouse_name_2': warehouse_name,
            'series': str(first_fact.document_series or first_fact.document_type or 'Κίνηση Αποθήκης'),
            'document_type': str(first_fact.document_type or 'Κίνηση Αποθήκης'),
            'status': '',
            'reason': '',
        },
        'movement': {
            'shipment_type': '',
            'carrier_name': '',
            'transport_medium': '',
            'transport_no': '',
            'route_name': '',
            'delivery_address': '',
            'delivery_zip': '',
            'delivery_area': '',
            'delivery_city': '',
            'loading_date': '',
            'delivery_date': '',
        },
        'notes': {
            'notes_1': '',
            'notes_2': '',
            'reason_1': '',
        },
        'audit': {
            'created_at': _raw_scalar(first_fact.created_at),
            'created_by': '',
            'updated_at': _raw_scalar(first_fact.updated_at),
            'updated_by': '',
        },
        'totals': {
            'gross_value': total_value,
            'net_value': total_value,
            'vat_value': 0.0,
            'expenses_value': total_value,
            'qty_total': total_qty,
            'line_count': len(line_rows),
            'cost_value': total_cost,
        },
        'lines': line_rows,
        'raw_fields': raw_fields,
    }


async def purchases_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    brand_values, brand_labels = await _purchase_item_dimension_options(
        db,
        date_from=date_from,
        date_to=date_to,
        dimension='brands',
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    category_values, category_labels = await _purchase_item_dimension_options(
        db,
        date_from=date_from,
        date_to=date_to,
        dimension='categories',
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    group_values, group_labels = await _purchase_item_dimension_options(
        db,
        date_from=date_from,
        date_to=date_to,
        dimension='groups',
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    labels['brands'].update(brand_labels)
    labels['categories'].update(category_labels)
    labels['groups'].update(group_labels)
    return {
        'branches': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.branch_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=None,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'warehouses': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.warehouse_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=None,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'brands': await _distinct_purchase_dimension_values(
            db,
            date_from=date_from,
            date_to=date_to,
            agg_column=AggPurchasesDaily.brand_ext_id,
            fact_column=FactPurchases.brand_ext_id,
            branches=branches,
            warehouses=warehouses,
            brands=None,
            categories=categories,
            groups=groups,
        ) or brand_values,
        'categories': await _distinct_purchase_dimension_values(
            db,
            date_from=date_from,
            date_to=date_to,
            agg_column=AggPurchasesDaily.category_ext_id,
            fact_column=FactPurchases.category_ext_id,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=None,
            groups=groups,
        ) or category_values,
        'groups': await _distinct_purchase_dimension_values(
            db,
            date_from=date_from,
            date_to=date_to,
            agg_column=AggPurchasesDaily.group_ext_id,
            fact_column=FactPurchases.group_ext_id,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=None,
        ) or group_values,
        'labels': labels,
    }


async def expenses_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
):
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'categories': await _expense_category_label_map(db),
    }
    branch_filter = _effective_branch_filter(branches)
    branch_stmt = (
        select(AggExpensesDaily.branch_ext_id)
        .where(*_date_range(AggExpensesDaily.expense_date, date_from, date_to))
        .where(AggExpensesDaily.branch_ext_id.is_not(None))
        .where(AggExpensesDaily.branch_ext_id != '')
    )
    if categories:
        branch_stmt = branch_stmt.where(AggExpensesDaily.expense_category_code.in_(categories))
    branch_stmt = branch_stmt.distinct().order_by(AggExpensesDaily.branch_ext_id)
    category_stmt = (
        select(AggExpensesDaily.expense_category_code)
        .where(*_date_range(AggExpensesDaily.expense_date, date_from, date_to))
        .where(AggExpensesDaily.expense_category_code.is_not(None))
        .where(AggExpensesDaily.expense_category_code != '')
    )
    if branch_filter is not None:
        category_stmt = category_stmt.where(AggExpensesDaily.branch_ext_id.in_(branch_filter))
    category_stmt = category_stmt.distinct().order_by(AggExpensesDaily.expense_category_code)
    return {
        'branches': [str(v) for v in (await db.execute(branch_stmt)).scalars().all() if v],
        'categories': [str(v) for v in (await db.execute(category_stmt)).scalars().all() if v],
        'labels': labels,
    }


async def expenses_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    branch_filter = _effective_branch_filter(branches)
    doc_key = func.coalesce(FactExpense.document_no, FactExpense.external_id)
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactExpense.document_no), func.max(FactExpense.external_id), literal('')).label('document_no'),
            func.max(FactExpense.expense_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactExpense.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(
                func.max(DimExpenseCategory.category_name),
                func.max(FactExpense.expense_category_code),
                literal('N/A'),
            ).label('category_name'),
            func.coalesce(func.max(FactExpense.document_type), literal('Παραστατικό Εξόδων')).label('document_type'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactExpense.supplier_ext_id), literal('N/A')).label('supplier_name'),
            func.coalesce(func.sum(FactExpense.amount_net), 0).label('amount_net'),
            func.coalesce(func.sum(FactExpense.amount_tax), 0).label('amount_tax'),
            func.coalesce(func.sum(FactExpense.amount_gross), 0).label('amount_gross'),
            func.count(FactExpense.id).label('line_count'),
            func.max(FactExpense.updated_at).label('last_update'),
        )
        .select_from(FactExpense)
        .join(DimBranch, DimBranch.id == FactExpense.branch_id, isouter=True)
        .join(DimExpenseCategory, DimExpenseCategory.id == FactExpense.category_id, isouter=True)
        .join(DimSupplier, DimSupplier.id == FactExpense.supplier_id, isouter=True)
        .where(*_date_range(FactExpense.expense_date, date_from, date_to))
    )
    if branch_filter is not None:
        base = base.where(FactExpense.branch_ext_id.in_(branch_filter))
    if categories:
        base = base.where(FactExpense.expense_category_code.in_(categories))
    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            func.lower(cast(func.coalesce(FactExpense.document_no, FactExpense.external_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactExpense.document_type, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimSupplier.name, FactExpense.supplier_ext_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimExpenseCategory.category_name, FactExpense.expense_category_code, literal('')), String)).like(like)
        )
    docs_sub = base.group_by(doc_key).subquery('expense_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.amount_net), 0).label('amount_net'),
                func.coalesce(func.sum(docs_sub.c.amount_tax), 0).label('amount_tax'),
                func.coalesce(func.sum(docs_sub.c.amount_gross), 0).label('amount_gross'),
            )
        )
    ).mappings().one()
    rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()
    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': str(r.get('branch_name') or 'N/A'),
                'category': str(r.get('category_name') or 'N/A'),
                'document_type': str(r.get('document_type') or 'Παραστατικό Εξόδων'),
                'supplier': str(r.get('supplier_name') or 'N/A'),
                'total_net_value': float(r.get('amount_net') or 0),
                'total_tax_value': float(r.get('amount_tax') or 0),
                'total_gross_value': float(r.get('amount_gross') or 0),
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )
    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'amount_net': float(totals_row['amount_net'] or 0),
            'amount_tax': float(totals_row['amount_tax'] or 0),
            'amount_gross': float(totals_row['amount_gross'] or 0),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def expense_document_detail(
    db: AsyncSession,
    document_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
):
    doc_id = str(document_id or '').strip()
    if not doc_id:
        raise ValueError('Missing document id')
    branch_filter = _effective_branch_filter(branches)
    doc_key = func.coalesce(FactExpense.document_no, FactExpense.external_id)
    stmt = (
        select(
            FactExpense,
            DimBranch.name.label('branch_name'),
            DimExpenseCategory.category_name.label('category_name'),
            DimSupplier.name.label('supplier_name'),
            DimAccount.name.label('account_name'),
        )
        .select_from(FactExpense)
        .join(DimBranch, DimBranch.id == FactExpense.branch_id, isouter=True)
        .join(DimExpenseCategory, DimExpenseCategory.id == FactExpense.category_id, isouter=True)
        .join(DimSupplier, DimSupplier.id == FactExpense.supplier_id, isouter=True)
        .join(DimAccount, DimAccount.id == FactExpense.account_id, isouter=True)
        .where(doc_key == doc_id)
    )
    if date_from is not None:
        stmt = stmt.where(FactExpense.expense_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactExpense.expense_date <= date_to)
    if branch_filter is not None:
        stmt = stmt.where(FactExpense.branch_ext_id.in_(branch_filter))
    if categories:
        stmt = stmt.where(FactExpense.expense_category_code.in_(categories))
    rows = (await db.execute(stmt.order_by(FactExpense.expense_date.desc(), FactExpense.external_id.asc()))).all()
    if not rows:
        raise ValueError('Expense document not found')
    first_fact: FactExpense = rows[0][0]
    branch_name = str(rows[0][1] or first_fact.branch_ext_id or 'N/A')
    category_name = str(rows[0][2] or first_fact.expense_category_code or 'N/A')
    supplier_name = str(rows[0][3] or first_fact.supplier_ext_id or 'N/A')
    account_name = str(rows[0][4] or first_fact.account_ext_id or '')
    line_rows = []
    total_net = 0.0
    total_tax = 0.0
    total_gross = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactExpense = row[0]
        amount_net = float(fact.amount_net or 0)
        amount_tax = float(fact.amount_tax or 0)
        amount_gross = float(fact.amount_gross or 0)
        total_net += amount_net
        total_tax += amount_tax
        total_gross += amount_gross
        line_rows.append(
            {
                'row_no': idx,
                'category': str(row[2] or fact.expense_category_code or ''),
                'supplier': str(row[3] or fact.supplier_ext_id or ''),
                'account': str(row[4] or fact.account_ext_id or ''),
                'net_value': amount_net,
                'tax_value': amount_tax,
                'gross_value': amount_gross,
                'external_id': str(fact.external_id or ''),
            }
        )
    return {
        'document_id': doc_id,
        'document_no': str(first_fact.document_no or doc_id),
        'document_date': first_fact.expense_date.isoformat() if first_fact.expense_date else '',
        'header': {
            'branch_name': branch_name,
            'category_name': category_name,
            'document_type': str(first_fact.document_type or 'Παραστατικό Εξόδων'),
            'supplier_name': supplier_name,
            'account_name': account_name,
            'payment_status': str(first_fact.payment_status or ''),
            'cost_center': str(first_fact.cost_center or ''),
        },
        'totals': {
            'net_value': total_net,
            'tax_value': total_tax,
            'gross_value': total_gross,
            'line_count': len(line_rows),
        },
        'lines': line_rows,
    }


async def sales_top_products(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 10,
):
    stmt = (
        select(
            FactSales.item_code,
            func.coalesce(func.max(DimItem.name), FactSales.item_code).label('item_name'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactSales.group_ext_id.in_(groups))
    stmt = stmt.group_by(FactSales.item_code).order_by(func.sum(FactSales.net_value).desc()).limit(max(1, min(limit, 50)))
    rows = (await db.execute(stmt)).all()
    return [
        {
            'item_code': r[0] or 'N/A',
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'net_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_slow_fast_movers(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    base_stmt = (
        select(
            FactSales.item_code,
            func.coalesce(func.max(DimItem.name), FactSales.item_code).label('item_name'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
        )
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        base_stmt = base_stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        base_stmt = base_stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        base_stmt = base_stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        base_stmt = base_stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        base_stmt = base_stmt.where(FactSales.group_ext_id.in_(groups))

    grouped = base_stmt.group_by(FactSales.item_code).where(FactSales.item_code.is_not(None))
    fast_rows = (await db.execute(grouped.order_by(func.sum(FactSales.qty).desc()).limit(5))).all()
    slow_rows = (
        await db.execute(grouped.having(func.sum(FactSales.qty) > 0).order_by(func.sum(FactSales.qty).asc()).limit(5))
    ).all()
    fast = [
        {
            'item_code': r[0],
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'qty': float(r[2] or 0),
            'net_value': float(r[3] or 0),
        }
        for r in fast_rows
    ]
    slow = [
        {
            'item_code': r[0],
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'qty': float(r[2] or 0),
            'net_value': float(r[3] or 0),
        }
        for r in slow_rows
    ]

    # Real purchases for the same fast-moving items and same filter period
    fast_item_codes = [str(r['item_code']) for r in fast if r.get('item_code')]
    item_name_map: dict[str, str] = {
        str(r['item_code']): str(r.get('item_name') or r['item_code'])
        for r in fast
        if r.get('item_code')
    }
    for r in slow:
        code = str(r.get('item_code') or '')
        if code and code not in item_name_map:
            item_name_map[code] = str(r.get('item_name') or code)

    purchases_map: dict[str, float] = {}
    if fast_item_codes:
        purchases_stmt = (
            select(
                FactPurchases.item_code,
                func.coalesce(func.max(DimItem.name), FactPurchases.item_code).label('item_name'),
                func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
            )
            .join(DimItem, DimItem.external_id == FactPurchases.item_code, isouter=True)
            .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
            .where(FactPurchases.item_code.in_(fast_item_codes))
        )
        purchases_stmt = _apply_fact_purchases_filters(
            purchases_stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code)
        purchase_rows = (await db.execute(purchases_stmt)).all()
        purchases_map = {str(r[0]): float(r[2] or 0) for r in purchase_rows if r[0]}
        for r in purchase_rows:
            code = str(r[0] or '')
            if code:
                item_name_map[code] = _clean_item_name(r[1], code)

    purchases = [
        {
            'item_code': code,
            'item_name': item_name_map.get(code, code),
            'qty': float(purchases_map.get(code, 0.0)),
        }
        for code in fast_item_codes
    ]
    return {'fast': fast, 'slow': slow, 'purchases': purchases}


async def sales_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    use_fact_source = _has_sales_turnover_series_rules() or await _should_use_fact_sales_source(
        db, date_from=date_from, date_to=date_to
    )
    if use_fact_source:
        month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
        stmt = (
            select(
                month_start_expr.label('month_start'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            )
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        stmt = _apply_fact_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        stmt = _apply_fact_sales_turnover_rules(stmt)
    else:
        month_start_expr = cast(func.date_trunc(literal_column("'month'"), AggSalesDaily.doc_date), Date)
        stmt = (
            select(
                month_start_expr.label('month_start'),
                func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(AggSalesDaily.qty), 0).label('qty'),
            )
            .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
        )
        stmt = _apply_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'gross_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_monthly_company_totals(
    db: AsyncSession,
    date_from: date,
    date_to: date,
):
    use_fact_source = _has_sales_turnover_series_rules() or await _should_use_fact_sales_source(
        db, date_from=date_from, date_to=date_to
    )
    if use_fact_source:
        month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
        stmt = (
            select(
                month_start_expr.label('month_start'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            )
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        stmt = _apply_fact_sales_turnover_rules(stmt)
        stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
        rows = (await db.execute(stmt)).all()
        return [
            {
                'month_start': str(r[0]),
                'net_value': float(r[1] or 0),
                'gross_value': float(r[2] or 0),
                'qty': float(r[3] or 0),
            }
            for r in rows
        ]

    month_start_expr = cast(func.date_trunc(literal_column("'month'"), AggSalesDailyCompany.doc_date), Date)
    stmt = (
        select(
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(AggSalesDailyCompany.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDailyCompany.gross_value), 0).label('gross_value'),
            func.coalesce(func.sum(AggSalesDailyCompany.qty), 0).label('qty'),
        )
        .where(*_date_range(AggSalesDailyCompany.doc_date, date_from, date_to))
        .group_by(month_start_expr)
        .order_by(month_start_expr)
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'gross_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_decision_pack(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    top_n: int = 10,
):
    current_stmt = (
        select(
            func.count(FactSales.id),
            func.coalesce(func.sum(FactSales.qty), 0),
            func.coalesce(func.sum(FactSales.net_value), 0),
            func.coalesce(func.sum(FactSales.cost_amount), 0),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    current_stmt = _apply_fact_sales_filters(
        current_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    current_stmt = _apply_fact_sales_turnover_rules(current_stmt)
    current_row = (await db.execute(current_stmt)).one()
    current = {
        'records': int(current_row[0] or 0),
        'qty': float(current_row[1] or 0),
        'net_value': float(current_row[2] or 0),
        'cost_amount': float(current_row[3] or 0),
    }
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    previous_stmt = (
        select(
            func.count(FactSales.id),
            func.coalesce(func.sum(FactSales.qty), 0),
            func.coalesce(func.sum(FactSales.net_value), 0),
            func.coalesce(func.sum(FactSales.cost_amount), 0),
        )
        .where(*_date_range(FactSales.doc_date, prev_from, prev_to))
    )
    previous_stmt = _apply_fact_sales_filters(
        previous_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    previous_stmt = _apply_fact_sales_turnover_rules(previous_stmt)
    previous_row = (await db.execute(previous_stmt)).one()
    previous = {
        'records': int(previous_row[0] or 0),
        'qty': float(previous_row[1] or 0),
        'net_value': float(previous_row[2] or 0),
        'cost_amount': float(previous_row[3] or 0),
    }
    turnover = float(current['net_value'])
    cost = float(current['cost_amount'])
    qty = float(current['qty'])
    gross_profit = turnover - cost
    margin_pct = (gross_profit / turnover * 100.0) if turnover > 0 else 0.0
    avg_basket = (turnover / current['records']) if current['records'] > 0 else 0.0
    avg_sale_per_day = turnover / days
    prev_turnover = float(previous['net_value'])
    growth_pct = ((turnover - prev_turnover) / prev_turnover * 100.0) if prev_turnover > 0 else None
    prev_cost = float(previous['cost_amount'])
    prev_margin_pct = ((prev_turnover - prev_cost) / prev_turnover * 100.0) if prev_turnover > 0 else None

    try:
        branch_stmt = (
            select(
                FactSales.branch_ext_id,
                func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
            )
            .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        branch_stmt = _apply_fact_sales_filters(
            branch_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        branch_stmt = _apply_fact_sales_turnover_rules(branch_stmt)
        branch_stmt = branch_stmt.group_by(FactSales.branch_ext_id).order_by(func.sum(FactSales.net_value).desc())
        branch_rows = (await db.execute(branch_stmt)).all()
        by_branch = []
        for r in branch_rows:
            net_value = float(r[2] or 0)
            cost_amount = float(r[3] or 0)
            branch_margin_pct = ((net_value - cost_amount) / net_value * 100.0) if net_value > 0 else 0.0
            contribution_pct = (net_value / turnover * 100.0) if turnover > 0 else 0.0
            by_branch.append(
                {
                    'branch': r[1] or r[0] or 'N/A',
                    'branch_code': r[0] or 'N/A',
                    'net_value': net_value,
                    'cost_amount': cost_amount,
                    'margin_pct': branch_margin_pct,
                    'contribution_pct': contribution_pct,
                }
            )
    except Exception:
        by_branch = []
    avg_margin_per_branch = 0.0
    if by_branch:
        avg_margin_per_branch = sum(float(x['margin_pct']) for x in by_branch) / len(by_branch)
    try:
        by_brand = await sales_by_brand(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        by_brand = []
    try:
        by_category = await sales_by_category(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        by_category = []
    try:
        by_group = await sales_by_group(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        by_group = []
    try:
        top_products = await sales_top_products(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            limit=top_n,
        )
    except Exception:
        top_products = []
    try:
        movers = await sales_slow_fast_movers(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        movers = {'fast': [], 'slow': [], 'purchases': []}
    try:
        trend = await sales_monthly_trend(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        trend = []
    try:
        seasonal = await sales_seasonality(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except Exception:
        seasonal = []
    try:
        new_codes = await new_item_codes_activity(
            db,
            date_from=date_from,
            date_to=date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            limit=10,
        )
    except Exception:
        new_codes = []

    insights: list[str] = []
    margin_alerts: list[str] = []
    if growth_pct is not None:
        if growth_pct <= -10:
            insights.append(f'Turnover decreased {abs(growth_pct):.1f}% vs previous period.')
        elif growth_pct >= 10:
            insights.append(f'Turnover increased {growth_pct:.1f}% vs previous period.')
    if by_branch and turnover > 0:
        top_branch = by_branch[0]
        top_share = (float(top_branch['net_value']) / turnover) * 100.0
        insights.append(f"Το κορυφαίο κατάστημα {top_branch['branch']} συνεισφέρει {top_share:.1f}% του τζίρου.")
    if margin_pct < 15:
        insights.append('Το περιθώριο είναι κάτω από 15%. Έλεγξε εκπτώσεις και κόστος αγοράς.')
        margin_alerts.append('Το τρέχον περιθώριο είναι κάτω από 15%.')
    if prev_margin_pct is not None:
        erosion_pp = margin_pct - prev_margin_pct
        if erosion_pp <= -2.0:
            margin_alerts.append(f'Εντοπίστηκε διάβρωση περιθωρίου: {abs(erosion_pp):.2f} μονάδες έναντι προηγούμενης περιόδου.')
        elif erosion_pp >= 2.0:
            margin_alerts.append(f'Βελτίωση περιθωρίου: {erosion_pp:.2f} μονάδες έναντι προηγούμενης περιόδου.')
    if not margin_alerts:
        margin_alerts.append('Δεν εντοπίστηκε σημαντική διάβρωση περιθωρίου στο επιλεγμένο διάστημα.')
    if not insights:
        insights.append('Η απόδοση πωλήσεων είναι σταθερή στο επιλεγμένο διάστημα.')
    try:
        insight_records = await list_recent_insights(
            db,
            limit=15,
            statuses=['open'],
            insight_types=[
                'SLS_DROP_PERIOD',
                'SLS_SPIKE_PERIOD',
                'PRF_DROP_PERIOD',
                'MRG_DROP_POINTS',
                'BR_UNDERPERFORM',
                'CAT_DROP',
                'SUP_DEPENDENCY',
                'INV_DEAD_STOCK',
                'INV_LOW_COVERAGE',
                'INV_OVERSTOCK_SLOW',
            ],
        )
    except Exception:
        insight_records = []

    def _name_with_fallback(entity: str, name: str | None, code: str | None) -> str:
        raw_name = str(name or '').strip()
        raw_code = str(code or '').strip()
        if entity == 'product':
            return _clean_item_name(raw_name or raw_code, raw_code or raw_name)
        return raw_name or raw_code or 'N/A'

    async def _best_for_entity(
        entity: str,
        code_col,
        dim_name_col=None,
        join_model=None,
        join_on=None,
    ):
        base = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name')
                if dim_name_col is not None
                else code_col.label('name'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
                func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        if join_model is not None and join_on is not None:
            base = base.join(join_model, join_on, isouter=True)
        base = _apply_fact_sales_filters(
            base, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        base = base.group_by(code_col)
        rows = (await db.execute(base)).all()
        if not rows:
            return None

        totals = []
        for r in rows:
            code = str(r[0] or '').strip()
            name = _name_with_fallback(entity, str(r[1] or '').strip(), code)
            net_value = float(r[2] or 0)
            cost_amount = float(r[3] or 0)
            qty_value = float(r[4] or 0)
            profit_value = net_value - cost_amount
            margin_pct_val = (profit_value / net_value * 100.0) if net_value > 0 else 0.0
            totals.append(
                {
                    'code': code or 'N/A',
                    'name': name,
                    'net_value': net_value,
                    'cost_amount': cost_amount,
                    'qty': qty_value,
                    'profit_value': profit_value,
                    'margin_pct': margin_pct_val,
                }
            )

        best_profit = max(totals, key=lambda x: x['profit_value'])
        best_qty = max(totals, key=lambda x: x['qty'])

        season_expr = _season_case(FactSales.doc_date).label('season')
        season_stmt = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name')
                if dim_name_col is not None
                else code_col.label('name'),
                season_expr,
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        if join_model is not None and join_on is not None:
            season_stmt = season_stmt.join(join_model, join_on, isouter=True)
        season_stmt = _apply_fact_sales_filters(
            season_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        season_stmt = season_stmt.group_by(code_col, season_expr)
        season_rows = (await db.execute(season_stmt)).all()

        season_map: dict[str, dict[str, float | str]] = {}
        for r in season_rows:
            code = str(r[0] or '').strip()
            if not code:
                continue
            name = _name_with_fallback(entity, str(r[1] or '').strip(), code)
            season = str(r[2] or 'unknown')
            net_value = float(r[3] or 0)
            cur = season_map.get(code)
            if cur is None:
                cur = {'name': name, 'total': 0.0, 'best_season': season, 'best_season_net': net_value}
                season_map[code] = cur
            cur['total'] = float(cur.get('total', 0.0)) + net_value
            if net_value >= float(cur.get('best_season_net', 0.0)):
                cur['best_season'] = season
                cur['best_season_net'] = net_value

        best_seasonality = None
        for code, cur in season_map.items():
            total_val = float(cur.get('total', 0.0))
            top_season_val = float(cur.get('best_season_net', 0.0))
            share_pct = (top_season_val / total_val * 100.0) if total_val > 0 else 0.0
            candidate = {
                'code': code,
                'name': str(cur.get('name') or code),
                'season': str(cur.get('best_season') or 'unknown'),
                'season_net_value': top_season_val,
                'season_share_pct': share_pct,
            }
            if best_seasonality is None or share_pct > float(best_seasonality.get('season_share_pct', 0.0)):
                best_seasonality = candidate

        return {
            'profitability': best_profit,
            'quantity': best_qty,
            'seasonality': best_seasonality,
        }

    try:
        best_entities = {
            'product': await _best_for_entity('product', FactSales.item_code, DimItem.name, DimItem, DimItem.external_id == FactSales.item_code),
            'brand': await _best_for_entity('brand', FactSales.brand_ext_id, DimBrand.name, DimBrand, DimBrand.external_id == FactSales.brand_ext_id),
            'category': await _best_for_entity(
                'category', FactSales.category_ext_id, DimCategory.name, DimCategory, DimCategory.external_id == FactSales.category_ext_id
            ),
            'group': await _best_for_entity('group', FactSales.group_ext_id, DimGroup.name, DimGroup, DimGroup.external_id == FactSales.group_ext_id),
            'branch': await _best_for_entity('branch', FactSales.branch_ext_id, DimBranch.name, DimBranch, DimBranch.external_id == FactSales.branch_ext_id),
        }
    except Exception:
        best_entities = {'product': None, 'brand': None, 'category': None, 'group': None, 'branch': None}

    return {
        'period': {
            'from': str(date_from),
            'to': str(date_to),
            'prev_from': str(prev_from),
            'prev_to': str(prev_to),
        },
        'cards': {
            'turnover': turnover,
            'gross_profit': gross_profit,
            'margin_pct': margin_pct,
            'qty_sold': qty,
            'avg_basket_value': avg_basket,
            'growth_pct': growth_pct,
            'avg_sale_per_day': avg_sale_per_day,
            'avg_margin_per_branch': avg_margin_per_branch,
        },
        'current_summary': current,
        'previous_summary': previous,
        'trend_monthly': trend,
        'by_branch': by_branch,
        'by_brand': by_brand,
        'by_category': by_category,
        'by_group': by_group,
        'top_products': top_products,
        'movers': movers,
        'seasonality': seasonal,
        'new_codes': new_codes,
        'best_entities': best_entities,
        'insights': insights,
        'margin_alerts': margin_alerts,
        'insight_records': insight_records,
    }


async def sales_entity_ranking(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    entity: str,
    metric: str = 'profitability',
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 50,
):
    entity_key = (entity or '').strip().lower()
    metric_key = (metric or '').strip().lower()
    if entity_key not in {'product', 'brand', 'category', 'group', 'branch'}:
        raise ValueError('unsupported_entity')
    if metric_key not in {'profitability', 'quantity', 'seasonality'}:
        raise ValueError('unsupported_metric')

    entity_map = {
        'product': (FactSales.item_code, DimItem, DimItem.external_id == FactSales.item_code, DimItem.name, 'item'),
        'brand': (FactSales.brand_ext_id, DimBrand, DimBrand.external_id == FactSales.brand_ext_id, DimBrand.name, 'brand'),
        'category': (
            FactSales.category_ext_id,
            DimCategory,
            DimCategory.external_id == FactSales.category_ext_id,
            DimCategory.name,
            'category',
        ),
        'group': (FactSales.group_ext_id, DimGroup, DimGroup.external_id == FactSales.group_ext_id, DimGroup.name, 'group'),
        'branch': (FactSales.branch_ext_id, DimBranch, DimBranch.external_id == FactSales.branch_ext_id, DimBranch.name, 'branch'),
    }
    code_col, dim_model, join_on, dim_name_col, label_key = entity_map[entity_key]

    base_stmt = (
        select(
            code_col.label('code'),
            func.coalesce(func.max(dim_name_col), code_col).label('name'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .select_from(FactSales)
        .join(dim_model, join_on, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(code_col.is_not(None))
        .where(code_col != '')
    )
    base_stmt = _apply_fact_sales_filters(
        base_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    base_stmt = base_stmt.group_by(code_col)
    base_rows = (await db.execute(base_stmt)).all()

    totals: list[dict[str, float | str]] = []
    for r in base_rows:
        code = str(r[0] or '').strip()
        if not code:
            continue
        raw_name = str(r[1] or code).strip()
        if entity_key == 'product':
            raw_name = _clean_item_name(raw_name, code)
        net_value = float(r[2] or 0)
        cost_amount = float(r[3] or 0)
        qty = float(r[4] or 0)
        profit_value = net_value - cost_amount
        margin_pct = (profit_value / net_value * 100.0) if net_value > 0 else 0.0
        totals.append(
            {
                'code': code,
                'name': raw_name or code,
                'net_value': net_value,
                'cost_amount': cost_amount,
                'qty': qty,
                'profit_value': profit_value,
                'margin_pct': margin_pct,
            }
        )

    if not totals:
        return {
            'entity': entity_key,
            'metric': metric_key,
            'label': label_key,
            'period': {'from': str(date_from), 'to': str(date_to)},
            'rows': [],
        }

    rows: list[dict[str, float | str]] = []
    if metric_key == 'profitability':
        sorted_rows = sorted(totals, key=lambda x: float(x['profit_value']), reverse=True)
        for row in sorted_rows[: max(1, min(limit, 200))]:
            rows.append(
                {
                    'code': row['code'],
                    'name': row['name'],
                    'metric_value': float(row['profit_value']),
                    'baseline_value': float(row['margin_pct']),
                    'delta_pct': float(row['net_value']),
                    'net_value': float(row['net_value']),
                    'qty': float(row['qty']),
                }
            )
    elif metric_key == 'quantity':
        total_qty = sum(float(x['qty']) for x in totals)
        sorted_rows = sorted(totals, key=lambda x: float(x['qty']), reverse=True)
        for row in sorted_rows[: max(1, min(limit, 200))]:
            qty_val = float(row['qty'])
            share_pct = (qty_val / total_qty * 100.0) if total_qty > 0 else 0.0
            rows.append(
                {
                    'code': row['code'],
                    'name': row['name'],
                    'metric_value': qty_val,
                    'baseline_value': share_pct,
                    'delta_pct': float(row['net_value']),
                    'net_value': float(row['net_value']),
                    'qty': qty_val,
                }
            )
    else:
        season_expr = _season_case(FactSales.doc_date).label('season')
        season_stmt = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name'),
                season_expr,
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            )
            .select_from(FactSales)
            .join(dim_model, join_on, isouter=True)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        season_stmt = _apply_fact_sales_filters(
            season_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        season_stmt = season_stmt.group_by(code_col, season_expr)
        season_rows = (await db.execute(season_stmt)).all()

        season_map: dict[str, dict[str, float | str]] = {}
        for r in season_rows:
            code = str(r[0] or '').strip()
            if not code:
                continue
            raw_name = str(r[1] or code).strip()
            if entity_key == 'product':
                raw_name = _clean_item_name(raw_name, code)
            season = str(r[2] or 'unknown')
            net_value = float(r[3] or 0)
            cur = season_map.get(code)
            if cur is None:
                cur = {'name': raw_name or code, 'total': 0.0, 'best_season': season, 'best_season_value': net_value}
                season_map[code] = cur
            cur['total'] = float(cur.get('total', 0.0)) + net_value
            if net_value >= float(cur.get('best_season_value', 0.0)):
                cur['best_season'] = season
                cur['best_season_value'] = net_value

        season_ranked = []
        for code, item in season_map.items():
            total_val = float(item.get('total', 0.0))
            best_val = float(item.get('best_season_value', 0.0))
            share_pct = (best_val / total_val * 100.0) if total_val > 0 else 0.0
            season_ranked.append(
                {
                    'code': code,
                    'name': str(item.get('name') or code),
                    'season': str(item.get('best_season') or 'unknown'),
                    'metric_value': best_val,
                    'baseline_value': share_pct,
                    'delta_pct': total_val,
                    'net_value': total_val,
                }
            )
        season_ranked.sort(key=lambda x: (float(x['baseline_value']), float(x['metric_value'])), reverse=True)
        rows = season_ranked[: max(1, min(limit, 200))]

    return {
        'entity': entity_key,
        'metric': metric_key,
        'label': label_key,
        'period': {'from': str(date_from), 'to': str(date_to)},
        'rows': rows,
    }


async def inventory_snapshot(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    snapshot_date_stmt = select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of)
    snapshot_date = (await db.execute(snapshot_date_stmt)).scalar_one_or_none()
    if snapshot_date is None:
        return {
            'snapshot_date': None,
            'qty_on_hand': 0.0,
            'qty_reserved': 0.0,
            'cost_amount': 0.0,
            'value_amount': 0.0,
        }

    stmt = (
        select(
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0),
            func.coalesce(func.sum(FactInventory.qty_reserved), 0),
            func.coalesce(func.sum(FactInventory.cost_amount), 0),
            func.coalesce(func.sum(FactInventory.value_amount), 0),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == snapshot_date)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    row = (await db.execute(stmt)).one()
    return {
        'snapshot_date': str(snapshot_date),
        'qty_on_hand': float(row[0] or 0),
        'qty_reserved': float(row[1] or 0),
        'cost_amount': float(row[2] or 0),
        'value_amount': float(row[3] or 0),
    }


async def stock_aging(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    d_30 = as_of - timedelta(days=30)
    d_60 = as_of - timedelta(days=60)
    d_90 = as_of - timedelta(days=90)
    bucket = case(
        (FactInventory.doc_date >= d_30, '0_30'),
        (FactInventory.doc_date >= d_60, '31_60'),
        (FactInventory.doc_date >= d_90, '61_90'),
        else_='90_plus',
    )

    stmt = (
        select(
            bucket.label('bucket'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.group_by(bucket).order_by(bucket)

    rows = (await db.execute(stmt)).all()
    out = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    for row in rows:
        out[str(row[0])] = {
            'qty_on_hand': float(row[1] or 0),
            'value_amount': float(row[2] or 0),
        }
    return out


async def _latest_stock_aging_snapshot_date(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
) -> date | None:
    stmt = select(func.max(AggStockAging.snapshot_date)).where(AggStockAging.snapshot_date <= as_of)
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    snapshot_date = (await db.execute(stmt)).scalar_one_or_none()
    if isinstance(snapshot_date, date):
        return snapshot_date
    return None


def _apply_stock_aging_dimension_filters(stmt, *, brands=None, categories=None, groups=None):
    stmt = stmt.where(_inventory_item_scope_predicate())
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if categories:
        stmt = stmt.where(DimCategory.external_id.in_(categories))
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    return stmt


async def inventory_snapshot_from_aggregates(
    db: AsyncSession,
    *,
    as_of: date,
    snapshot_date: date | None = None,
    branches: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    if snapshot_date is None:
        snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    if snapshot_date is None:
        return {
            'snapshot_date': None,
            'qty_on_hand': 0.0,
            'qty_reserved': 0.0,
            'cost_amount': 0.0,
            'value_amount': 0.0,
        }

    stmt = (
        select(
            func.coalesce(func.sum(AggStockAging.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(AggStockAging.stock_value), 0).label('stock_value'),
        )
        .select_from(AggStockAging)
        .where(AggStockAging.snapshot_date == snapshot_date)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    if brands or categories or groups:
        stmt = (
            stmt.join(DimItem, DimItem.external_id == AggStockAging.item_external_id, isouter=True)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        )
        stmt = _apply_stock_aging_dimension_filters(
            stmt,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    row = (await db.execute(stmt)).mappings().one()
    value_amount = float(row.get('stock_value') or 0)
    return {
        'snapshot_date': snapshot_date.isoformat(),
        'qty_on_hand': float(row.get('qty_on_hand') or 0),
        'qty_reserved': 0.0,
        'cost_amount': value_amount,
        'value_amount': value_amount,
    }


async def stock_aging_from_aggregates(
    db: AsyncSession,
    *,
    as_of: date,
    snapshot_date: date | None = None,
    branches: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    if snapshot_date is None:
        snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    out = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    if snapshot_date is None:
        return out

    stmt = (
        select(
            AggStockAging.aging_bucket.label('bucket'),
            func.coalesce(func.sum(AggStockAging.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(AggStockAging.stock_value), 0).label('value_amount'),
        )
        .select_from(AggStockAging)
        .where(AggStockAging.snapshot_date == snapshot_date)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    if brands or categories or groups:
        stmt = (
            stmt.join(DimItem, DimItem.external_id == AggStockAging.item_external_id, isouter=True)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        )
        stmt = _apply_stock_aging_dimension_filters(
            stmt,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    rows = (await db.execute(stmt.group_by(AggStockAging.aging_bucket))).mappings().all()
    for row in rows:
        bucket = str(row.get('bucket') or '').strip().lower()
        if bucket not in out:
            continue
        out[bucket] = {
            'qty_on_hand': float(row.get('qty_on_hand') or 0),
            'value_amount': float(row.get('value_amount') or 0),
        }
    return out


async def inventory_by_brand_from_aggregates(
    db: AsyncSession,
    *,
    as_of: date,
    snapshot_date: date | None = None,
    branches: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
) -> list[dict]:
    if snapshot_date is None:
        snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    if snapshot_date is None:
        return []
    stmt = (
        select(
            func.coalesce(DimBrand.name, DimBrand.external_id, literal('N/A')).label('brand'),
            func.coalesce(func.sum(AggStockAging.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(AggStockAging.stock_value), 0).label('value_amount'),
        )
        .select_from(AggStockAging)
        .join(DimItem, DimItem.external_id == AggStockAging.item_external_id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(AggStockAging.snapshot_date == snapshot_date)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    stmt = _apply_stock_aging_dimension_filters(
        stmt,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    rows = (
        await db.execute(
            stmt.group_by(DimBrand.name, DimBrand.external_id)
            .order_by(func.sum(AggStockAging.stock_value).desc())
            .limit(max(1, min(int(limit), 100)))
        )
    ).all()
    return [
        {
            'brand': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_by_group_from_aggregates(
    db: AsyncSession,
    *,
    as_of: date,
    snapshot_date: date | None = None,
    branches: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
) -> list[dict]:
    if snapshot_date is None:
        snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    if snapshot_date is None:
        return []
    stmt = (
        select(
            func.coalesce(DimGroup.name, DimGroup.external_id, literal('N/A')).label('commercial_category'),
            func.coalesce(func.sum(AggStockAging.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(AggStockAging.stock_value), 0).label('value_amount'),
        )
        .select_from(AggStockAging)
        .join(DimItem, DimItem.external_id == AggStockAging.item_external_id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(AggStockAging.snapshot_date == snapshot_date)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    stmt = _apply_stock_aging_dimension_filters(
        stmt,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    rows = (
        await db.execute(
            stmt.group_by(DimGroup.name, DimGroup.external_id)
            .order_by(func.sum(AggStockAging.stock_value).desc())
            .limit(max(1, min(int(limit), 100)))
        )
    ).all()
    return [
        {
            'commercial_category': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_summary_bundle_from_aggregates(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
) -> dict:
    snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    empty_aging = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    if snapshot_date is None:
        return {
            'snapshot': {
                'snapshot_date': None,
                'qty_on_hand': 0.0,
                'qty_reserved': 0.0,
                'cost_amount': 0.0,
                'value_amount': 0.0,
            },
            'aging': empty_aging,
            'by_brand': [],
            'by_commercial_category': [],
            'by_manufacturer': [],
        }

    stmt = (
        select(
            AggStockAging.aging_bucket.label('aging_bucket'),
            func.coalesce(AggStockAging.qty_on_hand, 0).label('qty_on_hand'),
            func.coalesce(AggStockAging.stock_value, 0).label('stock_value'),
            func.coalesce(DimBrand.name, DimBrand.external_id, literal('N/A')).label('brand_label'),
            func.coalesce(DimGroup.name, DimGroup.external_id, literal('N/A')).label('group_label'),
        )
        .select_from(AggStockAging)
        .join(DimItem, DimItem.external_id == AggStockAging.item_external_id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(AggStockAging.snapshot_date == snapshot_date)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    stmt = _apply_stock_aging_dimension_filters(
        stmt,
        brands=brands,
        categories=categories,
        groups=groups,
    )

    rows = (await db.execute(stmt)).mappings().all()
    total_qty = 0.0
    total_value = 0.0
    aging = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    by_brand: dict[str, dict[str, float]] = {}
    by_group: dict[str, dict[str, float]] = {}

    for row in rows:
        qty = float(row.get('qty_on_hand') or 0)
        val = float(row.get('stock_value') or 0)
        total_qty += qty
        total_value += val

        bucket = str(row.get('aging_bucket') or '').strip().lower()
        if bucket in aging:
            aging[bucket]['qty_on_hand'] += qty
            aging[bucket]['value_amount'] += val

        brand_label = str(row.get('brand_label') or 'N/A')
        brand_bucket = by_brand.setdefault(brand_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        brand_bucket['qty_on_hand'] += qty
        brand_bucket['value_amount'] += val

        group_label = str(row.get('group_label') or 'N/A')
        group_bucket = by_group.setdefault(group_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        group_bucket['qty_on_hand'] += qty
        group_bucket['value_amount'] += val

    ranked_brands = sorted(by_brand.items(), key=lambda item: item[1]['value_amount'], reverse=True)[
        : max(1, min(int(limit), 100))
    ]
    ranked_groups = sorted(by_group.items(), key=lambda item: item[1]['value_amount'], reverse=True)[
        : max(1, min(int(limit), 100))
    ]

    return {
        'snapshot': {
            'snapshot_date': snapshot_date.isoformat(),
            'qty_on_hand': float(total_qty),
            'qty_reserved': 0.0,
            'cost_amount': float(total_value),
            'value_amount': float(total_value),
        },
        'aging': aging,
        'by_brand': [
            {'brand': label, 'qty_on_hand': float(v['qty_on_hand']), 'value_amount': float(v['value_amount'])}
            for label, v in ranked_brands
        ],
        'by_commercial_category': [
            {'commercial_category': label, 'qty_on_hand': float(v['qty_on_hand']), 'value_amount': float(v['value_amount'])}
            for label, v in ranked_groups
        ],
        'by_manufacturer': [],
    }


async def inventory_summary_bundle_from_current_state(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
) -> dict:
    latest_date = (await db.execute(select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of))).scalar_one_or_none()
    empty_aging = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    if latest_date is None:
        return {
            'snapshot': {
                'snapshot_date': None,
                'qty_on_hand': 0.0,
                'qty_reserved': 0.0,
                'cost_amount': 0.0,
                'value_amount': 0.0,
            },
            'aging': empty_aging,
            'by_brand': [],
            'by_commercial_category': [],
            'by_manufacturer': [],
        }

    latest_inventory_rows = (
        select(
            FactInventory.branch_id.label('branch_id'),
            FactInventory.branch_ext_id.label('branch_ext_id'),
            FactInventory.warehouse_id.label('warehouse_id'),
            FactInventory.item_id.label('item_id'),
            FactInventory.item_code.label('item_code'),
            FactInventory.qty_on_hand.label('qty_on_hand'),
            FactInventory.qty_reserved.label('qty_reserved'),
            FactInventory.cost_amount.label('cost_amount'),
            FactInventory.value_amount.label('value_amount'),
            FactInventory.source_payload_json['manufacturer_code'].astext.label('payload_manufacturer_code'),
            FactInventory.source_payload_json['manufacturer_name'].astext.label('payload_manufacturer_name'),
            func.row_number()
            .over(
                partition_by=(
                    FactInventory.branch_id,
                    FactInventory.warehouse_id,
                    func.coalesce(cast(FactInventory.item_id, String), FactInventory.item_code, literal('')),
                ),
                order_by=(FactInventory.doc_date.desc(), FactInventory.updated_at.desc(), FactInventory.id.desc()),
            )
            .label('rn'),
        )
        .where(FactInventory.doc_date <= as_of)
        .subquery('inventory_current_state_rows')
    )

    item_code_expr = func.coalesce(DimItem.external_id, latest_inventory_rows.c.item_code)
    inv_base = (
        select(
            item_code_expr.label('item_code'),
            func.coalesce(func.max(DimBrand.name), func.max(DimBrand.external_id), literal('N/A')).label('brand_label'),
            func.coalesce(func.max(DimGroup.name), func.max(DimGroup.external_id), literal('N/A')).label('group_label'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.manufacturer_name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_manufacturer_name), '')),
                func.max(func.nullif(func.btrim(DimItem.manufacturer_code), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_manufacturer_code), '')),
                literal('N/A'),
            ).label('manufacturer_label'),
            func.coalesce(func.sum(latest_inventory_rows.c.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(latest_inventory_rows.c.qty_reserved), 0).label('qty_reserved'),
            func.coalesce(func.sum(latest_inventory_rows.c.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(latest_inventory_rows.c.value_amount), 0).label('stock_value'),
        )
        .select_from(latest_inventory_rows)
        .join(DimBranch, latest_inventory_rows.c.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, latest_inventory_rows.c.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, latest_inventory_rows.c.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(latest_inventory_rows.c.rn == 1)
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_inventory_rows.c.branch_ext_id,
    )
    inv_base = inv_base.group_by(item_code_expr).subquery('inventory_current_state_base')

    sales_last = (
        select(
            FactSales.item_code.label('item_code'),
            func.max(FactSales.doc_date).label('last_sale_date'),
        )
        .where(FactSales.doc_date <= as_of)
    )
    sales_last = _apply_fact_sales_filters(
        sales_last,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    sales_last = sales_last.group_by(FactSales.item_code).subquery('inventory_sales_last')

    rows = (
        await db.execute(
            select(
                inv_base.c.brand_label,
                inv_base.c.group_label,
                inv_base.c.qty_on_hand,
                inv_base.c.qty_reserved,
                inv_base.c.cost_amount,
                inv_base.c.stock_value,
                sales_last.c.last_sale_date,
                inv_base.c.manufacturer_label,
            )
            .select_from(inv_base)
            .join(sales_last, sales_last.c.item_code == inv_base.c.item_code, isouter=True)
        )
    ).mappings().all()

    total_qty = 0.0
    total_reserved = 0.0
    total_cost = 0.0
    total_value = 0.0
    aging = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    by_brand: dict[str, dict[str, float]] = {}
    by_group: dict[str, dict[str, float]] = {}
    by_manufacturer: dict[str, dict[str, float]] = {}

    for row in rows:
        qty = float(row.get('qty_on_hand') or 0)
        reserved = float(row.get('qty_reserved') or 0)
        cost = float(row.get('cost_amount') or 0)
        value = float(row.get('stock_value') or 0)
        total_qty += qty
        total_reserved += reserved
        total_cost += cost
        total_value += value

        last_sale_date = row.get('last_sale_date')
        days_since_last_sale = (as_of - last_sale_date).days if isinstance(last_sale_date, date) else None
        if days_since_last_sale is None:
            bucket = '90_plus'
        elif days_since_last_sale <= 30:
            bucket = '0_30'
        elif days_since_last_sale <= 60:
            bucket = '31_60'
        elif days_since_last_sale <= 90:
            bucket = '61_90'
        else:
            bucket = '90_plus'
        aging[bucket]['qty_on_hand'] += qty
        aging[bucket]['value_amount'] += value

        brand_label = str(row.get('brand_label') or 'N/A')
        group_label = str(row.get('group_label') or 'N/A')
        manufacturer_label = str(row.get('manufacturer_label') or 'N/A')

        brand_bucket = by_brand.setdefault(brand_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        brand_bucket['qty_on_hand'] += qty
        brand_bucket['value_amount'] += value

        group_bucket = by_group.setdefault(group_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        group_bucket['qty_on_hand'] += qty
        group_bucket['value_amount'] += value

        manufacturer_bucket = by_manufacturer.setdefault(manufacturer_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        manufacturer_bucket['qty_on_hand'] += qty
        manufacturer_bucket['value_amount'] += value

    ranked_brands = sorted(by_brand.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]
    ranked_groups = sorted(by_group.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]
    ranked_manufacturers = sorted(by_manufacturer.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]

    return {
        'snapshot': {
            'snapshot_date': str(latest_date),
            'qty_on_hand': float(total_qty),
            'qty_reserved': float(total_reserved),
            'cost_amount': float(total_cost),
            'value_amount': float(total_value),
        },
        'aging': aging,
        'by_brand': [
            {'brand': label, 'qty_on_hand': float(v['qty_on_hand']), 'value_amount': float(v['value_amount'])}
            for label, v in ranked_brands
        ],
        'by_commercial_category': [
            {'commercial_category': label, 'qty_on_hand': float(v['qty_on_hand']), 'value_amount': float(v['value_amount'])}
            for label, v in ranked_groups
        ],
        'by_manufacturer': [
            {'manufacturer': label, 'qty_on_hand': float(v['qty_on_hand']), 'value_amount': float(v['value_amount'])}
            for label, v in ranked_manufacturers
        ],
    }


def _normalize_cashflow_category(category: str | None) -> str:
    value = str(category or '').strip().lower()
    if not value:
        return ''
    value = value.replace('-', '_').replace(' ', '_')
    aliases: dict[str, str] = {
        'customer_collection': 'customer_collections',
        'cash_tx_customer_collections': 'customer_collections',
        'customer_collections_docs': 'customer_collections',
        'customer_collections_documents': 'customer_collections',
        'customer_transfer': 'customer_transfers',
        'cash_tx_customer_transfers': 'customer_transfers',
        'supplier_payment': 'supplier_payments',
        'cash_tx_supplier_payments': 'supplier_payments',
        'supplier_transfer': 'supplier_transfers',
        'cash_tx_supplier_transfers': 'supplier_transfers',
        'financial_account': 'financial_accounts',
        'cash_tx_financial_accounts': 'financial_accounts',
    }
    return aliases.get(value, value)


def _cashflow_entry_types_for_category(category: str | None) -> set[str] | None:
    normalized = _normalize_cashflow_category(category)
    if not normalized:
        return None
    mapping: dict[str, set[str]] = {
        'customer_collections': {
            'customer_collections',
            'customer_collection',
            'collections',
            'collection',
            'in',
            'inflow',
            'credit',
            'income',
        },
        'customer_transfers': {
            'customer_transfers',
            'customer_transfer',
            'customer_bank_transfer',
            'customer_wire_transfer',
            'customer_wire',
        },
        'supplier_payments': {
            'supplier_payments',
            'supplier_payment',
            'payments',
            'payment',
            'out',
            'outflow',
            'debit',
            'expense',
        },
        'supplier_transfers': {
            'supplier_transfers',
            'supplier_transfer',
            'supplier_bank_transfer',
            'supplier_wire_transfer',
            'supplier_wire',
        },
        'financial_accounts': {
            'financial_accounts',
            'financial_account',
            'account_transfer',
            'internal_transfer',
            'transfer',
        },
    }
    return mapping.get(normalized)


def _cashflow_subcategories_for_filter(category: str | None) -> set[str]:
    normalized = _normalize_cashflow_category(category)
    if not normalized:
        return set()
    if normalized == 'supplier_payments':
        # Keep parity with legacy BI where supplier transfers appeared in the same view.
        return {'supplier_payments', 'supplier_transfers'}
    return {normalized}


def _cashflow_subcategory_expr():
    subcategory_col = func.lower(cast(func.coalesce(FactCashflow.subcategory, literal('')), String))
    entry_col = func.lower(cast(func.coalesce(FactCashflow.entry_type, literal('')), String))
    return case(
        (subcategory_col != literal(''), subcategory_col),
        (entry_col.in_(sorted(_cashflow_entry_types_for_category('customer_collections') or set())), literal('customer_collections')),
        (entry_col.in_(sorted(_cashflow_entry_types_for_category('customer_transfers') or set())), literal('customer_transfers')),
        (entry_col.in_(sorted(_cashflow_entry_types_for_category('supplier_payments') or set())), literal('supplier_payments')),
        (entry_col.in_(sorted(_cashflow_entry_types_for_category('supplier_transfers') or set())), literal('supplier_transfers')),
        (entry_col.in_(sorted(_cashflow_entry_types_for_category('financial_accounts') or set())), literal('financial_accounts')),
        else_=literal('unknown'),
    )


def _cashflow_entry_label(entry_type: str | None) -> str:
    normalized = str(entry_type or '').strip().lower()
    labels = {
        'customer_collections': 'Είσπραξη πελάτη',
        'customer_collection': 'Είσπραξη πελάτη',
        'collections': 'Είσπραξη πελάτη',
        'collection': 'Είσπραξη πελάτη',
        'customer_transfers': 'Έμβασμα από πελάτη',
        'customer_transfer': 'Έμβασμα από πελάτη',
        'customer_bank_transfer': 'Έμβασμα από πελάτη',
        'customer_wire_transfer': 'Έμβασμα από πελάτη',
        'customer_wire': 'Έμβασμα από πελάτη',
        'supplier_payments': 'Πληρωμή προμηθευτή',
        'supplier_payment': 'Πληρωμή προμηθευτή',
        'payments': 'Πληρωμή προμηθευτή',
        'payment': 'Πληρωμή προμηθευτή',
        'supplier_transfers': 'Έμβασμα σε προμηθευτή',
        'supplier_transfer': 'Έμβασμα σε προμηθευτή',
        'supplier_bank_transfer': 'Έμβασμα σε προμηθευτή',
        'supplier_wire_transfer': 'Έμβασμα σε προμηθευτή',
        'supplier_wire': 'Έμβασμα σε προμηθευτή',
        'financial_accounts': 'Μεταφορά λογαριασμών',
        'financial_account': 'Μεταφορά λογαριασμών',
        'account_transfer': 'Μεταφορά λογαριασμών',
        'internal_transfer': 'Μεταφορά λογαριασμών',
        'transfer': 'Μεταφορά λογαριασμών',
        'in': 'Είσπραξη πελάτη',
        'inflow': 'Είσπραξη πελάτη',
        'credit': 'Είσπραξη πελάτη',
        'income': 'Είσπραξη πελάτη',
        'out': 'Πληρωμή προμηθευτή',
        'outflow': 'Πληρωμή προμηθευτή',
        'debit': 'Πληρωμή προμηθευτή',
        'expense': 'Πληρωμή προμηθευτή',
        'refund': 'Επιστροφή',
    }
    if normalized in labels:
        return labels[normalized]
    return str(entry_type or '').strip() or 'Κίνηση'


def _cashflow_category_label(category: str | None) -> str:
    normalized = _normalize_cashflow_category(category)
    labels = {
        'customer_collections': 'Εισπράξεις πελατών',
        'customer_transfers': 'Εμβάσματα πελατών',
        'supplier_payments': 'Πληρωμές προμηθευτών',
        'supplier_transfers': 'Εμβάσματα προμηθευτών',
        'financial_accounts': 'Χρημ. λογαριασμοί',
    }
    return labels.get(normalized, 'Ταμειακές Συναλλαγές')


def _cashflow_party_fallback(category: str | None) -> str:
    normalized = _normalize_cashflow_category(category)
    if normalized == 'customer_collections':
        return 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'
    if normalized == 'supplier_payments':
        return 'ΠΡΟΜΗΘΕΥΤΗΣ'
    if normalized in {'customer_transfers', 'supplier_transfers', 'financial_accounts'}:
        return 'ΧΡΗΜ. ΛΟΓΑΡΙΑΣΜΟΣ'
    return '-'


_ACCOUNT_CODE_RE = re.compile(r'(\d{1,3}(?:\.\d{1,3}){2,})')


def _cashflow_amount_sign(entry_type: str | None) -> float:
    normalized = str(entry_type or '').strip().lower()
    positive = {
        'in',
        'inflow',
        'credit',
        'income',
        'customer_collections',
        'customer_collection',
        'collections',
        'collection',
        'customer_transfers',
        'customer_transfer',
        'customer_bank_transfer',
        'customer_wire_transfer',
        'customer_wire',
    }
    negative = {
        'out',
        'outflow',
        'debit',
        'expense',
        'refund',
        'supplier_payments',
        'supplier_payment',
        'payments',
        'payment',
        'supplier_transfers',
        'supplier_transfer',
        'supplier_bank_transfer',
        'supplier_wire_transfer',
        'supplier_wire',
    }
    if normalized in positive:
        return 1.0
    if normalized in negative:
        return -1.0
    return 1.0


def _is_generic_cashflow_note(note: str | None) -> bool:
    txt = str(note or '').strip().lower()
    if not txt:
        return True
    generic_prefixes = (
        'daily inflow',
        'operational expenses',
        'refund',
        'cashflow',
        'ταμειακ',
    )
    generic_exact = {
        'refunds',
        'daily inflow',
        'operational expenses',
    }
    return txt in generic_exact or any(txt.startswith(prefix) for prefix in generic_prefixes)


def _derive_cashflow_account_identity(
    account_id: str | None,
    reference_no: str | None,
    notes: str | None,
    external_id: str | None,
    entry_type: str | None,
) -> tuple[str, str, str]:
    acc = str(account_id or '').strip()
    ref = str(reference_no or '').strip()
    note = str(notes or '').strip()
    ext = str(external_id or '').strip()

    code = acc
    for src in (ref, note):
        if not src:
            continue
        m = _ACCOUNT_CODE_RE.search(src)
        if m:
            code = m.group(1).strip()
            break
    if not code and ref:
        code = ref[:64]
    if not code:
        code = ext[:64] or 'ACC'

    name_candidates: list[str] = []
    if note:
        cleaned_note = note
        if code and code in cleaned_note:
            cleaned_note = cleaned_note.replace(code, '', 1).strip(' -:;|,/\\')
        if cleaned_note and not _is_generic_cashflow_note(cleaned_note):
            name_candidates.append(cleaned_note)
    if ref and ref != code and not _ACCOUNT_CODE_RE.fullmatch(ref):
        name_candidates.append(ref)
    label = _cashflow_entry_label(entry_type)
    if label:
        name_candidates.append(label)
    name = next((candidate for candidate in name_candidates if str(candidate or '').strip()), 'Χρημ.Λογαριασμός')
    account_id_final = acc or code or ext or name
    return account_id_final, code or account_id_final, str(name).strip() or 'Χρημ.Λογαριασμός'


def _build_cashflow_accounts_index(rows) -> dict[str, dict]:
    accounts: dict[str, dict] = {}
    for row in rows:
        fact: FactCashflow = row[0]
        branch_name = str(row[1] or 'N/A')
        account_id, account_code, account_name = _derive_cashflow_account_identity(
            fact.account_id,
            fact.reference_no,
            fact.notes,
            fact.external_id,
            fact.entry_type,
        )
        key = str(account_id or account_code or account_name).strip().lower()
        if not key:
            continue
        amount = float(fact.amount or 0)
        signed_amount = _cashflow_amount_sign(fact.subcategory or fact.entry_type) * abs(amount)
        note = str(fact.notes or '').strip()
        rec = accounts.get(key)
        if rec is None:
            rec = {
                'account_id': str(account_id or account_code or account_name),
                'account_code': str(account_code or account_id or ''),
                'account_name': str(account_name or 'Χρημ.Λογαριασμός'),
                'balance': 0.0,
                'tx_count': 0,
                'updated_at': None,
                'created_at': None,
                'notes_set': set(),
                'lines': [],
            }
            accounts[key] = rec
        rec['balance'] += signed_amount
        rec['tx_count'] += 1
        rec['updated_at'] = max((rec['updated_at'], fact.updated_at), key=lambda v: v or datetime.min)
        rec['created_at'] = min((rec['created_at'], fact.created_at), key=lambda v: v or datetime.max)
        if note and not _is_generic_cashflow_note(note):
            rec['notes_set'].add(note)
        rec['lines'].append(
            {
                'external_id': str(fact.external_id or ''),
                'reference_no': str(fact.reference_no or ''),
                'document_date': fact.doc_date.isoformat() if fact.doc_date else '',
                'entry_label': _cashflow_entry_label(fact.entry_type),
                'entry_type': str(fact.entry_type or ''),
                'amount': amount,
                'signed_amount': signed_amount,
                'branch_name': branch_name,
                'reason': note,
            }
        )
    return accounts


async def cashflow_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    category: str | None = None,
    branches: list[str] | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    normalized_category = _normalize_cashflow_category(category)
    allowed_subcategories = _cashflow_subcategories_for_filter(normalized_category)
    allowed_entry_types = _cashflow_entry_types_for_category(normalized_category)
    entry_type_col = func.lower(cast(func.coalesce(FactCashflow.entry_type, literal('')), String))
    subcategory_col = func.lower(cast(func.coalesce(FactCashflow.subcategory, literal('')), String))
    subcategory_col = func.lower(cast(func.coalesce(FactCashflow.subcategory, literal('')), String))
    doc_key = FactCashflow.external_id

    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactCashflow.reference_no), func.max(FactCashflow.external_id), literal('')).label('document_no'),
            func.max(FactCashflow.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimBranch.external_id), literal('')).label('branch_code'),
            func.coalesce(func.max(FactCashflow.entry_type), literal('unknown')).label('entry_type'),
            func.coalesce(func.max(FactCashflow.notes), literal('')).label('notes'),
            func.coalesce(func.sum(FactCashflow.amount), 0).label('total_value'),
            func.count(FactCashflow.id).label('line_count'),
            func.max(FactCashflow.updated_at).label('last_update'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
    )

    branches = _effective_branch_filter(branches)

    if branches is not None:
        base = base.where(DimBranch.external_id.in_(branches))
    if normalized_category:
        base = base.where(
            (subcategory_col.in_(sorted(allowed_subcategories or {normalized_category})))
            | (entry_type_col.in_(sorted(allowed_entry_types or set())))
        )

    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            func.lower(cast(func.coalesce(FactCashflow.reference_no, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactCashflow.notes, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactCashflow.external_id, literal('')), String)).like(like)
            | entry_type_col.like(like)
        )

    docs_sub = base.group_by(doc_key).subquery('cashflow_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.total_value), 0).label('value_total'),
                func.coalesce(func.sum(docs_sub.c.line_count), 0).label('line_count'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        notes_text = str(r.get('notes') or '').strip()
        party_name = notes_text if notes_text else _cashflow_party_fallback(normalized_category)
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': str(r.get('branch_name') or 'N/A'),
                'branch_code': str(r.get('branch_code') or ''),
                'series': _cashflow_entry_label(str(r.get('entry_type') or '')),
                'entry_type': str(r.get('entry_type') or ''),
                'party_name': party_name,
                'total_value': float(r.get('total_value') or 0),
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'value_total': float(totals_row['value_total'] or 0),
            'line_count': int(totals_row['line_count'] or 0),
            'category': normalized_category,
            'category_label': _cashflow_category_label(normalized_category),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def cashflow_document_detail(
    db: AsyncSession,
    document_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    category: str | None = None,
    branches: list[str] | None = None,
):
    doc_id = str(document_id or '').strip()
    if not doc_id:
        raise ValueError('Missing document id')

    normalized_category = _normalize_cashflow_category(category)
    allowed_subcategories = _cashflow_subcategories_for_filter(normalized_category)
    allowed_entry_types = _cashflow_entry_types_for_category(normalized_category)
    entry_type_col = func.lower(cast(func.coalesce(FactCashflow.entry_type, literal('')), String))
    subcategory_col = func.lower(cast(func.coalesce(FactCashflow.subcategory, literal('')), String))
    stmt = (
        select(
            FactCashflow,
            DimBranch.name.label('branch_name'),
            DimBranch.external_id.label('branch_code'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
        .where(FactCashflow.external_id == doc_id)
    )
    if date_from is not None:
        stmt = stmt.where(FactCashflow.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactCashflow.doc_date <= date_to)
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    if normalized_category:
        stmt = stmt.where(
            (subcategory_col.in_(sorted(allowed_subcategories or {normalized_category})))
            | (entry_type_col.in_(sorted(allowed_entry_types or set())))
        )

    rows = (
        await db.execute(
            stmt.order_by(
                FactCashflow.doc_date.desc(),
                FactCashflow.updated_at.desc(),
                FactCashflow.external_id.asc(),
            )
        )
    ).all()
    if not rows:
        raise ValueError('Cashflow document not found')

    first_fact: FactCashflow = rows[0][0]
    branch_name = str(rows[0][1] or 'N/A')
    branch_code = str(rows[0][2] or '')
    reason = str(first_fact.notes or '').strip()
    party_name = reason or _cashflow_party_fallback(normalized_category)
    category_label = _cashflow_category_label(normalized_category)

    is_transfer_category = normalized_category in {'customer_transfers', 'supplier_transfers', 'financial_accounts'}
    line_rows = []
    total_value = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactCashflow = row[0]
        line_amount = float(fact.amount or 0)
        total_value += line_amount
        line_reason = str(fact.notes or '').strip()
        counterparty_name = line_reason or party_name
        line_rows.append(
            {
                'row_no': idx,
                'entry_label': _cashflow_entry_label(fact.entry_type),
                'amount': line_amount,
                'reference_no': str(fact.reference_no or fact.external_id or ''),
                'due_date': fact.doc_date.isoformat() if fact.doc_date else '',
                'reason': '' if is_transfer_category else line_reason,
                'branch_name': branch_name,
                'counterparty_name': counterparty_name,
            }
        )

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'fact_cashflows.header', first_fact)
    _append_raw_field(raw_fields, 'dim_branches.name', branch_name)
    _append_raw_field(raw_fields, 'dim_branches.external_id', branch_code)

    return {
        'document_id': str(first_fact.external_id or doc_id),
        'document_no': str(first_fact.reference_no or first_fact.external_id or doc_id),
        'document_date': first_fact.doc_date.isoformat() if first_fact.doc_date else '',
        'header': {
            'branch_code': branch_code,
            'branch_name': branch_name,
            'series': _cashflow_entry_label(first_fact.entry_type),
            'document_type': _cashflow_entry_label(first_fact.entry_type),
            'cash_register': branch_name if branch_name and branch_name != 'N/A' else '',
            'account_name': party_name
            if normalized_category in {'customer_transfers', 'supplier_transfers', 'financial_accounts'}
            else '',
            'party_name': party_name,
            'party_branch': '',
            'customer_code': '',
            'customer_name': party_name,
            'customer_branch': '',
            'reason': reason,
        },
        'notes': {
            'notes_1': str(first_fact.notes or ''),
            'notes_2': '',
        },
        'audit': {
            'created_at': _raw_scalar(first_fact.created_at),
            'created_by': '',
            'updated_at': _raw_scalar(first_fact.updated_at),
            'updated_by': '',
        },
        'totals': {
            'expenses_value': 0.0,
            'total_value': total_value,
            'balance_value': 0.0,
            'cash_value': 0.0
            if normalized_category in {'supplier_payments', 'supplier_transfers'}
            else total_value,
            'line_count': len(line_rows),
        },
        'lines': line_rows,
        'category': normalized_category,
        'category_label': category_label,
        'raw_fields': raw_fields,
    }


async def cashflow_accounts_overview(
    db: AsyncSession,
    as_of: date | None = None,
    branches: list[str] | None = None,
    q: str | None = None,
    limit: int = 250,
    offset: int = 0,
    aggregate_only: bool = False,
):
    agg_accounts_has_rows = (await db.execute(select(AggCashAccounts.doc_date).limit(1))).first() is not None
    if agg_accounts_has_rows:
        branches = _effective_branch_filter(branches)
        if branches is not None:
            agg_stmt = (
                select(
                    AggCashDaily.account_id.label('account_id'),
                    func.coalesce(func.sum(AggCashDaily.entries), 0).label('tx_count'),
                    func.coalesce(func.sum(AggCashDaily.net_amount), 0).label('balance'),
                    func.max(AggCashDaily.updated_at).label('updated_at'),
                )
                .select_from(AggCashDaily)
                .where(AggCashDaily.account_id.is_not(None))
                .where(AggCashDaily.branch_ext_id.in_(branches))
            )
            if as_of is not None:
                agg_stmt = agg_stmt.where(AggCashDaily.doc_date <= as_of)
            agg_stmt = agg_stmt.group_by(AggCashDaily.account_id)
        else:
            agg_stmt = (
                select(
                    AggCashAccounts.account_id.label('account_id'),
                    func.coalesce(func.sum(AggCashAccounts.entries), 0).label('tx_count'),
                    func.coalesce(func.sum(AggCashAccounts.net_amount), 0).label('balance'),
                    func.max(AggCashAccounts.updated_at).label('updated_at'),
                )
                .select_from(AggCashAccounts)
                .where(AggCashAccounts.account_id.is_not(None))
            )
            if as_of is not None:
                agg_stmt = agg_stmt.where(AggCashAccounts.doc_date <= as_of)
            agg_stmt = agg_stmt.group_by(AggCashAccounts.account_id)

        rows = (await db.execute(agg_stmt)).mappings().all()
        items = [
            {
                'account_id': str(r.get('account_id') or ''),
                'account_code': str(r.get('account_id') or ''),
                'account_name': str(r.get('account_id') or ''),
                'balance': float(r.get('balance') or 0),
                'tx_count': int(r.get('tx_count') or 0),
                'updated_at': _raw_scalar(r.get('updated_at')),
            }
            for r in rows
            if str(r.get('account_id') or '').strip()
        ]

        q_clean = str(q or '').strip().lower()
        if q_clean:
            items = [
                item
                for item in items
                if q_clean in str(item.get('account_code') or '').lower()
                or q_clean in str(item.get('account_name') or '').lower()
            ]
        items.sort(key=lambda item: (str(item.get('account_code') or ''), str(item.get('account_name') or '')))
        total_accounts = len(items)
        paged = items[max(0, int(offset)) : max(0, int(offset)) + max(1, min(int(limit), 500))]
        return {
            'summary': {
                'accounts': int(total_accounts),
                'balance_total': float(sum(float(item.get('balance') or 0) for item in items)),
            },
            'limit': int(limit),
            'offset': int(offset),
            'rows': paged,
        }

    if aggregate_only:
        return {
            'summary': {'accounts': 0, 'balance_total': 0.0},
            'limit': int(limit),
            'offset': int(offset),
            'rows': [],
        }

    stmt = (
        select(
            FactCashflow,
            DimBranch.name.label('branch_name'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
    )
    if as_of is not None:
        stmt = stmt.where(FactCashflow.doc_date <= as_of)
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(DimBranch.external_id.in_(branches))

    rows = (
        await db.execute(
            stmt.order_by(
                FactCashflow.doc_date.desc(),
                FactCashflow.updated_at.desc(),
                FactCashflow.external_id.asc(),
            )
        )
    ).all()

    index = _build_cashflow_accounts_index(rows)
    items = list(index.values())
    q_clean = str(q or '').strip().lower()
    if q_clean:
        items = [
            item
            for item in items
            if q_clean in str(item.get('account_code') or '').lower()
            or q_clean in str(item.get('account_name') or '').lower()
        ]
    items.sort(key=lambda item: (str(item.get('account_code') or ''), str(item.get('account_name') or '')))

    total_accounts = len(items)
    paged = items[max(0, int(offset)) : max(0, int(offset)) + max(1, min(int(limit), 500))]
    rows_out = [
        {
            'account_id': str(item.get('account_id') or ''),
            'account_code': str(item.get('account_code') or ''),
            'account_name': str(item.get('account_name') or ''),
            'balance': float(item.get('balance') or 0),
            'tx_count': int(item.get('tx_count') or 0),
            'updated_at': _raw_scalar(item.get('updated_at')),
        }
        for item in paged
    ]
    return {
        'summary': {
            'accounts': int(total_accounts),
            'balance_total': float(sum(float(item.get('balance') or 0) for item in items)),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': rows_out,
    }


async def cashflow_account_detail(
    db: AsyncSession,
    account_id: str,
    as_of: date | None = None,
    branches: list[str] | None = None,
):
    target = str(account_id or '').strip().lower()
    if not target:
        raise ValueError('Missing account id')

    stmt = (
        select(
            FactCashflow,
            DimBranch.name.label('branch_name'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
    )
    if as_of is not None:
        stmt = stmt.where(FactCashflow.doc_date <= as_of)
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(DimBranch.external_id.in_(branches))

    rows = (
        await db.execute(
            stmt.order_by(
                FactCashflow.doc_date.desc(),
                FactCashflow.updated_at.desc(),
                FactCashflow.external_id.asc(),
            )
        )
    ).all()

    index = _build_cashflow_accounts_index(rows)
    selected = index.get(target)
    if selected is None:
        selected = next((item for item in index.values() if str(item.get('account_id') or '').lower() == target), None)
    if selected is None:
        raise ValueError('Cashflow account not found')

    notes_unique = list(selected.get('notes_set') or [])
    notes_text = '\n'.join(notes_unique[:12])
    raw_fields = [
        {'key': 'account.id', 'label': 'account.id', 'value': str(selected.get('account_id') or '')},
        {'key': 'account.code', 'label': 'account.code', 'value': str(selected.get('account_code') or '')},
        {'key': 'account.name', 'label': 'account.name', 'value': str(selected.get('account_name') or '')},
    ]

    for line in (selected.get('lines') or [])[:200]:
        raw_fields.append({'key': 'line.external_id', 'label': 'line.external_id', 'value': line.get('external_id')})
        raw_fields.append({'key': 'line.reference_no', 'label': 'line.reference_no', 'value': line.get('reference_no')})

    return {
        'account_id': str(selected.get('account_id') or ''),
        'header': {
            'account_code': str(selected.get('account_code') or ''),
            'account_name': str(selected.get('account_name') or ''),
            'bank_vat': '',
            'is_active': True,
        },
        'notes': {
            'notes_1': notes_text,
        },
        'audit': {
            'created_at': _raw_scalar(selected.get('created_at')),
            'created_by': '',
            'updated_at': _raw_scalar(selected.get('updated_at')),
            'updated_by': '',
        },
        'totals': {
            'balance': float(selected.get('balance') or 0),
            'tx_count': int(selected.get('tx_count') or 0),
        },
        'lines': selected.get('lines') or [],
        'raw_fields': raw_fields,
    }


async def customers_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    q: str | None = None,
    limit: int = 250,
    offset: int = 0,
    include_profile: bool = False,
    balance_only: bool = False,
):
    if balance_only:
        current_map = await _latest_customer_balances_map(
            db,
            as_of=date_to,
            aggregate_only=True,
        )
        q_clean = _normalize_search_term(q)
        rows: list[dict[str, object]] = []
        for customer_id, snapshot in current_map.items():
            code = str(snapshot.get('customer_code') or customer_id).strip()
            name = str(snapshot.get('customer_name') or code or customer_id).strip()
            if q_clean:
                haystack = f'{_normalize_search_term(code)} {_normalize_search_term(name)}'
                if q_clean not in haystack:
                    continue
            balance_date_val = snapshot.get('balance_date')
            rows.append(
                {
                    'customer_id': customer_id,
                    'code': code,
                    'name': name,
                    'afm': '',
                    'amka': '',
                    'address': '',
                    'city': '',
                    'phone_1': '',
                    'phone_2': '',
                    'profession': '',
                    'balance': float(snapshot.get('open_balance') or 0),
                    'open_balance': float(snapshot.get('open_balance') or 0),
                    'overdue_balance': float(snapshot.get('overdue_balance') or 0),
                    'aging_bucket_0_30': float(snapshot.get('aging_bucket_0_30') or 0),
                    'aging_bucket_31_60': float(snapshot.get('aging_bucket_31_60') or 0),
                    'aging_bucket_61_90': float(snapshot.get('aging_bucket_61_90') or 0),
                    'aging_bucket_90_plus': float(snapshot.get('aging_bucket_90_plus') or 0),
                    'last_collection_date': _raw_scalar(snapshot.get('last_collection_date')),
                    'trend_vs_previous': float(snapshot.get('trend_vs_previous') or 0),
                    'balance_date': balance_date_val.isoformat()
                    if isinstance(balance_date_val, date)
                    else str(balance_date_val or ''),
                    'turnover': 0.0,
                    'gross_turnover': 0.0,
                    'sales_docs': 0,
                    'last_sale_date': '',
                    'updated_at': _raw_scalar(snapshot.get('updated_at')),
                }
            )
        rows.sort(
            key=lambda item: (
                -float(item.get('open_balance') or 0),
                -float(item.get('overdue_balance') or 0),
                str(item.get('name') or '').lower(),
            )
        )
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        total = len(rows)
        paged_rows = rows[safe_offset : safe_offset + safe_limit]
        return {
            'summary': {
                'customers': int(total),
                'turnover': 0.0,
                'open_balance': float(sum(float(item.get('open_balance') or 0) for item in rows)),
                'overdue_balance': float(sum(float(item.get('overdue_balance') or 0) for item in rows)),
            },
            'limit': int(safe_limit),
            'offset': int(safe_offset),
            'rows': paged_rows,
        }

    customer_key = _sales_customer_key_expr()
    document_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    base = (
        select(
            customer_key.label('customer_id'),
            func.coalesce(func.max(FactSales.customer_code), literal('')).label('customer_code'),
            func.coalesce(func.max(FactSales.customer_name), literal('ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ')).label('customer_name'),
            func.coalesce(func.max(FactSales.delivery_address), literal('')).label('address'),
            func.coalesce(func.max(FactSales.delivery_city), literal('')).label('city'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('turnover'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_turnover'),
            func.count(func.distinct(document_key)).label('sales_docs'),
            func.max(FactSales.doc_date).label('last_sale_date'),
            func.max(func.coalesce(FactSales.source_updated_at, FactSales.updated_at)).label('updated_at'),
        )
        .select_from(FactSales)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )

    q_clean = _normalize_search_term(q)
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            _sql_normalized_text(FactSales.customer_code).like(like)
            | _sql_normalized_text(FactSales.customer_name).like(like)
            | _sql_normalized_text(FactSales.delivery_address).like(like)
            | _sql_normalized_text(FactSales.delivery_city).like(like)
        )

    grouped = base.group_by(customer_key).subquery('customer_rows')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('customers'),
                func.coalesce(func.sum(grouped.c.turnover), 0).label('turnover'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(grouped)
            .order_by(grouped.c.turnover.desc(), grouped.c.last_sale_date.desc(), grouped.c.customer_name.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    customer_ids = [str(r.get('customer_id') or '').strip() for r in rows if str(r.get('customer_id') or '').strip()]
    latest_profiles: dict[str, FactSales] = {}
    if include_profile and customer_ids:
        latest_rn = over(
            func.row_number(),
            partition_by=customer_key,
            order_by=(FactSales.doc_date.desc(), FactSales.updated_at.desc(), FactSales.external_id.desc()),
        ).label('rn')
        latest_sub = (
            select(
                customer_key.label('customer_id'),
                FactSales.id.label('fact_id'),
                latest_rn,
            )
            .where(customer_key.in_(customer_ids))
        ).subquery('customer_latest')
        latest_pairs = (
            await db.execute(select(latest_sub.c.customer_id, latest_sub.c.fact_id).where(latest_sub.c.rn == 1))
        ).all()
        latest_fact_ids = [pair[1] for pair in latest_pairs if pair[1] is not None]
        if latest_fact_ids:
            fact_rows = (
                await db.execute(select(FactSales).where(FactSales.id.in_(latest_fact_ids)))
            ).scalars().all()
            fact_map = {fact.id: fact for fact in fact_rows}
            for pair in latest_pairs:
                customer_id_val = str(pair[0] or '').strip()
                fact_obj = fact_map.get(pair[1])
                if customer_id_val and fact_obj is not None:
                    latest_profiles[customer_id_val] = fact_obj

    latest_balance_map = await _latest_customer_balances_map(
        db,
        as_of=date_to,
        customer_ids=customer_ids,
    )

    out_rows = []
    open_balance_total = 0.0
    overdue_balance_total = 0.0
    for row in rows:
        customer_id = str(row.get('customer_id') or '').strip()
        latest_fact = latest_profiles.get(customer_id)
        profile = _customer_profile_from_fact(latest_fact)
        balance_row = latest_balance_map.get(customer_id, {})
        code = str(balance_row.get('customer_code') or profile.get('customer_code') or row.get('customer_code') or customer_id)
        name = str(
            balance_row.get('customer_name')
            or profile.get('customer_name')
            or row.get('customer_name')
            or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'
        )
        address = profile.get('address') or str(row.get('address') or '')
        city = profile.get('city') or str(row.get('city') or '')
        last_sale = row.get('last_sale_date')
        open_balance = float(balance_row.get('open_balance') or profile.get('balance') or 0)
        overdue_balance = float(balance_row.get('overdue_balance') or 0)
        balance_date_val = balance_row.get('balance_date')
        last_collection_date_val = balance_row.get('last_collection_date')
        open_balance_total += open_balance
        overdue_balance_total += overdue_balance
        out_rows.append(
            {
                'customer_id': customer_id,
                'code': code,
                'name': name,
                'afm': str(profile.get('afm') or ''),
                'amka': str(profile.get('amka') or ''),
                'address': address,
                'city': city,
                'phone_1': str(profile.get('phone_1') or ''),
                'phone_2': str(profile.get('phone_2') or ''),
                'profession': str(profile.get('profession') or ''),
                'balance': open_balance,
                'open_balance': open_balance,
                'overdue_balance': overdue_balance,
                'aging_bucket_0_30': float(balance_row.get('aging_bucket_0_30') or 0),
                'aging_bucket_31_60': float(balance_row.get('aging_bucket_31_60') or 0),
                'aging_bucket_61_90': float(balance_row.get('aging_bucket_61_90') or 0),
                'aging_bucket_90_plus': float(balance_row.get('aging_bucket_90_plus') or 0),
                'last_collection_date': last_collection_date_val.isoformat()
                if isinstance(last_collection_date_val, date)
                else str(last_collection_date_val or ''),
                'trend_vs_previous': float(balance_row.get('trend_vs_previous') or 0),
                'balance_date': balance_date_val.isoformat() if isinstance(balance_date_val, date) else str(balance_date_val or ''),
                'turnover': float(row.get('turnover') or 0),
                'gross_turnover': float(row.get('gross_turnover') or 0),
                'sales_docs': int(row.get('sales_docs') or 0),
                'last_sale_date': last_sale.isoformat() if isinstance(last_sale, date) else str(last_sale or ''),
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    return {
        'summary': {
            'customers': int(totals_row.get('customers') or 0),
            'turnover': float(totals_row.get('turnover') or 0),
            'open_balance': float(open_balance_total),
            'overdue_balance': float(overdue_balance_total),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def suppliers_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    q: str | None = None,
    limit: int = 250,
    offset: int = 0,
    aggregate_only: bool = False,
):
    agg_purchases_has_rows = (await db.execute(select(AggPurchasesDaily.doc_date).limit(1))).first() is not None
    agg_supplier_balances_has_rows = (
        await db.execute(select(AggSupplierBalancesDaily.balance_date).limit(1))
    ).first() is not None
    if agg_purchases_has_rows and agg_supplier_balances_has_rows:
        supplier_key = func.coalesce(AggPurchasesDaily.supplier_ext_id, DimSupplier.external_id, DimSupplier.name, literal(''))
        base = (
            select(
                supplier_key.label('supplier_id'),
                func.coalesce(func.max(AggPurchasesDaily.supplier_ext_id), literal('')).label('supplier_code'),
                func.coalesce(func.max(DimSupplier.name), func.max(AggPurchasesDaily.supplier_ext_id), literal('N/A')).label(
                    'supplier_name'
                ),
                func.coalesce(func.sum(AggPurchasesDaily.net_value), 0).label('purchases_net'),
                func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0).label('purchases_cost'),
                literal(0).label('purchase_docs'),
                func.max(AggPurchasesDaily.doc_date).label('last_purchase_date'),
                func.max(AggPurchasesDaily.updated_at).label('updated_at'),
            )
            .select_from(AggPurchasesDaily)
            .join(DimSupplier, DimSupplier.external_id == AggPurchasesDaily.supplier_ext_id, isouter=True)
            .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
        )

        branches = _effective_branch_filter(branches)

        if branches is not None:
            base = base.where(AggPurchasesDaily.branch_ext_id.in_(branches))

        q_clean = _normalize_search_term(q)
        if q_clean:
            like = f'%{q_clean}%'
            base = base.where(
                _sql_normalized_text(AggPurchasesDaily.supplier_ext_id).like(like)
                | _sql_normalized_text(DimSupplier.name).like(like)
            )

        grouped = base.group_by(supplier_key).subquery('supplier_rows_agg')
        totals_row = (
            await db.execute(
                select(
                    func.coalesce(func.count(), 0).label('suppliers'),
                    func.coalesce(func.sum(grouped.c.purchases_net), 0).label('purchases_net'),
                    func.coalesce(func.sum(grouped.c.purchases_cost), 0).label('purchases_cost'),
                    func.coalesce(func.sum(grouped.c.purchase_docs), 0).label('purchase_docs'),
                )
            )
        ).mappings().one()

        rows = (
            await db.execute(
                select(grouped)
                .order_by(grouped.c.purchases_net.desc(), grouped.c.last_purchase_date.desc(), grouped.c.supplier_name.asc())
                .offset(max(0, int(offset)))
                .limit(max(1, min(int(limit), 500)))
            )
        ).mappings().all()
        supplier_ids = [str(r.get('supplier_id') or '').strip() for r in rows if str(r.get('supplier_id') or '').strip()]

        balances_map: dict[str, dict[str, object]] = {}
        if supplier_ids:
            balance_supplier_key = func.coalesce(
                AggSupplierBalancesDaily.supplier_ext_id,
                DimSupplier.external_id,
                DimSupplier.name,
                literal(''),
            )
            balances_by_day = (
                select(
                    balance_supplier_key.label('supplier_id'),
                    AggSupplierBalancesDaily.balance_date.label('balance_date'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.open_balance), 0).label('open_balance'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.overdue_balance), 0).label('overdue_balance'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
                    func.coalesce(func.sum(AggSupplierBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
                )
                .select_from(AggSupplierBalancesDaily)
                .join(DimSupplier, DimSupplier.external_id == AggSupplierBalancesDaily.supplier_ext_id, isouter=True)
                .where(AggSupplierBalancesDaily.balance_date <= date_to)
                .where(balance_supplier_key.in_(supplier_ids))
            )
            branches = _effective_branch_filter(branches)
            if branches is not None:
                balances_by_day = balances_by_day.where(AggSupplierBalancesDaily.branch_ext_id.in_(branches))
            balances_by_day = balances_by_day.group_by(
                balance_supplier_key,
                AggSupplierBalancesDaily.balance_date,
            ).subquery('supplier_agg_balances_by_day')

            ranked_balances = (
                select(
                    balances_by_day.c.supplier_id,
                    balances_by_day.c.balance_date,
                    balances_by_day.c.open_balance,
                    balances_by_day.c.overdue_balance,
                    balances_by_day.c.aging_bucket_0_30,
                    balances_by_day.c.aging_bucket_31_60,
                    balances_by_day.c.aging_bucket_61_90,
                    balances_by_day.c.aging_bucket_90_plus,
                    balances_by_day.c.trend_vs_previous,
                    over(
                        func.row_number(),
                        partition_by=balances_by_day.c.supplier_id,
                        order_by=balances_by_day.c.balance_date.desc(),
                    ).label('rn'),
                )
            ).subquery('supplier_agg_balances_ranked')
            latest_balances = (await db.execute(select(ranked_balances).where(ranked_balances.c.rn == 1))).mappings().all()
            balances_map = {
                str(r.get('supplier_id') or '').strip(): {
                    'balance_date': r.get('balance_date'),
                    'open_balance': float(r.get('open_balance') or 0),
                    'overdue_balance': float(r.get('overdue_balance') or 0),
                    'aging_bucket_0_30': float(r.get('aging_bucket_0_30') or 0),
                    'aging_bucket_31_60': float(r.get('aging_bucket_31_60') or 0),
                    'aging_bucket_61_90': float(r.get('aging_bucket_61_90') or 0),
                    'aging_bucket_90_plus': float(r.get('aging_bucket_90_plus') or 0),
                    'trend_vs_previous': float(r.get('trend_vs_previous') or 0),
                }
                for r in latest_balances
            }

        supplier_payments_stmt = (
            select(func.coalesce(func.sum(AggCashDaily.outflows), 0))
            .select_from(AggCashDaily)
            .where(*_date_range(AggCashDaily.doc_date, date_from, date_to))
            .where(AggCashDaily.subcategory.in_(['supplier_payments', 'supplier_transfers']))
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            supplier_payments_stmt = supplier_payments_stmt.where(AggCashDaily.branch_ext_id.in_(branches))
        supplier_payments_total = float((await db.execute(supplier_payments_stmt)).scalar_one_or_none() or 0)

        out_rows = []
        open_balance_total = 0.0
        overdue_balance_total = 0.0
        aging_bucket_0_30_total = 0.0
        aging_bucket_31_60_total = 0.0
        aging_bucket_61_90_total = 0.0
        aging_bucket_90_plus_total = 0.0

        for row in rows:
            last_purchase = row.get('last_purchase_date')
            supplier_id = str(row.get('supplier_id') or '').strip()
            balance = balances_map.get(supplier_id, {})
            open_balance = float(balance.get('open_balance') or 0)
            overdue_balance = float(balance.get('overdue_balance') or 0)
            aging_bucket_0_30 = float(balance.get('aging_bucket_0_30') or 0)
            aging_bucket_31_60 = float(balance.get('aging_bucket_31_60') or 0)
            aging_bucket_61_90 = float(balance.get('aging_bucket_61_90') or 0)
            aging_bucket_90_plus = float(balance.get('aging_bucket_90_plus') or 0)
            balance_date_val = balance.get('balance_date')

            open_balance_total += open_balance
            overdue_balance_total += overdue_balance
            aging_bucket_0_30_total += aging_bucket_0_30
            aging_bucket_31_60_total += aging_bucket_31_60
            aging_bucket_61_90_total += aging_bucket_61_90
            aging_bucket_90_plus_total += aging_bucket_90_plus

            out_rows.append(
                {
                    'supplier_id': supplier_id,
                    'code': str(row.get('supplier_code') or row.get('supplier_id') or '').strip(),
                    'name': str(row.get('supplier_name') or 'N/A'),
                    'purchases_net': float(row.get('purchases_net') or 0),
                    'purchases_cost': float(row.get('purchases_cost') or 0),
                    'payments_total': 0.0,
                    'open_balance': open_balance,
                    'overdue_balance': overdue_balance,
                    'aging_bucket_0_30': aging_bucket_0_30,
                    'aging_bucket_31_60': aging_bucket_31_60,
                    'aging_bucket_61_90': aging_bucket_61_90,
                    'aging_bucket_90_plus': aging_bucket_90_plus,
                    'trend_vs_previous': float(balance.get('trend_vs_previous') or 0),
                    'balance_date': balance_date_val.isoformat()
                    if isinstance(balance_date_val, date)
                    else str(balance_date_val or ''),
                    'purchase_docs': int(row.get('purchase_docs') or 0),
                    'last_purchase_date': last_purchase.isoformat()
                    if isinstance(last_purchase, date)
                    else str(last_purchase or ''),
                    'updated_at': _raw_scalar(row.get('updated_at')),
                }
            )

        return {
            'summary': {
                'suppliers': int(totals_row.get('suppliers') or 0),
                'purchases_net': float(totals_row.get('purchases_net') or 0),
                'purchases_cost': float(totals_row.get('purchases_cost') or 0),
                'purchase_docs': int(totals_row.get('purchase_docs') or 0),
                'payments_total': supplier_payments_total,
                'open_balance': float(open_balance_total),
                'overdue_balance': float(overdue_balance_total),
                'aging_bucket_0_30': float(aging_bucket_0_30_total),
                'aging_bucket_31_60': float(aging_bucket_31_60_total),
                'aging_bucket_61_90': float(aging_bucket_61_90_total),
                'aging_bucket_90_plus': float(aging_bucket_90_plus_total),
            },
            'limit': int(limit),
            'offset': int(offset),
            'rows': out_rows,
        }

    if aggregate_only:
        return {
            'summary': {
                'suppliers': 0,
                'purchases_net': 0.0,
                'purchases_cost': 0.0,
                'purchase_docs': 0,
                'payments_total': 0.0,
                'open_balance': 0.0,
                'overdue_balance': 0.0,
                'aging_bucket_0_30': 0.0,
                'aging_bucket_31_60': 0.0,
                'aging_bucket_61_90': 0.0,
                'aging_bucket_90_plus': 0.0,
            },
            'limit': int(limit),
            'offset': int(offset),
            'rows': [],
        }

    doc_key = _fact_purchases_document_key_expr()
    supplier_key = func.coalesce(FactPurchases.supplier_ext_id, DimSupplier.external_id, DimSupplier.name, literal(''))
    base = (
        select(
            supplier_key.label('supplier_id'),
            func.coalesce(func.max(FactPurchases.supplier_ext_id), literal('')).label('supplier_code'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactPurchases.supplier_ext_id), literal('N/A')).label(
                'supplier_name'
            ),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_net'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('purchases_cost'),
            func.count(func.distinct(doc_key)).label('purchase_docs'),
            func.max(FactPurchases.doc_date).label('last_purchase_date'),
            func.max(FactPurchases.updated_at).label('updated_at'),
        )
        .select_from(FactPurchases)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )

    branches = _effective_branch_filter(branches)

    if branches is not None:
        base = base.where(FactPurchases.branch_ext_id.in_(branches))

    q_clean = _normalize_search_term(q)
    if q_clean:
        like = f'%{q_clean}%'
        base = base.where(
            _sql_normalized_text(FactPurchases.supplier_ext_id).like(like)
            | _sql_normalized_text(DimSupplier.name).like(like)
        )

    grouped = base.group_by(supplier_key).subquery('supplier_rows')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('suppliers'),
                func.coalesce(func.sum(grouped.c.purchases_net), 0).label('purchases_net'),
                func.coalesce(func.sum(grouped.c.purchases_cost), 0).label('purchases_cost'),
                func.coalesce(func.sum(grouped.c.purchase_docs), 0).label('purchase_docs'),
            )
        )
    ).mappings().one()

    rows = (
        await db.execute(
            select(grouped)
            .order_by(grouped.c.purchases_net.desc(), grouped.c.last_purchase_date.desc(), grouped.c.supplier_name.asc())
            .offset(max(0, int(offset)))
            .limit(max(1, min(int(limit), 500)))
        )
    ).mappings().all()

    supplier_ids = [str(r.get('supplier_id') or '').strip() for r in rows if str(r.get('supplier_id') or '').strip()]

    payments_map: dict[str, float] = {}
    if supplier_ids:
        subcategory_expr = _cashflow_subcategory_expr()
        payment_key = func.coalesce(FactCashflow.counterparty_id, FactCashflow.reference_no, FactCashflow.external_id)
        payments_stmt = (
            select(
                payment_key.label('supplier_id'),
                func.coalesce(func.sum(func.abs(FactCashflow.amount)), 0).label('payments_total'),
            )
            .select_from(FactCashflow)
            .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
            .where(subcategory_expr.in_(['supplier_payments', 'supplier_transfers']))
            .where(payment_key.in_(supplier_ids))
            .group_by(payment_key)
        )
        payments_rows = (await db.execute(payments_stmt)).mappings().all()
        payments_map = {str(r.get('supplier_id') or '').strip(): float(r.get('payments_total') or 0) for r in payments_rows}

    balances_map: dict[str, dict[str, object]] = {}
    if supplier_ids:
        balance_supplier_key = func.coalesce(
            FactSupplierBalance.supplier_ext_id,
            DimSupplier.external_id,
            DimSupplier.name,
            literal(''),
        )
        balances_by_day = (
            select(
                balance_supplier_key.label('supplier_id'),
                FactSupplierBalance.balance_date.label('balance_date'),
                func.coalesce(func.sum(FactSupplierBalance.open_balance), 0).label('open_balance'),
                func.coalesce(func.sum(FactSupplierBalance.overdue_balance), 0).label('overdue_balance'),
                func.coalesce(func.sum(FactSupplierBalance.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
                func.coalesce(func.sum(FactSupplierBalance.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
                func.coalesce(func.sum(FactSupplierBalance.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
                func.coalesce(func.sum(FactSupplierBalance.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
                func.coalesce(func.sum(FactSupplierBalance.trend_vs_previous), 0).label('trend_vs_previous'),
            )
            .select_from(FactSupplierBalance)
            .join(DimSupplier, DimSupplier.external_id == FactSupplierBalance.supplier_ext_id, isouter=True)
            .where(FactSupplierBalance.balance_date <= date_to)
            .where(balance_supplier_key.in_(supplier_ids))
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            balances_by_day = balances_by_day.where(FactSupplierBalance.branch_ext_id.in_(branches))
        balances_by_day = balances_by_day.group_by(balance_supplier_key, FactSupplierBalance.balance_date).subquery(
            'supplier_balances_by_day'
        )

        ranked_balances = (
            select(
                balances_by_day.c.supplier_id,
                balances_by_day.c.balance_date,
                balances_by_day.c.open_balance,
                balances_by_day.c.overdue_balance,
                balances_by_day.c.aging_bucket_0_30,
                balances_by_day.c.aging_bucket_31_60,
                balances_by_day.c.aging_bucket_61_90,
                balances_by_day.c.aging_bucket_90_plus,
                balances_by_day.c.trend_vs_previous,
                over(
                    func.row_number(),
                    partition_by=balances_by_day.c.supplier_id,
                    order_by=balances_by_day.c.balance_date.desc(),
                ).label('rn'),
            )
        ).subquery('supplier_balances_ranked')

        latest_balances = (
            await db.execute(
                select(ranked_balances).where(ranked_balances.c.rn == 1)
            )
        ).mappings().all()

        balances_map = {
            str(r.get('supplier_id') or '').strip(): {
                'balance_date': r.get('balance_date'),
                'open_balance': float(r.get('open_balance') or 0),
                'overdue_balance': float(r.get('overdue_balance') or 0),
                'aging_bucket_0_30': float(r.get('aging_bucket_0_30') or 0),
                'aging_bucket_31_60': float(r.get('aging_bucket_31_60') or 0),
                'aging_bucket_61_90': float(r.get('aging_bucket_61_90') or 0),
                'aging_bucket_90_plus': float(r.get('aging_bucket_90_plus') or 0),
                'trend_vs_previous': float(r.get('trend_vs_previous') or 0),
            }
            for r in latest_balances
        }

    out_rows = []
    open_balance_total = 0.0
    overdue_balance_total = 0.0
    aging_bucket_0_30_total = 0.0
    aging_bucket_31_60_total = 0.0
    aging_bucket_61_90_total = 0.0
    aging_bucket_90_plus_total = 0.0

    for row in rows:
        last_purchase = row.get('last_purchase_date')
        supplier_id = str(row.get('supplier_id') or '').strip()
        payments_total = float(payments_map.get(supplier_id, 0.0))
        balance = balances_map.get(supplier_id, {})
        open_balance = float(balance.get('open_balance') or 0)
        overdue_balance = float(balance.get('overdue_balance') or 0)
        aging_bucket_0_30 = float(balance.get('aging_bucket_0_30') or 0)
        aging_bucket_31_60 = float(balance.get('aging_bucket_31_60') or 0)
        aging_bucket_61_90 = float(balance.get('aging_bucket_61_90') or 0)
        aging_bucket_90_plus = float(balance.get('aging_bucket_90_plus') or 0)
        balance_date_val = balance.get('balance_date')

        open_balance_total += open_balance
        overdue_balance_total += overdue_balance
        aging_bucket_0_30_total += aging_bucket_0_30
        aging_bucket_31_60_total += aging_bucket_31_60
        aging_bucket_61_90_total += aging_bucket_61_90
        aging_bucket_90_plus_total += aging_bucket_90_plus

        out_rows.append(
            {
                'supplier_id': supplier_id,
                'code': str(row.get('supplier_code') or row.get('supplier_id') or '').strip(),
                'name': str(row.get('supplier_name') or 'N/A'),
                'purchases_net': float(row.get('purchases_net') or 0),
                'purchases_cost': float(row.get('purchases_cost') or 0),
                'payments_total': payments_total,
                'open_balance': open_balance,
                'overdue_balance': overdue_balance,
                'aging_bucket_0_30': aging_bucket_0_30,
                'aging_bucket_31_60': aging_bucket_31_60,
                'aging_bucket_61_90': aging_bucket_61_90,
                'aging_bucket_90_plus': aging_bucket_90_plus,
                'trend_vs_previous': float(balance.get('trend_vs_previous') or 0),
                'balance_date': balance_date_val.isoformat() if isinstance(balance_date_val, date) else str(balance_date_val or ''),
                'purchase_docs': int(row.get('purchase_docs') or 0),
                'last_purchase_date': last_purchase.isoformat()
                if isinstance(last_purchase, date)
                else str(last_purchase or ''),
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    return {
        'summary': {
            'suppliers': int(totals_row.get('suppliers') or 0),
            'purchases_net': float(totals_row.get('purchases_net') or 0),
            'purchases_cost': float(totals_row.get('purchases_cost') or 0),
            'purchase_docs': int(totals_row.get('purchase_docs') or 0),
            'payments_total': float(sum(payments_map.values()) if payments_map else 0.0),
            'open_balance': float(open_balance_total),
            'overdue_balance': float(overdue_balance_total),
            'aging_bucket_0_30': float(aging_bucket_0_30_total),
            'aging_bucket_31_60': float(aging_bucket_31_60_total),
            'aging_bucket_61_90': float(aging_bucket_61_90_total),
            'aging_bucket_90_plus': float(aging_bucket_90_plus_total),
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': out_rows,
    }


async def receivables_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    aggregate_only: bool = False,
):
    current_map = await _latest_customer_balances_map(
        db,
        as_of=date_to,
        branches=branches,
        aggregate_only=aggregate_only,
    )
    previous_to = date_from - timedelta(days=1)
    previous_map = await _latest_customer_balances_map(
        db,
        as_of=previous_to,
        branches=branches,
        aggregate_only=aggregate_only,
    )

    current_open = float(sum(float(item.get('open_balance') or 0) for item in current_map.values()))
    current_overdue = float(sum(float(item.get('overdue_balance') or 0) for item in current_map.values()))
    previous_open = float(sum(float(item.get('open_balance') or 0) for item in previous_map.values()))
    growth_value = current_open - previous_open
    growth_pct = ((growth_value / previous_open) * 100.0) if previous_open > 0 else None
    overdue_ratio_pct = ((current_overdue / current_open) * 100.0) if current_open > 0 else 0.0

    top_customer_id = ''
    top_customer_name = ''
    top_customer_balance = 0.0
    for customer_id, snapshot in current_map.items():
        bal = float(snapshot.get('open_balance') or 0)
        if bal <= top_customer_balance:
            continue
        top_customer_balance = bal
        top_customer_id = customer_id
        top_customer_name = str(snapshot.get('customer_name') or customer_id)
    top_customer_share_pct = ((top_customer_balance / current_open) * 100.0) if current_open > 0 else 0.0

    return {
        'as_of': date_to.isoformat(),
        'summary': {
            'customers': int(len(current_map)),
            'total_receivables': current_open,
            'overdue_receivables': current_overdue,
            'overdue_ratio_pct': overdue_ratio_pct,
        },
        'growth_vs_previous': {
            'previous_as_of': previous_to.isoformat(),
            'previous_open_balance': previous_open,
            'value': growth_value,
            'pct': growth_pct,
        },
        'top_customer_exposure': {
            'customer_id': top_customer_id,
            'customer_name': top_customer_name,
            'open_balance': top_customer_balance,
            'share_pct': top_customer_share_pct,
        },
    }


async def receivables_aging(
    db: AsyncSession,
    date_to: date,
    branches: list[str] | None = None,
):
    current_map = await _latest_customer_balances_map(db, as_of=date_to, branches=branches)
    bucket_0_30 = float(sum(float(item.get('aging_bucket_0_30') or 0) for item in current_map.values()))
    bucket_31_60 = float(sum(float(item.get('aging_bucket_31_60') or 0) for item in current_map.values()))
    bucket_61_90 = float(sum(float(item.get('aging_bucket_61_90') or 0) for item in current_map.values()))
    bucket_90_plus = float(sum(float(item.get('aging_bucket_90_plus') or 0) for item in current_map.values()))
    total = bucket_0_30 + bucket_31_60 + bucket_61_90 + bucket_90_plus

    def _share(v: float) -> float:
        return (v / total * 100.0) if total > 0 else 0.0

    return {
        'as_of': date_to.isoformat(),
        'total_receivables': float(total),
        'aging': {
            'aging_bucket_0_30': float(bucket_0_30),
            'aging_bucket_31_60': float(bucket_31_60),
            'aging_bucket_61_90': float(bucket_61_90),
            'aging_bucket_90_plus': float(bucket_90_plus),
        },
        'shares_pct': {
            'aging_bucket_0_30': _share(bucket_0_30),
            'aging_bucket_31_60': _share(bucket_31_60),
            'aging_bucket_61_90': _share(bucket_61_90),
            'aging_bucket_90_plus': _share(bucket_90_plus),
        },
    }


async def receivables_top_customers(
    db: AsyncSession,
    date_to: date,
    branches: list[str] | None = None,
    q: str | None = None,
    limit: int = 250,
    offset: int = 0,
):
    current_map = await _latest_customer_balances_map(db, as_of=date_to, branches=branches)
    q_clean = _normalize_search_term(q)
    rows: list[dict[str, object]] = []
    total_open_balance = float(sum(float(item.get('open_balance') or 0) for item in current_map.values()))
    total_overdue_balance = float(sum(float(item.get('overdue_balance') or 0) for item in current_map.values()))

    for customer_id, snapshot in current_map.items():
        code = str(snapshot.get('customer_code') or customer_id).strip()
        name = str(snapshot.get('customer_name') or customer_id).strip()
        if q_clean:
            haystack = f'{_normalize_search_term(code)} {_normalize_search_term(name)}'
            if q_clean not in haystack:
                continue
        open_balance = float(snapshot.get('open_balance') or 0)
        overdue_balance = float(snapshot.get('overdue_balance') or 0)
        row = {
            'customer_id': customer_id,
            'code': code,
            'name': name or code or customer_id,
            'open_balance': open_balance,
            'overdue_balance': overdue_balance,
            'overdue_ratio_pct': ((overdue_balance / open_balance) * 100.0) if open_balance > 0 else 0.0,
            'aging_bucket_0_30': float(snapshot.get('aging_bucket_0_30') or 0),
            'aging_bucket_31_60': float(snapshot.get('aging_bucket_31_60') or 0),
            'aging_bucket_61_90': float(snapshot.get('aging_bucket_61_90') or 0),
            'aging_bucket_90_plus': float(snapshot.get('aging_bucket_90_plus') or 0),
            'last_collection_date': _raw_scalar(snapshot.get('last_collection_date')),
            'trend_vs_previous': float(snapshot.get('trend_vs_previous') or 0),
            'balance_date': _raw_scalar(snapshot.get('balance_date')),
            'updated_at': _raw_scalar(snapshot.get('updated_at')),
        }
        rows.append(row)

    rows.sort(key=lambda item: (-float(item.get('open_balance') or 0), -float(item.get('overdue_balance') or 0), str(item.get('name') or '').lower()))
    page = rows[max(0, int(offset)): max(0, int(offset)) + max(1, min(int(limit), 500))]
    for row in page:
        open_balance = float(row.get('open_balance') or 0)
        row['share_pct'] = (open_balance / total_open_balance * 100.0) if total_open_balance > 0 else 0.0

    return {
        'as_of': date_to.isoformat(),
        'summary': {
            'customers': int(len(rows)),
            'total_open_balance': total_open_balance,
            'total_overdue_balance': total_overdue_balance,
        },
        'limit': int(limit),
        'offset': int(offset),
        'rows': page,
    }


async def receivables_collection_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    agg_has_rows = (await db.execute(select(AggCustomerBalancesDaily.balance_date).limit(1))).first() is not None
    if agg_has_rows:
        stmt = (
            select(
                AggCustomerBalancesDaily.balance_date.label('balance_date'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
                func.max(AggCustomerBalancesDaily.updated_at).label('updated_at'),
            )
            .select_from(AggCustomerBalancesDaily)
            .where(*_date_range(AggCustomerBalancesDaily.balance_date, date_from, date_to))
            .group_by(AggCustomerBalancesDaily.balance_date)
            .order_by(AggCustomerBalancesDaily.balance_date.asc())
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            stmt = stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
    else:
        stmt = (
            select(
                FactCustomerBalance.balance_date.label('balance_date'),
                func.coalesce(func.sum(FactCustomerBalance.open_balance), 0).label('open_balance'),
                func.coalesce(func.sum(FactCustomerBalance.overdue_balance), 0).label('overdue_balance'),
                func.coalesce(func.sum(FactCustomerBalance.trend_vs_previous), 0).label('trend_vs_previous'),
                func.max(FactCustomerBalance.updated_at).label('updated_at'),
            )
            .select_from(FactCustomerBalance)
            .where(*_date_range(FactCustomerBalance.balance_date, date_from, date_to))
            .group_by(FactCustomerBalance.balance_date)
            .order_by(FactCustomerBalance.balance_date.asc())
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            stmt = stmt.where(FactCustomerBalance.branch_ext_id.in_(branches))

    rows = (await db.execute(stmt)).mappings().all()
    out_rows = []
    total_collections = 0.0
    total_new_outstanding = 0.0

    for row in rows:
        trend_val = float(row.get('trend_vs_previous') or 0)
        estimated_collections = abs(min(trend_val, 0.0))
        new_outstanding = max(trend_val, 0.0)
        total_collections += estimated_collections
        total_new_outstanding += new_outstanding
        out_rows.append(
            {
                'balance_date': _raw_scalar(row.get('balance_date')),
                'open_balance': float(row.get('open_balance') or 0),
                'overdue_balance': float(row.get('overdue_balance') or 0),
                'trend_vs_previous': trend_val,
                'estimated_collections': estimated_collections,
                'new_outstanding': new_outstanding,
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
        'summary': {
            'estimated_collections': float(total_collections),
            'new_outstanding': float(total_new_outstanding),
            'net_delta': float(total_new_outstanding - total_collections),
        },
        'rows': out_rows,
    }


async def customer_detail(
    db: AsyncSession,
    customer_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
):
    target = str(customer_id or '').strip()
    if not target:
        raise ValueError('Missing customer id')

    balance_map = await _latest_customer_balances_map(
        db,
        as_of=(date_to or date.today()),
        customer_ids=[target],
    )
    balance_snapshot = balance_map.get(target, {})

    customer_key = _sales_customer_key_expr()
    latest_fact = (
        await db.execute(
            select(FactSales)
            .where(customer_key == target)
            .order_by(FactSales.doc_date.desc(), FactSales.updated_at.desc(), FactSales.external_id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if latest_fact is None and not balance_snapshot:
        raise ValueError('Customer not found')

    profile = _customer_profile_from_fact(latest_fact)
    profile['customer_id'] = target
    profile['customer_code'] = str(balance_snapshot.get('customer_code') or profile.get('customer_code') or target)
    profile['customer_name'] = str(
        balance_snapshot.get('customer_name') or profile.get('customer_name') or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'
    )
    profile['balance'] = float(balance_snapshot.get('open_balance') or profile.get('balance') or 0)
    profile['open_balance'] = float(balance_snapshot.get('open_balance') or 0)
    profile['overdue_balance'] = float(balance_snapshot.get('overdue_balance') or 0)
    profile['aging_bucket_0_30'] = float(balance_snapshot.get('aging_bucket_0_30') or 0)
    profile['aging_bucket_31_60'] = float(balance_snapshot.get('aging_bucket_31_60') or 0)
    profile['aging_bucket_61_90'] = float(balance_snapshot.get('aging_bucket_61_90') or 0)
    profile['aging_bucket_90_plus'] = float(balance_snapshot.get('aging_bucket_90_plus') or 0)
    profile['trend_vs_previous'] = float(balance_snapshot.get('trend_vs_previous') or 0)
    profile['balance_date'] = _raw_scalar(balance_snapshot.get('balance_date'))
    profile['last_collection_date'] = _raw_scalar(balance_snapshot.get('last_collection_date'))

    document_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    sales_stmt = (
        select(
            document_key.label('document_id'),
            func.coalesce(func.max(FactSales.document_no), func.max(document_key), literal('')).label('document_no'),
            func.max(FactSales.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactSales.warehouse_ext_id), literal('N/A')).label('warehouse_name'),
            func.coalesce(func.max(FactSales.document_series), func.max(FactSales.document_type), literal('N/A')).label('series'),
            func.coalesce(func.max(FactSales.document_status), literal('')).label('status'),
            func.coalesce(func.max(FactSales.eshop_code), literal('')).label('eshop_code'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
            func.max(func.coalesce(FactSales.source_updated_at, FactSales.updated_at)).label('updated_at'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
        .where(customer_key == target)
    )
    if date_from is not None:
        sales_stmt = sales_stmt.where(FactSales.doc_date >= date_from)
    if date_to is not None:
        sales_stmt = sales_stmt.where(FactSales.doc_date <= date_to)
    sales_rows_raw = (
        await db.execute(
            sales_stmt.group_by(document_key).order_by(
                func.max(FactSales.doc_date).desc(),
                func.max(func.coalesce(FactSales.source_updated_at, FactSales.updated_at)).desc(),
                document_key.asc(),
            )
        )
    ).mappings().all()

    sales_rows = []
    for row in sales_rows_raw:
        doc_date_val = row.get('document_date')
        sales_rows.append(
            {
                'document_id': str(row.get('document_id') or ''),
                'document_no': str(row.get('document_no') or row.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': str(row.get('branch_name') or 'N/A'),
                'warehouse': str(row.get('warehouse_name') or 'N/A'),
                'series': str(row.get('series') or ''),
                'status': str(row.get('status') or ''),
                'eshop_code': str(row.get('eshop_code') or ''),
                'net_value': float(row.get('net_value') or 0),
                'gross_value': float(row.get('gross_value') or 0),
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    branch_stmt = (
        select(
            func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('N/A')).label('branch_name'),
            func.count(func.distinct(document_key)).label('documents'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('turnover'),
            func.max(FactSales.doc_date).label('last_sale_date'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .where(customer_key == target)
        .group_by(FactSales.branch_ext_id)
        .order_by(func.coalesce(func.sum(FactSales.net_value), 0).desc())
    )
    branch_rows_raw = (await db.execute(branch_stmt)).mappings().all()
    branch_rows = []
    for row in branch_rows_raw:
        last_sale = row.get('last_sale_date')
        branch_rows.append(
            {
                'branch': str(row.get('branch_name') or 'N/A'),
                'documents': int(row.get('documents') or 0),
                'turnover': float(row.get('turnover') or 0),
                'last_sale_date': last_sale.isoformat() if isinstance(last_sale, date) else str(last_sale or ''),
            }
        )

    collection_rows = []
    code_term = str(profile.get('customer_code') or '').strip().lower()
    name_term = str(profile.get('customer_name') or '').strip().lower()
    search_terms: list[str] = []
    if len(code_term) >= 3:
        search_terms.append(code_term)
    if len(name_term) >= 4:
        search_terms.append(name_term)

    if search_terms:
        entry_type_col = func.lower(cast(func.coalesce(FactCashflow.entry_type, literal('')), String))
        notes_col = func.lower(cast(func.coalesce(FactCashflow.notes, literal('')), String))
        ref_col = func.lower(cast(func.coalesce(FactCashflow.reference_no, literal('')), String))
        term_filters = [notes_col.like(f'%{term}%') | ref_col.like(f'%{term}%') for term in search_terms]
        collections_stmt = (
            select(
                FactCashflow.external_id.label('document_id'),
                func.coalesce(func.max(FactCashflow.reference_no), func.max(FactCashflow.external_id), literal('')).label(
                    'document_no'
                ),
                func.max(FactCashflow.doc_date).label('document_date'),
                func.coalesce(func.max(DimBranch.name), literal('N/A')).label('branch_name'),
                func.coalesce(func.max(FactCashflow.entry_type), literal('')).label('entry_type'),
                func.coalesce(func.max(FactCashflow.notes), literal('')).label('notes'),
                func.coalesce(func.sum(FactCashflow.amount), 0).label('total_value'),
                func.max(FactCashflow.updated_at).label('updated_at'),
            )
            .select_from(FactCashflow)
            .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
            .where(
                entry_type_col.in_(
                    [
                        'customer_collections',
                        'customer_collection',
                        'customer_transfers',
                        'customer_transfer',
                        'customer_bank_transfer',
                        'customer_wire_transfer',
                        'customer_wire',
                    ]
                )
            )
            .where(or_(*term_filters))
        )
        if date_from is not None:
            collections_stmt = collections_stmt.where(FactCashflow.doc_date >= date_from)
        if date_to is not None:
            collections_stmt = collections_stmt.where(FactCashflow.doc_date <= date_to)
        collections_rows_raw = (
            await db.execute(
                collections_stmt.group_by(FactCashflow.external_id).order_by(
                    func.max(FactCashflow.doc_date).desc(),
                    func.max(FactCashflow.updated_at).desc(),
                    FactCashflow.external_id.asc(),
                )
            )
        ).mappings().all()
        for row in collections_rows_raw:
            doc_date_val = row.get('document_date')
            collection_rows.append(
                {
                    'document_id': str(row.get('document_id') or ''),
                    'document_no': str(row.get('document_no') or row.get('document_id') or ''),
                    'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                    'branch': str(row.get('branch_name') or 'N/A'),
                    'series': _cashflow_entry_label(str(row.get('entry_type') or '')),
                    'reason': str(row.get('notes') or ''),
                    'total_value': float(row.get('total_value') or 0),
                    'updated_at': _raw_scalar(row.get('updated_at')),
                }
            )

    sales_turnover = float(sum(float(item.get('net_value') or 0) for item in sales_rows))
    collections_total = float(sum(float(item.get('total_value') or 0) for item in collection_rows))

    return {
        'customer_id': target,
        'customer': profile,
        'sales_history': sales_rows,
        'collections_history': collection_rows,
        'branch_history': branch_rows,
        'totals': {
            'sales_documents': len(sales_rows),
            'sales_turnover': sales_turnover,
            'collections_documents': len(collection_rows),
            'collections_total': collections_total,
            'open_balance': float(profile.get('open_balance') or 0),
            'overdue_balance': float(profile.get('overdue_balance') or 0),
        },
    }


async def cashflow_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    agg_has_rows = (await db.execute(select(AggCashDaily.doc_date).limit(1))).first() is not None
    if not agg_has_rows:
        return {'entries': 0, 'inflows': 0.0, 'outflows': 0.0, 'net': 0.0}

    stmt = (
        select(
            func.coalesce(func.sum(AggCashDaily.entries), 0),
            func.coalesce(func.sum(AggCashDaily.inflows), 0),
            func.coalesce(func.sum(AggCashDaily.outflows), 0),
        )
        .select_from(AggCashDaily)
        .where(*_date_range(AggCashDaily.doc_date, date_from, date_to))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(AggCashDaily.branch_ext_id.in_(branches))
    row = (await db.execute(stmt)).one()
    entries = int(row[0] or 0)
    inflows = float(row[1] or 0)
    outflows = float(row[2] or 0)
    return {
        'entries': entries,
        'inflows': inflows,
        'outflows': outflows,
        'net': inflows - outflows,
    }


async def inventory_by_brand(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    stmt = (
        select(
            func.coalesce(DimBrand.name, DimBrand.external_id, literal('N/A')).label('brand'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(DimBrand.name, DimBrand.external_id)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'brand': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_by_commercial_category(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    stmt = (
        select(
            func.coalesce(DimGroup.name, DimGroup.external_id, literal('N/A')).label('commercial_category'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(DimGroup.name, DimGroup.external_id)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'commercial_category': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_by_manufacturer(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    lookback_days: int = 365,
    limit: int = 12,
):
    manufacturer_label = func.coalesce(
        func.nullif(func.btrim(DimItem.manufacturer_name), ''),
        func.nullif(func.btrim(FactInventory.source_payload_json['manufacturer_name'].astext), ''),
        func.nullif(func.btrim(DimItem.manufacturer_code), ''),
        func.nullif(func.btrim(FactInventory.source_payload_json['manufacturer_code'].astext), ''),
        literal('N/A'),
    )

    stmt = (
        select(
            manufacturer_label.label('manufacturer'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(manufacturer_label)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'manufacturer': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_filter_options(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    category_path_expr = func.concat_ws(
        ' > ',
        func.nullif(func.btrim(DimItem.category_1), ''),
        func.nullif(func.btrim(DimItem.category_2), ''),
        func.nullif(func.btrim(DimItem.category_3), ''),
    )
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': {},
        'groups': await _dimension_label_map(db, DimGroup),
    }
    base = (
        select(
            DimBranch.external_id.label('branch'),
            DimWarehouse.external_id.label('warehouse'),
            DimBrand.external_id.label('brand'),
            category_path_expr.label('category'),
            DimGroup.external_id.label('group'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    base = _apply_inventory_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    ).subquery('inv_dims')

    async def _distinct(col_name: str) -> list[str]:
        col = getattr(base.c, col_name)
        rows = (await db.execute(select(col).where(col.is_not(None)).distinct().order_by(col))).scalars().all()
        return [str(x) for x in rows if x]

    categories_out = await _distinct('category')
    labels['categories'] = {value: value for value in categories_out}

    return {
        'branches': await _distinct('branch'),
        'warehouses': await _distinct('warehouse'),
        'brands': await _distinct('brand'),
        'categories': categories_out,
        'groups': await _distinct('group'),
        'labels': labels,
    }


async def cashflow_by_entry_type(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        agg_has_rows = (await db.execute(select(AggCashDaily.doc_date).limit(1))).first() is not None
        if not agg_has_rows:
            return []
        stmt = (
            select(
                AggCashDaily.subcategory,
                func.coalesce(func.sum(AggCashDaily.entries), 0).label('entries'),
                func.coalesce(func.sum(AggCashDaily.inflows), 0).label('inflows'),
                func.coalesce(func.sum(AggCashDaily.outflows), 0).label('outflows'),
                func.coalesce(func.sum(AggCashDaily.net_amount), 0).label('net_amount'),
            )
            .select_from(AggCashDaily)
            .where(*_date_range(AggCashDaily.doc_date, date_from, date_to))
            .where(AggCashDaily.branch_ext_id.in_(branches))
            .group_by(AggCashDaily.subcategory)
            .order_by(func.sum(AggCashDaily.net_amount).desc())
        )
    else:
        agg_has_rows = (await db.execute(select(AggCashByType.doc_date).limit(1))).first() is not None
        if not agg_has_rows:
            return []
        stmt = (
            select(
                AggCashByType.subcategory,
                func.coalesce(func.sum(AggCashByType.entries), 0).label('entries'),
                func.coalesce(func.sum(AggCashByType.inflows), 0).label('inflows'),
                func.coalesce(func.sum(AggCashByType.outflows), 0).label('outflows'),
                func.coalesce(func.sum(AggCashByType.net_amount), 0).label('net_amount'),
            )
            .select_from(AggCashByType)
            .where(*_date_range(AggCashByType.doc_date, date_from, date_to))
            .group_by(AggCashByType.subcategory)
            .order_by(func.sum(AggCashByType.net_amount).desc())
        )

    rows = (await db.execute(stmt)).all()
    return [
        {
            'entry_type': str(r[0] or 'unknown'),
            'entries': int(r[1] or 0),
            'inflows': float(r[2] or 0),
            'outflows': float(r[3] or 0),
            'net': float(r[4] or 0),
        }
        for r in rows
    ]


async def cashflow_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    agg_has_rows = (await db.execute(select(AggCashDaily.doc_date).limit(1))).first() is not None
    if not agg_has_rows:
        return []

    base = (
        select(
            func.date_trunc('month', cast(AggCashDaily.doc_date, Date)).label('month'),
            AggCashDaily.inflows.label('inflow_amount'),
            AggCashDaily.outflows.label('outflow_amount'),
        )
        .select_from(AggCashDaily)
        .where(*_date_range(AggCashDaily.doc_date, date_from, date_to))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        base = base.where(AggCashDaily.branch_ext_id.in_(branches))
    base = base.subquery('cashflow_monthly_base')

    stmt = (
        select(
            base.c.month,
            func.coalesce(func.sum(base.c.inflow_amount), 0).label('inflows'),
            func.coalesce(func.sum(base.c.outflow_amount), 0).label('outflows'),
        )
        .group_by(base.c.month)
        .order_by(base.c.month)
    )
    rows = (await db.execute(stmt)).all()
    out = []
    for r in rows:
        inflows = float(r[1] or 0)
        outflows = float(r[2] or 0)
        out.append(
            {
                'month': str(r[0].date() if hasattr(r[0], 'date') else r[0]),
                'inflows': inflows,
                'outflows': outflows,
                'net': inflows - outflows,
            }
        )
    return out


async def executive_dashboard_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    insights_limit: int = 12,
) -> dict:
    # Do not silently move the dashboard to the last day that has sales.
    # The day KPI must show the requested anchor date (today by default);
    # otherwise a delayed sync is hidden and users see "yesterday" as "today".
    anchor_date = date_to
    # "Τζίρος Ημέρας" is intentionally independent from broad date ranges:
    # - default/open dashboard: current day
    # - explicit one-day filter (from == to): that selected day
    # - any multi-day range: current day, while period KPIs still use the range
    day_anchor_date = date_from if date_from == date_to else date.today()
    day_from = day_anchor_date
    week_from = _start_of_week(anchor_date)
    month_from = _start_of_month(anchor_date)
    year_from = _start_of_year(anchor_date)
    current_year = anchor_date.year
    prev1_year = current_year - 1
    prev2_year = current_year - 2
    prev_ytd_from = date(prev1_year, 1, 1)
    prev_ytd_to = _safe_same_day(prev1_year, anchor_date.month, anchor_date.day)
    prev_year_full_from = date(prev1_year, 1, 1)
    prev_year_full_to = date(prev1_year, 12, 31)

    prev_month_date = month_from - timedelta(days=1)
    prev_month_from = prev_month_date.replace(day=1)
    prev_month_to = _safe_same_day(prev_month_date.year, prev_month_date.month, anchor_date.day)

    sales_windows = {
        'day': (day_from, day_anchor_date),
        'week': (week_from, anchor_date),
        'month': (month_from, anchor_date),
        'year': (year_from, anchor_date),
        'prev_year': (prev_ytd_from, prev_ytd_to),
        'prev_year_full': (prev_year_full_from, prev_year_full_to),
        'period_sales': (date_from, date_to),
    }
    sales_windows_data = await _sales_summaries_by_windows(
        db,
        windows=sales_windows,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    day = sales_windows_data.get('day', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    week = sales_windows_data.get('week', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    month = sales_windows_data.get('month', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    year = sales_windows_data.get('year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    prev_year = sales_windows_data.get('prev_year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    prev_year_full = sales_windows_data.get(
        'prev_year_full', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0}
    )
    period_sales = sales_windows_data.get('period_sales', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})

    branch_windows = await _sales_by_branch_windows(
        db,
        windows={
            'day': (day_from, day_anchor_date),
            'month': (month_from, anchor_date),
            'year': (year_from, anchor_date),
            'prev_month': (prev_month_from, prev_month_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    day_by_branch = branch_windows.get('day', [])
    month_by_branch = branch_windows.get('month', [])
    year_by_branch = branch_windows.get('year', [])
    prev_month_by_branch = branch_windows.get('prev_month', [])

    # Company trend must always represent company-wide monthly totals
    # (sum of all branches), independent from detail-dimension filters.
    trend_all = await sales_monthly_company_totals(
        db,
        date_from=date(prev2_year, 1, 1),
        date_to=date(current_year, 12, 31),
    )
    trend_by_year = {current_year: [], prev1_year: [], prev2_year: []}
    for row in trend_all:
        month_start = str(row.get('month_start') or '')
        try:
            row_year = int(month_start[:4])
        except (TypeError, ValueError):
            continue
        if row_year in trend_by_year:
            trend_by_year[row_year].append(row)
    trend_y0 = trend_by_year[current_year]
    trend_y1 = trend_by_year[prev1_year]
    trend_y2 = trend_by_year[prev2_year]

    purchase_windows_data = await _purchases_summaries_by_windows(
        db,
        windows={
            'purchases_period': (date_from, date_to),
            'purchases_year': (year_from, anchor_date),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    purchases_period = purchase_windows_data.get(
        'purchases_period', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_year = purchase_windows_data.get(
        'purchases_year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    key_alerts = await list_recent_insights(db, limit=max(1, min(int(insights_limit), 20)), statuses=['open'])
    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
        'anchors': {
            'day_from': day_from.isoformat(),
            'week_from': week_from.isoformat(),
            'month_from': month_from.isoformat(),
            'year_from': year_from.isoformat(),
            'prev_ytd_from': prev_ytd_from.isoformat(),
            'prev_ytd_to': prev_ytd_to.isoformat(),
            'prev_year_full_from': prev_year_full_from.isoformat(),
            'prev_year_full_to': prev_year_full_to.isoformat(),
            'prev_month_from': prev_month_from.isoformat(),
            'prev_month_to': prev_month_to.isoformat(),
            'current_year': current_year,
            'prev1_year': prev1_year,
            'prev2_year': prev2_year,
        },
        'cards': {
            'day': day,
            'week': week,
            'month': month,
            'year': year,
            'prev_year': prev_year,
            'prev_year_full': prev_year_full,
            'period_sales': period_sales,
            'purchases_period': purchases_period,
            'purchases_year': purchases_year,
        },
        'branch_breakdown': {
            'day': day_by_branch,
            'month': month_by_branch,
            'year': year_by_branch,
            'prev_month': prev_month_by_branch,
        },
        'trend': {
            'y0': {'year': current_year, 'rows': trend_y0},
            'y1': {'year': prev1_year, 'rows': trend_y1},
            'y2': {'year': prev2_year, 'rows': trend_y2},
        },
        'key_alerts': key_alerts,
    }


async def finance_dashboard_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    supplier_limit: int = 50,
    account_limit: int = 50,
) -> dict:
    current_map = await _latest_customer_balances_map(
        db,
        as_of=date_to,
        branches=branches,
        aggregate_only=True,
    )
    previous_to = date_from - timedelta(days=1)
    previous_map = await _latest_customer_balances_map(
        db,
        as_of=previous_to,
        branches=branches,
        aggregate_only=True,
    )
    current_open = float(sum(float(item.get('open_balance') or 0) for item in current_map.values()))
    current_overdue = float(sum(float(item.get('overdue_balance') or 0) for item in current_map.values()))
    previous_open = float(sum(float(item.get('open_balance') or 0) for item in previous_map.values()))
    growth_value = current_open - previous_open
    growth_pct = ((growth_value / previous_open) * 100.0) if previous_open > 0 else None
    overdue_ratio_pct = ((current_overdue / current_open) * 100.0) if current_open > 0 else 0.0
    bucket_0_30 = float(sum(float(item.get('aging_bucket_0_30') or 0) for item in current_map.values()))
    bucket_31_60 = float(sum(float(item.get('aging_bucket_31_60') or 0) for item in current_map.values()))
    bucket_61_90 = float(sum(float(item.get('aging_bucket_61_90') or 0) for item in current_map.values()))
    bucket_90_plus = float(sum(float(item.get('aging_bucket_90_plus') or 0) for item in current_map.values()))
    aging_total = bucket_0_30 + bucket_31_60 + bucket_61_90 + bucket_90_plus
    top_customer_id = ''
    top_customer_name = ''
    top_customer_balance = 0.0
    for customer_id, snapshot in current_map.items():
        balance = float(snapshot.get('open_balance') or 0)
        if balance <= top_customer_balance:
            continue
        top_customer_id = customer_id
        top_customer_name = str(snapshot.get('customer_name') or customer_id)
        top_customer_balance = balance

    trend_stmt = (
        select(
            AggCustomerBalancesDaily.balance_date.label('balance_date'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
            func.max(AggCustomerBalancesDaily.updated_at).label('updated_at'),
        )
        .select_from(AggCustomerBalancesDaily)
        .where(*_date_range(AggCustomerBalancesDaily.balance_date, date_from, date_to))
        .group_by(AggCustomerBalancesDaily.balance_date)
        .order_by(AggCustomerBalancesDaily.balance_date.asc())
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        trend_stmt = trend_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
    trend_rows_raw = (await db.execute(trend_stmt)).mappings().all()
    trend_rows: list[dict] = []
    total_collections = 0.0
    total_new_outstanding = 0.0
    for row in trend_rows_raw:
        trend_val = float(row.get('trend_vs_previous') or 0)
        estimated_collections = abs(min(trend_val, 0.0))
        new_outstanding = max(trend_val, 0.0)
        total_collections += estimated_collections
        total_new_outstanding += new_outstanding
        trend_rows.append(
            {
                'balance_date': _raw_scalar(row.get('balance_date')),
                'open_balance': float(row.get('open_balance') or 0),
                'overdue_balance': float(row.get('overdue_balance') or 0),
                'trend_vs_previous': trend_val,
                'estimated_collections': estimated_collections,
                'new_outstanding': new_outstanding,
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    suppliers = await suppliers_overview(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        limit=max(1, min(int(supplier_limit), 250)),
        aggregate_only=True,
    )
    cash_summary = await cashflow_summary(db, date_from=date_from, date_to=date_to, branches=branches)
    cash_types = await cashflow_by_entry_type(db, date_from=date_from, date_to=date_to, branches=branches)
    cash_accounts = await cashflow_accounts_overview(
        db,
        as_of=date_to,
        branches=branches,
        limit=max(1, min(int(account_limit), 250)),
        aggregate_only=True,
    )

    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
        'receivables_summary': {
            'as_of': date_to.isoformat(),
            'summary': {
                'customers': int(len(current_map)),
                'total_receivables': current_open,
                'overdue_receivables': current_overdue,
                'overdue_ratio_pct': overdue_ratio_pct,
            },
            'growth_vs_previous': {
                'previous_as_of': previous_to.isoformat(),
                'previous_open_balance': previous_open,
                'value': growth_value,
                'pct': growth_pct,
            },
            'top_customer_exposure': {
                'customer_id': top_customer_id,
                'customer_name': top_customer_name,
                'open_balance': top_customer_balance,
                'share_pct': ((top_customer_balance / current_open) * 100.0) if current_open > 0 else 0.0,
            },
        },
        'receivables_aging': {
            'as_of': date_to.isoformat(),
            'total_receivables': float(aging_total),
            'aging': {
                'aging_bucket_0_30': bucket_0_30,
                'aging_bucket_31_60': bucket_31_60,
                'aging_bucket_61_90': bucket_61_90,
                'aging_bucket_90_plus': bucket_90_plus,
            },
            'shares_pct': {
                'aging_bucket_0_30': (bucket_0_30 / aging_total * 100.0) if aging_total > 0 else 0.0,
                'aging_bucket_31_60': (bucket_31_60 / aging_total * 100.0) if aging_total > 0 else 0.0,
                'aging_bucket_61_90': (bucket_61_90 / aging_total * 100.0) if aging_total > 0 else 0.0,
                'aging_bucket_90_plus': (bucket_90_plus / aging_total * 100.0) if aging_total > 0 else 0.0,
            },
        },
        'receivables_trend': {
            'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
            'summary': {
                'estimated_collections': float(total_collections),
                'new_outstanding': float(total_new_outstanding),
                'net_delta': float(total_new_outstanding - total_collections),
            },
            'rows': trend_rows,
        },
        'suppliers': suppliers,
        'cash_summary': cash_summary,
        'cash_types': cash_types,
        'cash_accounts': cash_accounts,
    }


async def expenses_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
) -> dict:
    try:
        has_aggregate_rows = (
            await db.execute(
                select(func.count())
                .select_from(AggExpensesDaily)
                .where(*_date_range(AggExpensesDaily.expense_date, date_from, date_to))
            )
        ).scalar_one() or 0

        stmt = (
            select(
                func.coalesce(func.sum(FactExpense.amount_net), 0).label('amount_net'),
                func.coalesce(func.sum(FactExpense.amount_tax), 0).label('amount_tax'),
                func.coalesce(func.sum(FactExpense.amount_gross), 0).label('amount_gross'),
                func.count(FactExpense.id).label('entries'),
            )
            .where(*_date_range(FactExpense.expense_date, date_from, date_to))
        )
        if has_aggregate_rows:
            stmt = (
                select(
                    func.coalesce(func.sum(AggExpensesDaily.amount_net), 0).label('amount_net'),
                    func.coalesce(func.sum(AggExpensesDaily.amount_tax), 0).label('amount_tax'),
                    func.coalesce(func.sum(AggExpensesDaily.amount_gross), 0).label('amount_gross'),
                    func.coalesce(func.sum(AggExpensesDaily.entries), 0).label('entries'),
                )
                .where(*_date_range(AggExpensesDaily.expense_date, date_from, date_to))
            )
            stmt = _apply_expense_filters(stmt, branches=branches, categories=categories)
        else:
            stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
        row = (await db.execute(stmt)).mappings().first() or {}
        total_expenses = abs(float(row.get('amount_net') or 0))
        total_tax = abs(float(row.get('amount_tax') or 0))
        total_gross = abs(float(row.get('amount_gross') or 0))
        entries = int(row.get('entries') or 0)

        sales_stmt = (
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
        )
        branches = _effective_branch_filter(branches)
        if branches is not None:
            sales_stmt = sales_stmt.where(AggSalesDaily.branch_ext_id.in_(branches))
        total_revenue = float((await db.execute(sales_stmt)).scalar_one() or 0)
        expense_ratio_to_revenue_pct = ((total_expenses / total_revenue) * 100.0) if total_revenue > 0 else 0.0
        return {
            'total_expenses': total_expenses,
            'total_tax': total_tax,
            'total_gross': total_gross,
            'entries': entries,
            'total_revenue': total_revenue,
            'expense_ratio_to_revenue_pct': expense_ratio_to_revenue_pct,
        }
    except Exception:
        return {
            'total_expenses': 0.0,
            'total_tax': 0.0,
            'total_gross': 0.0,
            'entries': 0,
            'total_revenue': 0.0,
            'expense_ratio_to_revenue_pct': 0.0,
        }


async def expenses_by_category(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
    limit: int = 20,
) -> list[dict]:
    try:
        use_daily = bool(branches)
        if not use_daily:
            try:
                has_rows = (
                    await db.execute(
                        select(AggExpensesByCategoryDaily.expense_date)
                        .where(*_date_range(AggExpensesByCategoryDaily.expense_date, date_from, date_to))
                        .limit(1)
                    )
                ).first() is not None
                use_daily = not has_rows
            except Exception:
                use_daily = True

        if use_daily:
            stmt = (
                select(
                    FactExpense.expense_category_code.label('category_code'),
                    func.coalesce(func.sum(FactExpense.amount_net), 0).label('amount_net'),
                    func.count(FactExpense.id).label('entries'),
                )
                .where(*_date_range(FactExpense.expense_date, date_from, date_to))
            )
            stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
            stmt = stmt.group_by(FactExpense.expense_category_code)
        else:
            stmt = (
                select(
                    AggExpensesByCategoryDaily.expense_category_code.label('category_code'),
                    func.coalesce(func.sum(AggExpensesByCategoryDaily.amount_net), 0).label('amount_net'),
                    func.coalesce(func.sum(AggExpensesByCategoryDaily.entries), 0).label('entries'),
                )
                .where(*_date_range(AggExpensesByCategoryDaily.expense_date, date_from, date_to))
            )
            if categories:
                stmt = stmt.where(AggExpensesByCategoryDaily.expense_category_code.in_(categories))
            stmt = stmt.group_by(AggExpensesByCategoryDaily.expense_category_code)

        stmt = stmt.order_by(literal_column('amount_net').desc()).limit(max(1, min(int(limit), 100)))
        rows = (await db.execute(stmt)).mappings().all()

        category_codes = [str(row.get('category_code') or '').strip() for row in rows if str(row.get('category_code') or '').strip()]
        name_map: dict[str, str] = {}
        if category_codes:
            dim_rows = (
                await db.execute(
                    select(DimExpenseCategory.category_code, DimExpenseCategory.category_name).where(
                        DimExpenseCategory.category_code.in_(category_codes)
                    )
                )
            ).all()
            name_map = {str(code): str(name or code) for code, name in dim_rows}

        total = sum(abs(float(row.get('amount_net') or 0)) for row in rows)
        out: list[dict] = []
        for row in rows:
            code = str(row.get('category_code') or '').strip() or '-'
            amount_net = abs(float(row.get('amount_net') or 0))
            out.append(
                {
                    'category_code': code if code != '-' else None,
                    'category_name': name_map.get(code, code if code != '-' else 'N/A'),
                    'amount_net': amount_net,
                    'entries': int(row.get('entries') or 0),
                    'share_pct': (amount_net / total * 100.0) if total > 0 else 0.0,
                }
            )
        return out
    except Exception:
        return []


async def expenses_by_branch(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
    limit: int = 20,
) -> list[dict]:
    try:
        use_daily = bool(categories)
        if not use_daily:
            try:
                has_rows = (
                    await db.execute(
                        select(AggExpensesByBranchDaily.expense_date)
                        .where(*_date_range(AggExpensesByBranchDaily.expense_date, date_from, date_to))
                        .limit(1)
                    )
                ).first() is not None
                use_daily = not has_rows
            except Exception:
                use_daily = True

        if use_daily:
            stmt = (
                select(
                    FactExpense.branch_ext_id.label('branch_ext_id'),
                    func.coalesce(func.sum(FactExpense.amount_net), 0).label('amount_net'),
                    func.count(FactExpense.id).label('entries'),
                )
                .where(*_date_range(FactExpense.expense_date, date_from, date_to))
            )
            stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
            stmt = stmt.group_by(FactExpense.branch_ext_id)
        else:
            stmt = (
                select(
                    AggExpensesByBranchDaily.branch_ext_id.label('branch_ext_id'),
                    func.coalesce(func.sum(AggExpensesByBranchDaily.amount_net), 0).label('amount_net'),
                    func.coalesce(func.sum(AggExpensesByBranchDaily.entries), 0).label('entries'),
                )
                .where(*_date_range(AggExpensesByBranchDaily.expense_date, date_from, date_to))
            )
            branches = _effective_branch_filter(branches)
            if branches is not None:
                stmt = stmt.where(AggExpensesByBranchDaily.branch_ext_id.in_(branches))
            stmt = stmt.group_by(AggExpensesByBranchDaily.branch_ext_id)

        stmt = stmt.order_by(literal_column('amount_net').desc()).limit(max(1, min(int(limit), 100)))
        rows = (await db.execute(stmt)).mappings().all()
        branch_ids = [str(row.get('branch_ext_id') or '').strip() for row in rows if str(row.get('branch_ext_id') or '').strip()]
        branch_name_map: dict[str, str] = {}
        if branch_ids:
            branch_name_rows = (
                await db.execute(select(DimBranch.external_id, DimBranch.name).where(DimBranch.external_id.in_(branch_ids)))
            ).all()
            branch_name_map = {str(ext_id): str(name or ext_id) for ext_id, name in branch_name_rows}

        out: list[dict] = []
        for row in rows:
            ext_id = str(row.get('branch_ext_id') or '').strip()
            out.append(
                {
                    'branch_ext_id': ext_id or None,
                    'branch_name': branch_name_map.get(ext_id, ext_id or 'N/A'),
                    'amount_net': abs(float(row.get('amount_net') or 0)),
                    'entries': int(row.get('entries') or 0),
                }
            )
        return out
    except Exception:
        return []


async def expenses_trend(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
    limit: int = 60,
) -> list[dict]:
    try:
        stmt = (
            select(
                FactExpense.expense_date.label('expense_date'),
                func.coalesce(func.sum(FactExpense.amount_net), 0).label('amount_net'),
                func.coalesce(func.sum(FactExpense.amount_gross), 0).label('amount_gross'),
                func.count(FactExpense.id).label('entries'),
            )
            .where(*_date_range(FactExpense.expense_date, date_from, date_to))
        )
        stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
        stmt = (
            stmt.group_by(FactExpense.expense_date)
            .order_by(FactExpense.expense_date.asc())
            .limit(max(1, min(int(limit), 365)))
        )
        rows = (await db.execute(stmt)).mappings().all()
        return [
            {
                'date': _raw_scalar(row.get('expense_date')),
                'amount_net': abs(float(row.get('amount_net') or 0)),
                'amount_gross': abs(float(row.get('amount_gross') or 0)),
                'entries': int(row.get('entries') or 0),
            }
            for row in rows
        ]
    except Exception:
        return []


async def stream_expenses_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    categories: list[str] | None = None,
) -> dict:
    summary = await expenses_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        categories=categories,
    )
    by_category = await expenses_by_category(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        categories=categories,
        limit=20,
    )
    by_branch = await expenses_by_branch(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        categories=categories,
        limit=20,
    )
    trend = await expenses_trend(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        categories=categories,
        limit=90,
    )
    return {'summary': summary, 'by_category': by_category, 'by_branch': by_branch, 'trend': trend}


async def stream_sales_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    summary = await sales_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_branch = await sales_by_branch(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    trend = await sales_monthly_trend_from_monthly_agg(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return {'summary': summary, 'by_branch': by_branch, 'trend': trend}


async def stream_purchases_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    summary = await purchases_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_supplier = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    trend = await purchases_monthly_trend_from_monthly_agg(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return {'summary': summary, 'by_supplier': by_supplier, 'trend': trend}


async def stream_inventory_summary(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    return await inventory_summary_bundle_from_current_state(
        db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=12,
    )


async def stream_cash_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> dict:
    summary = await cashflow_summary(db, date_from=date_from, date_to=date_to, branches=branches)
    by_type = await cashflow_by_entry_type(db, date_from=date_from, date_to=date_to, branches=branches)
    trend = await cashflow_monthly_trend(db, date_from=date_from, date_to=date_to, branches=branches)
    return {'summary': summary, 'by_type': by_type, 'trend': trend}


async def stream_balances_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> dict:
    receivables = await receivables_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        aggregate_only=True,
    )
    suppliers = await suppliers_overview(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        limit=20,
        aggregate_only=True,
    )
    return {
        'receivables': receivables,
        'supplier_balances': suppliers.get('summary', {}),
        'top_suppliers': (suppliers.get('rows') or [])[:10],
    }


async def sales_seasonality(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    season_expr = _season_case(FactSales.doc_date).label('season')
    stmt = (
        select(
            season_expr,
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(season_expr)
    rows = (await db.execute(stmt)).all()
    order = ['winter', 'spring', 'summer', 'autumn']
    m = {str(r[0]): {'season': str(r[0]), 'net_value': float(r[1] or 0), 'qty': float(r[2] or 0)} for r in rows}
    out = [m.get(s, {'season': s, 'net_value': 0.0, 'qty': 0.0}) for s in order]
    total = sum(x['net_value'] for x in out)
    for x in out:
        x['share_pct'] = (x['net_value'] / total * 100.0) if total > 0 else 0.0
    return out


async def purchases_seasonality(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    season_expr = _season_case(FactPurchases.doc_date).label('season')
    stmt = (
        select(
            season_expr,
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_purchases_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.group_by(season_expr)
    rows = (await db.execute(stmt)).all()
    order = ['winter', 'spring', 'summer', 'autumn']
    m = {str(r[0]): {'season': str(r[0]), 'net_value': float(r[1] or 0), 'qty': float(r[2] or 0)} for r in rows}
    out = [m.get(s, {'season': s, 'net_value': 0.0, 'qty': 0.0}) for s in order]
    total = sum(x['net_value'] for x in out)
    for x in out:
        x['share_pct'] = (x['net_value'] / total * 100.0) if total > 0 else 0.0
    return out


async def new_item_codes_activity(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    first_inventory = (
        select(
            DimItem.external_id.label('item_code'),
            func.min(FactInventory.doc_date).label('first_seen'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .group_by(DimItem.external_id)
        .having(func.min(FactInventory.doc_date) >= date_from)
        .having(func.min(FactInventory.doc_date) <= date_to)
        .subquery('first_inventory')
    )

    sales_stmt = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    sales_stmt = _apply_fact_sales_filters(
        sales_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    sales_stmt = sales_stmt.group_by(FactSales.item_code).subquery('sales_stmt')

    purchases_stmt = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    purchases_stmt = _apply_fact_purchases_filters(
        purchases_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code).subquery('purchases_stmt')

    inv_latest_date = (await db.execute(select(func.max(FactInventory.doc_date)))).scalar_one_or_none()
    inv_latest = (
        select(
            DimItem.external_id.label('item_code'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .where(FactInventory.doc_date == inv_latest_date if inv_latest_date is not None else literal(False))
        .group_by(DimItem.external_id)
        .subquery('inv_latest')
    )

    stmt = (
        select(
            first_inventory.c.item_code,
            first_inventory.c.first_seen,
            func.coalesce(func.max(DimItem.name), first_inventory.c.item_code).label('item_name'),
            func.coalesce(sales_stmt.c.sales_qty, 0).label('sales_qty'),
            func.coalesce(sales_stmt.c.sales_value, 0).label('sales_value'),
            func.coalesce(purchases_stmt.c.purchases_qty, 0).label('purchases_qty'),
            func.coalesce(purchases_stmt.c.purchases_value, 0).label('purchases_value'),
            func.coalesce(inv_latest.c.qty_on_hand, 0).label('qty_on_hand'),
        )
        .select_from(first_inventory)
        .join(DimItem, DimItem.external_id == first_inventory.c.item_code, isouter=True)
        .join(sales_stmt, sales_stmt.c.item_code == first_inventory.c.item_code, isouter=True)
        .join(purchases_stmt, purchases_stmt.c.item_code == first_inventory.c.item_code, isouter=True)
        .join(inv_latest, inv_latest.c.item_code == first_inventory.c.item_code, isouter=True)
        .group_by(
            first_inventory.c.item_code,
            first_inventory.c.first_seen,
            sales_stmt.c.sales_qty,
            sales_stmt.c.sales_value,
            purchases_stmt.c.purchases_qty,
            purchases_stmt.c.purchases_value,
            inv_latest.c.qty_on_hand,
        )
        .order_by(first_inventory.c.first_seen.desc(), func.coalesce(sales_stmt.c.sales_value, 0).desc())
        .limit(max(1, min(limit, 50)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'item_code': str(r[0] or 'N/A'),
            'first_seen': str(r[1]) if r[1] else None,
            'item_name': _clean_item_name(r[2], r[0] or 'N/A'),
            'sales_qty': float(r[3] or 0),
            'sales_value': float(r[4] or 0),
            'purchases_qty': float(r[5] or 0),
            'purchases_value': float(r[6] or 0),
            'qty_on_hand': float(r[7] or 0),
        }
        for r in rows
    ]


async def inventory_items_overview(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    status: str = 'all',
    movement: str = 'all',
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
    classification_config: dict | None = None,
):
    resolved_classification = normalize_inventory_item_classification_config(classification_config)
    latest_date_stmt = select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of)
    latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
    if latest_date is None:
        return {'snapshot_date': None, 'summary': {}, 'rows': []}

    latest_inventory_rows = (
        select(
            FactInventory.id.label('fact_id'),
            FactInventory.branch_id.label('branch_id'),
            FactInventory.branch_ext_id.label('branch_ext_id'),
            FactInventory.warehouse_id.label('warehouse_id'),
            FactInventory.item_id.label('item_id'),
            FactInventory.item_code.label('item_code'),
            FactInventory.source_payload_json['item_name'].astext.label('payload_item_name'),
            FactInventory.source_payload_json['barcode'].astext.label('payload_barcode'),
            FactInventory.source_payload_json['alternate_barcodes'].astext.label('payload_alternate_barcodes'),
            FactInventory.source_payload_json['brand_name'].astext.label('payload_brand_name'),
            FactInventory.source_payload_json['manufacturer_code'].astext.label('payload_manufacturer_code'),
            FactInventory.source_payload_json['manufacturer_name'].astext.label('payload_manufacturer_name'),
            FactInventory.source_payload_json['group_name'].astext.label('payload_group_name'),
            FactInventory.source_payload_json['commercial_category'].astext.label('payload_commercial_category'),
            FactInventory.source_payload_json['category_1'].astext.label('payload_category_1'),
            FactInventory.source_payload_json['category_2'].astext.label('payload_category_2'),
            FactInventory.source_payload_json['category_3'].astext.label('payload_category_3'),
            FactInventory.source_payload_json['is_active'].astext.label('payload_is_active'),
            FactInventory.source_payload_json['is_active_source'].astext.label('payload_is_active_source'),
            FactInventory.qty_on_hand.label('qty_on_hand'),
            FactInventory.value_amount.label('value_amount'),
            func.row_number()
            .over(
                partition_by=(
                    FactInventory.branch_id,
                    FactInventory.warehouse_id,
                    func.coalesce(cast(FactInventory.item_id, String), FactInventory.item_code, literal('')),
                ),
                order_by=(FactInventory.doc_date.desc(), FactInventory.updated_at.desc(), FactInventory.id.desc()),
            )
            .label('rn'),
        )
        .where(FactInventory.doc_date <= as_of)
        .subquery('latest_inventory_rows')
    )
    item_code_expr = func.coalesce(DimItem.external_id, latest_inventory_rows.c.item_code)
    payload_active_raw = func.lower(
        func.trim(
            func.coalesce(
                cast(latest_inventory_rows.c.payload_is_active_source, String),
                cast(latest_inventory_rows.c.payload_is_active, String),
                literal(''),
            )
        )
    )
    payload_active_expr = case(
        (
            payload_active_raw.in_(
                ['1', 'true', 'yes', 'y', 'on', 'ναι']
            ),
            True,
        ),
        (
            payload_active_raw.in_(
                ['0', 'false', 'no', 'n', 'off', 'οχι', 'όχι']
            ),
            False,
        ),
        else_=None,
    )

    inv_base = (
        select(
            item_code_expr.label('item_code'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_item_name), '')),
                item_code_expr,
            ).label('item_name'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.barcode), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_barcode), '')),
                literal(''),
            ).label('barcode'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimBrand.name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_brand_name), '')),
                literal('N/A'),
            ).label('brand'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.manufacturer_name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_manufacturer_name), '')),
                func.max(func.nullif(func.btrim(DimItem.manufacturer_code), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_manufacturer_code), '')),
                literal('N/A'),
            ).label('manufacturer'),
            func.coalesce(func.max(func.nullif(func.btrim(DimCategory.name), '')), literal('N/A')).label('category'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimGroup.name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_group_name), '')),
                literal('N/A'),
            ).label('group_name'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.category_1), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_category_1), '')),
                literal(''),
            ).label('category_1'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.commercial_category), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_commercial_category), '')),
                literal(''),
            ).label('commercial_category'),
            func.coalesce(func.bool_or(DimItem.is_active_source), func.bool_or(payload_active_expr)).label('is_active_source'),
            func.coalesce(func.sum(latest_inventory_rows.c.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(latest_inventory_rows.c.value_amount), 0).label('stock_value'),
        )
        .select_from(latest_inventory_rows)
        .join(DimBranch, latest_inventory_rows.c.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, latest_inventory_rows.c.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, latest_inventory_rows.c.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(latest_inventory_rows.c.rn == 1)
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_inventory_rows.c.branch_ext_id,
    )
    inv_base = inv_base.group_by(item_code_expr).subquery('inv_base')

    sales_30 = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty_30'),
            func.max(FactSales.doc_date).label('last_sale_date'),
        )
        .where(FactSales.doc_date >= (as_of - timedelta(days=30)))
        .where(FactSales.doc_date <= as_of)
    )
    sales_30 = _apply_fact_sales_filters(
        sales_30, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    sales_30 = sales_30.group_by(FactSales.item_code).subquery('sales_30')

    purch_30 = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty_30'),
        )
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=30)))
        .where(FactPurchases.doc_date <= as_of)
    )
    purch_30 = _apply_fact_purchases_filters(
        purch_30,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    purch_30 = purch_30.group_by(FactPurchases.item_code).subquery('purch_30')

    first_seen = (
        select(
            func.coalesce(DimItem.external_id, FactInventory.item_code).label('item_code'),
            func.min(FactInventory.doc_date).label('first_seen'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .group_by(func.coalesce(DimItem.external_id, FactInventory.item_code))
        .subquery('first_seen')
    )

    stmt = (
        select(
            inv_base.c.item_code,
            inv_base.c.item_name,
            inv_base.c.barcode,
            inv_base.c.brand,
            inv_base.c.manufacturer,
            inv_base.c.category,
            inv_base.c.group_name,
            inv_base.c.category_1,
            inv_base.c.commercial_category,
            inv_base.c.qty_on_hand,
            inv_base.c.stock_value,
            func.coalesce(sales_30.c.sales_qty_30, 0).label('sales_qty_30'),
            sales_30.c.last_sale_date,
            func.coalesce(purch_30.c.purchases_qty_30, 0).label('purchases_qty_30'),
            first_seen.c.first_seen,
            inv_base.c.is_active_source,
        )
        .select_from(inv_base)
        .join(sales_30, sales_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(purch_30, purch_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(first_seen, first_seen.c.item_code == inv_base.c.item_code, isouter=True)
    )

    q_clean = (q or '').strip().lower()
    if q_clean:
        stmt = stmt.where(
            func.lower(cast(inv_base.c.item_name, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.item_code, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.barcode, String)).like(f'%{q_clean}%')
        )

    rows = (await db.execute(stmt)).all()
    mapped = []
    for r in rows:
        sales_qty_30 = float(r[11] or 0)
        last_sale_date = r[12]
        is_active_source = r[15]
        category_value = str(r[5] or 'N/A')
        if category_value == 'N/A':
            category_value = str(r[7] or r[8] or 'N/A')
        status_value, movement_level = _classify_inventory_item(
            as_of=as_of,
            last_sale_date=last_sale_date,
            sales_qty_30=sales_qty_30,
            config=resolved_classification,
            is_active_source=(bool(is_active_source) if is_active_source is not None else None),
        )
        mapped.append(
            {
                'item_code': str(r[0] or 'N/A'),
                'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
                'barcode': str(r[2] or ''),
                'brand': str(r[3] or 'N/A'),
                'manufacturer': str(r[4] or 'N/A'),
                'category': category_value,
                'group': str(r[6] or 'N/A'),
                'qty_on_hand': float(r[9] or 0),
                'stock_value': float(r[10] or 0),
                'sales_qty_30': sales_qty_30,
                'last_sale_date': str(last_sale_date) if last_sale_date else None,
                'purchases_qty_30': float(r[13] or 0),
                'first_seen': str(r[14]) if r[14] else None,
                'is_active_source': (bool(is_active_source) if is_active_source is not None else None),
                'status': status_value,
                'movement': movement_level,
            }
        )

    if status in {'active', 'inactive'}:
        mapped = [x for x in mapped if x['status'] == status]
    if movement in {'fast', 'slow', 'normal'}:
        mapped = [x for x in mapped if x['movement'] == movement]

    # Summary cards must reflect active filters (status/movement/search/etc.),
    # not the unfiltered snapshot.
    summary_source = list(mapped)

    mapped.sort(key=lambda x: (x['stock_value'], x['qty_on_hand']), reverse=True)
    safe_limit = max(1, min(int(limit), 500))
    safe_offset = max(0, int(offset))
    total = len(mapped)
    mapped = mapped[safe_offset : safe_offset + safe_limit]

    summary = {
        'total_items': len(summary_source),
        'active_items': sum(1 for x in summary_source if x.get('status') == 'active'),
        'inactive_items': sum(1 for x in summary_source if x.get('status') == 'inactive'),
        'fast_items': sum(1 for x in summary_source if x.get('movement') == 'fast'),
        'slow_items': sum(1 for x in summary_source if x.get('movement') == 'slow'),
        'stock_value': float(sum(float(x.get('stock_value') or 0) for x in summary_source)),
    }
    return {
        'snapshot_date': str(latest_date),
        'classification_config': resolved_classification,
        'summary': summary,
        'rows': mapped,
        'total': total,
        'limit': safe_limit,
        'offset': safe_offset,
    }


async def inventory_item_detail(
    db: AsyncSession,
    item_code: str,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    classification_config: dict | None = None,
):
    resolved_classification = normalize_inventory_item_classification_config(classification_config)
    code = (item_code or '').strip()
    if not code:
        raise ValueError('Missing item code')

    latest_date = (await db.execute(select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of))).scalar_one_or_none()
    if latest_date is None:
        raise ValueError('No inventory snapshot found')

    latest_item_rows = (
        select(
            FactInventory.id.label('fact_id'),
            FactInventory.branch_id.label('branch_id'),
            FactInventory.branch_ext_id.label('branch_ext_id'),
            FactInventory.warehouse_id.label('warehouse_id'),
            FactInventory.item_id.label('item_id'),
            FactInventory.item_code.label('item_code'),
            FactInventory.external_id.label('external_id'),
            FactInventory.qty_on_hand.label('qty_on_hand'),
            FactInventory.qty_reserved.label('qty_reserved'),
            FactInventory.cost_amount.label('cost_amount'),
            FactInventory.value_amount.label('value_amount'),
            FactInventory.source_payload_json.label('source_payload_json'),
            FactInventory.updated_at.label('updated_at'),
            FactInventory.created_at.label('created_at'),
            func.row_number()
            .over(
                partition_by=(
                    FactInventory.branch_id,
                    FactInventory.warehouse_id,
                    func.coalesce(cast(FactInventory.item_id, String), FactInventory.item_code, literal('')),
                ),
                order_by=(FactInventory.doc_date.desc(), FactInventory.updated_at.desc(), FactInventory.id.desc()),
            )
            .label('rn'),
        )
        .where(FactInventory.doc_date <= as_of)
        .subquery('latest_item_rows')
    )
    detail_item_code_expr = func.coalesce(DimItem.external_id, latest_item_rows.c.item_code)

    dim_join = (
        await db.execute(
            select(
                DimItem,
                DimBrand,
                DimCategory,
                DimGroup,
            )
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
            .where(DimItem.external_id == code)
        )
    ).one_or_none()
    dim_item = dim_join[0] if dim_join else None
    dim_brand = dim_join[1] if dim_join else None
    dim_category = dim_join[2] if dim_join else None
    dim_group = dim_join[3] if dim_join else None

    inv_stmt = (
        select(
            func.coalesce(func.sum(latest_item_rows.c.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(latest_item_rows.c.qty_reserved), 0).label('qty_reserved'),
            func.coalesce(func.sum(latest_item_rows.c.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(latest_item_rows.c.value_amount), 0).label('stock_value'),
            func.max(latest_item_rows.c.updated_at).label('inv_updated_at'),
            func.min(latest_item_rows.c.created_at).label('inv_created_at'),
            func.count(latest_item_rows.c.fact_id).label('inv_rows'),
            func.count(func.distinct(latest_item_rows.c.branch_id)).label('branch_count'),
            func.count(func.distinct(latest_item_rows.c.warehouse_id)).label('warehouse_count'),
        )
        .select_from(latest_item_rows)
        .join(DimBranch, latest_item_rows.c.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, latest_item_rows.c.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, latest_item_rows.c.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(latest_item_rows.c.rn == 1)
        .where(detail_item_code_expr == code)
    )
    inv_stmt = _apply_inventory_filters(
        inv_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_item_rows.c.branch_ext_id,
    )
    inv_row = (await db.execute(inv_stmt)).mappings().one()

    inv_detail_stmt = (
        select(
            FactInventory,
            DimBranch.external_id.label('branch_external_id'),
            DimBranch.name.label('branch_name'),
            DimWarehouse.external_id.label('warehouse_external_id'),
            DimWarehouse.name.label('warehouse_name'),
        )
        .select_from(latest_item_rows)
        .join(FactInventory, FactInventory.id == latest_item_rows.c.fact_id)
        .join(DimBranch, latest_item_rows.c.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, latest_item_rows.c.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, latest_item_rows.c.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(latest_item_rows.c.rn == 1)
        .where(detail_item_code_expr == code)
        .order_by(DimBranch.name.asc(), DimWarehouse.name.asc(), FactInventory.external_id.asc())
    )
    inv_detail_stmt = _apply_inventory_filters(
        inv_detail_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_item_rows.c.branch_ext_id,
    )
    inv_detail_rows = (await db.execute(inv_detail_stmt)).all()

    sales_30_stmt = (
        select(
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty_30'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value_30'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value_30'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('sales_cost_30'),
            func.coalesce(func.sum(FactSales.profit_amount), 0).label('sales_profit_30'),
            func.max(FactSales.doc_date).label('last_sale_date'),
            func.count(FactSales.id).label('sales_rows_30'),
        )
        .where(FactSales.item_code == code)
        .where(FactSales.doc_date >= (as_of - timedelta(days=30)))
        .where(FactSales.doc_date <= as_of)
    )
    sales_30_stmt = _apply_fact_sales_filters(
        sales_30_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    sales_30_row = (await db.execute(sales_30_stmt)).mappings().one()

    purch_30_stmt = (
        select(
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty_30'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value_30'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('purchases_cost_30'),
            func.max(FactPurchases.doc_date).label('last_purchase_date'),
            func.count(FactPurchases.id).label('purchase_rows_30'),
        )
        .where(FactPurchases.item_code == code)
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=30)))
        .where(FactPurchases.doc_date <= as_of)
    )
    purch_30_stmt = _apply_fact_purchases_filters(
        purch_30_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    purch_30_row = (await db.execute(purch_30_stmt)).mappings().one()

    first_seen_row = (
        await db.execute(
            select(
                func.min(FactInventory.doc_date).label('first_seen'),
                func.max(FactInventory.doc_date).label('last_inventory_date'),
            )
            .select_from(FactInventory)
            .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
            .where(func.coalesce(DimItem.external_id, FactInventory.item_code) == code)
        )
    ).mappings().one()

    qty_on_hand = float(inv_row['qty_on_hand'] or 0)
    qty_reserved = float(inv_row['qty_reserved'] or 0)
    cost_amount = float(inv_row['cost_amount'] or 0)
    stock_value = float(inv_row['stock_value'] or 0)

    sales_qty_30 = float(sales_30_row['sales_qty_30'] or 0)
    sales_value_30 = float(sales_30_row['sales_value_30'] or 0)
    gross_value_30 = float(sales_30_row['gross_value_30'] or 0)
    sales_cost_30 = float(sales_30_row['sales_cost_30'] or 0)
    sales_profit_30 = float(sales_30_row['sales_profit_30'] or 0)
    last_sale_date = sales_30_row['last_sale_date']
    sales_rows_30 = int(sales_30_row['sales_rows_30'] or 0)

    purchases_qty_30 = float(purch_30_row['purchases_qty_30'] or 0)
    purchases_value_30 = float(purch_30_row['purchases_value_30'] or 0)
    purchases_cost_30 = float(purch_30_row['purchases_cost_30'] or 0)
    last_purchase_date = purch_30_row['last_purchase_date']
    purchase_rows_30 = int(purch_30_row['purchase_rows_30'] or 0)

    first_seen = first_seen_row['first_seen']
    last_inventory_date = first_seen_row['last_inventory_date']

    status_value, movement_level = _classify_inventory_item(
        as_of=as_of,
        last_sale_date=last_sale_date,
        sales_qty_30=sales_qty_30,
        config=resolved_classification,
        is_active_source=None,
    )
    payloads = [getattr(row[0], 'source_payload_json', None) for row in inv_detail_rows if row and row[0] is not None]
    payload_item_name = next(
        (
            _payload_text(
                payload,
                'item_name',
                'name',
                'item_description',
                'description',
                'product_name',
                'mtrl_name',
                'mtrl_description',
            )
            for payload in payloads
            if _payload_text(
                payload,
                'item_name',
                'name',
                'item_description',
                'description',
                'product_name',
                'mtrl_name',
                'mtrl_description',
            )
        ),
        '',
    )
    payload_barcode = next((_payload_text(payload, 'barcode', 'code1') for payload in payloads if _payload_text(payload, 'barcode', 'code1')), '')
    payload_alt_barcodes = next(
        (_payload_text(payload, 'alternate_barcodes', 'alt_barcodes', 'substitute_codes') for payload in payloads if _payload_text(payload, 'alternate_barcodes', 'alt_barcodes', 'substitute_codes')),
        '',
    )
    payload_brand_name = next((_payload_text(payload, 'brand_name') for payload in payloads if _payload_text(payload, 'brand_name')), '')
    payload_manufacturer_code = next(
        (_payload_text(payload, 'manufacturer_code', 'manufacturer_external_id', 'manufacturer_ext_id') for payload in payloads if _payload_text(payload, 'manufacturer_code', 'manufacturer_external_id', 'manufacturer_ext_id')),
        '',
    )
    payload_manufacturer_name = next(
        (_payload_text(payload, 'manufacturer_name', 'manufacturer', 'manufacturer_label') for payload in payloads if _payload_text(payload, 'manufacturer_name', 'manufacturer', 'manufacturer_label')),
        '',
    )
    payload_group_name = next((_payload_text(payload, 'group_name') for payload in payloads if _payload_text(payload, 'group_name')), '')
    payload_category_1 = next((_payload_text(payload, 'category_1') for payload in payloads if _payload_text(payload, 'category_1')), '')
    payload_category_2 = next((_payload_text(payload, 'category_2') for payload in payloads if _payload_text(payload, 'category_2')), '')
    payload_category_3 = next((_payload_text(payload, 'category_3') for payload in payloads if _payload_text(payload, 'category_3')), '')
    payload_commercial_category = next((_payload_text(payload, 'commercial_category') for payload in payloads if _payload_text(payload, 'commercial_category')), '')
    payload_active_source = next(
        (
            _payload_bool(payload, 'is_active_source', 'is_active', 'active', 'enabled')
            for payload in payloads
            if _payload_bool(payload, 'is_active_source', 'is_active', 'active', 'enabled') is not None
        ),
        None,
    )

    effective_active_source = (
        bool(dim_item.is_active_source)
        if dim_item and dim_item.is_active_source is not None
        else payload_active_source
    )
    status_value, movement_level = _classify_inventory_item(
        as_of=as_of,
        last_sale_date=last_sale_date,
        sales_qty_30=sales_qty_30,
        config=resolved_classification,
        is_active_source=effective_active_source,
    )

    item_name = _clean_item_name(dim_item.name if dim_item else None, payload_item_name or code)
    barcode = str((dim_item.barcode if dim_item else None) or payload_barcode or '')
    alternate_barcodes = str((dim_item.alternate_barcodes if dim_item else None) or payload_alt_barcodes or '')
    brand_name = str((dim_brand.name if dim_brand and dim_brand.name else None) or payload_brand_name or 'N/A')
    manufacturer_name = str(
        (dim_item.manufacturer_name if dim_item and dim_item.manufacturer_name else None)
        or payload_manufacturer_name
        or (dim_item.manufacturer_code if dim_item and dim_item.manufacturer_code else None)
        or payload_manufacturer_code
        or 'N/A'
    )
    category_name = str(dim_category.name or 'N/A') if dim_category and dim_category.name else 'N/A'
    if category_name == 'N/A':
        category_name = payload_category_1 or payload_commercial_category or 'N/A'
    group_name = str((dim_group.name if dim_group and dim_group.name else None) or payload_group_name or 'N/A')

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'dim_items', dim_item)
    if dim_item is None:
        _append_raw_field(raw_fields, 'dim_items.external_id', code)
    _append_model_raw_fields(raw_fields, 'dim_brands', dim_brand)
    _append_model_raw_fields(raw_fields, 'dim_categories', dim_category)
    _append_model_raw_fields(raw_fields, 'dim_groups', dim_group)
    if payload_item_name:
        _append_raw_field(raw_fields, 'inventory.payload_item_name', payload_item_name)
    if payload_barcode:
        _append_raw_field(raw_fields, 'inventory.payload_barcode', payload_barcode)
    if alternate_barcodes:
        _append_raw_field(raw_fields, 'inventory.alternate_barcodes', alternate_barcodes)
    if payload_brand_name:
        _append_raw_field(raw_fields, 'inventory.payload_brand_name', payload_brand_name)
    if payload_manufacturer_code:
        _append_raw_field(raw_fields, 'inventory.payload_manufacturer_code', payload_manufacturer_code)
    if payload_manufacturer_name:
        _append_raw_field(raw_fields, 'inventory.payload_manufacturer_name', payload_manufacturer_name)
    if payload_group_name:
        _append_raw_field(raw_fields, 'inventory.payload_group_name', payload_group_name)
    if payload_commercial_category:
        _append_raw_field(raw_fields, 'inventory.payload_commercial_category', payload_commercial_category)
    if payload_category_1:
        _append_raw_field(raw_fields, 'inventory.payload_category_1', payload_category_1)
    if payload_category_2:
        _append_raw_field(raw_fields, 'inventory.payload_category_2', payload_category_2)
    if payload_category_3:
        _append_raw_field(raw_fields, 'inventory.payload_category_3', payload_category_3)

    _append_raw_field(raw_fields, 'inventory.snapshot_date', latest_date)
    _append_raw_field(raw_fields, 'inventory.rows_in_snapshot', int(inv_row['inv_rows'] or 0))
    _append_raw_field(raw_fields, 'inventory.branch_count', int(inv_row['branch_count'] or 0))
    _append_raw_field(raw_fields, 'inventory.warehouse_count', int(inv_row['warehouse_count'] or 0))
    _append_raw_field(raw_fields, 'inventory.qty_on_hand_sum', qty_on_hand)
    _append_raw_field(raw_fields, 'inventory.qty_reserved_sum', qty_reserved)
    _append_raw_field(raw_fields, 'inventory.cost_amount_sum', cost_amount)
    _append_raw_field(raw_fields, 'inventory.stock_value_sum', stock_value)

    for idx, row in enumerate(inv_detail_rows, start=1):
        fact_row = row[0]
        prefix = f'fact_inventory[{idx}]'
        _append_model_raw_fields(raw_fields, prefix, fact_row)
        _append_raw_field(raw_fields, f'{prefix}.branch_external_id', row[1])
        _append_raw_field(raw_fields, f'{prefix}.branch_name', row[2])
        _append_raw_field(raw_fields, f'{prefix}.warehouse_external_id', row[3])
        _append_raw_field(raw_fields, f'{prefix}.warehouse_name', row[4])

    _append_raw_field(raw_fields, 'sales_30d.sales_rows', sales_rows_30)
    _append_raw_field(raw_fields, 'sales_30d.sales_qty', sales_qty_30)
    _append_raw_field(raw_fields, 'sales_30d.sales_net_value', sales_value_30)
    _append_raw_field(raw_fields, 'sales_30d.sales_gross_value', gross_value_30)
    _append_raw_field(raw_fields, 'sales_30d.sales_cost_amount', sales_cost_30)
    _append_raw_field(raw_fields, 'sales_30d.sales_profit_amount', sales_profit_30)
    _append_raw_field(raw_fields, 'sales_30d.last_sale_date', last_sale_date)

    _append_raw_field(raw_fields, 'purchases_30d.purchase_rows', purchase_rows_30)
    _append_raw_field(raw_fields, 'purchases_30d.purchases_qty', purchases_qty_30)
    _append_raw_field(raw_fields, 'purchases_30d.purchases_net_value', purchases_value_30)
    _append_raw_field(raw_fields, 'purchases_30d.purchases_cost_amount', purchases_cost_30)
    _append_raw_field(raw_fields, 'purchases_30d.last_purchase_date', last_purchase_date)

    _append_raw_field(raw_fields, 'lifecycle.first_seen_inventory_date', first_seen)
    _append_raw_field(raw_fields, 'lifecycle.last_inventory_date', last_inventory_date)
    _append_raw_field(raw_fields, 'classification.status', status_value)
    _append_raw_field(raw_fields, 'classification.movement', movement_level)
    _append_raw_field(raw_fields, 'classification.status_source', resolved_classification['status_source'])
    _append_raw_field(raw_fields, 'classification.is_active_source', effective_active_source)
    _append_raw_field(raw_fields, 'classification.active_last_sale_days', resolved_classification['active_last_sale_days'])
    _append_raw_field(raw_fields, 'classification.fast_sales_qty_30d_min', resolved_classification['fast_sales_qty_30d_min'])
    _append_raw_field(raw_fields, 'classification.slow_sales_qty_30d_max', resolved_classification['slow_sales_qty_30d_max'])

    return {
        'item_code': code,
        'item_name': item_name,
        'barcode': barcode,
        'alternate_barcodes': alternate_barcodes,
        'brand': brand_name,
        'manufacturer': manufacturer_name,
        'category': category_name,
        'group': group_name,
        'qty_on_hand': qty_on_hand,
        'qty_reserved': qty_reserved,
        'cost_amount': cost_amount,
        'stock_value': stock_value,
        'sales_qty_30': sales_qty_30,
        'purchases_qty_30': purchases_qty_30,
        'last_sale_date': str(last_sale_date) if last_sale_date else None,
        'first_seen': str(first_seen) if first_seen else None,
        'status': status_value,
        'movement': movement_level,
        'snapshot_date': str(latest_date),
        'classification_config': resolved_classification,
        'raw_fields': raw_fields,
    }


async def price_control_filter_options(
    db: AsyncSession,
    supplier_ext_id: str | None = None,
    target_id: str | None = None,
):
    category_labels = await _dimension_label_map(db, DimCategory)
    group_labels = await _dimension_label_map(db, DimGroup)

    suppliers_rows = (
        await db.execute(
            select(DimSupplier.external_id, DimSupplier.name)
            .where(DimSupplier.external_id.is_not(None))
            .order_by(DimSupplier.name.asc())
        )
    ).all()
    categories_rows = (
        await db.execute(
            select(DimCategory.external_id)
            .where(DimCategory.external_id.is_not(None))
            .order_by(DimCategory.external_id.asc())
        )
    ).all()
    groups_rows = (
        await db.execute(
            select(DimGroup.external_id)
            .where(DimGroup.external_id.is_not(None))
            .order_by(DimGroup.external_id.asc())
        )
    ).all()
    targets_rows = (
        await db.execute(
            select(
                cast(SupplierTarget.id, String),
                SupplierTarget.name,
                SupplierTarget.supplier_ext_id,
                SupplierTarget.target_year,
                SupplierTarget.rebate_percent,
            )
            .where(SupplierTarget.is_active.is_(True))
            .order_by(SupplierTarget.target_year.desc(), SupplierTarget.name.asc())
        )
    ).all()

    selected_target_supplier: str | None = None
    selected_target_rebate = 0.0
    target_item_codes: set[str] = set()
    if target_id:
        target = (
            await db.execute(
                select(SupplierTarget).where(cast(SupplierTarget.id, String) == target_id, SupplierTarget.is_active.is_(True))
            )
        ).scalar_one_or_none()
        if target:
            selected_target_supplier = target.supplier_ext_id
            selected_target_rebate = float(target.rebate_percent or 0)
            t_items = (
                await db.execute(
                    select(SupplierTargetItem.item_external_id).where(SupplierTargetItem.supplier_target_id == target.id)
                )
            ).scalars().all()
            target_item_codes = {str(x) for x in t_items if x}

    effective_supplier = selected_target_supplier or (supplier_ext_id or None)

    items_stmt = (
        select(
            FactPurchases.item_code,
            func.coalesce(func.max(DimItem.name), FactPurchases.item_code).label('item_name'),
        )
        .join(DimItem, DimItem.external_id == FactPurchases.item_code, isouter=True)
        .where(FactPurchases.item_code.is_not(None))
    )
    if effective_supplier:
        items_stmt = items_stmt.where(FactPurchases.supplier_ext_id == effective_supplier)
    if target_item_codes:
        items_stmt = items_stmt.where(FactPurchases.item_code.in_(list(target_item_codes)))
    items_stmt = items_stmt.group_by(FactPurchases.item_code).order_by(func.max(DimItem.name).asc()).limit(1000)
    item_rows = (await db.execute(items_stmt)).all()

    categories = [
        {'value': str(r[0]), 'label': str(category_labels.get(str(r[0]), r[0]))}
        for r in categories_rows
        if r[0]
    ]
    groups = [
        {'value': str(r[0]), 'label': str(group_labels.get(str(r[0]), r[0]))}
        for r in groups_rows
        if r[0]
    ]
    categories.sort(key=lambda x: x['label'].casefold())
    groups.sort(key=lambda x: x['label'].casefold())

    return {
        'suppliers': [{'value': str(r[0]), 'label': str(r[1] or r[0])} for r in suppliers_rows],
        'categories': categories,
        'groups': groups,
        'targets': [
            {
                'value': str(r[0]),
                'label': f'{str(r[1] or "Target")} ({int(r[3] or 0)})',
                'supplier_ext_id': str(r[2] or ''),
                'rebate_percent': float(r[4] or 0),
            }
            for r in targets_rows
        ],
        'items': [{'value': str(r[0]), 'label': _clean_item_name(r[1], r[0])} for r in item_rows if r[0]],
        'target_supplier_ext_id': selected_target_supplier,
        'target_rebate_percent': selected_target_rebate,
    }


async def price_control_items(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    supplier_ext_id: str | None = None,
    target_id: str | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    item_codes: list[str] | None = None,
    target_margin_pct: float = 35.0,
    discount_pct: float = 0.0,
    limit: int = 500,
):
    selected_target_supplier: str | None = None
    selected_target_rebate = 0.0
    target_item_codes: set[str] = set()
    if target_id:
        target = (
            await db.execute(
                select(SupplierTarget).where(cast(SupplierTarget.id, String) == target_id, SupplierTarget.is_active.is_(True))
            )
        ).scalar_one_or_none()
        if target:
            selected_target_supplier = target.supplier_ext_id
            selected_target_rebate = float(target.rebate_percent or 0)
            t_items = (
                await db.execute(
                    select(SupplierTargetItem.item_external_id).where(SupplierTargetItem.supplier_target_id == target.id)
                )
            ).scalars().all()
            target_item_codes = {str(x) for x in t_items if x}

    effective_supplier = selected_target_supplier or (supplier_ext_id or None)
    effective_discount_pct = float(discount_pct if discount_pct > 0 else selected_target_rebate)
    effective_discount_pct = max(0.0, min(99.0, effective_discount_pct))
    target_margin_pct = max(0.0, min(95.0, float(target_margin_pct)))

    purchases_stmt = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value'),
            func.coalesce(
                func.sum(
                    case(
                        (func.coalesce(FactPurchases.cost_amount, 0) > 0, FactPurchases.cost_amount),
                        else_=FactPurchases.net_value,
                    )
                ),
                0,
            ).label('purchases_cost'),
            func.coalesce(func.max(FactPurchases.supplier_ext_id), literal('')).label('supplier_ext_id'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        .where(FactPurchases.item_code.is_not(None))
    )
    if effective_supplier:
        purchases_stmt = purchases_stmt.where(FactPurchases.supplier_ext_id == effective_supplier)
    if categories:
        purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
    if target_item_codes:
        purchases_stmt = purchases_stmt.where(FactPurchases.item_code.in_(list(target_item_codes)))
    if item_codes:
        purchases_stmt = purchases_stmt.where(FactPurchases.item_code.in_(item_codes))
    purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code).subquery('pc_purchases')

    sales_stmt = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.item_code.is_not(None))
    )
    if categories:
        sales_stmt = sales_stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        sales_stmt = sales_stmt.where(FactSales.group_ext_id.in_(groups))
    if target_item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(list(target_item_codes)))
    if item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(item_codes))
    sales_stmt = sales_stmt.group_by(FactSales.item_code).subquery('pc_sales')

    item_codes_rows = (await db.execute(select(purchases_stmt.c.item_code))).scalars().all()
    code_set = {str(x) for x in item_codes_rows if x}
    sales_codes = (await db.execute(select(sales_stmt.c.item_code))).scalars().all()
    code_set.update({str(x) for x in sales_codes if x})
    if not code_set:
        return {
            'summary': {'items': 0, 'avg_margin_pct': 0.0, 'target_margin_pct': target_margin_pct},
            'rows': [],
            'effective_discount_pct': effective_discount_pct,
        }

    meta_rows = (
        await db.execute(
            select(
                DimItem.external_id,
                func.coalesce(DimItem.name, DimItem.external_id).label('item_name'),
                func.coalesce(DimItem.barcode, literal('')).label('barcode'),
                func.coalesce(DimBrand.name, literal('N/A')).label('brand'),
                func.coalesce(DimCategory.name, literal('N/A')).label('category'),
                func.coalesce(DimGroup.name, literal('N/A')).label('group_name'),
            )
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
            .where(DimItem.external_id.in_(list(code_set)))
        )
    ).all()
    meta = {
        str(r[0]): {
            'item_name': _clean_item_name(r[1], r[0]),
            'barcode': str(r[2] or ''),
            'brand': str(r[3] or 'N/A'),
            'category': str(r[4] or 'N/A'),
            'group': str(r[5] or 'N/A'),
        }
        for r in meta_rows
    }

    supplier_name_map = {
        str(r[0]): str(r[1] or r[0])
        for r in (
            await db.execute(select(DimSupplier.external_id, DimSupplier.name).where(DimSupplier.external_id.is_not(None)))
        ).all()
    }

    purchases_map = {
        str(r[0]): {
            'qty': float(r[1] or 0),
            'value': float(r[2] or 0),
            'cost': float(r[3] or 0),
            'supplier_ext_id': str(r[4] or ''),
        }
        for r in (await db.execute(select(purchases_stmt))).all()
    }
    sales_map = {
        str(r[0]): {'qty': float(r[1] or 0), 'value': float(r[2] or 0)}
        for r in (await db.execute(select(sales_stmt))).all()
    }

    rows: list[dict[str, object]] = []
    for code in sorted(code_set):
        p = purchases_map.get(code, {'qty': 0.0, 'value': 0.0, 'cost': 0.0, 'supplier_ext_id': ''})
        s = sales_map.get(code, {'qty': 0.0, 'value': 0.0})

        wholesale_unit = (p['cost'] / p['qty']) if p['qty'] > 0 else 0.0
        acquisition_after_discount = wholesale_unit * (1 - effective_discount_pct / 100.0)
        sale_unit = (s['value'] / s['qty']) if s['qty'] > 0 else 0.0
        unit_profit = sale_unit - acquisition_after_discount
        margin_pct = ((unit_profit / sale_unit) * 100.0) if sale_unit > 0 else 0.0

        required_total_discount_pct = None
        recommended_extra_discount_pct = None
        if wholesale_unit > 0 and sale_unit > 0:
            max_cost_for_target = sale_unit * (1 - target_margin_pct / 100.0)
            req_total = max(0.0, min(99.0, (1 - (max_cost_for_target / wholesale_unit)) * 100.0))
            required_total_discount_pct = req_total
            recommended_extra_discount_pct = max(0.0, req_total - effective_discount_pct)

        m = meta.get(code, {'item_name': code, 'barcode': '', 'brand': 'N/A', 'category': 'N/A', 'group': 'N/A'})
        supplier_code = str(p.get('supplier_ext_id') or effective_supplier or '')
        rows.append(
            {
                'item_code': code,
                'item_name': m['item_name'],
                'barcode': m['barcode'],
                'supplier': supplier_name_map.get(supplier_code, supplier_code or 'N/A'),
                'brand': m['brand'],
                'category': m['category'],
                'group': m['group'],
                'sales_qty': float(s['qty']),
                'sales_value': float(s['value']),
                'purchases_qty': float(p['qty']),
                'wholesale_unit': wholesale_unit,
                'discount_pct': effective_discount_pct,
                'acquisition_after_discount': acquisition_after_discount,
                'sale_unit': sale_unit,
                'unit_profit': unit_profit,
                'margin_pct': margin_pct,
                'target_margin_pct': target_margin_pct,
                'required_total_discount_pct': required_total_discount_pct,
                'recommended_extra_discount_pct': recommended_extra_discount_pct,
            }
        )

    rows.sort(key=lambda x: float(x.get('sales_value', 0) or 0), reverse=True)
    rows = rows[: max(1, min(limit, 2000))]
    avg_margin = (sum(float(r['margin_pct']) for r in rows) / len(rows)) if rows else 0.0

    return {
        'summary': {
            'items': len(rows),
            'avg_margin_pct': round(avg_margin, 2),
            'target_margin_pct': round(target_margin_pct, 2),
        },
        'effective_discount_pct': round(effective_discount_pct, 2),
        'rows': rows,
    }


def _sellout_item_metadata_filters(
    stmt,
    *,
    item_column,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    if brands:
        dim_brand_items = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(or_(DimBrand.external_id.in_(brands), DimBrand.name.in_(brands)))
            .distinct()
        )
        inventory_brand_items = (
            select(FactInventory.item_code)
            .where(FactInventory.item_code.is_not(None))
            .where(
                or_(
                    FactInventory.source_payload_json['brand_external_id'].astext.in_(brands),
                    FactInventory.source_payload_json['brand_name'].astext.in_(brands),
                )
            )
            .distinct()
        )
        stmt = stmt.where(
            or_(
                FactSales.brand_ext_id.in_(brands),
                item_column.in_(dim_brand_items),
                item_column.in_(inventory_brand_items),
            )
        )
    if categories:
        dim_category_items = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(
                or_(
                    DimCategory.external_id.in_(categories),
                    DimCategory.name.in_(categories),
                    _item_category_path_expr().in_(categories),
                )
            )
            .distinct()
        )
        stmt = stmt.where(or_(FactSales.category_ext_id.in_(categories), item_column.in_(dim_category_items)))
    if groups:
        dim_group_items = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(or_(DimGroup.external_id.in_(groups), DimGroup.name.in_(groups)))
            .distinct()
        )
        inventory_group_items = (
            select(FactInventory.item_code)
            .where(FactInventory.item_code.is_not(None))
            .where(
                or_(
                    FactInventory.source_payload_json['group_external_id'].astext.in_(groups),
                    FactInventory.source_payload_json['group_name'].astext.in_(groups),
                )
            )
            .distinct()
        )
        stmt = stmt.where(
            or_(
                FactSales.group_ext_id.in_(groups),
                item_column.in_(dim_group_items),
                item_column.in_(inventory_group_items),
            )
        )
    return stmt


def _sellout_action(*, sales_qty: float, sales_value: float, profit_pct: float, stock_qty: float, days: int) -> str:
    avg_daily_qty = sales_qty / max(days, 1)
    stock_cover_days = (stock_qty / avg_daily_qty) if avg_daily_qty > 0 else None
    if sales_qty <= 0 and stock_qty > 0:
        return 'Νεκρό απόθεμα: ζήτησε επιστροφή, αντικατάσταση ή promo.'
    if sales_value > 0 and profit_pct < 15:
        return 'Χαμηλό περιθώριο: ζήτησε καλύτερη έκπτωση ή εμπορική πολιτική.'
    if stock_cover_days is not None and stock_cover_days < 20 and sales_qty > 0:
        return 'Ταχυκίνητο με χαμηλό απόθεμα: κλείσε διαθεσιμότητα/παραγγελία.'
    if stock_cover_days is not None and stock_cover_days > 180:
        return 'Υψηλό απόθεμα: ζήτησε στήριξη, promo ή αλλαγή κωδικών.'
    if sales_qty > 0 and profit_pct >= 30:
        return 'Καλή κίνηση και κέρδος: κράτησε χώρο και συζήτησε επιπλέον παροχή.'
    return 'Κανονική παρακολούθηση.'


async def _sellout_latest_inventory_rows(
    db: AsyncSession,
    item_codes: list[str],
    *,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
) -> dict[str, dict[str, object]]:
    if not item_codes:
        return {}
    latest_date = (
        await db.execute(
            select(func.max(FactInventory.doc_date))
            .where(FactInventory.doc_date <= date_to)
            .where(FactInventory.item_code.in_(item_codes))
        )
    ).scalar()
    if not latest_date:
        return {}
    stmt = (
        select(
            FactInventory.item_code,
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('stock_qty'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('stock_value'),
            func.coalesce(func.sum(FactInventory.cost_amount), 0).label('stock_cost'),
            func.max(FactInventory.source_payload_json['brand_external_id'].astext).label('brand_external_id'),
            func.max(FactInventory.source_payload_json['brand_name'].astext).label('brand_name'),
            func.max(FactInventory.source_payload_json['group_external_id'].astext).label('group_external_id'),
            func.max(FactInventory.source_payload_json['group_name'].astext).label('group_name'),
            func.max(FactInventory.source_payload_json['category_1'].astext).label('category_1'),
            func.max(FactInventory.source_payload_json['category_2'].astext).label('category_2'),
            func.max(FactInventory.source_payload_json['category_3'].astext).label('category_3'),
            func.max(FactInventory.source_payload_json['barcode'].astext).label('payload_barcode'),
            func.max(FactInventory.source_payload_json['color'].astext).label('color'),
            func.max(FactInventory.source_payload_json['size'].astext).label('size'),
        )
        .where(FactInventory.doc_date == latest_date)
        .where(FactInventory.item_code.in_(item_codes))
    )
    if branches:
        stmt = stmt.where(FactInventory.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactInventory.warehouse_ext_id.in_(warehouses))
    rows = (await db.execute(stmt.group_by(FactInventory.item_code))).mappings().all()
    return {str(row['item_code']): dict(row) for row in rows if row.get('item_code')}


async def sellout_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
):
    sold_items = (
        select(FactSales.item_code)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.item_code.is_not(None))
    )
    sold_items = _apply_fact_sales_filters(sold_items, branches=branches, warehouses=warehouses).distinct()

    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
        'suppliers': await _dimension_label_map(db, DimSupplier),
    }

    def add_option(values: list[str], label_map: dict[str, str], value: object, label: object = None) -> None:
        key = str(value or '').strip()
        if not key or key in {'0', '-'}:
            return
        if key not in values:
            values.append(key)
        label_text = str(label or label_map.get(key) or key).strip()
        if label_text and label_text not in {'0', '-'}:
            label_map[key] = label_text

    brands: list[str] = []
    brand_fact_stmt = (
        select(FactSales.brand_ext_id.label('value'), func.max(DimBrand.name).label('label'))
        .select_from(FactSales)
        .join(DimBrand, DimBrand.external_id == FactSales.brand_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.brand_ext_id.is_not(None))
        .where(FactSales.brand_ext_id != '')
        .group_by(FactSales.brand_ext_id)
        .order_by(func.max(DimBrand.name), FactSales.brand_ext_id)
    )
    brand_fact_stmt = _apply_fact_sales_filters(brand_fact_stmt, branches=branches, warehouses=warehouses)
    brand_fact_rows = (await db.execute(brand_fact_stmt)).all()
    for value, label in brand_fact_rows:
        add_option(brands, labels['brands'], value, label)

    brand_dim_rows = (
        await db.execute(
            select(DimBrand.external_id, DimBrand.name)
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id)
            .where(DimItem.external_id.in_(sold_items))
            .where(DimBrand.external_id.is_not(None))
            .distinct()
            .order_by(DimBrand.name, DimBrand.external_id)
        )
    ).all()
    for value, label in brand_dim_rows:
        add_option(brands, labels['brands'], value, label)

    brand_value_expr = literal_column("fact_inventory.source_payload_json ->> 'brand_external_id'")
    brand_label_expr = literal_column("fact_inventory.source_payload_json ->> 'brand_name'")
    brand_rows = (
        await db.execute(
            select(
                brand_value_expr.label('value'),
                func.max(brand_label_expr).label('label'),
            )
            .where(FactInventory.item_code.in_(sold_items))
            .where(brand_value_expr.is_not(None))
            .where(brand_value_expr != '')
            .group_by(brand_value_expr)
            .order_by(func.max(brand_label_expr))
        )
    ).all()
    for value, label in brand_rows:
        add_option(brands, labels['brands'], value, label)

    categories: list[str] = []
    category_fact_stmt = (
        select(FactSales.category_ext_id.label('value'), func.max(DimCategory.name).label('label'))
        .select_from(FactSales)
        .join(DimCategory, DimCategory.external_id == FactSales.category_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.category_ext_id.is_not(None))
        .where(FactSales.category_ext_id != '')
        .group_by(FactSales.category_ext_id)
        .order_by(func.max(DimCategory.name), FactSales.category_ext_id)
    )
    category_fact_stmt = _apply_fact_sales_filters(category_fact_stmt, branches=branches, warehouses=warehouses)
    category_fact_rows = (await db.execute(category_fact_stmt)).all()
    for value, label in category_fact_rows:
        add_option(categories, labels['categories'], value, label)

    category_dim_rows = (
        await db.execute(
            select(DimCategory.external_id, DimCategory.name)
            .select_from(DimItem)
            .join(DimCategory, DimItem.category_id == DimCategory.id)
            .where(DimItem.external_id.in_(sold_items))
            .where(DimCategory.external_id.is_not(None))
            .distinct()
            .order_by(DimCategory.name, DimCategory.external_id)
        )
    ).all()
    for value, label in category_dim_rows:
        add_option(categories, labels['categories'], value, label)

    category_rows = (
        await db.execute(
            select(_item_category_path_expr().label('value'))
            .where(DimItem.external_id.in_(sold_items))
            .where(
                or_(
                    _softone_clean_dimension_text(DimItem.category_1).is_not(None),
                    _softone_clean_dimension_text(DimItem.category_2).is_not(None),
                    _softone_clean_dimension_text(DimItem.category_3).is_not(None),
                )
            )
            .distinct()
        )
    ).scalars().all()
    for value in category_rows:
        add_option(categories, labels['categories'], value, value)

    groups: list[str] = []
    group_dim_rows = (
        await db.execute(
            select(DimGroup.external_id, DimGroup.name)
            .select_from(DimItem)
            .join(DimGroup, DimItem.group_id == DimGroup.id)
            .where(DimItem.external_id.in_(sold_items))
            .where(DimGroup.external_id.is_not(None))
            .distinct()
            .order_by(DimGroup.name, DimGroup.external_id)
        )
    ).all()
    for value, label in group_dim_rows:
        add_option(groups, labels['groups'], value, label)

    group_value_expr = literal_column("fact_inventory.source_payload_json ->> 'group_external_id'")
    group_label_expr = literal_column("fact_inventory.source_payload_json ->> 'group_name'")
    group_rows = (
        await db.execute(
            select(
                group_value_expr.label('value'),
                func.max(group_label_expr).label('label'),
            )
            .where(FactInventory.item_code.in_(sold_items))
            .where(group_value_expr.is_not(None))
            .where(group_value_expr != '')
            .group_by(group_value_expr)
            .order_by(func.max(group_label_expr))
        )
    ).all()
    for value, label in group_rows:
        add_option(groups, labels['groups'], value, label)

    supplier_rows = (
        await db.execute(
            select(FactPurchases.supplier_ext_id)
            .where(FactPurchases.item_code.in_(sold_items))
            .where(FactPurchases.supplier_ext_id.is_not(None))
            .where(FactPurchases.supplier_ext_id != '')
            .distinct()
            .order_by(FactPurchases.supplier_ext_id)
        )
    ).scalars().all()

    return {
        'branches': await _distinct_dimension_values(
            db, FactSales.doc_date, FactSales.branch_ext_id, date_from, date_to, _apply_fact_sales_filters,
            branches=None, warehouses=warehouses
        ),
        'warehouses': await _distinct_dimension_values(
            db, FactSales.doc_date, FactSales.warehouse_ext_id, date_from, date_to, _apply_fact_sales_filters,
            branches=branches, warehouses=None
        ),
        'brands': sorted(brands, key=lambda value: labels['brands'].get(value, value).lower()),
        'categories': sorted(categories, key=lambda value: labels['categories'].get(value, value).lower()),
        'groups': sorted(groups, key=lambda value: labels['groups'].get(value, value).lower()),
        'suppliers': [str(v) for v in supplier_rows if v],
        'labels': labels,
    }


async def sellout_report(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    suppliers: list[str] | None = None,
    q: str | None = None,
    limit: int = 250,
    offset: int = 0,
):
    supplier_item_filter = None
    if suppliers:
        supplier_item_filter = (
            select(FactPurchases.item_code)
            .where(FactPurchases.item_code.is_not(None))
            .where(FactPurchases.supplier_ext_id.in_(suppliers))
            .distinct()
        )

    profit_expr = func.coalesce(func.sum(FactSales.profit_amount), func.sum(FactSales.net_value - FactSales.cost_amount), 0)

    async def _sellout_monthly_sales(start_date: date, end_date: date) -> dict[date, float]:
        yr_expr = func.extract('year', FactSales.doc_date).label('yr')
        mo_expr = func.extract('month', FactSales.doc_date).label('mo')
        stmt = (
            select(
                yr_expr,
                mo_expr,
                func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, start_date, end_date))
            .where(FactSales.item_code.is_not(None))
        )
        stmt = _apply_fact_sales_filters(stmt, branches=branches, warehouses=warehouses)
        stmt = _sellout_item_metadata_filters(
            stmt,
            item_column=FactSales.item_code,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        if supplier_item_filter is not None:
            stmt = stmt.where(FactSales.item_code.in_(supplier_item_filter))
        if q_clean:
            like = f'%{q_clean}%'
            stmt = stmt.where(
                func.lower(cast(func.coalesce(FactSales.item_code, literal('')), String)).like(like)
            )
        rows = (await db.execute(stmt.group_by(yr_expr, mo_expr).order_by(yr_expr, mo_expr))).all()
        return {date(int(row[0]), int(row[1]), 1): float(row[2] or 0) for row in rows}

    async def _sellout_monthly_by_dim(
        start_date: date, end_date: date, dim: str
    ) -> list[tuple[str, str, date, float]]:
        month_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)

        # Build item→brand map from inventory payload (the primary brand source for this ERP)
        inv_brand_sq = (
            select(
                FactInventory.item_code.label('ic'),
                func.max(FactInventory.source_payload_json['brand_external_id'].astext).label('bid'),
                func.max(FactInventory.source_payload_json['brand_name'].astext).label('bname'),
            )
            .where(FactInventory.item_code.is_not(None))
            .where(FactInventory.source_payload_json['brand_external_id'].astext.is_not(None))
            .where(FactInventory.source_payload_json['brand_external_id'].astext != '')
            .group_by(FactInventory.item_code)
        ).subquery('ibsq')

        if dim == 'brand':
            resolved_id = func.coalesce(FactSales.brand_ext_id, DimBrand.external_id, inv_brand_sq.c.bid)
            resolved_name = func.coalesce(
                func.max(DimBrand.name),
                func.max(inv_brand_sq.c.bname),
                func.max(FactSales.brand_ext_id),
            )
            stmt = (
                select(
                    resolved_id.label('dim_id'),
                    resolved_name.label('dim_name'),
                    month_expr.label('month_start'),
                    func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
                )
                .select_from(FactSales)
                .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
                .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
                .join(inv_brand_sq, inv_brand_sq.c.ic == FactSales.item_code, isouter=True)
                .where(*_date_range(FactSales.doc_date, start_date, end_date))
                .where(FactSales.item_code.is_not(None))
                .where(resolved_id.is_not(None))
            )
            group_by_col = resolved_id
        else:
            # Categories live in DimItem.category_1/2/3 — use category_1 as primary group key
            cat_id_col = func.coalesce(
                FactSales.category_ext_id,
                _softone_clean_dimension_text(DimItem.category_1),
            )
            resolved_name = func.coalesce(
                func.max(_softone_clean_dimension_text(DimItem.category_1)),
                func.max(FactSales.category_ext_id),
            )
            stmt = (
                select(
                    cat_id_col.label('dim_id'),
                    resolved_name.label('dim_name'),
                    month_expr.label('month_start'),
                    func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
                )
                .select_from(FactSales)
                .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
                .where(*_date_range(FactSales.doc_date, start_date, end_date))
                .where(FactSales.item_code.is_not(None))
                .where(cat_id_col.is_not(None))
            )
            group_by_col = cat_id_col
        stmt = _apply_fact_sales_filters(stmt, branches=branches, warehouses=warehouses)
        stmt = _sellout_item_metadata_filters(
            stmt,
            item_column=FactSales.item_code,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        if supplier_item_filter is not None:
            stmt = stmt.where(FactSales.item_code.in_(supplier_item_filter))
        if q_clean:
            like = f'%{q_clean}%'
            stmt = stmt.where(
                func.lower(cast(func.coalesce(FactSales.item_code, literal('')), String)).like(like)
            )
        rows = (
            await db.execute(
                stmt.group_by(group_by_col, month_expr).order_by(group_by_col, month_expr)
            )
        ).all()
        return [(str(r[0]), str(r[1] or r[0]), r[2], float(r[3] or 0)) for r in rows]

    async def _sellout_monthly_by_brand_category(
        start_date: date, end_date: date
    ) -> list[tuple[str, str, str, date, float]]:
        """Returns (brand_id, brand_name, category_label, month_start, value) per brand×category."""
        month_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
        inv_brand_sq = (
            select(
                FactInventory.item_code.label('ic'),
                func.max(FactInventory.source_payload_json['brand_external_id'].astext).label('bid'),
                func.max(FactInventory.source_payload_json['brand_name'].astext).label('bname'),
            )
            .where(FactInventory.item_code.is_not(None))
            .where(FactInventory.source_payload_json['brand_external_id'].astext.is_not(None))
            .where(FactInventory.source_payload_json['brand_external_id'].astext != '')
            .group_by(FactInventory.item_code)
        ).subquery('ibsq2')
        resolved_brand = func.coalesce(FactSales.brand_ext_id, DimBrand.external_id, inv_brand_sq.c.bid)
        cat_label = func.coalesce(
            _softone_clean_dimension_text(DimItem.category_1),
            FactSales.category_ext_id,
            literal('N/A'),
        )
        stmt = (
            select(
                resolved_brand.label('brand_id'),
                func.coalesce(func.max(DimBrand.name), func.max(inv_brand_sq.c.bname), func.max(FactSales.brand_ext_id)).label('brand_name'),
                cat_label.label('cat_label'),
                month_expr.label('month_start'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
            )
            .select_from(FactSales)
            .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(inv_brand_sq, inv_brand_sq.c.ic == FactSales.item_code, isouter=True)
            .where(*_date_range(FactSales.doc_date, start_date, end_date))
            .where(FactSales.item_code.is_not(None))
            .where(resolved_brand.is_not(None))
        )
        stmt = _apply_fact_sales_filters(stmt, branches=branches, warehouses=warehouses)
        stmt = _sellout_item_metadata_filters(
            stmt, item_column=FactSales.item_code,
            brands=brands, categories=categories, groups=groups,
        )
        if supplier_item_filter is not None:
            stmt = stmt.where(FactSales.item_code.in_(supplier_item_filter))
        if q_clean:
            like = f'%{q_clean}%'
            stmt = stmt.where(
                func.lower(cast(func.coalesce(FactSales.item_code, literal('')), String)).like(like)
            )
        rows = (
            await db.execute(
                stmt.group_by(resolved_brand, cat_label, month_expr)
                .order_by(resolved_brand, cat_label, month_expr)
            )
        ).all()
        return [(str(r[0]), str(r[1] or r[0]), str(r[2] or 'N/A'), r[3], float(r[4] or 0)) for r in rows]

    sales_base = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.max(DimItem.name), func.max(FactSales.item_code), literal('N/A')).label('item_name'),
            func.coalesce(func.max(DimItem.barcode), literal('')).label('barcode'),
            func.coalesce(func.max(DimItem.alternate_barcodes), literal('')).label('alternate_barcodes'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('sales_cost'),
            profit_expr.label('gross_profit_value'),
            func.coalesce(func.avg(FactSales.discount_pct), 0).label('avg_discount_pct'),
            func.max(FactSales.doc_date).label('last_sale_date'),
        )
        .select_from(FactSales)
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.item_code.is_not(None))
    )
    sales_base = _apply_fact_sales_filters(sales_base, branches=branches, warehouses=warehouses)
    sales_base = _sellout_item_metadata_filters(
        sales_base,
        item_column=FactSales.item_code,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    if supplier_item_filter is not None:
        sales_base = sales_base.where(FactSales.item_code.in_(supplier_item_filter))
    q_clean = str(q or '').strip().lower()
    if q_clean:
        like = f'%{q_clean}%'
        sales_base = sales_base.where(
            func.lower(cast(func.coalesce(FactSales.item_code, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimItem.name, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(DimItem.barcode, literal('')), String)).like(like)
        )

    grouped = sales_base.group_by(FactSales.item_code).subquery('sellout_sales')
    totals = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('items'),
                func.coalesce(func.sum(grouped.c.sales_value), 0).label('sales_value'),
                func.coalesce(func.sum(grouped.c.sales_qty), 0).label('sales_qty'),
                func.coalesce(func.sum(grouped.c.sales_cost), 0).label('sales_cost'),
                func.coalesce(func.sum(grouped.c.gross_profit_value), 0).label('gross_profit_value'),
            )
        )
    ).mappings().one()

    row_limit = max(1, min(int(limit), 1000))
    row_offset = max(0, int(offset))
    sales_rows = (
        await db.execute(
            select(grouped)
            .order_by(grouped.c.sales_value.desc(), grouped.c.sales_qty.desc(), grouped.c.item_code.asc())
            .offset(row_offset)
            .limit(row_limit)
        )
    ).mappings().all()
    item_codes = [str(r['item_code']) for r in sales_rows if r.get('item_code')]

    inventory_map = await _sellout_latest_inventory_rows(
        db,
        item_codes,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
    )

    purchase_stmt = (
        select(
            FactPurchases.item_code,
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchase_value'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchase_qty'),
            func.coalesce(
                func.sum(
                    func.coalesce(FactPurchases.discount_amount, 0)
                    + func.coalesce(FactPurchases.discount1_amount, 0)
                    + func.coalesce(FactPurchases.discount2_amount, 0)
                    + func.coalesce(FactPurchases.discount3_amount, 0)
                ),
                0,
            ).label('purchase_discount_amount'),
            func.max(FactPurchases.supplier_ext_id).label('supplier_ext_id'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactPurchases.supplier_ext_id), literal('')).label('supplier_name'),
        )
        .select_from(FactPurchases)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(FactPurchases.item_code.in_(item_codes))
        .where(FactPurchases.doc_date <= date_to)
        .group_by(FactPurchases.item_code)
    )
    if suppliers:
        purchase_stmt = purchase_stmt.where(FactPurchases.supplier_ext_id.in_(suppliers))
    purchase_rows = (await db.execute(purchase_stmt)).mappings().all() if item_codes else []
    purchase_map = {str(r['item_code']): dict(r) for r in purchase_rows if r.get('item_code')}

    days = (date_to - date_from).days + 1
    dim_rows = (
        await db.execute(
            select(DimItem.external_id, DimItem.category_1, DimItem.category_2, DimItem.category_3).where(DimItem.external_id.in_(item_codes))
        )
    ).all() if item_codes else []
    dim_category_map = {
        str(code): [str(x).strip() for x in [c1, c2, c3] if x and str(x).strip() not in {'0', '-'}]
        for code, c1, c2, c3 in dim_rows
    }
    rows: list[dict[str, object]] = []
    stock_total = 0.0
    for r in sales_rows:
        code = str(r.get('item_code') or '')
        inv = inventory_map.get(code, {})
        pur = purchase_map.get(code, {})
        sales_value = float(r.get('sales_value') or 0)
        sales_qty = float(r.get('sales_qty') or 0)
        gross_profit_value = float(r.get('gross_profit_value') or 0)
        profit_pct = (gross_profit_value / sales_value * 100.0) if sales_value else 0.0
        stock_qty = float(inv.get('stock_qty') or 0)
        stock_value = float(inv.get('stock_value') or 0)
        stock_total += stock_qty
        category_parts = [
            str(x).strip()
            for x in [inv.get('category_1'), inv.get('category_2'), inv.get('category_3')]
            if x and str(x).strip() not in {'0', '-'}
        ]
        category_label = ' > '.join(category_parts or dim_category_map.get(code, [])) or 'N/A'
        purchase_value = float(pur.get('purchase_value') or 0)
        purchase_qty = float(pur.get('purchase_qty') or 0)
        purchase_discount_amount = float(pur.get('purchase_discount_amount') or 0)
        discount_base = abs(purchase_value) + abs(purchase_discount_amount)
        purchase_discount_pct = (purchase_discount_amount / discount_base * 100.0) if discount_base else 0.0
        rows.append(
            {
                'item_code': code,
                'product': _clean_item_name(str(r.get('item_name') or ''), code),
                'barcode': str(r.get('barcode') or inv.get('payload_barcode') or '-'),
                'alternate_barcodes': str(r.get('alternate_barcodes') or ''),
                'color': str(inv.get('color') or '-'),
                'size': str(inv.get('size') or '-'),
                'supplier_code': str(pur.get('supplier_ext_id') or ''),
                'supplier_name': str(pur.get('supplier_name') or 'N/A'),
                'brand': str(inv.get('brand_name') or inv.get('brand_external_id') or 'N/A'),
                'category': category_label,
                'group': str(inv.get('group_name') or inv.get('group_external_id') or 'N/A'),
                'sales_value': round(sales_value, 2),
                'sales_qty': round(sales_qty, 4),
                'gross_profit_pct': round(profit_pct, 2),
                'gross_profit_value': round(gross_profit_value, 2),
                'stock_qty': round(stock_qty, 4),
                'stock_value': round(stock_value, 2),
                'purchase_value': round(purchase_value, 2),
                'purchase_qty': round(purchase_qty, 4),
                'purchase_discount_pct': round(purchase_discount_pct, 2),
                'last_sale_date': r.get('last_sale_date').isoformat() if r.get('last_sale_date') else None,
                'action': _sellout_action(
                    sales_qty=sales_qty,
                    sales_value=sales_value,
                    profit_pct=profit_pct,
                    stock_qty=stock_qty,
                    days=days,
                ),
            }
        )

    total_sales = float(totals.get('sales_value') or 0)
    total_profit = float(totals.get('gross_profit_value') or 0)

    prev_year_from = _safe_same_day(date_from.year - 1, date_from.month, date_from.day)
    prev_year_to = _safe_same_day(date_to.year - 1, date_to.month, date_to.day)
    current_months = _month_sequence(date_from, date_to)
    current_month_map = await _sellout_monthly_sales(date_from, date_to) if current_months else {}
    previous_month_map = await _sellout_monthly_sales(prev_year_from, prev_year_to) if current_months else {}
    trend_labels = [_month_label(month) for month in current_months]
    trend_current = [round(float(current_month_map.get(month, 0.0)), 2) for month in current_months]
    trend_previous = [round(float(previous_month_map.get(month.replace(year=month.year - 1), 0.0)), 2) for month in current_months]
    trend_current_total = round(sum(trend_current), 2)
    trend_previous_total = round(sum(trend_previous), 2)
    trend_delta_pct = round(
        ((trend_current_total - trend_previous_total) / trend_previous_total * 100.0) if trend_previous_total else 0.0,
        2,
    )

    breakdown_dim = 'brand' if brands else ('category' if categories else None)
    breakdown: list[dict] = []
    breakdown_detail: list[dict] = []

    def _build_breakdown_series(raw_rows: list[tuple], months: list) -> list[dict]:
        """Build sorted breakdown list from (id, name, month, value) tuples."""
        maps: dict[str, dict] = {}
        names: dict[str, str] = {}
        for dim_id, dim_name, month, val in raw_rows:
            maps.setdefault(dim_id, {})[month] = val
            names[dim_id] = dim_name
        result = []
        for dim_id in sorted(maps.keys(), key=lambda x: names.get(x, x)):
            bd_vals = [round(float(maps.get(dim_id, {}).get(m, 0.0)), 2) for m in months]
            bd_total = round(sum(bd_vals), 2)
            result.append({'id': dim_id, 'name': names.get(dim_id, dim_id), 'current': bd_vals, 'current_total': bd_total})
        return result

    if breakdown_dim and current_months:
        curr_raw = await _sellout_monthly_by_dim(date_from, date_to, breakdown_dim)
        breakdown = _build_breakdown_series(curr_raw, current_months)

        # Detailed breakdown: per brand×category (only when brands are selected)
        if breakdown_dim == 'brand':
            detail_raw = await _sellout_monthly_by_brand_category(date_from, date_to)
            # detail_raw: (brand_id, brand_name, cat_label, month, value)
            detail_maps: dict[tuple, dict] = {}
            detail_meta: dict[tuple, tuple] = {}
            for brand_id, brand_name, cat_label, month, val in detail_raw:
                key = (brand_id, cat_label)
                detail_maps.setdefault(key, {})[month] = val
                detail_meta[key] = (brand_name, cat_label)
            for key in sorted(detail_maps.keys(), key=lambda k: (detail_meta[k][0], detail_meta[k][1])):
                brand_name, cat_label = detail_meta[key]
                vals = [round(float(detail_maps[key].get(m, 0.0)), 2) for m in current_months]
                total = round(sum(vals), 2)
                breakdown_detail.append({
                    'id': f'{key[0]}||{key[1]}',
                    'brand_id': key[0],
                    'brand_name': brand_name,
                    'cat_label': cat_label,
                    'name': f'{brand_name} · {cat_label}',
                    'current': vals,
                    'current_total': total,
                })

    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat(), 'days': days},
        'summary': {
            'items': int(totals.get('items') or 0),
            'sales_value': round(total_sales, 2),
            'sales_qty': round(float(totals.get('sales_qty') or 0), 4),
            'gross_profit_value': round(total_profit, 2),
            'gross_profit_pct': round((total_profit / total_sales * 100.0) if total_sales else 0.0, 2),
            'stock_qty': round(stock_total, 4),
        },
        'trend': {
            'labels': trend_labels,
            'current': trend_current,
            'previous': trend_previous,
            'current_label': str(date_from.year),
            'previous_label': str(date_from.year - 1),
            'current_period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
            'previous_period': {'from': prev_year_from.isoformat(), 'to': prev_year_to.isoformat()},
            'current_total': trend_current_total,
            'previous_total': trend_previous_total,
            'delta_pct': trend_delta_pct,
            'breakdown': breakdown,
            'breakdown_detail': breakdown_detail,
        },
        'rows': rows,
        'limit': row_limit,
        'offset': row_offset,
    }


async def sales_intraday(
    db: AsyncSession,
    day: date,
    branches: list[str] | None = None,
) -> dict:
    """Hourly sales breakdown by branch for a specific day."""
    effective_branches = _effective_branch_filter(branches)

    stmt = (
        select(
            func.extract('hour', FactSales.source_created_at).label('hour'),
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.sum(FactSales.net_value).label('net_value'),
        )
        .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
        .where(FactSales.doc_date == day)
        .where(FactSales.source_created_at.is_not(None))
        .group_by(
            func.extract('hour', FactSales.source_created_at),
            FactSales.branch_ext_id,
        )
        .order_by(func.extract('hour', FactSales.source_created_at))
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))

    rows = (await db.execute(stmt)).fetchall()

    data: dict[int, dict[str, float]] = {}
    branches_map: dict[str, str] = {}
    for hour, branch_ext_id, branch_name, net_value in rows:
        if branch_ext_id is None:
            continue
        h = int(hour)
        bname = branch_name or branch_ext_id
        branches_map[branch_ext_id] = bname
        data.setdefault(h, {})[branch_ext_id] = round(float(net_value or 0), 2)

    # Fallback: if no source_created_at, return daily totals at hour 12
    if not branches_map:
        stmt2 = (
            select(
                FactSales.branch_ext_id,
                func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
                func.sum(FactSales.net_value).label('net_value'),
            )
            .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
            .where(FactSales.doc_date == day)
            .group_by(FactSales.branch_ext_id)
        )
        if effective_branches is not None:
            stmt2 = stmt2.where(FactSales.branch_ext_id.in_(effective_branches))
        rows2 = (await db.execute(stmt2)).fetchall()
        for branch_ext_id, branch_name, net_value in rows2:
            if branch_ext_id is None:
                continue
            branches_map[branch_ext_id] = branch_name or branch_ext_id
            data.setdefault(12, {})[branch_ext_id] = round(float(net_value or 0), 2)

    return {'data': data, 'branches': branches_map}


async def sales_daily_by_branch(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> dict:
    """Daily sales net_value by branch for a date range."""
    effective_branches = _effective_branch_filter(branches)

    stmt = (
        select(
            cast(FactSales.doc_date, String).label('doc_date'),
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.sum(FactSales.net_value).label('net_value'),
        )
        .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .group_by(FactSales.doc_date, FactSales.branch_ext_id)
        .order_by(FactSales.doc_date, FactSales.branch_ext_id)
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))

    rows = (await db.execute(stmt)).fetchall()

    data: dict[str, dict[str, float]] = {}
    branches_map: dict[str, str] = {}
    all_dates: list[str] = []

    for doc_date, branch_ext_id, branch_name, net_value in rows:
        if branch_ext_id is None:
            continue
        d = str(doc_date)
        branches_map[branch_ext_id] = branch_name or branch_ext_id
        data.setdefault(d, {})[branch_ext_id] = round(float(net_value or 0), 2)

    # Build full date range (include days with no data as gaps)
    current = date_from
    while current <= date_to:
        all_dates.append(current.isoformat())
        current += timedelta(days=1)

    return {'data': data, 'branches': branches_map, 'dates': all_dates}


async def sales_monthly_by_branch(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> dict:
    """Monthly sales net_value by branch for a date range."""
    effective_branches = _effective_branch_filter(branches)

    month_start = cast(func.date_trunc('month', FactSales.doc_date), Date).label('month_start')
    stmt = (
        select(
            month_start,
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.sum(FactSales.net_value).label('net_value'),
        )
        .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .group_by(month_start, FactSales.branch_ext_id)
        .order_by(month_start, FactSales.branch_ext_id)
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))

    rows = (await db.execute(stmt)).fetchall()

    data: dict[str, dict[str, float]] = {}
    branches_map: dict[str, str] = {}

    for month_dt, branch_ext_id, branch_name, net_value in rows:
        if branch_ext_id is None:
            continue
        m = month_dt.isoformat() if hasattr(month_dt, 'isoformat') else str(month_dt)
        branches_map[branch_ext_id] = branch_name or branch_ext_id
        data.setdefault(m, {})[branch_ext_id] = round(float(net_value or 0), 2)

    # Build month list
    months: list[str] = []
    cur_month = date(date_from.year, date_from.month, 1)
    end_month = date(date_to.year, date_to.month, 1)
    while cur_month <= end_month:
        months.append(cur_month.isoformat())
        y, mo = cur_month.year, cur_month.month
        if mo == 12:
            cur_month = date(y + 1, 1, 1)
        else:
            cur_month = date(y, mo + 1, 1)

    return {'data': data, 'branches': branches_map, 'months': months}


# ---------------------------------------------------------------------------
# POS (Φυσικό Σημείο Πώλησης) queries
# document_type = 'sales_11351' → SOSOURCE=1351, SOREDIR=10000 (POS redirect)
# ---------------------------------------------------------------------------

_POS_DOCUMENT_TYPE = 'sales_11351'


def _pos_series_label(raw_category: str | None) -> str:
    value = str(raw_category or '').strip()
    if not value:
        return 'Λιανική Πώληση'

    lower_value = value.lower()
    if any(ch.isalpha() for ch in lower_value):
        return value

    if value.endswith('02'):
        return f'Επιστροφή Λιανικής ({value})'
    if value.endswith('10'):
        return f'Ειδική Λιανική ({value})'
    return f'Λιανική Πώληση ({value})'


async def pos_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> dict:
    effective_branches = _effective_branch_filter(branches)
    stmt = (
        select(
            func.count(func.distinct(FactSales.document_id)).label('receipts'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.count(FactSales.id).label('lines'),
        )
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .where(FactSales.document_type == _POS_DOCUMENT_TYPE)
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))
    row = (await db.execute(stmt)).one()
    receipts = int(row.receipts or 0)
    gross = float(row.gross_value or 0)
    net = float(row.net_value or 0)
    lines = int(row.lines or 0)
    avg_receipt = round(gross / receipts, 2) if receipts else 0.0
    avg_items = round(lines / receipts, 2) if receipts else 0.0
    return {
        'receipts': receipts,
        'gross_value': round(gross, 2),
        'net_value': round(net, 2),
        'avg_receipt_value': avg_receipt,
        'avg_items_per_receipt': avg_items,
    }


async def pos_branches(db: AsyncSession) -> list[dict]:
    stmt = (
        select(
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('name'),
        )
        .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
        .where(FactSales.document_type == _POS_DOCUMENT_TYPE)
        .where(FactSales.branch_ext_id.is_not(None))
        .group_by(FactSales.branch_ext_id)
        .order_by(FactSales.branch_ext_id)
    )
    rows = (await db.execute(stmt)).fetchall()
    return [{'ext_id': r.branch_ext_id, 'name': r.name or r.branch_ext_id} for r in rows]


async def pos_daily_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> list[dict]:
    effective_branches = _effective_branch_filter(branches)
    stmt = (
        select(
            FactSales.doc_date,
            func.count(func.distinct(FactSales.document_id)).label('receipts'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('value'),
        )
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .where(FactSales.document_type == _POS_DOCUMENT_TYPE)
        .group_by(FactSales.doc_date)
        .order_by(FactSales.doc_date)
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))
    rows = (await db.execute(stmt)).fetchall()
    return [
        {
            'date': r.doc_date.isoformat() if hasattr(r.doc_date, 'isoformat') else str(r.doc_date),
            'receipts': int(r.receipts or 0),
            'value': round(float(r.value or 0), 2),
        }
        for r in rows
    ]


async def pos_by_payment_method(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> list[dict]:
    effective_branches = _effective_branch_filter(branches)
    # Do not misclassify missing payment metadata as cash. Surface it explicitly
    # so operators can distinguish real cash sales from rows pending enrichment.
    pm_label = func.coalesce(
        func.nullif(func.trim(FactSales.payment_method), ''),
        'Χωρίς καταγεγραμμένο τρόπο πληρωμής',
    )
    stmt = (
        select(
            pm_label.label('payment_method'),
            func.count(func.distinct(FactSales.document_id)).label('receipts'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('value'),
        )
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .where(FactSales.document_type == _POS_DOCUMENT_TYPE)
        .group_by(pm_label)
        .order_by(func.sum(FactSales.gross_value).desc())
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))
    rows = (await db.execute(stmt)).fetchall()
    total = sum(float(r.value or 0) for r in rows)
    return [
        {
            'name': r.payment_method or 'Χωρίς καταγεγραμμένο τρόπο πληρωμής',
            'receipts': int(r.receipts or 0),
            'value': round(float(r.value or 0), 2),
            'pct': round(float(r.value or 0) / total * 100, 1) if total else 0.0,
        }
        for r in rows
    ]


async def pos_by_category(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
) -> list[dict]:
    effective_branches = _effective_branch_filter(branches)
    group_name_expr = func.nullif(func.btrim(DimGroup.name), '')
    group_code_expr = func.nullif(func.btrim(func.coalesce(FactSales.group_ext_id, DimGroup.external_id)), '')
    cat_key = func.coalesce(
        func.concat(literal('group:'), group_code_expr),
        literal('group:unknown'),
    )
    cat_label = func.coalesce(
        group_name_expr,
        group_code_expr,
        literal('Χωρίς Ομάδα'),
    )
    stmt = (
        select(
            cat_key.label('category_key'),
            cat_label.label('category'),
            func.count(func.distinct(FactSales.document_id)).label('receipts'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('value'),
        )
        .outerjoin(DimItem, DimItem.external_id == FactSales.item_code)
        .outerjoin(DimGroup, DimGroup.id == DimItem.group_id)
        .where(FactSales.doc_date >= date_from)
        .where(FactSales.doc_date <= date_to)
        .where(FactSales.document_type == _POS_DOCUMENT_TYPE)
        .where(func.coalesce(FactSales.gross_value, 0) > 0)
        .group_by(cat_key, cat_label)
        .order_by(func.sum(FactSales.gross_value).desc())
    )
    if effective_branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(effective_branches))
    rows = (await db.execute(stmt)).fetchall()
    total = sum(float(r.value or 0) for r in rows)
    return [
        {
            'name': str(r.category or 'Χωρίς Ομάδα'),
            'receipts': int(r.receipts or 0),
            'value': round(float(r.value or 0), 2),
            'pct': round(float(r.value or 0) / total * 100, 1) if total else 0.0,
        }
        for r in rows
    ]
