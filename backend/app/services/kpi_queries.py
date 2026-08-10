from datetime import date, datetime, timedelta
from decimal import Decimal
import re
import unicodedata
from uuid import UUID

from sqlalchemy import Date, Integer, Numeric, String, and_, case, cast, exists, func, literal, literal_column, not_, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import over

from app.services.intelligence_service import list_recent_insights
from app.services.kpi_participation_scope import get_current_sales_kpi_participation_config
from app.services.request_scope import get_allowed_branch_scope
from app.models.tenant import (
    AggPurchasesDaily,
    AggPurchasesDailyCompany,
    AggPurchasesMonthly,
    AggSalesDaily,
    AggSalesDailyCompany,
    AggSalesDailyBranch,
    AggSalesItemDaily,
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
    DimDocumentType,
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
    # Default for all tenants: active = SoftOne ENERGO (ISACTIVE) AND availability
    # (stock OR a sale within inventory_scope_sold_days).
    'status_source': 'active_available',
    'active_last_sale_days': 60,
    'movement_window_days': 30,
    # Inventory ("Αποθέματα") scope: items in stock OR sold within this many days.
    'inventory_scope_sold_days': 120,
    'fast_sales_qty_30d_min': 50,
    'slow_sales_qty_30d_max': 5,
}

# For status_source='commercial': a commercial_status in this set marks the item as INACTIVE
# (discontinued / expired). Everything else with a non-empty commercial_status is ACTIVE.
_INACTIVE_COMMERCIAL_STATUSES = {'καταργημένο', 'ληγμένο'}

_DEFAULT_ESHOP_FULFILLMENT_RULES = {
    'pickup_warehouses': {
        '1001': 'E-Shop Αγίου Δημητρίου',
        '1007': 'E-Shop Κηφισίας',
        '1010': 'E-Shop Ελληνικού',
        '2001': 'E-Shop Σπάτων',
        '8001': 'E-Shop Περιστερίου',
    },
    'store_warehouses': {},
    'pure_eshop_warehouses': ['1004'],
    'three_pl_warehouses': ['3000'],
    'shipping_method_labels': {},
    'sales_series_channel_labels': {},
    'physical_branch_names': [],
}

_DEFAULT_PRICE_MARGIN_TARGETS = {
    'default_margin_pct': 35.0,
    'category_margin_pct': {},
    'group_margin_pct': {},
}


def _bounded_pct(raw: object, default: float = 35.0, min_value: float = 0.0, max_value: float = 95.0) -> float:
    try:
        parsed = float(str(raw).strip().replace(',', '.'))
    except Exception:
        parsed = float(default)
    return max(min_value, min(max_value, parsed))


def normalize_price_margin_targets_config(raw: dict | None) -> dict[str, object]:
    source = raw if isinstance(raw, dict) else {}
    default_margin_pct = _bounded_pct(source.get('default_margin_pct'), _DEFAULT_PRICE_MARGIN_TARGETS['default_margin_pct'])

    def _clean_map(raw_map: object) -> dict[str, float]:
        out: dict[str, float] = {}
        if not isinstance(raw_map, dict):
            return out
        for key, value in raw_map.items():
            key_clean = str(key or '').strip()
            if not key_clean:
                continue
            out[key_clean] = _bounded_pct(value, default_margin_pct)
        return out

    return {
        'default_margin_pct': default_margin_pct,
        'category_margin_pct': _clean_map(source.get('category_margin_pct')),
        'group_margin_pct': _clean_map(source.get('group_margin_pct')),
    }


def _norm_margin_key(value: object) -> str:
    text = unicodedata.normalize('NFKD', str(value or '').strip().lower())
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r'\s+', ' ', text)


def resolve_price_margin_target(
    config: dict | None,
    *,
    category: object = None,
    group: object = None,
    fallback_pct: float = 35.0,
) -> tuple[float, str]:
    rules = normalize_price_margin_targets_config(config)
    default_margin = _bounded_pct(rules.get('default_margin_pct'), fallback_pct)

    group_text = str(group or '').strip()
    group_rules = rules.get('group_margin_pct') if isinstance(rules.get('group_margin_pct'), dict) else {}
    group_norm = _norm_margin_key(group_text)
    for key, pct in group_rules.items():
        if group_norm and group_norm == _norm_margin_key(key):
            return _bounded_pct(pct, default_margin), f'Ομάδα: {key}'

    category_text = str(category or '').strip()
    category_rules = rules.get('category_margin_pct') if isinstance(rules.get('category_margin_pct'), dict) else {}
    category_norm = _norm_margin_key(category_text)
    category_parts = {_norm_margin_key(part) for part in re.split(r'>|/|\\|,', category_text) if str(part or '').strip()}
    for key, pct in category_rules.items():
        key_norm = _norm_margin_key(key)
        if category_norm and (category_norm == key_norm or key_norm in category_parts):
            return _bounded_pct(pct, default_margin), f'Κατηγορία: {key}'

    return default_margin, 'Default'


def normalize_inventory_item_classification_config(raw: dict | None) -> dict[str, object]:
    source = raw if isinstance(raw, dict) else {}
    status_source_raw = str(source.get('status_source') or '').strip().lower()
    # Active/Inactive must come from the SoftOne primary source, not from a sales-recency policy.
    # 'commercial' = lifecycle from commercial_status; 'softone' = raw ISACTIVE flag;
    # 'active_available' = ISACTIVE + availability; 'active_status12' = ISACTIVE + both statuses;
    # 'sales_window' = legacy recency rule. Default = active_available (see _DEFAULT_...).
    if status_source_raw in {'commercial', 'commercial_status', 'status'}:
        status_source = 'commercial'
    elif status_source_raw in {'active_available', 'active_stock_sales', 'softone_available'}:
        # ISACTIVE flag AND availability (in stock OR sold within inventory_scope_sold_days).
        status_source = 'active_available'
    elif status_source_raw in {'active_status12', 'active_both_status', 'status12'}:
        # ISACTIVE flag AND a non-empty value in BOTH status_1 (manual_order_category)
        # and status_2 (commercial_status) — regardless of what those values are.
        status_source = 'active_status12'
    elif status_source_raw in {'softone', 'source', 'source_flag'}:
        status_source = 'softone'
    elif status_source_raw in {'sales_window', 'sales', 'window', 'recency'}:
        status_source = 'sales_window'
    else:
        status_source = _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['status_source']

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
    movement_window_days = _int_value(
        'movement_window_days',
        _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['movement_window_days'],
        1,
        3650,
    )
    inventory_scope_sold_days = _int_value(
        'inventory_scope_sold_days',
        _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['inventory_scope_sold_days'],
        1,
        3650,
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


def normalize_eshop_fulfillment_config(raw: dict | None) -> dict[str, object]:
    source = raw if isinstance(raw, dict) else {}
    use_defaults = source.get('use_defaults', True) is not False
    raw_pickup = source.get('pickup_warehouses')
    pickup_source = raw_pickup if isinstance(raw_pickup, dict) else {}
    pickup_clean: dict[str, str] = {}
    for code, label in pickup_source.items():
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            pickup_clean[code_clean] = label_clean
    if not pickup_clean and use_defaults:
        pickup_clean = dict(_DEFAULT_ESHOP_FULFILLMENT_RULES['pickup_warehouses'])

    raw_store_warehouses = source.get('store_warehouses')
    store_source = raw_store_warehouses if isinstance(raw_store_warehouses, dict) else {}
    store_clean: dict[str, str] = {}
    for code, label in store_source.items():
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            store_clean[code_clean] = label_clean

    raw_shipping_method_labels = source.get('shipping_method_labels')
    shipping_method_label_source = raw_shipping_method_labels if isinstance(raw_shipping_method_labels, dict) else {}
    shipping_method_label_clean: dict[str, str] = {}
    for code, label in shipping_method_label_source.items():
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            shipping_method_label_clean[code_clean] = label_clean

    raw_sales_series_channel_labels = source.get('sales_series_channel_labels')
    series_channel_source = raw_sales_series_channel_labels if isinstance(raw_sales_series_channel_labels, dict) else {}
    series_channel_clean: dict[str, str] = {}
    for code, label in series_channel_source.items():
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            series_channel_clean[code_clean] = label_clean

    def _list_clean(value: object, default: list[str]) -> list[str]:
        if isinstance(value, list):
            raw_items = value
        else:
            raw_items = default if use_defaults else []
        clean: list[str] = []
        seen: set[str] = set()
        for item in raw_items:
            item_clean = str(item or '').strip()
            if item_clean and item_clean not in seen:
                clean.append(item_clean)
                seen.add(item_clean)
        return clean or (list(default) if use_defaults else [])

    return {
        'use_defaults': use_defaults,
        'pickup_warehouses': pickup_clean,
        'store_warehouses': store_clean,
        'pure_eshop_warehouses': _list_clean(
            source.get('pure_eshop_warehouses'),
            list(_DEFAULT_ESHOP_FULFILLMENT_RULES['pure_eshop_warehouses']),
        ),
        'three_pl_warehouses': _list_clean(
            source.get('three_pl_warehouses'),
            list(_DEFAULT_ESHOP_FULFILLMENT_RULES['three_pl_warehouses']),
        ),
        'shipping_method_labels': shipping_method_label_clean,
        'sales_series_channel_labels': series_channel_clean,
        'physical_branch_names': _list_clean(
            source.get('physical_branch_names'),
            list(_DEFAULT_ESHOP_FULFILLMENT_RULES['physical_branch_names']),
        ),
    }


def normalize_document_series_labels_config(raw: dict | None) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    clean: dict[str, str] = {}
    for code, label in source.items():
        code_clean = str(code or '').strip()
        label_clean = str(label or '').strip()
        if code_clean and label_clean:
            clean[code_clean] = label_clean
    return clean


def _document_series_label(
    series_code: object,
    fallback: object = None,
    document_series_labels: dict[str, str] | None = None,
) -> str:
    code = str(series_code or '').strip()
    labels = normalize_document_series_labels_config(document_series_labels)
    if code:
        mapped = labels.get(code)
        if mapped:
            return mapped
        first_token = code.split(' ', 1)[0].strip()
        if first_token and first_token != code:
            mapped = labels.get(first_token)
            if mapped:
                return mapped
    fallback_txt = str(fallback or '').strip()
    if fallback_txt:
        return fallback_txt
    return code or 'N/A'


def _fold_text_for_match(value: object) -> str:
    txt = str(value or '').strip().lower()
    if not txt:
        return ''
    normalized = unicodedata.normalize('NFD', txt)
    return ''.join(ch for ch in normalized if not unicodedata.combining(ch))


def _classify_inventory_item(
    *,
    as_of: date,
    last_sale_date: date | None,
    sales_qty: float,
    config: dict[str, object],
    is_active_source: bool | None = None,
    commercial_status: str | None = None,
    manual_order_category: str | None = None,
    qty_on_hand: float = 0.0,
) -> tuple[str, str]:
    status_source = str(config.get('status_source') or 'softone').strip().lower()
    active_days = int(config.get('active_last_sale_days') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['active_last_sale_days'])
    fast_min = int(config.get('fast_sales_qty_30d_min') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['fast_sales_qty_30d_min'])
    slow_max = int(config.get('slow_sales_qty_30d_max') or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['slow_sales_qty_30d_max'])

    if status_source == 'commercial':
        cs = str(commercial_status or '').strip().lower()
        is_active = bool(cs) and cs not in _INACTIVE_COMMERCIAL_STATUSES
    elif status_source in {'active_available', 'active_stock_sales', 'softone_available'}:
        # "Ενεργό" = SoftOne ISACTIVE flag AND availability: has stock OR sold within the
        # configured window (inventory_scope_sold_days).
        sold_days = int(config.get('inventory_scope_sold_days') or 90)
        available = float(qty_on_hand or 0) != 0 or bool(
            last_sale_date and last_sale_date >= (as_of - timedelta(days=sold_days))
        )
        is_active = bool(is_active_source) and available
    elif status_source == 'active_status12':
        # ENERGO flag AND a non-empty value in both status_1 (manual_order_category)
        # and status_2 (commercial_status). The value content is irrelevant.
        s1 = str(manual_order_category or '').strip()
        s2 = str(commercial_status or '').strip()
        is_active = bool(is_active_source) and bool(s1) and bool(s2)
    elif status_source == 'softone' and is_active_source is not None:
        is_active = bool(is_active_source)
    else:
        is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=active_days)))
    movement = 'fast' if sales_qty >= fast_min else ('slow' if sales_qty <= slow_max else 'normal')
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


def _blank_zero_text(value: object, fallback: str = '') -> str:
    txt = str(value or '').strip()
    if not txt:
        return fallback
    compact = txt.replace(',', '.').strip().lower()
    if compact in {'0', '0.0', '-', 'null', 'none', 'n/a'}:
        return fallback
    return txt


def _warehouse_matches_eshop_channel_fallback(warehouse_code: object, fulfillment_config: dict | None = None) -> bool:
    rules = normalize_eshop_fulfillment_config(fulfillment_config)
    wh = str(warehouse_code or '').strip()
    pure_eshop = {str(x).strip() for x in (rules.get('pure_eshop_warehouses') or []) if str(x).strip()}
    three_pl = {str(x).strip() for x in (rules.get('three_pl_warehouses') or []) if str(x).strip()}
    return wh in pure_eshop or wh in three_pl


def _normalize_sales_channel_name(
    channel_name: object,
    warehouse_code: object,
    fulfillment_config: dict | None = None,
    series_code: object = None,
) -> str:
    txt = str(channel_name or '').strip()
    if txt:
        return txt
    rules = normalize_eshop_fulfillment_config(fulfillment_config)
    series = str(series_code or '').strip()
    sales_series_channel_labels = rules.get('sales_series_channel_labels') or {}
    if series and isinstance(sales_series_channel_labels, dict):
        mapped = str(sales_series_channel_labels.get(series) or '').strip()
        if mapped:
            return mapped
        first_token = series.split(' ', 1)[0].strip()
        if first_token and first_token != series:
            mapped = str(sales_series_channel_labels.get(first_token) or '').strip()
            if mapped:
                return mapped
    if _warehouse_matches_eshop_channel_fallback(warehouse_code, fulfillment_config):
        return 'Site'
    return ''


def _normalize_shipping_method_label(shipping_method: object, fulfillment_config: dict | None = None) -> str:
    txt = _blank_zero_text(shipping_method, '')
    if not txt:
        return ''
    rules = normalize_eshop_fulfillment_config(fulfillment_config)
    shipping_method_labels = rules.get('shipping_method_labels') or {}
    if isinstance(shipping_method_labels, dict):
        mapped = str(shipping_method_labels.get(txt) or '').strip()
        if mapped:
            return mapped
    return txt


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
    net_expr = _fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.net_value, 0))
    discount_breakdown_total = (
        func.coalesce(FactPurchases.discount1_amount, 0)
        + func.coalesce(FactPurchases.discount2_amount, 0)
        + func.coalesce(FactPurchases.discount3_amount, 0)
    )
    discount_total = case(
        (func.abs(func.coalesce(FactPurchases.discount_amount, 0)) > 0, func.coalesce(FactPurchases.discount_amount, 0)),
        else_=discount_breakdown_total,
    )
    abs_discount = func.abs(discount_total)
    return case((net_expr < 0, -abs_discount), else_=abs_discount)


def _fact_purchases_before_discount_expr():
    payload = FactPurchases.source_payload_json
    payload_value = cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'no_discount_amount'),
            _json_numeric_text_expr(payload, 'before_discount_amount'),
            _json_numeric_text_expr(payload, 'line_before_discount_amount'),
            _json_numeric_text_expr(payload, 'nodscamnt'),
            _json_numeric_text_expr(payload, 'NODSCAMNT'),
            literal('0'),
        ),
        Numeric,
    )
    fallback = func.coalesce(FactPurchases.net_value, 0) + _fact_purchase_signed_discount_expr()
    return case(
        (func.abs(func.coalesce(payload_value, 0)) > 0, _fact_purchases_signed_amount_expr(payload_value)),
        else_=fallback,
    )


_PURCHASE_CREDIT_BEHAVIOR_CODES = (151, 152)


def _fact_purchases_behavior_text_expr():
    return func.btrim(
        cast(
            func.coalesce(
                FactPurchases.source_payload_json['document_behavior_code'].astext,
                FactPurchases.source_payload_json['DOCUMENT_BEHAVIOR_CODE'].astext,
                FactPurchases.source_payload_json['source_transaction_type_id'].astext,
                FactPurchases.source_payload_json['SOURCE_TRANSACTION_TYPE_ID'].astext,
                FactPurchases.source_payload_json['behavior_code'].astext,
                FactPurchases.source_payload_json['behavior'].astext,
                FactPurchases.source_payload_json['tfprms'].astext,
                FactPurchases.source_payload_json['TFPRMS'].astext,
                literal(''),
            ),
            String,
        )
    )


def _fact_purchases_is_credit_expr():
    behavior_text = _fact_purchases_behavior_text_expr()
    is_credit_behavior = behavior_text.in_([str(code) for code in _PURCHASE_CREDIT_BEHAVIOR_CODES])

    series_name_expr = func.lower(
        cast(
            func.coalesce(
                FactPurchases.source_payload_json['document_series_name'].astext,
                FactPurchases.document_series,
                literal(''),
            ),
            String,
        )
    )
    doc_type_expr = func.lower(
        cast(
            func.coalesce(
                FactPurchases.source_payload_json['document_type'].astext,
                FactPurchases.document_type,
                literal(''),
            ),
            String,
        )
    )
    return or_(
        is_credit_behavior,
        series_name_expr.like('%πιστωτ%'),
        series_name_expr.like('%credit%'),
        doc_type_expr.like('%πιστωτ%'),
        doc_type_expr.like('%credit%'),
    )


def _fact_purchases_signed_amount_expr(amount_expr):
    is_credit = _fact_purchases_is_credit_expr()
    return case((and_(is_credit, amount_expr > 0), -amount_expr), else_=amount_expr)


def _fact_purchases_analysis_qty_expr():
    """Purchase analytics quantity rule: 102/103 positive, 151 negative, everything else ignored."""
    behavior_text = _fact_purchases_behavior_text_expr()
    qty_abs = func.abs(func.coalesce(FactPurchases.qty, 0))
    return case(
        (behavior_text == literal('102'), qty_abs),
        (behavior_text == literal('103'), qty_abs),
        (behavior_text == literal('151'), -qty_abs),
        else_=0,
    )


def _fact_purchases_payload_net_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'doc_net_total'),
            _json_numeric_text_expr(payload, 'DOC_NET_TOTAL'),
            _json_numeric_text_expr(payload, 'net_total'),
            _json_numeric_text_expr(payload, 'net_value_total'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_payload_expenses_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'doc_expenses_total'),
            _json_numeric_text_expr(payload, 'DOC_EXPENSES_TOTAL'),
            _json_numeric_text_expr(payload, 'expenses_value'),
            _json_numeric_text_expr(payload, 'expense_value'),
            _json_numeric_text_expr(payload, 'expenses_amount'),
            _json_numeric_text_expr(payload, 'expense_amount'),
            _json_numeric_text_expr(payload, 'total_expenses'),
            _json_numeric_text_expr(payload, 'expenses_total'),
            _json_numeric_text_expr(payload, 'expn'),
            _json_numeric_text_expr(payload, 'EXPN'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_payload_line_value_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'line_value'),
            _json_numeric_text_expr(payload, 'LINE_VALUE'),
            _json_numeric_text_expr(payload, 'lineval'),
            _json_numeric_text_expr(payload, 'LINEVAL'),
            cast(FactPurchases.net_value, String),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_payload_vat_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'doc_tax_total'),
            _json_numeric_text_expr(payload, 'DOC_TAX_TOTAL'),
            _json_numeric_text_expr(payload, 'vat_total'),
            _json_numeric_text_expr(payload, 'vat_value'),
            _json_numeric_text_expr(payload, 'total_vat'),
            _json_numeric_text_expr(payload, 'tax_total'),
            _json_numeric_text_expr(payload, 'tax_amount'),
            _json_numeric_text_expr(payload, 'fpa_total'),
            _json_numeric_text_expr(payload, 'fpa_amount'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_payload_gross_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'doc_gross_total'),
            _json_numeric_text_expr(payload, 'DOC_GROSS_TOTAL'),
            _json_numeric_text_expr(payload, 'gross_total'),
            _json_numeric_text_expr(payload, 'total_gross'),
            _json_numeric_text_expr(payload, 'amount_total'),
            _json_numeric_text_expr(payload, 'total_value'),
            _json_numeric_text_expr(payload, 'value_total'),
            _json_numeric_text_expr(payload, 'gross_value'),
            _json_numeric_text_expr(payload, 'GROSS_VALUE'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_payload_line_vat_expr():
    payload = FactPurchases.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'vat_amount'),
            _json_numeric_text_expr(payload, 'VAT_AMOUNT'),
            _json_numeric_text_expr(payload, 'tax_amount'),
            _json_numeric_text_expr(payload, 'line_vat'),
            _json_numeric_text_expr(payload, 'line_tax'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_purchases_supplier_afm_expr():
    payload = FactPurchases.source_payload_json
    return func.nullif(
        func.btrim(
            cast(
                func.coalesce(
                    payload['supplier_afm'].astext,
                    payload['supplier_vat_no'].astext,
                    payload['supplier_vat_number'].astext,
                    payload['supplier_tax_id'].astext,
                    payload['afm'].astext,
                    payload['vat_no'].astext,
                    payload['vat_number'].astext,
                    literal(''),
                ),
                String,
            )
        ),
        '',
    )


def _purchase_is_credit_payload(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    behavior_keys = ('source_transaction_type_id', 'behavior_code', 'behavior', 'tfprms')
    for key in behavior_keys:
        raw = payload.get(key)
        try:
            if int(str(raw).strip()) in _PURCHASE_CREDIT_BEHAVIOR_CODES:
                return True
        except (TypeError, ValueError):
            continue
    series_name = str(payload.get('document_series_name') or '').strip().lower()
    doc_type = str(payload.get('document_type') or '').strip().lower()
    return ('πιστωτ' in series_name) or ('πιστωτ' in doc_type) or ('credit' in series_name) or ('credit' in doc_type)


def _normalize_purchase_credit_sign(value: float, is_credit: bool) -> float:
    amount = float(value or 0.0)
    if is_credit and amount > 0:
        return -amount
    return amount


def _fact_expenses_is_credit_expr():
    doc_type_expr = func.lower(cast(func.coalesce(FactExpense.document_type, literal('')), String))
    return or_(
        doc_type_expr.like('%πιστωτ%'),
        doc_type_expr.like('%credit%'),
        doc_type_expr.like('102%'),
        doc_type_expr.like('% 102%'),
        doc_type_expr.like('11.4%'),
        doc_type_expr.like('%11.4%'),
    )


def _fact_expenses_signed_amount_expr(amount_expr):
    is_credit = _fact_expenses_is_credit_expr()
    return case((and_(is_credit, amount_expr > 0), -amount_expr), else_=amount_expr)


def _expense_is_credit_document_type(document_type: str | None) -> bool:
    doc_type = str(document_type or '').strip().lower()
    if not doc_type:
        return False
    return (
        ('πιστωτ' in doc_type)
        or ('credit' in doc_type)
        or doc_type.startswith('102')
        or (' 102' in doc_type)
        or doc_type.startswith('11.4')
        or ('11.4' in doc_type)
    )


def _normalize_expense_credit_sign(value: float, document_type: str | None) -> float:
    amount = float(value or 0.0)
    if _expense_is_credit_document_type(document_type) and amount > 0:
        return -amount
    return amount


def _normalize_expense_document_type_label(raw_type: str | None, *, category_name: str | None = None) -> str:
    txt = str(raw_type or '').strip()
    if not txt:
        return str(category_name or 'Παραστατικό Εξόδων')
    lowered = txt.lower()
    category = str(category_name or '').strip()
    if 'purchase_expense_' in lowered:
        behavior = txt.split(' ', 1)[0].strip()
        if behavior == '102' and category:
            return f'Πιστωτικό {category}'
        if behavior == '101' and category:
            return category
        return category or txt
    if lowered.startswith('expense_') or lowered.startswith('softone_series_'):
        return category or txt
    return txt


def _normalize_expense_branch_label(branch_name: str | None, branch_ext_id: str | None) -> str:
    label = str(branch_name or '').strip()
    if label and label.upper() != 'N/A':
        return label
    ext = str(branch_ext_id or '').strip()
    if ':' in ext:
        right = ext.split(':', 1)[1].strip()
        if right:
            return right
    return ext or 'N/A'


_TECHNICAL_PURCHASE_TYPE_RE = re.compile(r'^(?:\d+\s+)?purchase_\d+$', re.IGNORECASE)


def _strip_tenant_prefix(value: str | None) -> str:
    txt = str(value or '').strip()
    if ':' not in txt:
        return txt
    right = txt.split(':', 1)[1].strip()
    return right or txt


def _normalize_purchase_branch_label(branch_name: str | None, branch_ext_id: str | None) -> str:
    label = str(branch_name or '').strip()
    if label and label.upper() != 'N/A':
        return _strip_tenant_prefix(label)
    ext = _strip_tenant_prefix(branch_ext_id)
    return ext or 'N/A'


_KNOWN_SALES_BRANCH_LABELS = {
    '1000': 'Εδρα',
    '1001': 'Μαρίνου Αντύπα',
    '1002': 'Κηφισιά Κασσαβέτη',
    '1003': 'Ελληνικό',
    '2000': 'Σπάτα',
    '8000': 'Περιστέρι',
}


def _normalize_sales_branch_label(branch_name: str | None, branch_ext_id: str | None) -> str:
    ext = _strip_tenant_prefix(branch_ext_id)
    if ext in _KNOWN_SALES_BRANCH_LABELS:
        return _KNOWN_SALES_BRANCH_LABELS[ext]
    label = str(branch_name or '').strip()
    if label and label.upper() != 'N/A':
        return _strip_tenant_prefix(label)
    return ext or 'N/A'


def _normalize_purchase_document_type_label(
    raw_type: str | None,
    *,
    series_label: str | None = None,
    payload: dict | None = None,
) -> str:
    value = str(raw_type or '').strip()
    if value and not _TECHNICAL_PURCHASE_TYPE_RE.match(value):
        return value

    if isinstance(payload, dict):
        for key in (
            'document_series_name',
            'series_name',
            'document_type_name',
            'doc_type_name',
            'type_name',
            'tfprms_name',
        ):
            txt = str(payload.get(key) or '').strip()
            if txt and not _TECHNICAL_PURCHASE_TYPE_RE.match(txt) and not txt.isdigit():
                return txt

    fallback_series = str(series_label or '').strip()
    if fallback_series and not fallback_series.isdigit() and not _TECHNICAL_PURCHASE_TYPE_RE.match(fallback_series):
        return fallback_series

    behavior = value.split(' ', 1)[0].strip() if value else ''
    if behavior in {'102', '152'}:
        return 'Πιστωτικό Αγορών'
    if behavior == '103':
        return 'Τιμολόγιο / Δελτίο Αγορών'
    return 'Παραστατικό Αγορών'


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


def _item_category_key_expr():
    return func.concat(literal('ITEMCAT:'), func.substring(func.md5(_item_category_path_expr()), 1, 24))


def _inventory_category_path_expr(c1_col, c2_col, c3_col):
    return func.concat_ws(
        ' > ',
        _softone_clean_dimension_text(c1_col),
        _softone_clean_dimension_text(c2_col),
        _softone_clean_dimension_text(c3_col),
    )


def _json_text(json_col, key: str):
    return json_col[key].astext


def _apply_fact_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None, channels=None):
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        category_item_codes = (
            select(DimItem.external_id)
            .where(DimItem.external_id.is_not(None))
            .where(
                or_(
                    _item_category_key_expr().in_(categories),
                    _item_category_path_expr().in_(categories),
                )
            )
        )
        stmt = stmt.where(or_(FactSales.category_ext_id.in_(categories), FactSales.item_code.in_(category_item_codes)))
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
    # The behaviour code drives the participation filter on every sales KPI, so
    # it used to force a JSON extract + cast across all 1.2M fact_sales rows per
    # query. It is now denormalised into fact_sales.behavior_code at ingest time
    # and backfilled for existing rows; the JSON stays as the fallback so a row
    # the backfill or a connector missed still resolves to the same value.
    return func.coalesce(
        FactSales.behavior_code,
        cast(FactSales.source_payload_json['source_transaction_type_id'].astext, Integer),
    )


_CREDIT_BEHAVIOR_CODES_FOR_VAT_SIGN_FIX: tuple[int, ...] = ()


def _is_credit_behavior_code_for_vat_sign_fix(value: object) -> bool:
    try:
        return int(str(value).strip()) in _CREDIT_BEHAVIOR_CODES_FOR_VAT_SIGN_FIX
    except (TypeError, ValueError):
        return False


def _normalize_credit_vat_sign(vat_value: float, net_value: float, behavior_code: object) -> float:
    if not _is_credit_behavior_code_for_vat_sign_fix(behavior_code):
        return float(vat_value)
    if net_value < 0 and vat_value > 0:
        return -float(vat_value)
    if net_value > 0 and vat_value < 0:
        return -float(vat_value)
    return float(vat_value)


def _fact_sales_behavior_code_text_expr():
    # Same denormalisation as _fact_sales_behavior_code_expr, in text form.
    return func.coalesce(
        cast(FactSales.behavior_code, String),
        cast(FactSales.source_payload_json['source_transaction_type_id'].astext, String),
        literal(''),
    )


def _fact_sales_credit_signed_amount_expr(amount_expr):
    behavior_code_text = _fact_sales_behavior_code_text_expr()
    is_credit_behavior = behavior_code_text.in_([str(code) for code in _CREDIT_BEHAVIOR_CODES_FOR_VAT_SIGN_FIX])
    return case((and_(is_credit_behavior, amount_expr > 0), -amount_expr), else_=amount_expr)


def _fact_sales_vat_amount_expr():
    gross_expr = func.coalesce(FactSales.gross_value, 0)
    net_expr = func.coalesce(FactSales.net_value, 0)
    raw_vat_expr = case(
        (FactSales.vat_amount.is_not(None), FactSales.vat_amount),
        else_=(gross_expr - net_expr),
    )
    behavior_code_text = _fact_sales_behavior_code_text_expr()
    is_credit_behavior = behavior_code_text.in_([str(code) for code in _CREDIT_BEHAVIOR_CODES_FOR_VAT_SIGN_FIX])
    return case(
        (and_(is_credit_behavior, net_expr < 0, raw_vat_expr > 0), -raw_vat_expr),
        (and_(is_credit_behavior, net_expr > 0, raw_vat_expr < 0), -raw_vat_expr),
        else_=raw_vat_expr,
    )


def _fact_sales_customer_afm_expr():
    payload = FactSales.source_payload_json
    return func.nullif(
        func.btrim(
            cast(
                func.coalesce(
                    payload['customer_afm'].astext,
                    payload['customer_vat_no'].astext,
                    payload['customer_vat_number'].astext,
                    payload['customer_tax_id'].astext,
                    payload['afm'].astext,
                    payload['vat_no'].astext,
                    payload['vat_number'].astext,
                    literal(''),
                ),
                String,
            )
        ),
        '',
    )


def _json_numeric_text_expr(json_col, key: str):
    raw = func.nullif(func.btrim(cast(json_col[key].astext, String)), '')
    return func.replace(raw, ',', '.')


def _fact_sales_payload_expenses_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'charge_revenue_net_value'),
            _json_numeric_text_expr(payload, 'shipping_expense_value'),
            _json_numeric_text_expr(payload, 'doc_expenses_total'),
            _json_numeric_text_expr(payload, 'DOC_EXPENSES_TOTAL'),
            _json_numeric_text_expr(payload, 'shipping_charge_net_value'),
            _json_numeric_text_expr(payload, 'cod_charge_net_value'),
            _json_numeric_text_expr(payload, 'charge_revenue_total_net_value'),
            _json_numeric_text_expr(payload, 'expenses_value'),
            _json_numeric_text_expr(payload, 'expense_value'),
            _json_numeric_text_expr(payload, 'expenses_amount'),
            _json_numeric_text_expr(payload, 'expense_amount'),
            _json_numeric_text_expr(payload, 'total_expenses'),
            _json_numeric_text_expr(payload, 'expenses_total'),
            _json_numeric_text_expr(payload, 'other_charges'),
            _json_numeric_text_expr(payload, 'charges_amount'),
            _json_numeric_text_expr(payload, 'shipping_cost'),
            _json_numeric_text_expr(payload, 'fees_amount'),
            _json_numeric_text_expr(payload, 'value_expenses'),
            _json_numeric_text_expr(payload, 'axia_exodon'),
            _json_numeric_text_expr(payload, 'charge_revenue_gross_value'),
            _json_numeric_text_expr(payload, 'charge_revenue_total_gross_value'),
            _json_numeric_text_expr(payload, 'EXPENSES_VALUE'),
            _json_numeric_text_expr(payload, 'EXPENSE_AMOUNT'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_document_key_expr():
    return func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)


def _fact_sales_signed_net_expr():
    return func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)


def _fact_sales_signed_gross_expr():
    return func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)


def _fact_sales_signed_cost_expr():
    return func.coalesce(FactSales.cost_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)


def _fact_sales_signed_expenses_expr():
    return _fact_sales_payload_expenses_expr() * _fact_sales_behavior_sign_expr(quantity=False)


def _fact_sales_payload_vat_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'vat_total'),
            _json_numeric_text_expr(payload, 'vat_value'),
            _json_numeric_text_expr(payload, 'total_vat'),
            _json_numeric_text_expr(payload, 'tax_total'),
            _json_numeric_text_expr(payload, 'tax_amount'),
            _json_numeric_text_expr(payload, 'fpa_total'),
            _json_numeric_text_expr(payload, 'fpa_amount'),
            _json_numeric_text_expr(payload, 'doc_tax_total'),
            _json_numeric_text_expr(payload, 'DOC_TAX_TOTAL'),
            _json_numeric_text_expr(payload, 'VAT_AMOUNT'),
            _json_numeric_text_expr(payload, 'TAX_AMOUNT'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_payload_gross_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'doc_gross_total'),
            _json_numeric_text_expr(payload, 'DOC_GROSS_TOTAL'),
            _json_numeric_text_expr(payload, 'gross_total'),
            _json_numeric_text_expr(payload, 'total_gross'),
            _json_numeric_text_expr(payload, 'amount_total'),
            _json_numeric_text_expr(payload, 'total_value'),
            _json_numeric_text_expr(payload, 'value_total'),
            _json_numeric_text_expr(payload, 'gross_value'),
            _json_numeric_text_expr(payload, 'GROSS_VALUE'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_payload_shipping_expense_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'shipping_charge_net_value'),
            _json_numeric_text_expr(payload, 'shipping_expense_value'),
            _json_numeric_text_expr(payload, 'shipping_cost'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_payload_cod_charge_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'cod_charge_net_value'),
            _json_numeric_text_expr(payload, 'cod_charge_gross_value'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_payload_gift_charge_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'gift_charge_net_value'),
            _json_numeric_text_expr(payload, 'gift_charge_gross_value'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_payload_other_charge_expr():
    payload = FactSales.source_payload_json
    return cast(
        func.coalesce(
            _json_numeric_text_expr(payload, 'other_charge_net_value'),
            _json_numeric_text_expr(payload, 'other_charge_gross_value'),
            literal('0'),
        ),
        Numeric,
    )


def _fact_sales_eshop_document_expr():
    channel_expr = func.lower(cast(func.coalesce(FactSales.channel_name, literal('')), String))
    eshop_expr = func.nullif(func.btrim(cast(func.coalesce(FactSales.eshop_code, literal('')), String)), '')
    return or_(eshop_expr.is_not(None), channel_expr.like('%site%'), channel_expr.like('%eshop%'), channel_expr.like('%e-shop%'))


def _fact_sales_behavior_sign_expr(*, quantity: bool):
    sign_map = _sales_behavior_sign_map('quantity_sign_by_behavior' if quantity else 'amount_sign_by_behavior')
    if not sign_map or all(float(sign) == 1.0 for sign in sign_map.values()):
        return literal(1.0)
    behavior_code = _fact_sales_behavior_code_expr()
    whens = [(behavior_code == int(code), float(sign)) for code, sign in sign_map.items()]
    return case(*whens, else_=literal(1.0))


def _has_sales_sign_overrides() -> bool:
    """True when participation config flips the sign of some behaviour code."""
    amount_sign_map = _sales_behavior_sign_map('amount_sign_by_behavior')
    quantity_sign_map = _sales_behavior_sign_map('quantity_sign_by_behavior')
    return any(float(sign) != 1.0 for sign in amount_sign_map.values()) or any(
        float(sign) != 1.0 for sign in quantity_sign_map.values()
    )


def _can_use_behavior_aware_sales_aggregate() -> bool:
    """Whether the behaviour whitelist can be applied on the aggregates instead
    of forcing a fact_sales scan.

    The aggregates now carry behavior_code as a dimension, so the whitelist is a
    plain WHERE on read. Two rule families still cannot be expressed there:

    * turnover series rules filter on document_series, which is not an aggregate
      dimension (and is per-document, not per-group);
    * sign overrides have to be applied before summation — the aggregate has
      already summed using the document rules' signs.

    Either of those still needs the fact path.
    """
    return not _has_sales_turnover_series_rules() and not _has_sales_sign_overrides()


def _apply_behavior_filter_to_aggregate(stmt, behavior_code_column):
    """Apply the participation whitelist to an aggregate query."""
    codes = _sales_behavior_codes()
    if not codes:
        return stmt
    return stmt.where(behavior_code_column.in_(codes))


def _has_sales_behavior_rules() -> bool:
    amount_sign_map = _sales_behavior_sign_map('amount_sign_by_behavior')
    quantity_sign_map = _sales_behavior_sign_map('quantity_sign_by_behavior')
    has_amount_sign_overrides = any(float(sign) != 1.0 for sign in amount_sign_map.values())
    has_quantity_sign_overrides = any(float(sign) != 1.0 for sign in quantity_sign_map.values())
    return bool(
        _sales_behavior_codes()
        or has_amount_sign_overrides
        or has_quantity_sign_overrides
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
            func.coalesce(func.max(FactCustomerBalance.customer_afm), literal('')).label('customer_afm'),
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
            snapshots.c.customer_afm,
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
            'customer_afm': str(row.get('customer_afm') or '').strip(),
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


async def _customer_balances_summary_snapshot(
    db: AsyncSession,
    *,
    as_of: date,
    branches: list[str] | None = None,
    include_top: bool = True,
) -> dict[str, object]:
    agg_has_rows = (await db.execute(select(AggCustomerBalancesDaily.balance_date).limit(1))).first() is not None
    if not agg_has_rows:
        fallback_map = await _latest_customer_balances_map(db, as_of=as_of, branches=branches, aggregate_only=True)
        return {
            'customers': int(len(fallback_map)),
            'open_balance': float(sum(float(item.get('open_balance') or 0) for item in fallback_map.values())),
            'overdue_balance': float(sum(float(item.get('overdue_balance') or 0) for item in fallback_map.values())),
            'aging_bucket_0_30': float(sum(float(item.get('aging_bucket_0_30') or 0) for item in fallback_map.values())),
            'aging_bucket_31_60': float(sum(float(item.get('aging_bucket_31_60') or 0) for item in fallback_map.values())),
            'aging_bucket_61_90': float(sum(float(item.get('aging_bucket_61_90') or 0) for item in fallback_map.values())),
            'aging_bucket_90_plus': float(sum(float(item.get('aging_bucket_90_plus') or 0) for item in fallback_map.values())),
            'top_customer_id': max(
                fallback_map.items(),
                key=lambda pair: float(pair[1].get('open_balance') or 0),
                default=('', {}),
            )[0]
            if include_top
            else '',
            'top_customer_name': '',
            'top_customer_balance': float(
                max((float(item.get('open_balance') or 0) for item in fallback_map.values()), default=0.0)
                if include_top
                else 0.0
            ),
        }

    customer_key_expr = func.nullif(AggCustomerBalancesDaily.customer_ext_id, '')
    branches = _effective_branch_filter(branches)
    latest_date_stmt = select(func.max(AggCustomerBalancesDaily.balance_date)).where(
        AggCustomerBalancesDaily.balance_date <= as_of
    )
    if branches is not None:
        latest_date_stmt = latest_date_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
    latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
    if latest_date is None:
        return {
            'customers': 0,
            'open_balance': 0.0,
            'overdue_balance': 0.0,
            'aging_bucket_0_30': 0.0,
            'aging_bucket_31_60': 0.0,
            'aging_bucket_61_90': 0.0,
            'aging_bucket_90_plus': 0.0,
            'top_customer_id': '',
            'top_customer_name': '',
            'top_customer_balance': 0.0,
        }

    summary_stmt = (
        select(
            func.coalesce(func.count(func.distinct(customer_key_expr)), 0).label('customers'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
            func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
        )
        .select_from(AggCustomerBalancesDaily)
        .where(AggCustomerBalancesDaily.balance_date == latest_date)
        .where(customer_key_expr.is_not(None))
    )
    if branches is not None:
        summary_stmt = summary_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))

    summary_row = (await db.execute(summary_stmt)).mappings().one()
    top_row = {}
    if include_top:
        top_stmt = (
            select(
                customer_key_expr.label('customer_id'),
                func.coalesce(func.max(AggCustomerBalancesDaily.customer_ext_id), literal('')).label('customer_name'),
                func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
            )
            .select_from(AggCustomerBalancesDaily)
            .where(AggCustomerBalancesDaily.balance_date == latest_date)
            .where(customer_key_expr.is_not(None))
        )
        if branches is not None:
            top_stmt = top_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
        top_rows = top_stmt.group_by(customer_key_expr).subquery('customer_balance_top_latest')
        top_row = (
            await db.execute(
                select(top_rows.c.customer_id, top_rows.c.customer_name, top_rows.c.open_balance)
                .order_by(top_rows.c.open_balance.desc(), top_rows.c.customer_id.asc())
                .limit(1)
            )
        ).mappings().first() or {}

    top_customer_id = str(top_row.get('customer_id') or '').strip()
    top_customer_name = str(top_row.get('customer_name') or top_customer_id).strip()
    return {
        'customers': int(summary_row.get('customers') or 0),
        'open_balance': float(summary_row.get('open_balance') or 0),
        'overdue_balance': float(summary_row.get('overdue_balance') or 0),
        'aging_bucket_0_30': float(summary_row.get('aging_bucket_0_30') or 0),
        'aging_bucket_31_60': float(summary_row.get('aging_bucket_31_60') or 0),
        'aging_bucket_61_90': float(summary_row.get('aging_bucket_61_90') or 0),
        'aging_bucket_90_plus': float(summary_row.get('aging_bucket_90_plus') or 0),
        'top_customer_id': top_customer_id,
        'top_customer_name': top_customer_name,
        'top_customer_balance': float(top_row.get('open_balance') or 0),
    }


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
    warehouse_ext_col=None,
    brand_ext_col=None,
    brand_label_col=None,
    category_1_col=None,
    category_2_col=None,
    category_3_col=None,
    group_ext_col=None,
    group_label_col=None,
    commercial_category_col=None,
):
    stmt = stmt.where(_inventory_item_scope_predicate())
    branches = _effective_branch_filter(branches)
    if branches is not None:
        if branch_ext_col is not None:
            stmt = stmt.where(or_(DimBranch.external_id.in_(branches), branch_ext_col.in_(branches)))
        else:
            stmt = stmt.where(DimBranch.external_id.in_(branches))
    if warehouses:
        if warehouse_ext_col is not None:
            stmt = stmt.where(or_(DimWarehouse.external_id.in_(warehouses), warehouse_ext_col.in_(warehouses)))
        else:
            stmt = stmt.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        brand_predicates = [DimBrand.external_id.in_(brands)]
        if brand_ext_col is not None:
            brand_predicates.append(brand_ext_col.in_(brands))
        if brand_label_col is not None:
            brand_predicates.append(brand_label_col.in_(brands))
        stmt = stmt.where(or_(*brand_predicates))
    if categories:
        category_predicates = [
            DimCategory.external_id.in_(categories),
            _inventory_category_path_expr(DimItem.category_1, DimItem.category_2, DimItem.category_3).in_(categories),
        ]
        if category_1_col is not None and category_2_col is not None and category_3_col is not None:
            category_predicates.append(_inventory_category_path_expr(category_1_col, category_2_col, category_3_col).in_(categories))
        stmt = stmt.where(or_(*category_predicates))
    if groups:
        group_predicates = [DimGroup.external_id.in_(groups)]
        if group_ext_col is not None:
            group_predicates.append(group_ext_col.in_(groups))
        if group_label_col is not None:
            group_predicates.append(group_label_col.in_(groups))
        if commercial_category_col is not None:
            group_predicates.append(commercial_category_col.in_(groups))
        stmt = stmt.where(or_(*group_predicates))
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
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    fact_min, fact_max = (await db.execute(fact_stmt)).one()
    if fact_max is None:
        return False

    agg_stmt = (
        select(
            func.min(AggSalesDaily.doc_date),
            func.max(AggSalesDaily.doc_date),
        )
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    agg_min, agg_max = (await db.execute(agg_stmt)).one()
    if agg_max is None:
        return True

    if agg_min > fact_min or agg_max < fact_max:
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

    if not any([branches, warehouses, brands, categories, groups]) and _effective_branch_filter(None) is None:
        global_from, global_to = _window_bounds(windows)
        cols = []
        for key, (window_from, window_to) in windows.items():
            cond = AggSalesDailyCompany.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    literal(0).label(f'{key}_records'),
                    func.coalesce(func.sum(AggSalesDailyCompany.qty).filter(cond), 0).label(f'{key}_qty'),
                    func.coalesce(func.sum(AggSalesDailyCompany.net_value).filter(cond), 0).label(f'{key}_net_value'),
                    func.coalesce(func.sum(AggSalesDailyCompany.gross_value).filter(cond), 0).label(f'{key}_gross_value'),
                ]
            )
        row = (
            await db.execute(
                select(*cols)
                .select_from(AggSalesDailyCompany)
                .where(*_date_range(AggSalesDailyCompany.doc_date, global_from, global_to))
            )
        ).mappings().one()
        return {
            key: {
                'records': int(row.get(f'{key}_records') or 0),
                'qty': float(row.get(f'{key}_qty') or 0),
                'net_value': float(row.get(f'{key}_net_value') or 0),
                'gross_value': float(row.get(f'{key}_gross_value') or 0),
            }
            for key in windows
        }

    global_from, global_to = _window_bounds(windows)
    doc_key = _fact_sales_document_key_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            func.max(FactSales.doc_date).label('doc_date'),
            func.count(FactSales.id).label('records'),
            func.coalesce(func.sum(func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)), 0).label('qty'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
        )
        .where(*_date_range(FactSales.doc_date, global_from, global_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key).subquery('sales_doc_amounts')

    cols = []
    for key, (window_from, window_to) in windows.items():
        cond = doc_rows.c.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.coalesce(func.sum(doc_rows.c.records).filter(cond), 0).label(f'{key}_records'),
                func.coalesce(func.sum(doc_rows.c.qty).filter(cond), 0).label(f'{key}_qty'),
                func.coalesce(func.sum((doc_rows.c.net_value + doc_rows.c.expenses_value)).filter(cond), 0).label(f'{key}_net_value'),
                func.coalesce(func.sum(doc_rows.c.gross_value).filter(cond), 0).label(f'{key}_gross_value'),
            ]
        )

    stmt = select(*cols).select_from(doc_rows)
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

    # The purchase summary cards need document-level fields that are not fully
    # represented in the daily company aggregate: NODSCAMNT for before-discount
    # purchases and document expenses for after-discount cost.
    use_company_aggregate = True
    if use_company_aggregate and not any([branches, warehouses, brands, categories, groups]) and _effective_branch_filter(None) is None:
        global_from, global_to = _window_bounds(windows)
        cols = []
        for key, (window_from, window_to) in windows.items():
            cond = AggPurchasesDailyCompany.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    literal(0).label(f'{key}_records'),
                    func.coalesce(func.sum(AggPurchasesDailyCompany.qty).filter(cond), 0).label(f'{key}_qty'),
                    func.coalesce(func.sum(AggPurchasesDailyCompany.net_value).filter(cond), 0).label(f'{key}_net_value'),
                    func.coalesce(func.sum(AggPurchasesDailyCompany.cost_amount).filter(cond), 0).label(f'{key}_cost_amount'),
                    func.coalesce(func.sum(AggPurchasesDailyCompany.cost_amount).filter(cond), 0).label(f'{key}_gross_value'),
                    literal(0).label(f'{key}_discount_amount'),
                ]
            )
        row = (
            await db.execute(
                select(*cols)
                .select_from(AggPurchasesDailyCompany)
                .where(*_date_range(AggPurchasesDailyCompany.doc_date, global_from, global_to))
            )
        ).mappings().one()
        return {
            key: {
                'records': int(row.get(f'{key}_records') or 0),
                'qty': float(row.get(f'{key}_qty') or 0),
                'net_value': float(row.get(f'{key}_net_value') or 0),
                'cost_amount': float(row.get(f'{key}_cost_amount') or 0),
                'gross_value': float(row.get(f'{key}_gross_value') or 0),
                'discount_amount': float(row.get(f'{key}_discount_amount') or 0),
            }
            for key in windows
        }

    global_from, global_to = _window_bounds(windows)
    doc_key = _fact_purchases_document_key_expr()
    net_expr = _fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.net_value, 0))
    before_discount_expr = _fact_purchases_before_discount_expr()
    expenses_expr = _fact_purchases_signed_amount_expr(_fact_purchases_payload_expenses_expr())
    discount_expr = _fact_purchase_signed_discount_expr()
    qty_expr = _fact_purchases_analysis_qty_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            func.max(FactPurchases.doc_date).label('doc_date'),
            func.count(FactPurchases.id).label('records'),
            func.coalesce(func.sum(qty_expr), 0).label('qty'),
            func.coalesce(func.sum(net_expr), 0).label('net_value'),
            func.coalesce(func.max(expenses_expr), 0).label('expenses_value'),
            func.coalesce(func.sum(before_discount_expr), 0).label('cost_amount'),
            func.coalesce(func.sum(before_discount_expr), 0).label('gross_value'),
            func.coalesce(func.sum(discount_expr), 0).label('discount_amount'),
        )
        .where(*_date_range(FactPurchases.doc_date, global_from, global_to))
    )
    doc_rows = _apply_fact_purchase_filters(
        doc_rows,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    doc_rows = doc_rows.group_by(doc_key).subquery('purchase_doc_amounts')

    cols = []
    for key, (window_from, window_to) in windows.items():
        cond = doc_rows.c.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.coalesce(func.sum(doc_rows.c.records).filter(cond), 0).label(f'{key}_records'),
                func.coalesce(func.sum(doc_rows.c.qty).filter(cond), 0).label(f'{key}_qty'),
                func.coalesce(func.sum((doc_rows.c.net_value + doc_rows.c.expenses_value)).filter(cond), 0).label(f'{key}_net_value'),
                func.coalesce(func.sum(doc_rows.c.cost_amount).filter(cond), 0).label(f'{key}_cost_amount'),
                func.coalesce(func.sum(doc_rows.c.gross_value).filter(cond), 0).label(f'{key}_gross_value'),
                func.coalesce(func.sum(doc_rows.c.discount_amount).filter(cond), 0).label(f'{key}_discount_amount'),
            ]
        )

    stmt = select(*cols).select_from(doc_rows)
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
        branch_code = row.get('branch_ext_id') or 'N/A'
        branch = _normalize_sales_branch_label(row.get('branch_name'), branch_code)
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

    if not any([branches, warehouses, brands, categories, groups]) and _effective_branch_filter(None) is None:
        global_from, global_to = _window_bounds(windows)
        cols = [
            AggSalesDailyBranch.branch_ext_id.label('branch_ext_id'),
            func.coalesce(func.max(DimBranch.name), AggSalesDailyBranch.branch_ext_id).label('branch_name'),
        ]
        for key, (window_from, window_to) in windows.items():
            cond = AggSalesDailyBranch.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    func.coalesce(func.sum(AggSalesDailyBranch.net_value).filter(cond), 0).label(f'{key}_net'),
                    func.coalesce(func.sum(AggSalesDailyBranch.gross_value).filter(cond), 0).label(f'{key}_gross'),
                    func.coalesce(func.sum(AggSalesDailyBranch.cost_amount).filter(cond), 0).label(f'{key}_cost'),
                ]
            )
        stmt = (
            select(*cols)
            .select_from(AggSalesDailyBranch)
            .join(DimBranch, DimBranch.external_id == AggSalesDailyBranch.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDailyBranch.doc_date, global_from, global_to))
            .group_by(AggSalesDailyBranch.branch_ext_id)
        )
        rows = (await db.execute(stmt)).mappings().all()
        out: dict[str, list[dict]] = {}
        for key, (window_from, window_to) in windows.items():
            mapped = _map_branch_window_rows(rows, key_prefix=key)
            out[key] = _apply_sales_branch_adjustments_to_rows(
                mapped,
                date_from=window_from,
                date_to=window_to,
            )
        return out

    global_from, global_to = _window_bounds(windows)
    doc_key = _fact_sales_document_key_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            FactSales.branch_ext_id.label('branch_ext_id'),
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.max(FactSales.doc_date).label('doc_date'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.sum(_fact_sales_signed_cost_expr()), 0).label('cost_amount'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, global_from, global_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, FactSales.branch_ext_id).subquery('sales_branch_doc_amounts')

    cols = [
        doc_rows.c.branch_ext_id.label('branch_ext_id'),
        func.coalesce(func.max(doc_rows.c.branch_name), doc_rows.c.branch_ext_id).label('branch_name'),
    ]
    for key, (window_from, window_to) in windows.items():
        cond = doc_rows.c.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.coalesce(func.sum((doc_rows.c.net_value + doc_rows.c.expenses_value)).filter(cond), 0).label(f'{key}_net'),
                func.coalesce(func.sum((doc_rows.c.gross_value + doc_rows.c.expenses_value)).filter(cond), 0).label(f'{key}_gross'),
                func.coalesce(func.sum(doc_rows.c.cost_amount).filter(cond), 0).label(f'{key}_cost'),
            ]
        )

    stmt = select(*cols).select_from(doc_rows)
    rows = (await db.execute(stmt.group_by(doc_rows.c.branch_ext_id))).mappings().all()

    out: dict[str, list[dict]] = {}
    for key, (window_from, window_to) in windows.items():
        mapped = _map_branch_window_rows(rows, key_prefix=key)
        out[key] = _apply_sales_branch_adjustments_to_rows(
            mapped,
            date_from=window_from,
            date_to=window_to,
        )
    return out


async def _sales_by_warehouse_windows(
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
    # The behaviour whitelist is now a dimension on agg_sales_daily, so it no
    # longer forces the fact path — only series rules and sign overrides do.
    use_fact_source = not _can_use_behavior_aware_sales_aggregate() or await _should_use_fact_sales_source(
        db, date_from=global_from, date_to=global_to
    )
    if not use_fact_source:
        cols = [
            AggSalesDaily.warehouse_ext_id.label('warehouse_ext_id'),
            func.coalesce(func.max(DimWarehouse.name), AggSalesDaily.warehouse_ext_id, literal('N/A')).label('warehouse_name'),
            func.coalesce(func.max(AggSalesDaily.branch_ext_id), literal('')).label('branch_ext_id'),
            func.coalesce(func.max(DimBranch.name), func.max(AggSalesDaily.branch_ext_id), literal('')).label('branch_name'),
        ]
        for key, (window_from, window_to) in windows.items():
            cond = AggSalesDaily.doc_date.between(window_from, window_to)
            cols.extend(
                [
                    func.coalesce(func.sum(AggSalesDaily.net_value).filter(cond), 0).label(f'{key}_net'),
                    func.coalesce(func.sum(AggSalesDaily.gross_value).filter(cond), 0).label(f'{key}_gross'),
                    literal(0).label(f'{key}_cost'),
                ]
            )

        stmt = (
            select(*cols)
            .select_from(AggSalesDaily)
            .join(DimWarehouse, DimWarehouse.external_id == AggSalesDaily.warehouse_ext_id, isouter=True)
            .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDaily.doc_date, global_from, global_to))
        )
        stmt = _apply_behavior_filter_to_aggregate(stmt, AggSalesDaily.behavior_code)
        stmt = _apply_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        rows = (await db.execute(stmt.group_by(AggSalesDaily.warehouse_ext_id))).mappings().all()

        out: dict[str, list[dict]] = {}
        for key, (window_from, window_to) in windows.items():
            mapped: list[dict] = []
            for row in rows:
                warehouse_code = str(row.get('warehouse_ext_id') or '').strip() or 'N/A'
                net_value = float(row.get(f'{key}_net') or 0)
                gross_value = float(row.get(f'{key}_gross') or 0)
                cost_amount = float(row.get(f'{key}_cost') or 0)
                if abs(net_value) < 0.0001 and abs(gross_value) < 0.0001 and abs(cost_amount) < 0.0001:
                    continue
                mapped.append(
                    {
                        'warehouse': str(row.get('warehouse_name') or warehouse_code),
                        'warehouse_code': warehouse_code,
                        'warehouse_ext_id': warehouse_code,
                        'branch': str(row.get('branch_name') or row.get('branch_ext_id') or ''),
                        'branch_code': str(row.get('branch_ext_id') or ''),
                        'net_value': net_value,
                        'gross_value': gross_value,
                        'cost_amount': cost_amount,
                    }
                )
            out[key] = sorted(mapped, key=lambda item: float(item.get('net_value') or 0), reverse=True)
        return out

    net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    gross_expr = func.coalesce(FactSales.gross_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    cost_expr = func.coalesce(FactSales.cost_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    cols = [
        FactSales.warehouse_ext_id.label('warehouse_ext_id'),
        func.coalesce(func.max(DimWarehouse.name), FactSales.warehouse_ext_id, literal('N/A')).label('warehouse_name'),
        func.coalesce(func.max(FactSales.branch_ext_id), literal('')).label('branch_ext_id'),
        func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('')).label('branch_name'),
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
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
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
    rows = (await db.execute(stmt.group_by(FactSales.warehouse_ext_id))).mappings().all()

    out: dict[str, list[dict]] = {}
    for key, (window_from, window_to) in windows.items():
        mapped: list[dict] = []
        for row in rows:
            warehouse_code = str(row.get('warehouse_ext_id') or '').strip() or 'N/A'
            net_value = float(row.get(f'{key}_net') or 0)
            gross_value = float(row.get(f'{key}_gross') or 0)
            cost_amount = float(row.get(f'{key}_cost') or 0)
            if abs(net_value) < 0.0001 and abs(gross_value) < 0.0001 and abs(cost_amount) < 0.0001:
                continue
            mapped.append(
                {
                    'warehouse_ext_id': warehouse_code,
                    'warehouse': str(row.get('warehouse_name') or warehouse_code),
                    'branch_ext_id': str(row.get('branch_ext_id') or ''),
                    'branch': str(row.get('branch_name') or row.get('branch_ext_id') or ''),
                    'net_value': net_value,
                    'gross_value': gross_value,
                    'cost_amount': cost_amount,
                    'from': window_from.isoformat(),
                    'to': window_to.isoformat(),
                }
            )
        mapped.sort(key=lambda item: float(item.get('net_value') or 0), reverse=True)
        out[key] = mapped
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
    document_series_labels: dict[str, str] | None = None,
):
    summary = await _sales_summaries_by_windows(
        db,
        windows={'current': (date_from, date_to)},
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return summary.get('current', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})


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
    rows_by_window = await _sales_by_branch_windows(
        db,
        windows={'current': (date_from, date_to)},
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return rows_by_window.get('current', [])


def _sales_fulfillment_point_expr(warehouse_col, branch_name_col=None, fulfillment_config: dict | None = None):
    rules = normalize_eshop_fulfillment_config(fulfillment_config)
    wh = cast(warehouse_col, String)
    cases = []
    for code, label in (rules.get('store_warehouses') or {}).items():
        cases.append((wh == str(code), literal(str(label))))
    for code, label in (rules.get('pickup_warehouses') or {}).items():
        cases.append((wh == str(code), literal(str(label))))
    for code in rules.get('three_pl_warehouses') or []:
        cases.append((wh == str(code), literal('3PL / Courier στον πελάτη')))
    for code in rules.get('pure_eshop_warehouses') or []:
        cases.append((wh == str(code), literal('Καθαρό E-Shop')))
    fallback_values = [warehouse_col, literal('N/A')]
    if branch_name_col is not None:
        fallback_values.insert(0, branch_name_col)
    fallback = func.coalesce(*fallback_values)
    return case(*cases, else_=fallback)


async def sales_by_fulfillment_point(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    fulfillment_config: dict | None = None,
):
    point_label = _sales_fulfillment_point_expr(
        AggSalesDaily.warehouse_ext_id,
        DimBranch.name,
        fulfillment_config,
    )
    stmt = (
        select(
            point_label.label('branch_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .select_from(AggSalesDaily)
        .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(point_label).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    total_net = sum(float(r[1] or 0) for r in rows)
    avg_net = (total_net / len(rows)) if rows else 0.0
    out = []
    for branch_name, net_value_raw, gross_value_raw in rows:
        name = str(branch_name or 'N/A').strip() or 'N/A'
        net_value = float(net_value_raw or 0)
        contribution_pct = (net_value / total_net * 100.0) if total_net > 0 else 0.0
        performance_index_pct = (net_value / avg_net * 100.0) if avg_net > 0 else 0.0
        out.append(
            {
                'branch': name,
                'branch_code': f'FULFILLMENT:{name}',
                'net_value': net_value,
                'gross_value': float(gross_value_raw or 0),
                'cost_amount': 0.0,
                'contribution_pct': contribution_pct,
                'margin_pct': 0.0,
                'performance_index_pct': performance_index_pct,
            }
        )
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
    doc_key = _fact_sales_document_key_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            FactSales.brand_ext_id.label('brand_ext_id'),
            func.coalesce(func.max(DimBrand.name), FactSales.brand_ext_id).label('brand_name'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
        )
        .select_from(FactSales)
        .join(DimBrand, DimBrand.external_id == FactSales.brand_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, FactSales.brand_ext_id).subquery('sales_brand_doc_amounts')
    stmt = (
        select(
            doc_rows.c.brand_ext_id,
            func.coalesce(func.max(doc_rows.c.brand_name), doc_rows.c.brand_ext_id).label('brand_name'),
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.gross_value + doc_rows.c.expenses_value), 0).label('gross_value'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.brand_ext_id)
        .order_by(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value).desc())
    )
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
    item_category_label = func.coalesce(
        _softone_clean_dimension_text(DimItem.category_1),
        _softone_clean_dimension_text(DimItem.commercial_category),
        _softone_clean_dimension_text(DimGroup.name),
    )
    fact_category_code = _softone_clean_dimension_text(FactSales.category_ext_id)
    category_code = func.coalesce(
        fact_category_code,
        func.concat(literal('ITEMCAT:'), func.substring(func.md5(item_category_label), 1, 24)),
        literal('N/A'),
    )
    category_name = func.coalesce(
        _softone_clean_dimension_text(DimCategory.name),
        item_category_label,
        fact_category_code,
        literal('N/A'),
    )
    doc_key = _fact_sales_document_key_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            category_code.label('category_ext_id'),
            func.max(category_name).label('category_name'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
        )
        .select_from(FactSales)
        .join(DimCategory, DimCategory.external_id == FactSales.category_ext_id, isouter=True)
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .join(DimGroup, DimGroup.id == DimItem.group_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, category_code).subquery('sales_category_doc_amounts')
    stmt = (
        select(
            doc_rows.c.category_ext_id,
            func.coalesce(func.max(doc_rows.c.category_name), doc_rows.c.category_ext_id).label('category_name'),
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.gross_value + doc_rows.c.expenses_value), 0).label('gross_value'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.category_ext_id)
        .order_by(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value).desc())
    )
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
    doc_key = _fact_sales_document_key_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            FactSales.group_ext_id.label('group_ext_id'),
            func.coalesce(func.max(DimGroup.name), FactSales.group_ext_id).label('group_name'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
        )
        .select_from(FactSales)
        .join(DimGroup, DimGroup.external_id == FactSales.group_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, FactSales.group_ext_id).subquery('sales_group_doc_amounts')
    stmt = (
        select(
            doc_rows.c.group_ext_id,
            func.coalesce(func.max(doc_rows.c.group_name), doc_rows.c.group_ext_id).label('group_name'),
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.gross_value + doc_rows.c.expenses_value), 0).label('gross_value'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.group_ext_id)
        .order_by(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value).desc())
    )
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
    summary = await _purchases_summaries_by_windows(
        db,
        windows={'current': (date_from, date_to)},
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    current = summary.get(
        'current',
        {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0, 'gross_value': 0.0, 'discount_amount': 0.0},
    )
    current['before_discount_value'] = current.get('cost_amount', current.get('gross_value', 0.0))
    current['after_discount_value'] = current.get('net_value', 0.0)
    return current


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
    doc_key = _fact_purchases_document_key_expr()
    net_expr = _fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.net_value, 0))
    before_discount_expr = _fact_purchases_before_discount_expr()
    expenses_expr = _fact_purchases_signed_amount_expr(_fact_purchases_payload_expenses_expr())
    doc_rows = (
        select(
            doc_key.label('document_key'),
            FactPurchases.supplier_ext_id.label('supplier_ext_id'),
            func.coalesce(func.max(DimSupplier.name), FactPurchases.supplier_ext_id).label('supplier_name'),
            func.coalesce(func.sum(net_expr), 0).label('net_value'),
            func.coalesce(func.max(expenses_expr), 0).label('expenses_value'),
            func.coalesce(func.sum(before_discount_expr), 0).label('cost_amount'),
            func.coalesce(func.sum(_fact_purchase_signed_discount_expr()), 0).label('discount_amount'),
        )
        .select_from(FactPurchases)
        .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_purchases_filters(
        doc_rows, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    doc_rows = doc_rows.group_by(doc_key, FactPurchases.supplier_ext_id).subquery('purchase_supplier_doc_amounts')
    stmt = (
        select(
            doc_rows.c.supplier_ext_id,
            func.coalesce(func.max(doc_rows.c.supplier_name), doc_rows.c.supplier_ext_id).label('supplier_name'),
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(doc_rows.c.discount_amount), 0).label('discount_amount'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.supplier_ext_id)
        .order_by(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value).desc())
    )
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
    doc_key = _fact_purchases_document_key_expr()
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactPurchases.doc_date), Date)
    net_expr = _fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.net_value, 0))
    before_discount_expr = _fact_purchases_before_discount_expr()
    expenses_expr = _fact_purchases_signed_amount_expr(_fact_purchases_payload_expenses_expr())
    qty_expr = _fact_purchases_analysis_qty_expr()
    doc_rows = (
        select(
            doc_key.label('document_key'),
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(net_expr), 0).label('net_value'),
            func.coalesce(func.max(expenses_expr), 0).label('expenses_value'),
            func.coalesce(func.sum(before_discount_expr), 0).label('cost_amount'),
            func.coalesce(func.sum(qty_expr), 0).label('qty'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_purchases_filters(
        doc_rows, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    doc_rows = doc_rows.group_by(doc_key, month_start_expr).subquery('purchase_month_doc_amounts')
    stmt = (
        select(
            doc_rows.c.month_start,
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(doc_rows.c.qty), 0).label('qty'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.month_start)
        .order_by(doc_rows.c.month_start)
    )
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
    return await sales_monthly_trend(
        db,
        date_from,
        date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


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
    return await purchases_monthly_trend(
        db,
        date_from,
        date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


async def purchases_margin_by_supplier(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    supplier_rows: list[dict] | None = None,
):
    rows = supplier_rows
    if rows is None:
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
        # In the supplier margin table:
        # net_value is displayed as "Αγορές προ εκπτώσεων" and comes from SoftOne NODSCAMNT.
        # cost_amount is displayed as "Κόστος" and equals clean value + document expenses.
        actual_cost_value = float(r.get('net_value') or 0)
        before_discount_value = float(r.get('cost_amount') or 0)
        margin_value = before_discount_value - actual_cost_value
        margin_pct = (margin_value / before_discount_value * 100.0) if before_discount_value > 0 else 0.0
        enriched.append(
            {
                'supplier': r['supplier'],
                'supplier_code': r.get('supplier_code'),
                'net_value': before_discount_value,
                'cost_amount': actual_cost_value,
                'before_discount_value': before_discount_value,
                'actual_cost_value': actual_cost_value,
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
    current_rows: list[dict] | None = None,
):
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    current = current_rows
    if current is None:
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
        line_value_expr = _fact_purchases_signed_amount_expr(_fact_purchases_payload_line_value_expr())
        line_discount_pct_expr = func.abs(
            func.coalesce(FactPurchases.discount1_pct, 0)
            + func.coalesce(FactPurchases.discount2_pct, 0)
            + func.coalesce(FactPurchases.discount3_pct, 0)
        )
        qty_expr = _fact_purchases_analysis_qty_expr()
        stmt = (
            select(
                FactPurchases.item_code.label('item_code'),
                func.coalesce(func.sum(line_value_expr), 0).label('value'),
                func.coalesce(func.sum(qty_expr), 0).label('qty'),
                func.coalesce(func.avg(line_discount_pct_expr), 0).label('avg_discount_pct'),
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
            },
        )
        return row

    for code, value, qty, avg_discount_pct in ytd_rows:
        item = ensure(str(code))
        item['ytd_value'] = float(value or 0)
        item['ytd_qty'] = float(qty or 0)
        item['ytd_discount_pct'] = float(avg_discount_pct or 0)

    for code, value, qty, avg_discount_pct in pytd_rows:
        item = ensure(str(code))
        item['pytd_value'] = float(value or 0)
        item['pytd_qty'] = float(qty or 0)
        item['pytd_discount_pct'] = float(avg_discount_pct or 0)

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
        supplier_rows=supplier_distribution,
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
        current_rows=supplier_distribution,
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


async def _sales_item_category_options(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    groups: list[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    sales_items = (
        select(FactSales.item_code)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.item_code.is_not(None))
    )
    sales_items = _apply_fact_sales_filters(
        sales_items,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=None,
        groups=groups,
    ).distinct()

    key_expr = _item_category_key_expr().label('value')
    path_expr = _item_category_path_expr().label('label')
    rows = (
        await db.execute(
            select(key_expr, path_expr)
            .where(DimItem.external_id.in_(sales_items))
            .where(
                or_(
                    _softone_clean_dimension_text(DimItem.category_1).is_not(None),
                    _softone_clean_dimension_text(DimItem.category_2).is_not(None),
                    _softone_clean_dimension_text(DimItem.category_3).is_not(None),
                )
            )
            .distinct()
            .order_by(path_expr)
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
    item_category_values, item_category_labels = await _sales_item_category_options(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        groups=groups,
    )
    if item_category_values:
        existing_categories = set(options.get('categories') or [])
        merged_categories = list(options.get('categories') or [])
        for value in item_category_values:
            if value not in existing_categories:
                existing_categories.add(value)
                merged_categories.append(value)
        options['categories'] = merged_categories
        labels['categories'].update(item_category_labels)

    # Drop category keys that no longer resolve to a readable name — stale
    # ITEMCAT:<hash> values left in the aggregates from a previous category
    # mapping. Filtering still matches current items via dim_items, so these
    # only pollute the dropdown. Keep only options with a real label.
    _cat_labels = labels.get('categories') or {}
    options['categories'] = [
        v for v in (options.get('categories') or [])
        if _cat_labels.get(v) and str(_cat_labels.get(v)) != str(v) and not str(_cat_labels.get(v)).startswith('ITEMCAT:')
    ]

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
    behaviors: list[int] | None = None,
    limit: int = 200,
    offset: int = 0,
    fulfillment_config: dict | None = None,
    document_series_labels: dict[str, str] | None = None,
):
    doc_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    vat_line_expr = _fact_sales_vat_amount_expr()
    net_line_expr = _fact_sales_credit_signed_amount_expr(func.coalesce(FactSales.net_value, 0))
    qty_line_expr = func.abs(_fact_sales_credit_signed_amount_expr(func.coalesce(FactSales.qty, 0)))
    gross_line_expr = case(
        (FactSales.gross_value.is_not(None), _fact_sales_credit_signed_amount_expr(FactSales.gross_value)),
        else_=(net_line_expr + vat_line_expr),
    )
    payload_vat_doc_expr = func.coalesce(func.max(_fact_sales_payload_vat_expr()), 0)
    payload_gross_doc_expr = func.coalesce(func.max(_fact_sales_payload_gross_expr()), 0)
    payload_expenses_doc_expr = func.coalesce(func.max(_fact_sales_payload_expenses_expr()), 0)
    payload_vat_signed_doc_expr = case(
        (func.sum(net_line_expr) < 0, -func.abs(payload_vat_doc_expr)),
        else_=func.abs(payload_vat_doc_expr),
    )
    vat_doc_expr = case(
        (func.abs(payload_vat_doc_expr) > 0.0001, payload_vat_signed_doc_expr),
        else_=func.coalesce(func.sum(vat_line_expr), 0),
    )
    payload_gross_signed_doc_expr = case(
        (func.sum(net_line_expr) < 0, -func.abs(payload_gross_doc_expr)),
        else_=func.abs(payload_gross_doc_expr),
    )
    source_gross_doc_expr = case(
        (func.abs(payload_gross_doc_expr) > 0.0001, payload_gross_signed_doc_expr),
        else_=func.coalesce(func.sum(gross_line_expr), 0),
    )
    expenses_doc_expr = payload_expenses_doc_expr
    gross_doc_expr = func.coalesce(func.sum(net_line_expr), 0) + vat_doc_expr + expenses_doc_expr
    base = (
        select(
            doc_key.label('document_id'),
            func.max(FactSales.document_no).label('document_no'),
            func.max(FactSales.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(FactSales.warehouse_ext_id), literal('')).label('warehouse_code'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactSales.warehouse_ext_id), literal('N/A')).label(
                'warehouse_name'
            ),
            func.coalesce(func.max(FactSales.document_series), literal('')).label('series_code'),
            func.coalesce(
                func.max(FactSales.source_payload_json['document_series_name'].astext),
                func.max(FactSales.document_series),
                func.max(FactSales.document_type),
                literal('N/A'),
            ).label(
                'series_label'
            ),
            func.coalesce(func.max(FactSales.document_status), literal('N/A')).label('status_label'),
            func.coalesce(func.max(FactSales.document_type), literal('N/A')).label('document_type'),
            func.coalesce(func.max(FactSales.channel_name), literal('')).label('channel_name'),
            func.coalesce(func.max(FactSales.eshop_code), literal('')).label('eshop_code'),
            func.coalesce(
                func.max(FactSales.customer_name),
                func.max(FactSales.customer_code),
                literal('ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
            ).label('customer_name'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty_total'),
            func.coalesce(func.sum(func.coalesce(FactSales.qty_executed, FactSales.qty)), 0).label('qty_exec_total'),
            func.coalesce(func.sum(net_line_expr), 0).label('net_value'),
            vat_doc_expr.label('vat_value'),
            gross_doc_expr.label('gross_value'),
            expenses_doc_expr.label('expenses_value'),
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
    if behaviors:
        behavior_vals = [str(int(b)) for b in behaviors if str(b).strip().isdigit()]
        if behavior_vals:
            base = base.where(_fact_sales_behavior_code_text_expr().in_(behavior_vals))

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

    gross_total_expr = gross_doc_expr
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

    page_limit = max(1, min(int(limit), 500))
    page_offset = max(0, int(offset))
    fast_page = (
        gross_min is None
        and gross_max is None
        and not q_clean
        and not channels
        and not behaviors
        and status_clean in {'', 'all'}
        and not series_clean
        and not document_no_clean
        and not eshop_code_clean
        and not customer_clean
        and not from_ref_clean
        and not to_ref_clean
    )
    summary_partial = False
    summary_docs_count = 0
    summary_gross_value = 0.0
    summary_net_value = 0.0
    summary_vat_value = 0.0
    summary_expenses_value = 0.0
    summary_qty_total = 0.0

    if fast_page:
        candidate = (
            select(
                doc_key.label('document_id'),
                func.max(FactSales.doc_date).label('document_date'),
                func.max(func.coalesce(FactSales.source_updated_at, FactSales.updated_at)).label('last_update'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        candidate = _apply_fact_sales_filters(
            candidate,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            channels=channels,
        )
        if behaviors and behavior_vals:
            candidate = candidate.where(_fact_sales_behavior_code_text_expr().in_(behavior_vals))
        if status_clean not in {'', 'all'}:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.document_status, literal('')), String)).like(f'%{status_clean}%')
            )
        if series_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.document_series, FactSales.document_type, literal('')), String)).like(
                    f'%{series_clean}%'
                )
            )
        if document_no_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.document_no, FactSales.document_id, FactSales.external_id), String)).like(
                    f'%{document_no_clean}%'
                )
            )
        if eshop_code_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.eshop_code, literal('')), String)).like(f'%{eshop_code_clean}%')
            )
        if customer_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.customer_name, FactSales.customer_code, literal('')), String)).like(
                    f'%{customer_clean}%'
                )
            )
        if from_ref_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.origin_ref, literal('')), String)).like(f'%{from_ref_clean}%')
            )
        if to_ref_clean:
            candidate = candidate.where(
                func.lower(cast(func.coalesce(FactSales.destination_ref, literal('')), String)).like(f'%{to_ref_clean}%')
            )
        candidate_rows = (
            await db.execute(
                candidate.group_by(doc_key)
                .order_by(
                    literal_column('document_date').desc(),
                    literal_column('last_update').desc(),
                    literal_column('document_id').asc(),
                )
                .offset(page_offset)
                .limit(page_limit + 1)
            )
        ).mappings().all()
        has_more = len(candidate_rows) > page_limit
        candidate_ids = [str(r['document_id']) for r in candidate_rows[:page_limit] if r.get('document_id') is not None]
        if not candidate_ids:
            rows = []
        else:
            docs_sub = base.where(doc_key.in_(candidate_ids)).group_by(doc_key).subquery('sales_docs')
            rows = (
                await db.execute(
                    select(docs_sub).order_by(
                        docs_sub.c.document_date.desc(),
                        docs_sub.c.last_update.desc(),
                        docs_sub.c.document_id.asc(),
                    )
                )
            ).mappings().all()
        summary_values = await sales_summary(
            db,
            date_from,
            date_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        count_stmt = select(func.coalesce(func.count(func.distinct(doc_key)), 0)).select_from(FactSales).where(
            *_date_range(FactSales.doc_date, date_from, date_to)
        )
        count_stmt = _apply_fact_sales_filters(
            count_stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            channels=channels,
        )
        if behaviors and behavior_vals:
            count_stmt = count_stmt.where(_fact_sales_behavior_code_text_expr().in_(behavior_vals))
        if status_clean not in {'', 'all'}:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.document_status, literal('')), String)).like(f'%{status_clean}%')
            )
        if series_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.document_series, FactSales.document_type, literal('')), String)).like(
                    f'%{series_clean}%'
                )
            )
        if document_no_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.document_no, FactSales.document_id, FactSales.external_id), String)).like(
                    f'%{document_no_clean}%'
                )
            )
        if eshop_code_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.eshop_code, literal('')), String)).like(f'%{eshop_code_clean}%')
            )
        if customer_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.customer_name, FactSales.customer_code, literal('')), String)).like(
                    f'%{customer_clean}%'
                )
            )
        if from_ref_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.origin_ref, literal('')), String)).like(f'%{from_ref_clean}%')
            )
        if to_ref_clean:
            count_stmt = count_stmt.where(
                func.lower(cast(func.coalesce(FactSales.destination_ref, literal('')), String)).like(f'%{to_ref_clean}%')
            )
        expenses_doc_sub = (
            select(
                doc_key.label('document_id'),
                func.coalesce(func.max(_fact_sales_payload_expenses_expr()), 0).label('expenses_value'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
        )
        expenses_doc_sub = _apply_fact_sales_filters(
            expenses_doc_sub,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            channels=channels,
        )
        expenses_doc_sub = expenses_doc_sub.group_by(doc_key).subquery('sales_docs_expenses')
        expenses_total = (
            await db.execute(select(func.coalesce(func.sum(expenses_doc_sub.c.expenses_value), 0)).select_from(expenses_doc_sub))
        ).scalar_one()
        summary_docs_count = int((await db.execute(count_stmt)).scalar_one() or 0)
        summary_gross_value = float(summary_values.get('gross_value') or 0)
        summary_net_value = float(summary_values.get('net_value') or 0)
        summary_qty_total = float(summary_values.get('qty') or 0)
        summary_expenses_value = float(expenses_total or 0)
        summary_vat_value = summary_gross_value - summary_net_value
    else:
        docs_sub = base.group_by(doc_key).subquery('sales_docs')
        rows = (
            await db.execute(
                select(
                    docs_sub,
                    func.count().over().label('_docs_count'),
                    func.coalesce(func.sum(docs_sub.c.gross_value).over(), 0).label('_summary_gross_value'),
                    func.coalesce(func.sum(docs_sub.c.net_value).over(), 0).label('_summary_net_value'),
                    func.coalesce(func.sum(docs_sub.c.vat_value).over(), 0).label('_summary_vat_value'),
                    func.coalesce(func.sum(docs_sub.c.expenses_value).over(), 0).label('_summary_expenses_value'),
                    func.coalesce(func.sum(docs_sub.c.qty_total).over(), 0).label('_summary_qty_total'),
                )
                .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
                .offset(page_offset)
                .limit(page_limit)
            )
        ).mappings().all()
        first_row = rows[0] if rows else {}
        summary_docs_count = int(first_row.get('_docs_count') or 0)
        summary_gross_value = float(first_row.get('_summary_gross_value') or 0)
        summary_net_value = float(first_row.get('_summary_net_value') or 0)
        summary_vat_value = float(first_row.get('_summary_vat_value') or 0)
        summary_expenses_value = float(first_row.get('_summary_expenses_value') or 0)
        summary_qty_total = float(first_row.get('_summary_qty_total') or 0)

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
                'warehouse_code': str(r.get('warehouse_code') or ''),
                'warehouse': str(r.get('warehouse_name') or 'N/A'),
                'series': _document_series_label(r.get('series_code'), r.get('series_label') or 'N/A', document_series_labels),
                'document_type': str(r.get('document_type') or 'N/A'),
                'status': str(r.get('status_label') or 'N/A'),
                'eshop_code': str(r.get('eshop_code') or ''),
                'customer': str(r.get('customer_name') or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
                'channel_name': _normalize_sales_channel_name(
                    r.get('channel_name'),
                    r.get('warehouse_code'),
                    fulfillment_config,
                    r.get('series_label'),
                ),
                'total_qty': float(r.get('qty_total') or 0),
                'total_qty_executed': float(r.get('qty_exec_total') or 0),
                'total_net_value': float(r.get('net_value') or 0),
                'total_vat_value': float(r.get('vat_value') or 0),
                'total_gross_value': float(r.get('gross_value') or 0),
                'total_expenses_value': float(r.get('expenses_value') or 0),
                'line_count': int(r.get('line_count') or 0),
                'from_ref': str(r.get('origin_ref') or ''),
                'to_ref': str(r.get('destination_ref') or ''),
                'delivery_info': ' | '.join([part for part in delivery_parts if _blank_zero_text(part)]),
                'comments_info': notes_preview[:220],
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    return {
        'summary': {
            'documents': int(summary_docs_count or 0),
            'gross_value': float(summary_gross_value or 0),
            'net_value': float(summary_net_value or 0),
            'vat_value': float(summary_vat_value or 0),
            'expenses_value': float(summary_expenses_value or 0),
            'qty_total': float(summary_qty_total or 0),
            'partial': bool(summary_partial),
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
    behaviors: list[int] | None = None,
    fulfillment_config: dict | None = None,
    document_series_labels: dict[str, str] | None = None,
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
    if behaviors:
        behavior_vals = [str(int(b)) for b in behaviors if str(b).strip().isdigit()]
        if behavior_vals:
            stmt = stmt.where(_fact_sales_behavior_code_text_expr().in_(behavior_vals))
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
        net_value = float(_normalize_purchase_credit_sign(float(fact.net_value or 0), _is_credit_behavior_code_for_vat_sign_fix(source_payload_row.get('source_transaction_type_id'))))
        gross_value = float(_normalize_purchase_credit_sign(float(fact.gross_value or 0), _is_credit_behavior_code_for_vat_sign_fix(source_payload_row.get('source_transaction_type_id'))))
        behavior_code = source_payload_row.get('source_transaction_type_id')
        vat_value_raw = float(fact.vat_amount if fact.vat_amount is not None else (gross_value - net_value))
        vat_value = _normalize_credit_vat_sign(vat_value_raw, net_value, behavior_code)
        line_total_value = gross_value if fact.gross_value is not None else (net_value + vat_value)
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
        total_gross += line_total_value

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
                'line_total': line_total_value,
                'line_net': net_value,
                'line_external_id': str(fact.external_id or ''),
            }
        )

    doc_no = str(first_fact.document_no or first_fact.document_id or first_fact.external_id or '')
    doc_key_value = str(first_fact.document_id or first_fact.document_no or first_fact.external_id or '')
    expenses_value = _payload_float(
        source_payload,
        'charge_revenue_net_value',
        'shipping_expense_value',
        'shipping_charge_net_value',
        'cod_charge_net_value',
        'charge_revenue_total_net_value',
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
        'charge_revenue_gross_value',
        'charge_revenue_total_gross_value',
    )
    if expenses_value is None:
        residual = total_gross - total_net - total_vat
        expenses_value = float(residual) if abs(residual) > 0.0001 else 0.0
    expenses_value = (
        -abs(float(expenses_value or 0))
        if total_net < 0
        else abs(float(expenses_value or 0))
    )

    doc_net_total = _payload_float(
        source_payload,
        'doc_net_total',
        'net_total',
        'net_value',
        'total_net',
        'DOC_NET_TOTAL',
        'NET_VALUE',
    )
    doc_tax_total = _payload_float(
        source_payload,
        'doc_tax_total',
        'vat_total',
        'vat_value',
        'total_vat',
        'tax_total',
        'tax_amount',
        'DOC_TAX_TOTAL',
        'VAT_AMOUNT',
        'TAX_AMOUNT',
    )
    doc_gross_total = _payload_float(
        source_payload,
        'doc_gross_total',
        'gross_total',
        'gross_value',
        'total_gross',
        'amount_total',
        'DOC_GROSS_TOTAL',
        'GROSS_VALUE',
    )
    payload_totals_are_document_level = doc_net_total is not None and abs(float(doc_net_total) - total_net) < 0.05
    display_vat = float(doc_tax_total) if payload_totals_are_document_level and doc_tax_total is not None else total_vat
    display_gross = total_net + display_vat + float(expenses_value or 0)

    header_series = _payload_code_name(
        source_payload,
        ['series_code', 'series_id', 'series_no'],
        ['series_name', 'series_description', 'document_series'],
        fallback=str(first_fact.document_series or ''),
    )
    header_series = _document_series_label(first_fact.document_series, header_series, document_series_labels)
    header_type = _payload_code_name(
        source_payload,
        ['document_type_code', 'doc_type_code', 'type_code'],
        ['document_type_name', 'doc_type_name', 'document_type', 'type_name'],
        fallback=str(first_fact.document_type or ''),
    )
    header_type = _normalize_sales_document_type_label(
        header_type,
        series_label=header_series,
        payload=source_payload,
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
    header_shipping = _normalize_shipping_method_label(header_shipping, fulfillment_config)
    header_shipping_expense_description = _payload_text(
        source_payload,
        'charge_revenue_description',
        'shipping_expense_description',
        'expense_description',
        'shipping_expense_description',
        'expense_name',
        'expenses_name',
        fallback='',
    )
    header_shipping_expense_value = _payload_float(
        source_payload,
        'shipping_expense_value',
        'shipping_cost',
        'expense_value',
        'expenses_value',
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
            'shipping_expense_description': header_shipping_expense_description,
            'shipping_expense_value': float(header_shipping_expense_value or 0),
            'shipping_method': header_shipping,
            'reason': _blank_zero_text(first_fact.reason),
            'from_ref': str(first_fact.origin_ref or ''),
            'to_ref': str(first_fact.destination_ref or ''),
            'channel_name': _normalize_sales_channel_name(
                getattr(first_fact, 'channel_name', ''),
                first_fact.warehouse_ext_id,
                fulfillment_config,
                header_series,
            ),
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
                fallback=_blank_zero_text(first_fact.delivery_area),
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
            'gross_value': display_gross,
            'net_value': total_net,
            'vat_value': display_vat,
            'expenses_value': expenses_value,
            'qty_total': total_qty,
            'qty_exec_total': total_exec,
            'line_count': len(line_rows),
        },
        'lines': line_rows,
        'lines_note': '',
        'raw_fields': raw_fields,
    }


async def e_shop_analysis_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    page: int = 1,
    page_size: int = 25,
    fulfillment_config: dict | None = None,
):
    rules = normalize_eshop_fulfillment_config(fulfillment_config)
    pickup_warehouses = dict(rules.get('pickup_warehouses') or {})
    store_warehouses = dict(rules.get('store_warehouses') or {})
    pure_eshop_warehouses = {str(x).strip() for x in (rules.get('pure_eshop_warehouses') or []) if str(x).strip()}
    three_pl_warehouses = {str(x).strip() for x in (rules.get('three_pl_warehouses') or []) if str(x).strip()}
    physical_branch_names = {
        _fold_text_for_match(x)
        for x in (rules.get('physical_branch_names') or [])
        if _fold_text_for_match(x)
    }

    def _store_point_label(
        *,
        warehouse_code: str | None = None,
        warehouse_name: str | None = None,
        branch_name: str | None = None,
        shipping_label: str | None = None,
    ) -> str:
        wh_name = _blank_zero_text(warehouse_name, '')
        br_name = _blank_zero_text(branch_name, '')
        ship = _blank_zero_text(shipping_label, '')
        wh_fold = _fold_text_for_match(wh_name)
        br_fold = _fold_text_for_match(br_name)
        ship_fold = _fold_text_for_match(ship)

        if 'wolt' in ship_fold and 'φαρμακει' in wh_fold:
            parts = wh_name.split()
            if parts and _fold_text_for_match(parts[-1]).startswith('φαρμακει'):
                place = ' '.join(parts[:-1]).strip()
                if place:
                    return f'Φαρμακείο {place}'
            return wh_name

        for prefix in ('αποστολη με efood ', 'efood ', 'παραλαβη απο '):
            if ship_fold.startswith(prefix):
                label = ship[len(prefix):].strip(' -')
                if label and _fold_text_for_match(label) != 'wolt':
                    return label

        if 'φαρμακει' in wh_fold:
            parts = wh_name.split()
            if parts and _fold_text_for_match(parts[-1]).startswith('φαρμακει'):
                place = ' '.join(parts[:-1]).strip()
                if place:
                    return f'Φαρμακείο {place}'
            return wh_name
        if br_name and br_name not in {'N/A', 'Χωρίς υποκατάστημα'}:
            return br_name
        return wh_name or str(warehouse_code or '').strip() or 'Χωρίς σημείο'

    def _warehouse_flow(
        code: str | None,
        warehouse_name: str | None = None,
        branch_name: str | None = None,
        shipping_method: str | None = None,
    ) -> tuple[str, str]:
        cleaned = str(code or '').strip()
        shipping_label = _normalize_shipping_method_label(shipping_method, rules)
        shipping_fold = _fold_text_for_match(shipping_label)
        warehouse_fold = _fold_text_for_match(warehouse_name)
        branch_fold = _fold_text_for_match(branch_name)
        if cleaned in pickup_warehouses:
            return ('Παραλαβή από κατάστημα', pickup_warehouses[cleaned])
        if cleaned in store_warehouses:
            if shipping_fold.startswith('παραλαβη απο ') and 'efood' not in shipping_fold and 'wolt' not in shipping_fold:
                return ('Παραλαβή από κατάστημα', store_warehouses[cleaned])
            return ('Αποστολή από κατάστημα', store_warehouses[cleaned])
        if cleaned in pure_eshop_warehouses:
            return ('Καθαρό E-Shop', 'Καθαρή αποθήκη e-shop')
        if cleaned in three_pl_warehouses:
            return ('3PL / Courier πελάτη', 'Κεντρική αποθήκη 3PL')
        looks_physical_store = (
            bool(warehouse_fold and 'φαρμακει' in warehouse_fold)
            or bool(branch_fold and branch_fold in physical_branch_names)
        )
        if looks_physical_store or shipping_fold.startswith(('παραλαβη απο ', 'αποστολη με efood ', 'efood ')):
            point_label = _store_point_label(
                warehouse_code=cleaned,
                warehouse_name=warehouse_name,
                branch_name=branch_name,
                shipping_label=shipping_label,
            )
            if shipping_fold.startswith('παραλαβη απο ') and 'efood' not in shipping_fold and 'wolt' not in shipping_fold:
                return ('Παραλαβή από κατάστημα', point_label)
            return ('Αποστολή από κατάστημα', point_label)
        return ('Λοιπό / Άγνωστο', cleaned or 'Χωρίς αποθήκη')

    def _display_carrier_label(
        carrier_name: str | None,
        warehouse_code: str | None,
        warehouse_name: str | None = None,
        branch_name: str | None = None,
        shipping_method: str | None = None,
    ) -> str:
        carrier = _normalize_shipping_method_label(carrier_name, rules)
        flow_type, flow_label = _warehouse_flow(warehouse_code, warehouse_name, branch_name, shipping_method)
        if carrier and carrier != 'Χωρίς μεταφορική':
            return carrier
        if flow_type == 'Παραλαβή από κατάστημα':
            return f'Παραλαβή από κατάστημα - {flow_label}'
        if flow_type == 'Αποστολή από κατάστημα':
            normalized_shipping = _normalize_shipping_method_label(shipping_method, rules)
            return normalized_shipping or f'Αποστολή από κατάστημα - {flow_label}'
        return 'Χωρίς μεταφορική'

    doc_key = func.coalesce(FactSales.document_id, FactSales.document_no, FactSales.external_id)
    vat_line_expr = _fact_sales_vat_amount_expr()
    net_line_expr = _fact_sales_credit_signed_amount_expr(func.coalesce(FactSales.net_value, 0))
    qty_line_expr = func.abs(_fact_sales_credit_signed_amount_expr(func.coalesce(FactSales.qty, 0)))
    gross_line_expr = case(
        (FactSales.gross_value.is_not(None), _fact_sales_credit_signed_amount_expr(FactSales.gross_value)),
        else_=(net_line_expr + vat_line_expr),
    )
    shipping_charge_doc_expr = func.coalesce(func.max(_fact_sales_payload_shipping_expense_expr()), 0)
    cod_charge_doc_expr = func.coalesce(func.max(_fact_sales_payload_cod_charge_expr()), 0)
    gift_charge_doc_expr = func.coalesce(func.max(_fact_sales_payload_gift_charge_expr()), 0)
    other_charge_doc_expr = func.coalesce(func.max(_fact_sales_payload_other_charge_expr()), 0)
    total_charge_doc_expr = shipping_charge_doc_expr + cod_charge_doc_expr + gift_charge_doc_expr + other_charge_doc_expr
    shipment_doc_expr = func.coalesce(
        func.max(func.nullif(FactSales.shipping_method, '')),
        func.max(func.nullif(FactSales.carrier_name, '')),
        literal('Χωρίς μεταφορική'),
    )
    channel_doc_expr = func.coalesce(func.max(FactSales.channel_name), literal(''))
    city_doc_expr = func.coalesce(func.max(func.nullif(FactSales.delivery_city, '')), literal('Χωρίς πόλη'))
    payment_doc_expr = func.coalesce(func.max(func.nullif(FactSales.payment_method, '')), literal('Χωρίς τρόπο πληρωμής'))

    base = (
        select(
            doc_key.label('document_id'),
            func.max(FactSales.document_no).label('document_no'),
            func.max(FactSales.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactSales.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(FactSales.warehouse_ext_id), literal('')).label('warehouse_code'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactSales.warehouse_ext_id), literal('Χωρίς αποθήκη')).label('warehouse_name'),
            func.coalesce(func.max(FactSales.eshop_code), literal('')).label('eshop_code'),
            channel_doc_expr.label('channel_name'),
            func.coalesce(func.max(FactSales.customer_name), literal('ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ')).label('customer_name'),
            shipment_doc_expr.label('carrier_name'),
            city_doc_expr.label('delivery_city'),
            payment_doc_expr.label('payment_method'),
            func.coalesce(func.sum(qty_line_expr), 0).label('qty_total'),
            func.coalesce(func.sum(net_line_expr), 0).label('net_value'),
            func.coalesce(func.sum(vat_line_expr), 0).label('vat_value'),
            func.coalesce(func.sum(gross_line_expr), 0).label('gross_value'),
            total_charge_doc_expr.label('shipping_expense_value'),
            shipping_charge_doc_expr.label('shipping_charge_value'),
            cod_charge_doc_expr.label('cod_charge_value'),
            gift_charge_doc_expr.label('gift_charge_value'),
            other_charge_doc_expr.label('other_charge_value'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(_fact_sales_eshop_document_expr())
    )
    base = _apply_fact_sales_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=None,
        categories=None,
        groups=None,
        channels=None,
    )
    docs_sub = base.group_by(doc_key).subquery('eshop_docs')

    period_days = max((date_to - date_from).days + 1, 1)
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=period_days - 1)
    prev_base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.sum(qty_line_expr), 0).label('qty_total'),
            func.coalesce(func.sum(gross_line_expr), 0).label('gross_value'),
            total_charge_doc_expr.label('shipping_expense_value'),
        )
        .select_from(FactSales)
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, prev_from, prev_to))
        .where(_fact_sales_eshop_document_expr())
    )
    prev_base = _apply_fact_sales_filters(
        prev_base,
        branches=branches,
        warehouses=warehouses,
        brands=None,
        categories=None,
        groups=None,
        channels=None,
    )
    prev_docs_sub = prev_base.group_by(doc_key).subquery('eshop_prev_docs')

    physical_branch_filter = None
    if physical_branch_names:
        normalized_branch_name = func.lower(func.trim(docs_sub.c.branch_name))
        for accented, plain in (
            ('ά', 'α'),
            ('έ', 'ε'),
            ('ή', 'η'),
            ('ί', 'ι'),
            ('ϊ', 'ι'),
            ('ΐ', 'ι'),
            ('ό', 'ο'),
            ('ύ', 'υ'),
            ('ϋ', 'υ'),
            ('ΰ', 'υ'),
            ('ώ', 'ω'),
        ):
            normalized_branch_name = func.replace(normalized_branch_name, accented, plain)
        physical_branch_filter = normalized_branch_name.in_(physical_branch_names)

    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('orders'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(docs_sub.c.shipping_expense_value), 0).label('shipping_cost'),
                func.coalesce(func.sum(docs_sub.c.shipping_charge_value), 0).label('shipping_charge_value'),
                func.coalesce(func.sum(docs_sub.c.cod_charge_value), 0).label('cod_charge_value'),
                func.coalesce(func.sum(docs_sub.c.gift_charge_value), 0).label('gift_charge_value'),
                func.coalesce(func.sum(docs_sub.c.other_charge_value), 0).label('other_charge_value'),
                func.coalesce(func.sum(docs_sub.c.qty_total), 0).label('qty_total'),
                func.coalesce(func.sum(case((docs_sub.c.carrier_name != 'Χωρίς μεταφορική', 1), else_=0)), 0).label('shipments'),
            )
        )
    ).mappings().one()

    previous_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('orders'),
                func.coalesce(func.sum(prev_docs_sub.c.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(prev_docs_sub.c.shipping_expense_value), 0).label('shipping_cost'),
                func.coalesce(func.sum(prev_docs_sub.c.qty_total), 0).label('qty_total'),
            )
        )
    ).mappings().one()

    physical_branch_rows = (
        await db.execute(
            select(
                docs_sub.c.branch_name,
                func.coalesce(func.count(), 0).label('orders'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(docs_sub.c.shipping_expense_value), 0).label('shipping_cost'),
                func.coalesce(func.sum(docs_sub.c.shipping_charge_value), 0).label('shipping_charge_value'),
                func.coalesce(func.sum(docs_sub.c.cod_charge_value), 0).label('cod_charge_value'),
                func.coalesce(func.sum(docs_sub.c.gift_charge_value), 0).label('gift_charge_value'),
                func.coalesce(func.sum(docs_sub.c.other_charge_value), 0).label('other_charge_value'),
            )
            .where(physical_branch_filter if physical_branch_filter is not None else true())
            .group_by(docs_sub.c.branch_name)
            .order_by(func.sum(docs_sub.c.gross_value).desc(), docs_sub.c.branch_name.asc())
        )
    ).mappings().all()

    city_rows = (
        await db.execute(
            select(
                docs_sub.c.delivery_city,
                func.coalesce(func.count(), 0).label('orders'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
            )
            .group_by(docs_sub.c.delivery_city)
            .order_by(func.count().desc(), func.sum(docs_sub.c.gross_value).desc(), docs_sub.c.delivery_city.asc())
            .limit(10)
        )
    ).mappings().all()

    payment_rows = (
        await db.execute(
            select(
                docs_sub.c.payment_method,
                func.coalesce(func.count(), 0).label('orders'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
            )
            .group_by(docs_sub.c.payment_method)
            .order_by(func.sum(docs_sub.c.gross_value).desc(), docs_sub.c.payment_method.asc())
        )
    ).mappings().all()

    all_docs_rows = (
        await db.execute(
            select(docs_sub)
        )
    ).mappings().all()

    recent_total = int(
        (
            await db.execute(
                select(func.coalesce(func.count(), 0))
                .select_from(docs_sub)
            )
        ).scalar_one()
        or 0
    )
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 25), 200), 5)
    total_pages = max((recent_total + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * page_size

    recent_rows = (
        await db.execute(
            select(docs_sub)
            .order_by(docs_sub.c.document_date.desc(), docs_sub.c.document_id.desc())
            .offset(offset)
            .limit(page_size)
        )
    ).mappings().all()

    orders = int(totals_row['orders'] or 0)
    gross_value = float(totals_row['gross_value'] or 0)
    shipping_cost = float(totals_row['shipping_cost'] or 0)
    shipping_charge_value = float(totals_row['shipping_charge_value'] or 0)
    cod_charge_value = float(totals_row['cod_charge_value'] or 0)
    gift_charge_value = float(totals_row['gift_charge_value'] or 0)
    other_charge_value = float(totals_row['other_charge_value'] or 0)
    qty_total = float(totals_row['qty_total'] or 0)
    shipments = int(totals_row['shipments'] or 0)
    shipping_cost_pct = (shipping_cost / gross_value) if gross_value else 0.0
    avg_order_value = (gross_value / orders) if orders else 0.0
    items_per_order = (qty_total / orders) if orders else 0.0
    prev_orders = int(previous_row['orders'] or 0)
    prev_gross_value = float(previous_row['gross_value'] or 0)
    prev_shipping_cost = float(previous_row['shipping_cost'] or 0)
    prev_qty_total = float(previous_row['qty_total'] or 0)
    prev_avg_order_value = (prev_gross_value / prev_orders) if prev_orders else 0.0
    prev_items_per_order = (prev_qty_total / prev_orders) if prev_orders else 0.0
    prev_shipping_cost_pct = (prev_shipping_cost / prev_gross_value) if prev_gross_value else 0.0
    orders_without_carrier = 0
    top_branch = None
    top_carrier = None
    top_city = None
    top_payment_method = None
    top_flow = None
    flow_rollup: dict[str, dict[str, object]] = {}
    carrier_rollup: dict[str, dict[str, object]] = {}

    for row in all_docs_rows:
        warehouse_code = str(row['warehouse_code'] or '').strip()
        warehouse_name = str(row['warehouse_name'] or '')
        branch_name = str(row['branch_name'] or '')
        shipping_method = str(row['carrier_name'] or '')
        flow_type, flow_label = _warehouse_flow(warehouse_code, warehouse_name, branch_name, shipping_method)
        display_carrier = _display_carrier_label(
            str(row['carrier_name'] or ''),
            warehouse_code,
            warehouse_name,
            branch_name,
            shipping_method,
        )
        bucket = flow_rollup.setdefault(
            flow_type,
            {
                'flow_type': flow_type,
                'flow_label': flow_label,
                'orders': 0,
                'gross_value': 0.0,
                'shipping_cost': 0.0,
                'shipping_charge_value': 0.0,
                'cod_charge_value': 0.0,
                'gift_charge_value': 0.0,
                'other_charge_value': 0.0,
            },
        )
        bucket['orders'] = int(bucket['orders']) + 1
        bucket['gross_value'] = float(bucket['gross_value']) + float(row['gross_value'] or 0)
        bucket['shipping_cost'] = float(bucket['shipping_cost']) + float(row['shipping_expense_value'] or 0)
        bucket['shipping_charge_value'] = float(bucket['shipping_charge_value']) + float(row['shipping_charge_value'] or 0)
        bucket['cod_charge_value'] = float(bucket['cod_charge_value']) + float(row['cod_charge_value'] or 0)
        bucket['gift_charge_value'] = float(bucket['gift_charge_value']) + float(row['gift_charge_value'] or 0)
        bucket['other_charge_value'] = float(bucket['other_charge_value']) + float(row['other_charge_value'] or 0)

        carrier_bucket = carrier_rollup.setdefault(
            display_carrier,
            {
                'carrier_name': display_carrier,
                'orders': 0,
                'gross_value': 0.0,
                'shipping_cost': 0.0,
                'shipping_charge_value': 0.0,
                'cod_charge_value': 0.0,
                'gift_charge_value': 0.0,
                'other_charge_value': 0.0,
            },
        )
        carrier_bucket['orders'] = int(carrier_bucket['orders']) + 1
        carrier_bucket['gross_value'] = float(carrier_bucket['gross_value']) + float(row['gross_value'] or 0)
        carrier_bucket['shipping_cost'] = float(carrier_bucket['shipping_cost']) + float(row['shipping_expense_value'] or 0)
        carrier_bucket['shipping_charge_value'] = float(carrier_bucket['shipping_charge_value']) + float(row['shipping_charge_value'] or 0)
        carrier_bucket['cod_charge_value'] = float(carrier_bucket['cod_charge_value']) + float(row['cod_charge_value'] or 0)
        carrier_bucket['gift_charge_value'] = float(carrier_bucket['gift_charge_value']) + float(row['gift_charge_value'] or 0)
        carrier_bucket['other_charge_value'] = float(carrier_bucket['other_charge_value']) + float(row['other_charge_value'] or 0)

    flow_rows = sorted(
        [
            {
                'flow_type': str(v['flow_type']),
                'flow_label': str(v['flow_label']),
                'orders': int(v['orders']),
                'gross_value': float(v['gross_value']),
                'shipping_cost': float(v['shipping_cost']),
                'shipping_charge_value': float(v['shipping_charge_value']),
                'cod_charge_value': float(v['cod_charge_value']),
                'gift_charge_value': float(v['gift_charge_value']),
                'other_charge_value': float(v['other_charge_value']),
            }
            for v in flow_rollup.values()
        ],
        key=lambda r: (-r['orders'], -r['gross_value'], r['flow_type']),
    )
    if flow_rows:
        top_flow = flow_rows[0]

    carrier_rows = sorted(
        [
            {
                'carrier_name': str(v['carrier_name']),
                'orders': int(v['orders']),
                'gross_value': float(v['gross_value']),
                'shipping_cost': float(v['shipping_cost']),
                'shipping_charge_value': float(v['shipping_charge_value']),
                'cod_charge_value': float(v['cod_charge_value']),
                'gift_charge_value': float(v['gift_charge_value']),
                'other_charge_value': float(v['other_charge_value']),
            }
            for v in carrier_rollup.values()
        ],
        key=lambda r: (-r['shipping_cost'], -r['orders'], r['carrier_name']),
    )

    branch_rows = physical_branch_rows

    if physical_branch_rows:
        best_branch = max(physical_branch_rows, key=lambda r: (float(r['gross_value'] or 0), int(r['orders'] or 0), str(r['branch_name'] or '')))
        top_branch = {
            'branch_name': str(best_branch['branch_name'] or 'N/A'),
            'orders': int(best_branch['orders'] or 0),
            'gross_value': float(best_branch['gross_value'] or 0),
        }

    if carrier_rows:
        best_carrier = max(carrier_rows, key=lambda r: (int(r['orders'] or 0), float(r['shipping_cost'] or 0), str(r['carrier_name'] or '')))
        top_carrier = {
            'carrier_name': str(best_carrier['carrier_name'] or 'Χωρίς μεταφορική'),
            'orders': int(best_carrier['orders'] or 0),
            'shipping_cost': float(best_carrier['shipping_cost'] or 0),
        }
        orders_without_carrier = sum(
            int(r['orders'] or 0)
            for r in carrier_rows
            if str(r['carrier_name'] or 'Χωρίς μεταφορική') == 'Χωρίς μεταφορική'
        )

    if city_rows:
        best_city = max(city_rows, key=lambda r: (int(r['orders'] or 0), float(r['gross_value'] or 0), str(r['delivery_city'] or '')))
        top_city = {
            'delivery_city': str(best_city['delivery_city'] or 'Χωρίς πόλη'),
            'orders': int(best_city['orders'] or 0),
            'gross_value': float(best_city['gross_value'] or 0),
        }

    if payment_rows:
        best_payment = max(payment_rows, key=lambda r: (float(r['gross_value'] or 0), int(r['orders'] or 0), str(r['payment_method'] or '')))
        top_payment_method = {
            'payment_method': str(best_payment['payment_method'] or 'Χωρίς τρόπο πληρωμής'),
            'orders': int(best_payment['orders'] or 0),
            'gross_value': float(best_payment['gross_value'] or 0),
        }

    def _eshop_insight(
        severity: str,
        title: str,
        message: str,
        action: str | None = None,
        metric_label: str | None = None,
        metric_value: float | int | str | None = None,
        metric_kind: str = 'text',
        icon: str = 'activity',
    ) -> dict[str, object]:
        return {
            'severity': severity,
            'title': title,
            'message': message,
            'action': action,
            'metric_label': metric_label,
            'metric_value': metric_value,
            'metric_kind': metric_kind,
            'icon': icon,
        }

    def _pct_delta(current: float, previous: float) -> float | None:
        if previous == 0:
            return None
        return (current - previous) / abs(previous)

    def _pp_delta(current: float, previous: float) -> float | None:
        if previous == 0 and current == 0:
            return 0.0
        return current - previous

    insights: list[dict[str, object]] = []
    if orders <= 0:
        insights.append(
            _eshop_insight(
                'info',
                'Δεν βρέθηκαν e-shop παραγγελίες',
                'Για το επιλεγμένο διάστημα δεν υπάρχει e-shop όγκος με τα τρέχοντα φίλτρα.',
                'Άλλαξε διάστημα ή φίλτρα και έλεγξε ότι τα παραστατικά έχουν e-shop κωδικό ή κανάλι.',
                icon='info',
            )
        )
    else:
        orders_delta = _pct_delta(float(orders), float(prev_orders))
        revenue_delta = _pct_delta(gross_value, prev_gross_value)
        avg_order_delta = _pct_delta(avg_order_value, prev_avg_order_value)
        items_per_order_delta = _pct_delta(items_per_order, prev_items_per_order)
        shipping_pct_delta = _pp_delta(shipping_cost_pct, prev_shipping_cost_pct)

        if revenue_delta is not None and abs(revenue_delta) >= 0.08:
            if revenue_delta < 0:
                insights.append(
                    _eshop_insight(
                        'warning',
                        'Πτώση e-shop εσόδων',
                        'Τα e-shop έσοδα είναι χαμηλότερα από την προηγούμενη αντίστοιχη περίοδο.',
                        'Δες αν η πτώση έρχεται από λιγότερες παραγγελίες, χαμηλότερη μέση παραγγελία ή λιγότερα τεμάχια ανά καλάθι.',
                        'Μεταβολή',
                        revenue_delta,
                        'percent',
                        'trending-down',
                    )
                )
            else:
                insights.append(
                    _eshop_insight(
                        'success',
                        'Άνοδος e-shop εσόδων',
                        'Τα e-shop έσοδα κινούνται καλύτερα από την προηγούμενη αντίστοιχη περίοδο.',
                        'Κράτα σημείωση ποιο μοντέλο εκτέλεσης, κανάλι ή μεταφορική οδηγεί την αύξηση και ενίσχυσέ το.',
                        'Μεταβολή',
                        revenue_delta,
                        'percent',
                        'trending-up',
                    )
                )

        if orders_delta is not None and abs(orders_delta) >= 0.08:
            insights.append(
                _eshop_insight(
                    'warning' if orders_delta < 0 else 'success',
                    'Μεταβολή όγκου παραγγελιών',
                    'Ο αριθμός e-shop παραγγελιών άλλαξε αισθητά σε σχέση με την προηγούμενη αντίστοιχη περίοδο.',
                    'Αν η αξία δεν ακολουθεί τον όγκο, έλεγξε μέση παραγγελία, promo mix και προϊόντα χαμηλής αξίας.',
                    'Μεταβολή',
                    orders_delta,
                    'percent',
                    'shopping-bag',
                )
            )

        if avg_order_delta is not None and avg_order_delta <= -0.05:
            insights.append(
                _eshop_insight(
                    'warning',
                    'Μειώθηκε η μέση παραγγελία',
                    'Ο πελάτης αφήνει λιγότερη αξία ανά παραγγελία από πριν.',
                    'Δούλεψε bundles, όριο δωρεάν μεταφορικών, cross-sell στο checkout και έλεγξε αν τρέχουν εκπτώσεις που χαμηλώνουν το καλάθι.',
                    'Μεταβολή',
                    avg_order_delta,
                    'percent',
                    'trending-down',
                )
            )
        elif avg_order_delta is not None and avg_order_delta >= 0.05:
            insights.append(
                _eshop_insight(
                    'success',
                    'Ανέβηκε η μέση παραγγελία',
                    'Η αξία ανά e-shop παραγγελία βελτιώθηκε σε σχέση με την προηγούμενη περίοδο.',
                    'Βρες ποια προϊόντα ή κανάλια τη σήκωσαν και κράτα την ίδια εμπορική λογική.',
                    'Μεταβολή',
                    avg_order_delta,
                    'percent',
                    'trending-up',
                )
            )

        if items_per_order_delta is not None and items_per_order_delta <= -0.06:
            insights.append(
                _eshop_insight(
                    'warning',
                    'Μειώθηκαν τα τεμάχια ανά παραγγελία',
                    'Το καλάθι έχει λιγότερα τεμάχια ανά παραγγελία, άρα πιθανόν πέφτει το add-on ή το replenishment καλάθι.',
                    'Έλεγξε προτάσεις συμπληρωματικών προϊόντων, minimum basket offers και κατηγορίες που έχασαν τεμάχια.',
                    'Μεταβολή',
                    items_per_order_delta,
                    'percent',
                    'box',
                )
            )
        elif items_per_order_delta is not None and items_per_order_delta >= 0.06:
            insights.append(
                _eshop_insight(
                    'success',
                    'Ανέβηκαν τα τεμάχια ανά παραγγελία',
                    'Το e-shop πουλά περισσότερα τεμάχια ανά καλάθι από πριν.',
                    'Κράτα ενεργά τα bundles και δες ποια κατηγορία τραβάει πολλαπλές τεμάχιες για να την προωθήσεις.',
                    'Μεταβολή',
                    items_per_order_delta,
                    'percent',
                    'box',
                )
            )

        if shipping_pct_delta is not None and shipping_pct_delta >= 0.01:
            insights.append(
                _eshop_insight(
                    'warning',
                    'Αυξήθηκε το βάρος των επιβαρύνσεων',
                    'Οι επιβαρύνσεις αποστολής/αντικαταβολής τρώνε μεγαλύτερο κομμάτι του e-shop revenue από πριν.',
                    'Δες αν άλλαξε courier mix, αντικαταβολές ή παραλαβές. Συζήτησε χρεώσεις με μεταφορικές όπου υπάρχει όγκος.',
                    'Μεταβολή',
                    shipping_pct_delta,
                    'percent',
                    'percent',
                )
            )

        if top_flow:
            flow_share = float(top_flow['orders'] or 0) / orders if orders else 0.0
            insights.append(
                _eshop_insight(
                    'success',
                    'Κύριο μοντέλο εκτέλεσης',
                    f"{top_flow['flow_type']} από {top_flow['flow_label']} συγκεντρώνει {top_flow['orders']} παραγγελίες.",
                    'Χρησιμοποίησέ το σαν baseline για SLA, προσωπικό και stock allocation.',
                    'Μερίδιο',
                    flow_share,
                    'percent',
                    'package',
                )
            )

        if top_branch:
            branch_share = float(top_branch['orders'] or 0) / orders if orders else 0.0
            insights.append(
                _eshop_insight(
                    'info',
                    'Top κατάστημα εκτέλεσης',
                    f"{top_branch['branch_name']} εκτέλεσε {top_branch['orders']} e-shop παραγγελίες.",
                    'Έλεγξε αν το συγκεκριμένο κατάστημα έχει επάρκεια stock και ανθρώπους για τον όγκο που σηκώνει.',
                    'Έσοδα',
                    top_branch['gross_value'],
                    'money',
                    'home',
                )
            )
            if branch_share >= 0.45:
                insights.append(
                    _eshop_insight(
                        'warning',
                        'Συγκέντρωση εκτέλεσης',
                        f"Το {top_branch['branch_name']} κρατά πάνω από το 45% του e-shop όγκου. Θέλει έλεγχο χωρητικότητας και SLA.",
                        'Μοίρασε μέρος του fulfillment ή προετοίμασε stock σε δεύτερο σημείο για να μην γίνει bottleneck.',
                        'Μερίδιο',
                        branch_share,
                        'percent',
                        'alert-triangle',
                    )
                )

        if top_carrier:
            carrier_name = str(top_carrier['carrier_name'] or 'Χωρίς μεταφορική')
            if carrier_name == 'Χωρίς μεταφορική':
                insights.append(
                    _eshop_insight(
                        'warning',
                        'Λείπει μεταφορική',
                        'Η κύρια ομάδα αποστολών δεν έχει καθαρή μεταφορική. Πιθανό mapping ή πληροφορία που δεν κατεβαίνει από SoftOne.',
                        'Διόρθωσε πρώτα το mapping, αλλιώς οι courier αναλύσεις θα οδηγούν σε λάθος αποφάσεις.',
                        'Παραγγελίες',
                        top_carrier['orders'],
                        'number',
                        'alert-circle',
                    )
                )
            else:
                insights.append(
                    _eshop_insight(
                        'info',
                        'Κύρια μεταφορική',
                        f"{carrier_name} είναι η βασική μεταφορική στο επιλεγμένο διάστημα.",
                        'Χρησιμοποίησε τον όγκο σαν διαπραγματευτικό χαρτί για κόστος και χρόνους παράδοσης.',
                        'Αποστολές',
                        top_carrier['orders'],
                        'number',
                        'truck',
                    )
                )

        missing_carrier_share = orders_without_carrier / orders if orders else 0.0
        if orders_without_carrier > 0:
            insights.append(
                _eshop_insight(
                    'warning' if missing_carrier_share >= 0.10 else 'info',
                    'Παραγγελίες χωρίς μεταφορική',
                    f"{orders_without_carrier} e-shop παραγγελίες δεν έχουν ξεκάθαρη μεταφορική. Αν είναι παραλαβές από κατάστημα είναι σωστό, αλλιώς θέλει mapping.",
                    'Χώρισε ξεκάθαρα παραλαβές από κατάστημα και courier για να ξέρεις τι κόστος έχει κάθε μοντέλο.',
                    'Μερίδιο',
                    missing_carrier_share,
                    'percent',
                    'alert-circle',
                )
            )

        if shipping_cost_pct >= 0.05:
            insights.append(
                _eshop_insight(
                    'warning',
                    'Υψηλές επιβαρύνσεις αποστολής',
                    'Οι επιβαρύνσεις αποστολής, αντικαταβολής και λοιπών χρεώσεων είναι υψηλές σε σχέση με τα e-shop έσοδα.',
                    'Δες δωρεάν μεταφορικά, minimum basket, courier τιμοκατάλογο και πόσο συχνά μπαίνει αντικαταβολή.',
                    'Επιβάρυνση / έσοδα',
                    shipping_cost_pct,
                    'percent',
                    'percent',
                )
            )
        elif shipping_cost > 0:
            insights.append(
                _eshop_insight(
                    'success',
                    'Ελεγχόμενες επιβαρύνσεις',
                    'Οι επιβαρύνσεις αποστολής κινούνται σε χαμηλό ποσοστό του e-shop revenue.',
                    'Κράτα τη σημερινή πολιτική και παρακολούθα μόνο αν αλλάξει το courier mix.',
                    'Επιβάρυνση / έσοδα',
                    shipping_cost_pct,
                    'percent',
                    'percent',
                )
            )

        if cod_charge_value > 0:
            insights.append(
                _eshop_insight(
                    'info',
                    'Ξεχωριστή εικόνα αντικαταβολής',
                    'Υπάρχει αξία αντικαταβολής και πλέον διαχωρίζεται από τα έξοδα αποστολής για καθαρότερη ανάλυση.',
                    'Μέτρα αν η αντικαταβολή αξίζει το λειτουργικό κόστος ή αν πρέπει να σπρώξεις online πληρωμές.',
                    'Αντικαταβολή',
                    cod_charge_value,
                    'money',
                    'credit-card',
                )
            )

        if top_city:
            city_name = str(top_city['delivery_city'] or 'Χωρίς πόλη').strip()
            if city_name in {'0', 'Χωρίς πόλη'}:
                insights.append(
                    _eshop_insight(
                        'warning',
                        'Πόλεις παράδοσης χωρίς καθαρή τιμή',
                        'Η μεγαλύτερη ομάδα παραδόσεων εμφανίζεται χωρίς πόλη. Αυτό μειώνει την αξία των γεωγραφικών αναλύσεων.',
                        'Πρώτη προτεραιότητα είναι καθαρισμός/συμπλήρωση πόλης από το παραστατικό παραγγελίας.',
                        'Παραγγελίες',
                        top_city['orders'],
                        'number',
                        'map-pin',
                    )
                )
            else:
                insights.append(
                    _eshop_insight(
                        'info',
                        'Top πόλη παράδοσης',
                        f"{city_name} είναι η πόλη με τον μεγαλύτερο e-shop όγκο.",
                        'Δες αν συμφέρει ειδική courier ρύθμιση, τοπικό stock ή targeted καμπάνια στην περιοχή.',
                        'Έσοδα',
                        top_city['gross_value'],
                        'money',
                        'map-pin',
                    )
                )

        if top_payment_method:
            payment_name = str(top_payment_method['payment_method'] or 'Χωρίς τρόπο πληρωμής').strip()
            if payment_name == 'Χωρίς τρόπο πληρωμής':
                insights.append(
                    _eshop_insight(
                        'warning',
                        'Λείπουν τρόποι πληρωμής',
                        'Δεν υπάρχει καθαρός τρόπος πληρωμής στην κυρίαρχη ομάδα. Θέλει έλεγχο στο πεδίο πληρωμής ή στο παραστατικό παραγγελίας.',
                        'Χωρίς πληρωμή δεν μπορείς να μετρήσεις αντικαταβολές, online payments και cashflow σωστά.',
                        'Παραγγελίες',
                        top_payment_method['orders'],
                        'number',
                        'credit-card',
                    )
                )
            else:
                insights.append(
                    _eshop_insight(
                        'info',
                        'Κύριος τρόπος πληρωμής',
                        f"{payment_name} φέρνει τον μεγαλύτερο όγκο e-shop εσόδων.",
                        'Σύνδεσέ το με κόστος πληρωμών, επιστροφές και χρόνο είσπραξης πριν αλλάξεις εμπορική πολιτική.',
                        'Έσοδα',
                        top_payment_method['gross_value'],
                        'money',
                        'credit-card',
                    )
                )

    return {
        'summary': {
            'orders': orders,
            'gross_value': gross_value,
            'shipping_cost': shipping_cost,
            'shipping_charge_value': shipping_charge_value,
            'cod_charge_value': cod_charge_value,
            'gift_charge_value': gift_charge_value,
            'other_charge_value': other_charge_value,
            'shipments': shipments,
            'qty_total': qty_total,
            'avg_order_value': avg_order_value,
            'items_per_order': items_per_order,
            'avg_shipping_cost': (shipping_cost / shipments) if shipments else 0.0,
            'shipping_cost_pct': shipping_cost_pct,
            'orders_without_carrier': orders_without_carrier,
            'previous_period': {
                'from': prev_from.isoformat(),
                'to': prev_to.isoformat(),
                'orders': prev_orders,
                'gross_value': prev_gross_value,
                'shipping_cost': prev_shipping_cost,
                'qty_total': prev_qty_total,
                'avg_order_value': prev_avg_order_value,
                'items_per_order': prev_items_per_order,
                'shipping_cost_pct': prev_shipping_cost_pct,
            },
            'deltas': {
                'orders_pct': _pct_delta(float(orders), float(prev_orders)),
                'gross_value_pct': _pct_delta(gross_value, prev_gross_value),
                'avg_order_value_pct': _pct_delta(avg_order_value, prev_avg_order_value),
                'items_per_order_pct': _pct_delta(items_per_order, prev_items_per_order),
                'shipping_cost_pct_points': _pp_delta(shipping_cost_pct, prev_shipping_cost_pct),
            },
            'top_branch': top_branch,
            'top_carrier': top_carrier,
            'top_city': top_city,
            'top_payment_method': top_payment_method,
            'top_flow': top_flow,
        },
        'insights': insights,
        'by_branch': [
            {
                'branch_name': str(r['branch_name'] or 'N/A'),
                'orders': int(r['orders'] or 0),
                'gross_value': float(r['gross_value'] or 0),
                'shipping_cost': float(r['shipping_cost'] or 0),
                'shipping_charge_value': float(r['shipping_charge_value'] or 0),
                'cod_charge_value': float(r['cod_charge_value'] or 0),
                'gift_charge_value': float(r['gift_charge_value'] or 0),
                'other_charge_value': float(r['other_charge_value'] or 0),
            }
            for r in branch_rows
        ],
        'by_carrier': [
            {
                'carrier_name': str(r['carrier_name'] or 'Χωρίς μεταφορική'),
                'orders': int(r['orders'] or 0),
                'gross_value': float(r['gross_value'] or 0),
                'shipping_cost': float(r['shipping_cost'] or 0),
                'shipping_charge_value': float(r['shipping_charge_value'] or 0),
                'cod_charge_value': float(r['cod_charge_value'] or 0),
                'gift_charge_value': float(r['gift_charge_value'] or 0),
                'other_charge_value': float(r['other_charge_value'] or 0),
            }
            for r in carrier_rows
        ],
        'by_city': [
            {
                'delivery_city': str(r['delivery_city'] or 'Χωρίς πόλη'),
                'orders': int(r['orders'] or 0),
                'gross_value': float(r['gross_value'] or 0),
            }
            for r in city_rows
        ],
        'by_payment_method': [
            {
                'payment_method': str(r['payment_method'] or 'Χωρίς τρόπο πληρωμής'),
                'orders': int(r['orders'] or 0),
                'gross_value': float(r['gross_value'] or 0),
            }
            for r in payment_rows
        ],
        'by_flow': flow_rows,
        'recent_orders': [
            {
                'document_id': str(r['document_id'] or ''),
                'document_no': str(r['document_no'] or r['document_id'] or ''),
                'document_date': r['document_date'].isoformat() if isinstance(r['document_date'], date) else str(r['document_date'] or ''),
                'branch_name': str(r['branch_name'] or 'N/A'),
                'warehouse_code': str(r['warehouse_code'] or ''),
                'warehouse_name': str(r['warehouse_name'] or 'Χωρίς αποθήκη'),
                'eshop_code': str(r['eshop_code'] or ''),
                'customer_name': str(r['customer_name'] or 'ΠΕΛΑΤΗΣ ΛΙΑΝΙΚΗΣ'),
                'channel_name': str(r['channel_name'] or ''),
                'carrier_name': _display_carrier_label(
                    str(r['carrier_name'] or ''),
                    str(r['warehouse_code'] or ''),
                    str(r['warehouse_name'] or ''),
                    str(r['branch_name'] or ''),
                    str(r['carrier_name'] or ''),
                ),
                'delivery_city': str(r['delivery_city'] or 'Χωρίς πόλη'),
                'payment_method': str(r['payment_method'] or 'Χωρίς τρόπο πληρωμής'),
                'flow_type': _warehouse_flow(
                    str(r['warehouse_code'] or ''),
                    str(r['warehouse_name'] or ''),
                    str(r['branch_name'] or ''),
                    str(r['carrier_name'] or ''),
                )[0],
                'flow_label': _warehouse_flow(
                    str(r['warehouse_code'] or ''),
                    str(r['warehouse_name'] or ''),
                    str(r['branch_name'] or ''),
                    str(r['carrier_name'] or ''),
                )[1],
                'gross_value': float(r['gross_value'] or 0),
                'shipping_expense_value': float(r['shipping_expense_value'] or 0),
                'shipping_charge_value': float(r['shipping_charge_value'] or 0),
                'cod_charge_value': float(r['cod_charge_value'] or 0),
                'gift_charge_value': float(r['gift_charge_value'] or 0),
                'other_charge_value': float(r['other_charge_value'] or 0),
            }
            for r in recent_rows
        ],
        'recent_orders_pagination': {
            'page': page,
            'page_size': page_size,
            'total': recent_total,
            'total_pages': total_pages,
            'has_prev': page > 1,
            'has_next': page < total_pages,
        },
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
    series: str | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
    document_series_labels: dict[str, str] | None = None,
):
    doc_key = _fact_purchases_document_key_expr()
    doc_no_expr = _fact_purchases_document_no_expr(doc_key)
    line_net_doc_expr = func.coalesce(
        func.sum(_fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.net_value, 0))),
        0,
    )
    line_vat_doc_expr = func.coalesce(
        func.sum(_fact_purchases_signed_amount_expr(_fact_purchases_payload_line_vat_expr())),
        0,
    )
    payload_net_doc_expr = func.coalesce(func.max(_fact_purchases_payload_net_expr()), 0)
    payload_expenses_doc_expr = func.coalesce(func.max(_fact_purchases_payload_expenses_expr()), 0)
    payload_vat_doc_expr = func.coalesce(func.max(_fact_purchases_payload_vat_expr()), 0)
    payload_gross_doc_expr = func.coalesce(func.max(_fact_purchases_payload_gross_expr()), 0)
    # Purchase documents must keep item net value and document expenses split.
    # Some SoftOne purchase headers expose NETAMNT with EXPN already included,
    # so the document list trusts MTRLINES for net value whenever lines exist.
    doc_net_expr = case(
        (func.abs(line_net_doc_expr) > 0.0001, line_net_doc_expr),
        else_=payload_net_doc_expr,
    )
    doc_expenses_expr = payload_expenses_doc_expr
    doc_vat_expr = case(
        (func.abs(payload_vat_doc_expr) > 0.0001, payload_vat_doc_expr),
        (func.abs(payload_gross_doc_expr) > 0.0001, payload_gross_doc_expr - doc_net_expr - doc_expenses_expr),
        else_=line_vat_doc_expr,
    )
    doc_gross_expr = doc_net_expr + doc_expenses_expr + doc_vat_expr
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(doc_no_expr), literal('')).label('document_no'),
            func.max(FactPurchases.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), func.max(FactPurchases.branch_ext_id), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimWarehouse.name), func.max(FactPurchases.warehouse_ext_id), literal('N/A')).label(
                'warehouse_name'
            ),
            func.coalesce(func.max(FactPurchases.document_series), literal('')).label('series_code'),
            func.coalesce(
                func.max(FactPurchases.source_payload_json['document_series_name'].astext),
                func.max(FactPurchases.document_series),
                func.max(FactPurchases.document_type),
                literal('Αγορές'),
            ).label(
                'series_label'
            ),
            literal('').label('status_label'),
            func.coalesce(func.max(FactPurchases.document_type), literal('Παραστατικό Αγορών')).label('document_type'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactPurchases.supplier_ext_id), literal('N/A')).label(
                'supplier_name'
            ),
            literal('').label('reason'),
            func.max(func.nullif(func.btrim(FactPurchases.source_payload_json['payment_method'].astext), '')).label(
                'payment_method'
            ),
            func.max(cast(func.nullif(FactPurchases.source_payload_json['payment_terms_days'].astext, ''), Numeric)).label(
                'payment_terms_days'
            ),
            func.max(func.nullif(func.btrim(FactPurchases.source_payload_json['purchase_flow'].astext), '')).label(
                'purchase_flow'
            ),
            func.coalesce(func.sum(_fact_purchases_analysis_qty_expr()), 0).label('qty_total'),
            doc_net_expr.label('net_value'),
            doc_expenses_expr.label('expenses_value'),
            doc_vat_expr.label('vat_value'),
            doc_gross_expr.label('gross_value'),
            func.coalesce(func.sum(_fact_purchases_signed_amount_expr(func.coalesce(FactPurchases.cost_amount, 0))), 0).label(
                'cost_value'
            ),
            func.coalesce(func.sum(func.abs(func.coalesce(FactPurchases.discount_amount, 0))), 0).label('discount_value'),
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

    series_clean = str(series or '').strip().lower()
    if series_clean:
        like = f'%{series_clean}%'
        base = base.where(
            func.lower(cast(func.coalesce(FactPurchases.document_series, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.document_type, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.source_payload_json['document_series_name'].astext, literal('')), String)).like(
                like
            )
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

    page_limit = max(1, min(int(limit), 500))
    page_offset = max(0, int(offset))
    fast_page = not q_clean and not series_clean

    docs_sub = base.group_by(doc_key).subquery('purchase_docs')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('docs_count'),
                func.coalesce(func.sum(docs_sub.c.net_value), 0).label('net_value'),
                func.coalesce(func.sum(docs_sub.c.expenses_value), 0).label('expenses_value'),
                func.coalesce(func.sum(docs_sub.c.vat_value), 0).label('vat_value'),
                func.coalesce(func.sum(docs_sub.c.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(docs_sub.c.cost_value), 0).label('cost_value'),
                func.coalesce(func.sum(docs_sub.c.discount_value), 0).label('discount_value'),
                func.coalesce(func.sum(docs_sub.c.qty_total), 0).label('qty_total'),
                func.coalesce(func.count(func.distinct(docs_sub.c.supplier_name)), 0).label('supplier_count'),
                func.coalesce(func.sum(docs_sub.c.line_count), 0).label('line_count'),
                func.coalesce(func.sum(case((docs_sub.c.cost_value > 0, docs_sub.c.cost_value), else_=0)), 0).label(
                    'purchase_value'
                ),
                func.coalesce(func.sum(case((docs_sub.c.cost_value > 0, 1), else_=0)), 0).label('purchase_docs'),
                func.coalesce(func.sum(case((docs_sub.c.cost_value < 0, func.abs(docs_sub.c.cost_value)), else_=0)), 0).label(
                    'credit_value'
                ),
                func.coalesce(func.sum(case((docs_sub.c.cost_value < 0, 1), else_=0)), 0).label('credit_docs'),
                func.coalesce(func.avg(docs_sub.c.payment_terms_days), 0).label('avg_payment_terms_days'),
                func.coalesce(func.count(func.distinct(docs_sub.c.payment_method)), 0).label('payment_method_count'),
                func.coalesce(func.count(docs_sub.c.payment_terms_days), 0).label('payment_terms_docs'),
            )
        )
    ).mappings().one()

    supplier_spend_expr = func.coalesce(
        func.sum(case((docs_sub.c.cost_value > 0, docs_sub.c.cost_value), else_=0)),
        0,
    )
    supplier_rows = (
        await db.execute(
            select(
                docs_sub.c.supplier_name.label('supplier_name'),
                supplier_spend_expr.label('spend_value'),
                func.coalesce(func.count(), 0).label('documents'),
            )
            .group_by(docs_sub.c.supplier_name)
            .having(supplier_spend_expr > 0)
            .order_by(supplier_spend_expr.desc())
            .limit(8)
        )
    ).mappings().all()

    if fast_page:
        candidate = (
            select(
                doc_key.label('document_id'),
                func.max(FactPurchases.doc_date).label('document_date'),
                func.max(FactPurchases.updated_at).label('last_update'),
            )
            .select_from(FactPurchases)
            .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        )
        candidate = _apply_fact_purchases_filters(
            candidate,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        candidate_rows = (
            await db.execute(
                candidate.group_by(doc_key)
                .order_by(
                    literal_column('document_date').desc(),
                    literal_column('last_update').desc(),
                    literal_column('document_id').asc(),
                )
                .offset(page_offset)
                .limit(page_limit)
            )
        ).mappings().all()
        candidate_ids = [str(r['document_id']) for r in candidate_rows if r.get('document_id') is not None]
        if candidate_ids:
            page_docs_sub = base.where(doc_key.in_(candidate_ids)).group_by(doc_key).subquery('purchase_docs_page')
            rows = (
                await db.execute(
                    select(page_docs_sub).order_by(
                        page_docs_sub.c.document_date.desc(),
                        page_docs_sub.c.last_update.desc(),
                        page_docs_sub.c.document_id.asc(),
                    )
                )
            ).mappings().all()
        else:
            rows = []
    else:
        rows = (
            await db.execute(
                select(docs_sub)
                .order_by(docs_sub.c.document_date.desc(), docs_sub.c.last_update.desc(), docs_sub.c.document_id.asc())
                .offset(page_offset)
                .limit(page_limit)
            )
        ).mappings().all()

    out_rows = []
    for r in rows:
        doc_date_val = r.get('document_date')
        branch_label = _normalize_purchase_branch_label(r.get('branch_name'), None)
        series_label = _document_series_label(r.get('series_code'), r.get('series_label'), document_series_labels)
        document_type_label = _normalize_purchase_document_type_label(
            r.get('document_type'),
            series_label=series_label,
        )
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': branch_label,
                'warehouse': str(r.get('warehouse_name') or 'N/A'),
                'series': series_label or document_type_label,
                'document_type': document_type_label,
                'status': str(r.get('status_label') or ''),
                'supplier': str(r.get('supplier_name') or 'N/A'),
                'reason': str(r.get('reason') or ''),
                'total_qty': float(r.get('qty_total') or 0),
                'total_net_value': float(r.get('net_value') or 0),
                'total_expenses_value': float(r.get('expenses_value') or 0),
                'total_vat_value': float(r.get('vat_value') or 0),
                'total_gross_value': float(r.get('gross_value') or 0),
                'total_cost_value': float(r.get('cost_value') or 0),
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )

    docs_count = int(totals_row['docs_count'] or 0)
    net_spend_value = float(totals_row['cost_value'] or 0)
    period_spend_value = float(totals_row['net_value'] or 0) + float(totals_row['expenses_value'] or 0)
    spend_value = period_spend_value
    purchase_docs = int(totals_row['purchase_docs'] or 0)
    supplier_count = int(totals_row['supplier_count'] or 0)
    discount_value = float(totals_row['discount_value'] or 0)
    credit_value = float(totals_row['credit_value'] or 0)
    credit_docs = int(totals_row['credit_docs'] or 0)
    line_count = int(totals_row['line_count'] or 0)
    avg_payment_terms_days = float(totals_row['avg_payment_terms_days'] or 0)
    payment_method_count = int(totals_row['payment_method_count'] or 0)
    payment_terms_docs = int(totals_row['payment_terms_docs'] or 0)
    payment_terms_coverage_pct = (payment_terms_docs / docs_count * 100.0) if docs_count else 0.0
    avg_document_value = spend_value / purchase_docs if purchase_docs else 0.0
    avg_lines_per_doc = line_count / docs_count if docs_count else 0.0
    spend_base = abs(spend_value)
    discount_pct = (discount_value / (spend_base + discount_value) * 100.0) if (spend_base + discount_value) > 0 else 0.0
    credit_pct = (credit_value / spend_base * 100.0) if spend_base > 0 else 0.0
    top_supplier = supplier_rows[0] if supplier_rows else {}
    top_supplier_value = float(top_supplier.get('spend_value') or 0)
    top_supplier_share = (top_supplier_value / spend_value * 100.0) if spend_value > 0 else 0.0
    period_days = max(1, (date_to - date_from).days + 1)
    avg_daily_spend = spend_value / period_days

    return {
        'summary': {
            'documents': docs_count,
            'net_value': float(totals_row['net_value'] or 0),
            'cost_value': spend_value,
            'vat_value': float(totals_row['vat_value'] or 0),
            'expenses_value': float(totals_row['expenses_value'] or 0),
            'gross_value': float(totals_row['gross_value'] or 0),
            'qty_total': float(totals_row['qty_total'] or 0),
        },
        'intelligence': {
            'period_days': period_days,
            'spend_value': spend_value,
            'net_spend_value': net_spend_value,
            'purchase_docs': purchase_docs,
            'avg_daily_spend': avg_daily_spend,
            'documents': docs_count,
            'supplier_count': supplier_count,
            'avg_document_value': avg_document_value,
            'avg_lines_per_document': avg_lines_per_doc,
            'discount_value': discount_value,
            'discount_pct': discount_pct,
            'credit_value': credit_value,
            'credit_docs': credit_docs,
            'credit_pct': credit_pct,
            'avg_payment_terms_days': avg_payment_terms_days,
            'payment_method_count': payment_method_count,
            'payment_terms_coverage_pct': payment_terms_coverage_pct,
            'top_supplier_name': str(top_supplier.get('supplier_name') or 'N/A'),
            'top_supplier_value': top_supplier_value,
            'top_supplier_share': top_supplier_share,
            'top_suppliers': [
                {
                    'supplier': str(row.get('supplier_name') or 'N/A'),
                    'spend_value': float(row.get('spend_value') or 0),
                    'documents': int(row.get('documents') or 0),
                    'share_pct': (float(row.get('spend_value') or 0) / spend_value * 100.0) if spend_value > 0 else 0.0,
                }
                for row in supplier_rows
            ],
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
    document_series_labels: dict[str, str] | None = None,
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
    first_payload = first_fact.source_payload_json if isinstance(first_fact.source_payload_json, dict) else {}
    branch_code = _strip_tenant_prefix(first_fact.branch_ext_id)
    branch_name = _normalize_purchase_branch_label(rows[0][2], first_fact.branch_ext_id)
    warehouse_name = str(rows[0][3] or first_fact.warehouse_ext_id or 'N/A')
    supplier_name = str(rows[0][4] or first_fact.supplier_ext_id or 'N/A')
    document_no = _purchase_document_no_from_fact(first_fact, doc_id)
    series_label = _document_series_label(
        first_fact.document_series,
        first_payload.get('document_series_name') or first_fact.document_series or first_fact.document_type or 'Αγορές',
        document_series_labels,
    )
    document_type_label = _normalize_purchase_document_type_label(
        first_fact.document_type,
        series_label=series_label,
        payload=first_payload,
    )

    line_rows = []
    total_qty = 0.0
    total_net = 0.0
    total_vat = 0.0
    total_gross = 0.0
    total_cost = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactPurchases = row[0]
        payload = fact.source_payload_json if isinstance(fact.source_payload_json, dict) else {}
        is_credit_doc = _purchase_is_credit_payload(payload)
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
        raw_net_value = float(fact.net_value or 0)
        raw_cost_value = float(fact.cost_amount or 0)
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
            vat_value = gross_value - raw_net_value
        if vat_value is None:
            vat_value = 0.0
        if gross_value is None:
            gross_value = raw_net_value + vat_value

        net_value = _normalize_purchase_credit_sign(raw_net_value, is_credit_doc)
        cost_value = _normalize_purchase_credit_sign(raw_cost_value, is_credit_doc)
        vat_value = _normalize_purchase_credit_sign(float(vat_value), is_credit_doc)
        gross_value = _normalize_purchase_credit_sign(float(gross_value), is_credit_doc)
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

    is_credit_header = _purchase_is_credit_payload(first_payload)
    header_net = _payload_float(first_payload, 'doc_net_total', 'DOC_NET_TOTAL', 'net_total', 'net_value_total')
    header_expenses = _payload_float(
        first_payload,
        'doc_expenses_total',
        'DOC_EXPENSES_TOTAL',
        'expenses_value',
        'expense_value',
        'expenses_amount',
        'expense_amount',
        'total_expenses',
        'expenses_total',
        'expn',
        'EXPN',
    )
    header_vat = _payload_float(
        first_payload,
        'doc_tax_total',
        'DOC_TAX_TOTAL',
        'vat_total',
        'vat_value',
        'total_vat',
        'tax_total',
        'tax_amount',
        'fpa_total',
        'fpa_amount',
    )
    header_gross = _payload_float(
        first_payload,
        'doc_gross_total',
        'DOC_GROSS_TOTAL',
        'gross_total',
        'total_gross',
        'amount_total',
        'total_value',
        'value_total',
        'gross_value',
        'GROSS_VALUE',
    )
    if abs(total_net) <= 0.0001 and header_net is not None and abs(header_net) > 0.0001:
        total_net = _normalize_purchase_credit_sign(header_net, is_credit_header)
    if header_vat is not None:
        total_vat = _normalize_purchase_credit_sign(header_vat, is_credit_header)
    expenses_value = (
        _normalize_purchase_credit_sign(header_expenses, is_credit_header)
        if header_expenses is not None
        else total_gross - total_net - total_vat
    )
    total_gross = total_net + expenses_value + total_vat
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
            'branch_code': branch_code,
            'branch_name': branch_name,
            'warehouse_code': str(first_fact.warehouse_ext_id or ''),
            'warehouse_name': warehouse_name,
            'series': series_label,
            'document_type': document_type_label,
            'document_type_label': document_type_label,
            'document_type_raw': str(first_fact.document_type or ''),
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
    series: str | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
    document_series_labels: dict[str, str] | None = None,
):
    doc_key = _fact_inventory_document_key_expr()
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactInventory.document_no), func.max(doc_key), literal('')).label('document_no'),
            func.max(FactInventory.doc_date).label('document_date'),
            func.coalesce(
                func.max(DimBranch.name),
                func.max(cast(FactInventory.source_payload_json['branch_name'].astext, String)),
                func.max(FactInventory.branch_ext_id),
                literal('N/A'),
            ).label('branch_name'),
            func.coalesce(func.max(FactInventory.branch_ext_id), literal('')).label('branch_code'),
            func.coalesce(
                func.max(DimWarehouse.name),
                func.max(cast(FactInventory.source_payload_json['warehouse_name'].astext, String)),
                func.max(FactInventory.warehouse_ext_id),
                literal('N/A'),
            ).label('warehouse_name'),
            func.coalesce(func.max(FactInventory.warehouse_ext_id), literal('')).label('warehouse_code'),
            func.coalesce(func.max(FactInventory.document_series), literal('')).label('series_code'),
            func.coalesce(
                func.max(FactInventory.source_payload_json['document_series_name'].astext),
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
        base = base.where(FactInventory.branch_ext_id.in_(branches))
    if warehouses:
        base = base.where(FactInventory.warehouse_ext_id.in_(warehouses))
    if brands:
        base = base.where(DimBrand.external_id.in_(brands))
    if categories:
        base = base.where(DimCategory.external_id.in_(categories))
    if groups:
        base = base.where(DimGroup.external_id.in_(groups))

    series_clean = str(series or '').strip().lower()
    if series_clean:
        like = f'%{series_clean}%'
        base = base.where(
            func.lower(cast(func.coalesce(FactInventory.document_series, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactInventory.document_type, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactInventory.source_payload_json['document_series_name'].astext, literal('')), String)).like(
                like
            )
        )

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
                'series': _document_series_label(
                    r.get('series_code'),
                    r.get('series_label') or r.get('document_type') or 'Κίνηση Αποθήκης',
                    document_series_labels,
                ),
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
            'net_value': float(totals_row['value_total'] or 0),
            'vat_value': 0.0,
            'expenses_value': 0.0,
            'gross_value': float(totals_row['value_total'] or 0),
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
    document_series_labels: dict[str, str] | None = None,
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
        stmt = stmt.where(FactInventory.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactInventory.warehouse_ext_id.in_(warehouses))
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
    first_payload = first_fact.source_payload_json if isinstance(first_fact.source_payload_json, dict) else {}
    branch_code = str(rows[0][3] or first_fact.branch_ext_id or '')
    branch_name = str(rows[0][4] or first_payload.get('branch_name') or first_fact.branch_ext_id or 'N/A')
    warehouse_code = str(rows[0][5] or first_fact.warehouse_ext_id or '')
    warehouse_name = str(rows[0][6] or first_payload.get('warehouse_name') or first_fact.warehouse_ext_id or 'N/A')

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
            'series': _document_series_label(
                first_fact.document_series,
                first_payload.get('document_series_name') or first_fact.document_series or first_fact.document_type or 'Κίνηση Αποθήκης',
                document_series_labels,
            ),
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
    series: str | None = None,
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    branch_filter = _effective_branch_filter(branches)
    branch_name_dim = aliased(DimBranch)
    category_dim = aliased(DimExpenseCategory)
    document_type_dim = aliased(DimDocumentType)
    doc_key = func.coalesce(FactExpense.document_no, FactExpense.external_id)
    net_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_net, 0))
    tax_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_tax, 0))
    gross_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_gross, 0))
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactExpense.document_no), func.max(FactExpense.external_id), literal('')).label('document_no'),
            func.max(FactExpense.expense_date).label('document_date'),
            func.coalesce(
                func.max(DimBranch.name),
                func.max(branch_name_dim.name),
                func.max(FactExpense.branch_ext_id),
                literal('N/A'),
            ).label('branch_name'),
            func.coalesce(
                func.max(DimExpenseCategory.category_name),
                func.max(category_dim.category_name),
                func.max(FactExpense.expense_category_code),
                literal('N/A'),
            ).label('category_name'),
            func.coalesce(
                func.max(document_type_dim.name),
                func.max(FactExpense.document_type),
                literal('Παραστατικό Εξόδων'),
            ).label('document_type'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactExpense.supplier_ext_id), literal('N/A')).label('supplier_name'),
            func.coalesce(func.sum(net_amount_expr), 0).label('amount_net'),
            func.coalesce(func.sum(tax_amount_expr), 0).label('amount_tax'),
            func.coalesce(func.sum(gross_amount_expr), 0).label('amount_gross'),
            func.count(FactExpense.id).label('line_count'),
            func.max(FactExpense.updated_at).label('last_update'),
        )
        .select_from(FactExpense)
        .join(DimBranch, DimBranch.id == FactExpense.branch_id, isouter=True)
        .join(DimExpenseCategory, DimExpenseCategory.id == FactExpense.category_id, isouter=True)
        .join(branch_name_dim, branch_name_dim.external_id == FactExpense.branch_ext_id, isouter=True)
        .join(category_dim, category_dim.category_code == FactExpense.expense_category_code, isouter=True)
        .join(document_type_dim, document_type_dim.external_id == FactExpense.document_type, isouter=True)
        .join(DimSupplier, DimSupplier.id == FactExpense.supplier_id, isouter=True)
        .where(*_date_range(FactExpense.expense_date, date_from, date_to))
    )
    if branch_filter is not None:
        base = base.where(FactExpense.branch_ext_id.in_(branch_filter))
    if categories:
        base = base.where(FactExpense.expense_category_code.in_(categories))
    series_clean = str(series or '').strip().lower()
    if series_clean:
        base = base.where(
            func.lower(cast(func.coalesce(FactExpense.document_type, literal('')), String)).like(f'%{series_clean}%')
        )
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
        branch_name = _normalize_expense_branch_label(r.get('branch_name'), None)
        category_name = str(r.get('category_name') or 'N/A')
        document_type = _normalize_expense_document_type_label(r.get('document_type'), category_name=category_name)
        amount_net = float(r.get('amount_net') or 0)
        amount_tax = float(r.get('amount_tax') or 0)
        amount_gross = float(r.get('amount_gross') or 0)
        out_rows.append(
            {
                'document_id': str(r.get('document_id') or ''),
                'document_no': str(r.get('document_no') or r.get('document_id') or ''),
                'document_date': doc_date_val.isoformat() if isinstance(doc_date_val, date) else str(doc_date_val or ''),
                'branch': branch_name,
                'category': category_name,
                'document_type': document_type,
                'supplier': str(r.get('supplier_name') or 'N/A'),
                'total_net_value': amount_net,
                'total_expenses_value': 0.0,
                'total_tax_value': amount_tax,
                'total_gross_value': amount_gross,
                'line_count': int(r.get('line_count') or 0),
                'last_update': _raw_scalar(r.get('last_update')),
            }
        )
    summary_net = float(totals_row['amount_net'] or 0)
    return {
        'summary': {
            'documents': int(totals_row['docs_count'] or 0),
            'amount_net': float(totals_row['amount_net'] or 0),
            'amount_tax': float(totals_row['amount_tax'] or 0),
            'amount_gross': float(totals_row['amount_gross'] or 0),
            'net_value': summary_net,
            'vat_value': float(totals_row['amount_tax'] or 0),
            'gross_value': float(totals_row['amount_gross'] or 0),
            'expenses_value': 0.0,
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
    branch_name_dim = aliased(DimBranch)
    category_dim = aliased(DimExpenseCategory)
    document_type_dim = aliased(DimDocumentType)
    doc_key = func.coalesce(FactExpense.document_no, FactExpense.external_id)
    stmt = (
        select(
            FactExpense,
            DimBranch.name.label('branch_name'),
            DimExpenseCategory.category_name.label('category_name'),
            branch_name_dim.name.label('branch_name_fallback'),
            category_dim.category_name.label('category_name_fallback'),
            document_type_dim.name.label('document_type_name'),
            DimSupplier.name.label('supplier_name'),
            DimAccount.name.label('account_name'),
        )
        .select_from(FactExpense)
        .join(DimBranch, DimBranch.id == FactExpense.branch_id, isouter=True)
        .join(DimExpenseCategory, DimExpenseCategory.id == FactExpense.category_id, isouter=True)
        .join(branch_name_dim, branch_name_dim.external_id == FactExpense.branch_ext_id, isouter=True)
        .join(category_dim, category_dim.category_code == FactExpense.expense_category_code, isouter=True)
        .join(document_type_dim, document_type_dim.external_id == FactExpense.document_type, isouter=True)
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
    branch_name = _normalize_expense_branch_label(rows[0][1] or rows[0][3], first_fact.branch_ext_id)
    category_name = str(rows[0][2] or rows[0][4] or first_fact.expense_category_code or 'N/A')
    document_type_name = _normalize_expense_document_type_label(rows[0][5] or first_fact.document_type, category_name=category_name)
    supplier_name = str(rows[0][6] or first_fact.supplier_ext_id or 'N/A')
    account_name = str(rows[0][7] or first_fact.account_ext_id or '')
    line_rows = []
    total_net = 0.0
    total_tax = 0.0
    total_gross = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactExpense = row[0]
        line_category_name = str(row[2] or row[4] or fact.expense_category_code or '')
        doc_type = _normalize_expense_document_type_label(row[5] or fact.document_type or first_fact.document_type, category_name=line_category_name)
        amount_net = _normalize_expense_credit_sign(float(fact.amount_net or 0), doc_type)
        amount_tax = _normalize_expense_credit_sign(float(fact.amount_tax or 0), doc_type)
        amount_gross = _normalize_expense_credit_sign(float(fact.amount_gross or 0), doc_type)
        total_net += amount_net
        total_tax += amount_tax
        total_gross += amount_gross
        line_rows.append(
            {
                'row_no': idx,
                'category': line_category_name,
                'supplier': str(row[6] or fact.supplier_ext_id or ''),
                'account': str(row[7] or fact.account_ext_id or ''),
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
            'document_type': document_type_name,
            'supplier_name': supplier_name,
            'account_name': account_name,
            'payment_status': str(first_fact.payment_status or ''),
            'cost_center': str(first_fact.cost_center or ''),
        },
        'totals': {
            'net_value': total_net,
            'expenses_value': 0.0,
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
                func.coalesce(func.sum(_fact_purchases_analysis_qty_expr()), 0).label('qty'),
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
    if _can_use_behavior_aware_sales_aggregate():
        agg_stmt = (
            select(
                AggSalesMonthly.month_start,
                func.coalesce(func.sum(AggSalesMonthly.net_value), 0).label('net_value'),
                func.coalesce(func.sum(AggSalesMonthly.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(AggSalesMonthly.qty), 0).label('qty'),
            )
            .where(*_date_range(AggSalesMonthly.month_start, date_from, date_to))
            .group_by(AggSalesMonthly.month_start)
            .order_by(AggSalesMonthly.month_start)
        )
        agg_stmt = _apply_behavior_filter_to_aggregate(agg_stmt, AggSalesMonthly.behavior_code)
        agg_stmt = _apply_sales_monthly_filters(
            agg_stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        agg_rows = (await db.execute(agg_stmt)).all()
        if agg_rows:
            return [
                {
                    'month_start': str(r[0]),
                    'net_value': float(r[1] or 0),
                    'gross_value': float(r[2] or 0),
                    'qty': float(r[3] or 0),
                }
                for r in agg_rows
            ]

    doc_key = _fact_sales_document_key_expr()
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
    doc_rows = (
        select(
            doc_key.label('document_key'),
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
            func.coalesce(func.sum(func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)), 0).label('qty'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_sales_filters(
        doc_rows,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, month_start_expr).subquery('sales_month_doc_amounts')
    stmt = (
        select(
            doc_rows.c.month_start,
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.gross_value + doc_rows.c.expenses_value), 0).label('gross_value'),
            func.coalesce(func.sum(doc_rows.c.qty), 0).label('qty'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.month_start)
        .order_by(doc_rows.c.month_start)
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


async def sales_monthly_company_totals(
    db: AsyncSession,
    date_from: date,
    date_to: date,
):
    if _can_use_behavior_aware_sales_aggregate():
        # agg_sales_daily_company is grouped per (doc_date, behavior_code) and its
        # net/gross already include the per-document expenses, exactly matching what
        # the fact path below computes. The participation whitelist is applied on
        # read, which is why this no longer has to fall back to scanning fact_sales.
        month_expr = cast(func.date_trunc(literal_column("'month'"), AggSalesDailyCompany.doc_date), Date)
        agg_stmt = (
            select(
                month_expr.label('month_start'),
                func.coalesce(func.sum(AggSalesDailyCompany.net_value), 0).label('net_value'),
                func.coalesce(func.sum(AggSalesDailyCompany.gross_value), 0).label('gross_value'),
                func.coalesce(func.sum(AggSalesDailyCompany.qty), 0).label('qty'),
            )
            .where(*_date_range(AggSalesDailyCompany.doc_date, date_from, date_to))
        )
        agg_stmt = _apply_behavior_filter_to_aggregate(agg_stmt, AggSalesDailyCompany.behavior_code)
        agg_stmt = agg_stmt.group_by(month_expr).order_by(month_expr)
        agg_rows = (await db.execute(agg_stmt)).all()
        if agg_rows:
            return [
                {
                    'month_start': str(r[0]),
                    'net_value': float(r[1] or 0),
                    'gross_value': float(r[2] or 0),
                    'qty': float(r[3] or 0),
                }
                for r in agg_rows
            ]

    doc_key = _fact_sales_document_key_expr()
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), FactSales.doc_date), Date)
    doc_rows = (
        select(
            doc_key.label('document_key'),
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(_fact_sales_signed_net_expr()), 0).label('net_value'),
            func.coalesce(func.sum(_fact_sales_signed_gross_expr()), 0).label('gross_value'),
            func.coalesce(func.max(_fact_sales_signed_expenses_expr()), 0).label('expenses_value'),
            func.coalesce(func.sum(func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)), 0).label('qty'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    doc_rows = _apply_fact_sales_behavior_rules(doc_rows)
    doc_rows = _apply_fact_sales_turnover_rules(doc_rows)
    doc_rows = doc_rows.group_by(doc_key, month_start_expr).subquery('sales_company_month_doc_amounts')
    stmt = (
        select(
            doc_rows.c.month_start,
            func.coalesce(func.sum(doc_rows.c.net_value + doc_rows.c.expenses_value), 0).label('net_value'),
            func.coalesce(func.sum(doc_rows.c.gross_value + doc_rows.c.expenses_value), 0).label('gross_value'),
            func.coalesce(func.sum(doc_rows.c.qty), 0).label('qty'),
        )
        .select_from(doc_rows)
        .group_by(doc_rows.c.month_start)
        .order_by(doc_rows.c.month_start)
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
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    current = await sales_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    previous = await sales_summary(
        db,
        date_from=prev_from,
        date_to=prev_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )

    async def _sales_cost_for_period(start: date, end: date) -> float:
        stmt = (
            select(func.coalesce(func.sum(_fact_sales_signed_cost_expr()), 0))
            .where(*_date_range(FactSales.doc_date, start, end))
        )
        stmt = _apply_fact_sales_filters(
            stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        stmt = _apply_fact_sales_behavior_rules(stmt)
        stmt = _apply_fact_sales_turnover_rules(stmt)
        return float((await db.execute(stmt)).scalar_one() or 0)

    current['cost_amount'] = await _sales_cost_for_period(date_from, date_to)
    previous['cost_amount'] = await _sales_cost_for_period(prev_from, prev_to)
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
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return {
            'snapshot_date': None,
            'qty_on_hand': 0.0,
            'qty_reserved': 0.0,
            'cost_amount': 0.0,
            'value_amount': 0.0,
        }

    if not any([branches, warehouses, brands, categories, groups]) and _effective_branch_filter(None) is None:
        agg_row = (
            await db.execute(
                select(
                    func.coalesce(func.sum(AggInventorySnapshotDaily.qty_on_hand), 0),
                    func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0),
                )
                .select_from(AggInventorySnapshotDaily)
                .where(AggInventorySnapshotDaily.snapshot_date == snapshot_date)
            )
        ).one()
        if float(agg_row[0] or 0) or float(agg_row[1] or 0):
            return {
                'snapshot_date': str(snapshot_date),
                'qty_on_hand': float(agg_row[0] or 0),
                'qty_reserved': 0.0,
                'cost_amount': float(agg_row[1] or 0),
                'value_amount': float(agg_row[1] or 0),
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
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    )
    row = (await db.execute(stmt)).one()
    return {
        'snapshot_date': str(snapshot_date),
        'qty_on_hand': float(row[0] or 0),
        'qty_reserved': float(row[1] or 0),
        'cost_amount': float(row[2] or 0),
        'value_amount': float(row[3] or 0),
    }


async def _latest_inventory_snapshot_date(db: AsyncSession, as_of: date) -> date | None:
    snapshot_date = (
        await db.execute(
            select(func.max(FactInventory.doc_date)).where(
                FactInventory.doc_date <= as_of,
                FactInventory.movement_type == 'snapshot',
            )
        )
    ).scalar_one_or_none()
    if isinstance(snapshot_date, date):
        return snapshot_date
    fallback_date = (
        await db.execute(
            select(func.min(FactInventory.doc_date)).where(
                FactInventory.movement_type == 'snapshot',
            )
        )
    ).scalar_one_or_none()
    if isinstance(fallback_date, date):
        return fallback_date
    return None


def _deduped_snapshot_fact_ids(snapshot_date: date):
    """IDs of the fact_inventory rows to keep for a snapshot date: one per
    (branch, warehouse, item) — the freshest. Guards every raw fact_inventory
    aggregation against double-counting when more than one snapshot set lands on a
    day (the nightly STKSNAP refresh + a full-sync pull). item_code is the stable
    cross-source key; item_id may be resolved inconsistently. Same partition as
    inventory_summary_bundle_from_current_state and _refresh_inventory_aggregates.
    """
    ranked = (
        select(
            FactInventory.id.label('fid'),
            func.row_number()
            .over(
                partition_by=(
                    func.coalesce(FactInventory.branch_ext_id, literal('')),
                    func.coalesce(FactInventory.warehouse_ext_id, literal('')),
                    func.coalesce(func.nullif(FactInventory.item_code, ''), cast(FactInventory.item_id, String), literal('')),
                ),
                order_by=FactInventory.updated_at.desc(),
            )
            .label('rn'),
        )
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
        .subquery('deduped_snapshot_ranked')
    )
    return select(ranked.c.fid).where(ranked.c.rn == 1)


async def stock_aging(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}

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
            func.coalesce(func.sum(FactInventory.cost_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    )
    stmt = stmt.where(FactInventory.id.in_(_deduped_snapshot_fact_ids(snapshot_date)))
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
                'retail_value_amount': 0.0,
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
    latest_date = await _latest_inventory_snapshot_date(db, as_of)
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
            FactInventory.warehouse_ext_id.label('warehouse_ext_id'),
            FactInventory.item_id.label('item_id'),
            FactInventory.item_code.label('item_code'),
            FactInventory.qty_on_hand.label('qty_on_hand'),
            FactInventory.qty_reserved.label('qty_reserved'),
            FactInventory.cost_amount.label('cost_amount'),
            FactInventory.value_amount.label('value_amount'),
            cast(func.nullif(FactInventory.source_payload_json['retail_value_amount'].astext, ''), Numeric).label(
                'retail_value_amount'
            ),
            FactInventory.source_payload_json['brand_name'].astext.label('payload_brand_name'),
            FactInventory.source_payload_json['brand_external_id'].astext.label('payload_brand_external_id'),
            FactInventory.source_payload_json['category_1'].astext.label('payload_category_1'),
            FactInventory.source_payload_json['category_2'].astext.label('payload_category_2'),
            FactInventory.source_payload_json['category_3'].astext.label('payload_category_3'),
            FactInventory.source_payload_json['group_external_id'].astext.label('payload_group_external_id'),
            FactInventory.source_payload_json['group_name'].astext.label('payload_group_name'),
            FactInventory.source_payload_json['commercial_category'].astext.label('payload_commercial_category'),
            FactInventory.source_payload_json['manufacturer_code'].astext.label('payload_manufacturer_code'),
            FactInventory.source_payload_json['manufacturer_name'].astext.label('payload_manufacturer_name'),
            # Dedup key: when more than one snapshot set lands on the same day
            # (e.g. the nightly STKSNAP refresh + a full-sync IS pull), keep only the
            # freshest row per (branch, warehouse, item) so totals are not double-counted.
            # Same partition as _refresh_inventory_aggregates (ext_ids; item_id is NULL).
            func.row_number()
            .over(
                partition_by=(
                    func.coalesce(FactInventory.branch_ext_id, literal('')),
                    func.coalesce(FactInventory.warehouse_ext_id, literal('')),
                    # item_code is the stable natural key across snapshot sources; item_id
                    # may be resolved inconsistently (or left NULL) by different ingests,
                    # so prefer item_code and only fall back to item_id.
                    func.coalesce(func.nullif(FactInventory.item_code, ''), cast(FactInventory.item_id, String), literal('')),
                ),
                order_by=FactInventory.updated_at.desc(),
            )
            .label('rn'),
        )
        .where(
            FactInventory.doc_date == latest_date,
            FactInventory.movement_type == 'snapshot',
        )
        .subquery('inventory_current_state_rows')
    )

    item_code_expr = func.coalesce(DimItem.external_id, latest_inventory_rows.c.item_code)
    inv_base = (
        select(
            item_code_expr.label('item_code'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimBrand.name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_brand_name), '')),
                func.max(func.nullif(func.btrim(DimBrand.external_id), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_brand_external_id), '')),
                literal('N/A'),
            ).label('brand_label'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimGroup.name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_group_name), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_commercial_category), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_category_1), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_category_2), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_category_3), '')),
                func.max(func.nullif(func.btrim(DimGroup.external_id), '')),
                func.max(func.nullif(func.btrim(latest_inventory_rows.c.payload_group_external_id), '')),
                literal('N/A'),
            ).label('group_label'),
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
            func.coalesce(func.sum(latest_inventory_rows.c.retail_value_amount), 0).label('retail_value_amount'),
        )
        .select_from(latest_inventory_rows)
        .join(DimBranch, latest_inventory_rows.c.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, latest_inventory_rows.c.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, latest_inventory_rows.c.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_inventory_rows.c.branch_ext_id,
        warehouse_ext_col=latest_inventory_rows.c.warehouse_ext_id,
        brand_ext_col=latest_inventory_rows.c.payload_brand_external_id,
        brand_label_col=latest_inventory_rows.c.payload_brand_name,
        category_1_col=latest_inventory_rows.c.payload_category_1,
        category_2_col=latest_inventory_rows.c.payload_category_2,
        category_3_col=latest_inventory_rows.c.payload_category_3,
        group_ext_col=latest_inventory_rows.c.payload_group_external_id,
        group_label_col=latest_inventory_rows.c.payload_group_name,
        commercial_category_col=latest_inventory_rows.c.payload_commercial_category,
    )
    inv_base = inv_base.where(latest_inventory_rows.c.rn == 1).group_by(item_code_expr).subquery('inventory_current_state_base')

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
                inv_base.c.retail_value_amount,
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
    total_retail_value = 0.0
    aging = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    by_brand: dict[str, dict[str, float]] = {}
    by_group: dict[str, dict[str, float]] = {}
    by_manufacturer: dict[str, dict[str, float]] = {}

    for row in rows:
        qty = float(row.get('qty_on_hand') or 0)
        reserved = float(row.get('qty_reserved') or 0)
        cost = float(row.get('cost_amount') or 0)
        value = float(row.get('stock_value') or 0)
        retail_value = float(row.get('retail_value_amount') or 0)
        total_qty += qty
        total_reserved += reserved
        total_cost += cost
        total_value += value
        total_retail_value += retail_value

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
        aging[bucket]['value_amount'] += cost

        brand_label = str(row.get('brand_label') or 'N/A')
        group_label = str(row.get('group_label') or 'N/A')
        manufacturer_label = str(row.get('manufacturer_label') or 'N/A')

        brand_bucket = by_brand.setdefault(brand_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        brand_bucket['qty_on_hand'] += qty
        brand_bucket['value_amount'] += cost

        group_bucket = by_group.setdefault(group_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        group_bucket['qty_on_hand'] += qty
        group_bucket['value_amount'] += cost

        manufacturer_bucket = by_manufacturer.setdefault(manufacturer_label, {'qty_on_hand': 0.0, 'value_amount': 0.0})
        manufacturer_bucket['qty_on_hand'] += qty
        manufacturer_bucket['value_amount'] += cost

    ranked_brands = sorted(by_brand.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]
    ranked_groups = sorted(by_group.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]
    ranked_manufacturers = sorted(by_manufacturer.items(), key=lambda item: item[1]['value_amount'], reverse=True)[: max(1, min(int(limit), 100))]

    sales_window_days = 30
    sales_from = as_of - timedelta(days=sales_window_days - 1)
    sales_net_expr = func.coalesce(FactSales.net_value, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    sales_cost_expr = func.coalesce(FactSales.cost_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    sales_qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    sales_stmt = (
        select(
            func.coalesce(func.sum(sales_net_expr), 0).label('net_value'),
            func.coalesce(func.sum(sales_cost_expr), 0).label('cost_amount'),
            func.coalesce(func.sum(sales_qty_expr), 0).label('qty'),
        )
        .select_from(FactSales)
        .where(FactSales.doc_date.between(sales_from, as_of))
    )
    sales_stmt = _apply_fact_sales_filters(
        sales_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    sales_stmt = _apply_fact_sales_behavior_rules(sales_stmt)
    sales_stmt = _apply_fact_sales_turnover_rules(sales_stmt)
    sales_row = (await db.execute(sales_stmt)).mappings().one()
    sales_net_30d = max(0.0, float(sales_row.get('net_value') or 0))
    sales_cost_30d = max(0.0, float(sales_row.get('cost_amount') or 0))
    sales_qty_30d = max(0.0, float(sales_row.get('qty') or 0))
    gross_margin_30d = max(0.0, sales_net_30d - sales_cost_30d)
    annual_factor = 365.0 / float(sales_window_days)
    daily_cost_rate = sales_cost_30d / float(sales_window_days) if sales_cost_30d > 0 else 0.0
    inventory_turnover = (sales_cost_30d * annual_factor / total_cost) if total_cost > 0 else 0.0
    days_of_supply = (total_cost / daily_cost_rate) if daily_cost_rate > 0 else 0.0
    gmroi = (gross_margin_30d * annual_factor / total_cost) if total_cost > 0 else 0.0
    sell_through_pct = (sales_qty_30d / (sales_qty_30d + total_qty) * 100.0) if (sales_qty_30d + total_qty) > 0 else 0.0
    inactive_value = float(aging.get('90_plus', {}).get('value_amount') or 0)
    inactive_value_pct = (inactive_value / total_cost * 100.0) if total_cost > 0 else 0.0
    stock_to_sales_ratio = (total_cost / sales_cost_30d) if sales_cost_30d > 0 else 0.0

    return {
        'snapshot': {
            'snapshot_date': str(latest_date),
            'qty_on_hand': float(total_qty),
            'qty_reserved': float(total_reserved),
            'cost_amount': float(total_cost),
            'value_amount': float(total_value),
            'retail_value_amount': float(total_retail_value),
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
        'intelligence': {
            'window_days': sales_window_days,
            'sales_net_30d': float(sales_net_30d),
            'sales_cost_30d': float(sales_cost_30d),
            'sales_qty_30d': float(sales_qty_30d),
            'gross_margin_30d': float(gross_margin_30d),
            'inventory_turnover_annualized': float(inventory_turnover),
            'days_of_supply': float(days_of_supply),
            'sell_through_pct': float(sell_through_pct),
            'gmroi_annualized': float(gmroi),
            'inactive_value': float(inactive_value),
            'inactive_value_pct': float(inactive_value_pct),
            'stock_to_sales_ratio': float(stock_to_sales_ratio),
        },
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
            'debtor_collections',
            'debtor_collection',
            'other_collections',
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
            'debtor_transfers',
            'debtor_transfer',
            'other_transfers',
            'customer_bank_transfer',
            'customer_wire_transfer',
            'customer_wire',
        },
        'supplier_payments': {
            'supplier_payments',
            'supplier_payment',
            'creditor_payment',
            'creditor_payments',
            'other_payment',
            'other_payments',
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
            'creditor_transfer',
            'creditor_transfers',
            'other_supplier_transfer',
            'other_supplier_transfers',
            'other_transfer_out',
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


def _customer_collection_subcategories() -> list[str]:
    return [
        'customer_collections',
        'customer_transfers',
        'customer_cheques',
        'debtor_cheques',
        'other_customer_cheques',
    ]


def _supplier_payment_subcategories() -> list[str]:
    return [
        'supplier_payments',
        'supplier_transfers',
        'supplier_cheques',
        'creditor_cheques',
        'other_supplier_cheques',
    ]


async def _cashflow_totals_by_counterparty(
    db: AsyncSession,
    *,
    counterparty_ids: list[str],
    date_from: date,
    date_to: date,
    subcategories: list[str],
    branches: list[str] | None = None,
) -> dict[str, float]:
    ids = [str(item or '').strip() for item in counterparty_ids if str(item or '').strip()]
    if not ids:
        return {}
    subcategory_expr = _cashflow_subcategory_expr()
    payment_key = func.coalesce(FactCashflow.counterparty_id, FactCashflow.reference_no, FactCashflow.external_id)
    tx_col = func.lower(cast(func.coalesce(FactCashflow.transaction_type, literal('')), String))
    amount_expr = func.coalesce(FactCashflow.amount, 0)
    display_amount_expr = case(
        (tx_col.like('101%'), amount_expr),
        (or_(tx_col.like('102%'), tx_col == '2'), -func.abs(amount_expr)),
        else_=func.abs(amount_expr),
    )
    stmt = (
        select(
            payment_key.label('counterparty_id'),
            func.coalesce(func.sum(display_amount_expr), 0).label('total_value'),
        )
        .select_from(FactCashflow)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
        .where(subcategory_expr.in_(subcategories))
        .where(payment_key.in_(ids))
        .group_by(payment_key)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactCashflow.branch_id.in_(select(DimBranch.id).where(DimBranch.external_id.in_(branches))))
    rows = (await db.execute(stmt)).mappings().all()
    return {str(r.get('counterparty_id') or '').strip(): float(r.get('total_value') or 0) for r in rows}


async def _cashflow_total(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    subcategories: list[str],
    branches: list[str] | None = None,
) -> float:
    subcategory_expr = _cashflow_subcategory_expr()
    tx_col = func.lower(cast(func.coalesce(FactCashflow.transaction_type, literal('')), String))
    amount_expr = func.coalesce(FactCashflow.amount, 0)
    display_amount_expr = case(
        (tx_col.like('101%'), amount_expr),
        (or_(tx_col.like('102%'), tx_col == '2'), -func.abs(amount_expr)),
        else_=func.abs(amount_expr),
    )
    stmt = (
        select(func.coalesce(func.sum(display_amount_expr), 0))
        .select_from(FactCashflow)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
        .where(subcategory_expr.in_(subcategories))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactCashflow.branch_id.in_(select(DimBranch.id).where(DimBranch.external_id.in_(branches))))
    return float((await db.execute(stmt)).scalar_one_or_none() or 0)


async def _sales_totals_by_customer(
    db: AsyncSession,
    *,
    customer_ids: list[str],
    date_from: date,
    date_to: date,
) -> dict[str, dict[str, float]]:
    ids = [str(item or '').strip() for item in customer_ids if str(item or '').strip()]
    if not ids:
        return {}
    customer_key = _sales_customer_key_expr()
    stmt = (
        select(
            customer_key.label('customer_id'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('turnover'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_turnover'),
        )
        .select_from(FactSales)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(or_(customer_key.in_(ids), FactSales.customer_code.in_(ids)))
        .group_by(customer_key)
    )
    rows = (await db.execute(stmt)).mappings().all()
    return {
        str(row.get('customer_id') or '').strip(): {
            'turnover': float(row.get('turnover') or 0),
            'gross_turnover': float(row.get('gross_turnover') or 0),
        }
        for row in rows
        if str(row.get('customer_id') or '').strip()
    }


async def _sales_total_gross(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
) -> float:
    stmt = (
        select(func.coalesce(func.sum(FactSales.gross_value), 0))
        .select_from(FactSales)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    return float((await db.execute(stmt)).scalar_one_or_none() or 0)


async def _sales_profiles_by_customer(
    db: AsyncSession,
    *,
    customer_ids: list[str],
) -> dict[str, dict[str, str]]:
    ids = [str(item or '').strip() for item in customer_ids if str(item or '').strip()]
    if not ids:
        return {}
    customer_key = _sales_customer_key_expr()
    customer_code_expr = func.nullif(func.btrim(cast(func.coalesce(FactSales.customer_code, literal('')), String)), '')
    customer_name_expr = func.nullif(func.btrim(cast(func.coalesce(FactSales.customer_name, literal('')), String)), '')
    afm_expr = _fact_sales_customer_afm_expr()
    stmt = (
        select(
            customer_key.label('customer_id'),
            func.max(customer_code_expr).label('customer_code'),
            func.max(customer_name_expr).label('customer_name'),
            func.max(afm_expr).label('afm'),
            func.max(FactSales.delivery_address).label('address'),
            func.max(FactSales.delivery_city).label('city'),
        )
        .select_from(FactSales)
        .where(or_(customer_key.in_(ids), customer_code_expr.in_(ids)))
        .group_by(customer_key)
    )
    rows = (await db.execute(stmt)).mappings().all()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        customer_id = str(row.get('customer_id') or '').strip()
        customer_code = str(row.get('customer_code') or '').strip()
        profile = {
            'customer_code': customer_code,
            'customer_name': str(row.get('customer_name') or '').strip(),
            'afm': str(row.get('afm') or '').strip(),
            'address': str(row.get('address') or '').strip(),
            'city': str(row.get('city') or '').strip(),
        }
        for key in {customer_id, customer_code}:
            if key:
                out[key] = profile
    return out


async def _supplier_balance_afm_map(
    db: AsyncSession,
    *,
    supplier_ids: list[str],
    as_of: date,
    branches: list[str] | None = None,
) -> dict[str, str]:
    ids = [str(item or '').strip() for item in supplier_ids if str(item or '').strip()]
    if not ids:
        return {}
    stmt = (
        select(
            FactSupplierBalance.supplier_ext_id.label('supplier_id'),
            func.max(FactSupplierBalance.supplier_afm).label('supplier_afm'),
        )
        .select_from(FactSupplierBalance)
        .where(FactSupplierBalance.balance_date <= as_of)
        .where(FactSupplierBalance.supplier_ext_id.in_(ids))
        .group_by(FactSupplierBalance.supplier_ext_id)
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactSupplierBalance.branch_ext_id.in_(branches))
    rows = (await db.execute(stmt)).all()
    return {str(supplier_id or '').strip(): str(afm or '').strip() for supplier_id, afm in rows if str(supplier_id or '').strip()}


def _cashflow_subcategories_for_filter(category: str | None) -> set[str]:
    normalized = _normalize_cashflow_category(category)
    if not normalized:
        return set()
    if normalized == 'supplier_payments':
        return {'supplier_payments', 'creditor_payments', 'other_payments'}
    if normalized == 'supplier_transfers':
        return {'supplier_transfers', 'creditor_transfers', 'other_supplier_transfers'}
    if normalized == 'customer_collections':
        return {'customer_collections', 'debtor_collections', 'other_collections'}
    if normalized == 'customer_transfers':
        return {'customer_transfers', 'debtor_transfers', 'other_transfers'}
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
        'debtor_collections': 'Είσπραξη χρεώστη',
        'debtor_collection': 'Είσπραξη χρεώστη',
        'other_collections': 'Λοιπή είσπραξη',
        'collections': 'Είσπραξη πελάτη',
        'collection': 'Είσπραξη πελάτη',
        'customer_transfers': 'Έμβασμα από πελάτη',
        'customer_transfer': 'Έμβασμα από πελάτη',
        'debtor_transfers': 'Έμβασμα χρεώστη',
        'debtor_transfer': 'Έμβασμα χρεώστη',
        'other_transfers': 'Λοιπό έμβασμα',
        'customer_bank_transfer': 'Έμβασμα από πελάτη',
        'customer_wire_transfer': 'Έμβασμα από πελάτη',
        'customer_wire': 'Έμβασμα από πελάτη',
        'supplier_payments': 'Πληρωμή προμηθευτή',
        'supplier_payment': 'Πληρωμή προμηθευτή',
        'creditor_payment': 'Πληρωμή πιστωτή',
        'creditor_payments': 'Πληρωμή πιστωτή',
        'other_payment': 'Λοιπή πληρωμή',
        'other_payments': 'Λοιπή πληρωμή',
        'payments': 'Πληρωμή προμηθευτή',
        'payment': 'Πληρωμή προμηθευτή',
        'supplier_transfers': 'Έμβασμα σε προμηθευτή',
        'supplier_transfer': 'Έμβασμα σε προμηθευτή',
        'creditor_transfer': 'Έμβασμα σε πιστωτή',
        'creditor_transfers': 'Έμβασμα σε πιστωτή',
        'other_supplier_transfer': 'Λοιπό έμβασμα προμηθευτή',
        'other_supplier_transfers': 'Λοιπό έμβασμα προμηθευτή',
        'other_transfer_out': 'Λοιπό έμβασμα προμηθευτή',
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
_TECHNICAL_SALES_TYPE_RE = re.compile(r'^sales_\d+$', re.IGNORECASE)


def _cashflow_amount_sign(entry_type: str | None) -> float:
    return _cashflow_amount_sign_by_behavior_or_type(None, entry_type)


def _normalize_sales_document_type_label(raw_type: str | None, *, series_label: str | None, payload: dict | None) -> str:
    value = str(raw_type or '').strip()
    if value and not _TECHNICAL_SALES_TYPE_RE.match(value):
        return value
    if isinstance(payload, dict):
        for key in (
            'document_series_name',
            'series_name',
            'document_type_name',
            'doc_type_name',
            'type_name',
        ):
            txt = str(payload.get(key) or '').strip()
            if txt:
                return txt
    fallback_series = str(series_label or '').strip()
    if fallback_series:
        return fallback_series
    return 'Παραστατικό Πώλησης'


def _cashflow_amount_sign_by_behavior_or_type(transaction_type: str | None, entry_type: str | None) -> float:
    tx = str(transaction_type or '').strip().lower()
    if tx.startswith('101'):
        return 1.0
    if tx.startswith('102') or tx == '2':
        return -1.0
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
        'creditor_payment',
        'creditor_payments',
        'other_payment',
        'other_payments',
        'payments',
        'payment',
        'supplier_transfers',
        'supplier_transfer',
        'creditor_transfer',
        'creditor_transfers',
        'other_supplier_transfer',
        'other_supplier_transfers',
        'other_transfer_out',
        'supplier_bank_transfer',
        'supplier_wire_transfer',
        'supplier_wire',
    }
    if normalized in positive:
        return 1.0
    if normalized in negative:
        return -1.0
    return 1.0


def _normalize_cashflow_signed_amount(raw_amount: float, transaction_type: str | None, entry_type: str | None) -> float:
    amount = float(raw_amount or 0.0)
    tx = str(transaction_type or '').strip().lower()
    if tx.startswith('101'):
        return amount
    if tx.startswith('102') or tx == '2':
        return -abs(amount)
    sign = _cashflow_amount_sign_by_behavior_or_type(None, entry_type)
    return sign * abs(amount)


def _cashflow_signed_amount_expr():
    tx_col = func.lower(cast(func.coalesce(FactCashflow.transaction_type, literal('')), String))
    entry_col = func.lower(cast(func.coalesce(FactCashflow.entry_type, literal('')), String))
    subcategory_col = func.lower(cast(func.coalesce(FactCashflow.subcategory, literal('')), String))
    positive = sorted(_cashflow_entry_types_for_category('customer_collections') or set())
    negative = sorted((_cashflow_entry_types_for_category('supplier_payments') or set()) | {'refund'})
    sign_expr = case(
        (tx_col.like('101%'), literal(0.0)),
        (or_(tx_col.like('102%'), tx_col == '2'), literal(-1.0)),
        (subcategory_col.in_(positive), literal(1.0)),
        (entry_col.in_(positive), literal(1.0)),
        (subcategory_col.in_(negative), literal(-1.0)),
        (entry_col.in_(negative), literal(-1.0)),
        else_=literal(1.0),
    )
    amount_expr = func.coalesce(FactCashflow.amount, 0)
    return case(
        (tx_col.like('101%'), amount_expr),
        (or_(tx_col.like('102%'), tx_col == '2'), -func.abs(amount_expr)),
        else_=(sign_expr * func.abs(amount_expr)),
    )


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
        signed_amount = _normalize_cashflow_signed_amount(amount, fact.transaction_type, fact.subcategory or fact.entry_type)
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
    series: str | None = None,
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

    signed_amount_expr = _cashflow_signed_amount_expr()
    base = (
        select(
            doc_key.label('document_id'),
            func.coalesce(func.max(FactCashflow.reference_no), func.max(FactCashflow.external_id), literal('')).label('document_no'),
            func.max(FactCashflow.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimBranch.external_id), literal('')).label('branch_code'),
            func.coalesce(func.max(FactCashflow.entry_type), literal('unknown')).label('entry_type'),
            func.coalesce(func.max(FactCashflow.notes), literal('')).label('notes'),
            func.coalesce(func.sum(signed_amount_expr), 0).label('total_value'),
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
    series_clean = str(series or '').strip().lower()
    if series_clean:
        like = f'%{series_clean}%'
        base = base.where(entry_type_col.like(like) | subcategory_col.like(like))

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
            'net_value': float(totals_row['value_total'] or 0),
            'vat_value': 0.0,
            'expenses_value': 0.0,
            'gross_value': float(totals_row['value_total'] or 0),
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
        raw_amount = float(fact.amount or 0)
        line_amount = _normalize_cashflow_signed_amount(raw_amount, fact.transaction_type, fact.subcategory or fact.entry_type)
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
            agg_source = AggCashDaily
            agg_stmt = (
                select(
                    agg_source.account_id.label('account_id'),
                    func.coalesce(func.sum(agg_source.entries), 0).label('tx_count'),
                    func.coalesce(func.sum(agg_source.net_amount), 0).label('balance'),
                    func.max(agg_source.updated_at).label('updated_at'),
                )
                .select_from(agg_source)
                .where(agg_source.account_id.is_not(None))
                .where(agg_source.branch_ext_id.in_(branches))
            )
            if as_of is not None:
                agg_stmt = agg_stmt.where(agg_source.doc_date <= as_of)
            agg_stmt = agg_stmt.group_by(agg_source.account_id)
        else:
            agg_source = AggCashAccounts
            agg_stmt = (
                select(
                    agg_source.account_id.label('account_id'),
                    func.coalesce(func.sum(agg_source.entries), 0).label('tx_count'),
                    func.coalesce(func.sum(agg_source.net_amount), 0).label('balance'),
                    func.max(agg_source.updated_at).label('updated_at'),
                )
                .select_from(agg_source)
                .where(agg_source.account_id.is_not(None))
            )
            if as_of is not None:
                agg_stmt = agg_stmt.where(agg_source.doc_date <= as_of)
            agg_stmt = agg_stmt.group_by(agg_source.account_id)

        grouped = agg_stmt.subquery('cashflow_account_rows_agg')
        q_clean = str(q or '').strip().lower()
        filtered = select(grouped).where(func.nullif(func.btrim(cast(grouped.c.account_id, String)), '').is_not(None))
        if q_clean:
            like = f'%{q_clean}%'
            filtered = filtered.where(_sql_normalized_text(grouped.c.account_id).like(like))
        filtered = filtered.subquery('cashflow_account_rows_filtered')

        totals_row = (
            await db.execute(
                select(
                    func.coalesce(func.count(), 0).label('accounts'),
                    func.coalesce(func.sum(filtered.c.balance), 0).label('balance_total'),
                ).select_from(filtered)
            )
        ).mappings().one()
        page_rows = (
            await db.execute(
                select(filtered)
                .order_by(filtered.c.account_id.asc())
                .offset(max(0, int(offset)))
                .limit(max(1, min(int(limit), 500)))
            )
        ).mappings().all()
        paged = [
            {
                'account_id': str(r.get('account_id') or '').strip(),
                'account_code': str(r.get('account_id') or '').strip(),
                'account_name': str(r.get('account_id') or '').strip(),
                'balance': float(r.get('balance') or 0),
                'tx_count': int(r.get('tx_count') or 0),
                'updated_at': _raw_scalar(r.get('updated_at')),
            }
            for r in page_rows
            if str(r.get('account_id') or '').strip()
            ]
        return {
            'summary': {
                'accounts': int(totals_row.get('accounts') or 0),
                'balance_total': float(totals_row.get('balance_total') or 0),
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

    async def _cashflow_account_detail_rows(condition):
        result = await db.execute(
            stmt.where(condition).order_by(
                FactCashflow.doc_date.desc(),
                FactCashflow.updated_at.desc(),
                FactCashflow.external_id.asc(),
            )
        )
        return result.all()

    rows = await _cashflow_account_detail_rows(FactCashflow.external_id == account_id)
    if not rows:
        rows = await _cashflow_account_detail_rows(FactCashflow.account_id == account_id)
    if not rows:
        rows = await _cashflow_account_detail_rows(FactCashflow.reference_no == account_id)

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
        agg_has_rows = (await db.execute(select(AggCustomerBalancesDaily.balance_date).limit(1))).first() is not None
        if agg_has_rows:
            branches = _effective_branch_filter(None)
            latest_date_stmt = select(func.max(AggCustomerBalancesDaily.balance_date)).where(
                AggCustomerBalancesDaily.balance_date <= date_to
            )
            if branches is not None:
                latest_date_stmt = latest_date_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
            latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
            if latest_date is not None:
                customer_id_expr = func.nullif(
                    func.btrim(cast(func.coalesce(AggCustomerBalancesDaily.customer_ext_id, literal('')), String)),
                    '',
                )
                customer_code_expr = func.coalesce(
                    func.nullif(func.btrim(cast(func.coalesce(DimCustomer.customer_code, literal('')), String)), ''),
                    customer_id_expr,
                )
                customer_name_expr = func.coalesce(
                    func.nullif(func.btrim(cast(func.coalesce(DimCustomer.name, literal('')), String)), ''),
                    customer_code_expr,
                    customer_id_expr,
                )
                grouped = (
                    select(
                        customer_id_expr.label('customer_id'),
                        customer_code_expr.label('customer_code'),
                        customer_name_expr.label('customer_name'),
                        AggCustomerBalancesDaily.balance_date.label('balance_date'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
                        func.coalesce(func.sum(AggCustomerBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
                        func.max(AggCustomerBalancesDaily.updated_at).label('updated_at'),
                    )
                    .select_from(AggCustomerBalancesDaily)
                    .join(DimCustomer, DimCustomer.external_id == AggCustomerBalancesDaily.customer_ext_id, isouter=True)
                    .where(AggCustomerBalancesDaily.balance_date == latest_date)
                    .where(customer_id_expr.is_not(None))
                )
                if branches is not None:
                    grouped = grouped.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
                q_clean = _normalize_search_term(q)
                if q_clean:
                    like = f'%{q_clean}%'
                    grouped = grouped.where(
                        _sql_normalized_text(customer_id_expr).like(like)
                        | _sql_normalized_text(customer_code_expr).like(like)
                        | _sql_normalized_text(customer_name_expr).like(like)
                    )
                grouped = grouped.group_by(
                    customer_id_expr,
                    customer_code_expr,
                    customer_name_expr,
                    AggCustomerBalancesDaily.balance_date,
                ).subquery('customer_balance_rows_agg')

                totals_row = (
                    await db.execute(
                        select(
                            func.coalesce(func.count(), 0).label('customers'),
                            func.coalesce(func.sum(grouped.c.open_balance), 0).label('open_balance'),
                            func.coalesce(func.sum(grouped.c.overdue_balance), 0).label('overdue_balance'),
                        ).select_from(grouped)
                    )
                ).mappings().one()
                safe_limit = max(1, min(int(limit), 500))
                safe_offset = max(0, int(offset))
                page_rows = (
                    await db.execute(
                        select(grouped)
                        .order_by(
                            grouped.c.open_balance.desc(),
                            grouped.c.overdue_balance.desc(),
                            grouped.c.customer_name.asc(),
                        )
                        .offset(safe_offset)
                        .limit(safe_limit)
                    )
                ).mappings().all()
                page_ids = [
                    str(item.get('customer_id') or '').strip()
                    for item in page_rows
                    if str(item.get('customer_id') or '').strip()
                ]
                page_profiles = await _sales_profiles_by_customer(db, customer_ids=page_ids)
                page_sales_map = await _sales_totals_by_customer(
                    db,
                    customer_ids=page_ids,
                    date_from=date_from,
                    date_to=date_to,
                )
                gross_turnover_total = await _sales_total_gross(db, date_from=date_from, date_to=date_to)
                collections_map = await _cashflow_totals_by_counterparty(
                    db,
                    counterparty_ids=page_ids,
                    date_from=date_from,
                    date_to=date_to,
                    subcategories=_customer_collection_subcategories(),
                )
                collections_total = await _cashflow_total(
                    db,
                    date_from=date_from,
                    date_to=date_to,
                    subcategories=_customer_collection_subcategories(),
                )
                out_rows = []
                for row in page_rows:
                    customer_id = str(row.get('customer_id') or '').strip()
                    profile = page_profiles.get(customer_id) or {}
                    code_val = str(row.get('customer_code') or customer_id).strip()
                    raw_name = str(row.get('customer_name') or row.get('customer_code') or customer_id).strip()
                    profile_name = str(profile.get('customer_name') or '').strip()
                    name_val = profile_name if profile_name and profile_name not in {customer_id, code_val} else raw_name
                    balance_date_val = row.get('balance_date')
                    sales_totals = page_sales_map.get(customer_id, {})
                    out_rows.append(
                        {
                            'customer_id': customer_id,
                            'code': code_val,
                            'name': name_val,
                            'afm': str(profile.get('afm') or ''),
                            'amka': '',
                            'address': str(profile.get('address') or ''),
                            'city': str(profile.get('city') or ''),
                            'phone_1': '',
                            'phone_2': '',
                            'profession': '',
                            'balance': float(row.get('open_balance') or 0),
                            'open_balance': float(row.get('open_balance') or 0),
                            'overdue_balance': float(row.get('overdue_balance') or 0),
                            'aging_bucket_0_30': float(row.get('aging_bucket_0_30') or 0),
                            'aging_bucket_31_60': float(row.get('aging_bucket_31_60') or 0),
                            'aging_bucket_61_90': float(row.get('aging_bucket_61_90') or 0),
                            'aging_bucket_90_plus': float(row.get('aging_bucket_90_plus') or 0),
                            'last_collection_date': '',
                            'trend_vs_previous': float(row.get('trend_vs_previous') or 0),
                            'balance_date': balance_date_val.isoformat()
                            if isinstance(balance_date_val, date)
                            else str(balance_date_val or ''),
                            'turnover': float(sales_totals.get('turnover') or 0),
                            'gross_turnover': float(sales_totals.get('gross_turnover') or 0),
                            'collections_total': float(collections_map.get(customer_id, 0.0)),
                            'sales_docs': 0,
                            'last_sale_date': '',
                            'updated_at': _raw_scalar(row.get('updated_at')),
                        }
                    )
                return {
                    'summary': {
                        'customers': int(totals_row.get('customers') or 0),
                        'turnover': gross_turnover_total,
                        'collections_total': collections_total,
                        'open_balance': float(totals_row.get('open_balance') or 0),
                        'overdue_balance': float(totals_row.get('overdue_balance') or 0),
                    },
                    'limit': int(safe_limit),
                    'offset': int(safe_offset),
                    'rows': out_rows,
                }

        current_map = await _latest_customer_balances_map(
            db,
            as_of=date_to,
            aggregate_only=False,
        )
        q_clean = _normalize_search_term(q)
        rows: list[dict[str, object]] = []
        for customer_id, snapshot in current_map.items():
            code = str(snapshot.get('customer_code') or customer_id).strip()
            raw_name = str(snapshot.get('customer_name') or code or customer_id).strip()
            name = raw_name
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
                    'afm': str(snapshot.get('customer_afm') or ''),
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
                    'collections_total': 0.0,
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
        page_ids = [str(item.get('customer_id') or '').strip() for item in paged_rows if str(item.get('customer_id') or '').strip()]
        page_profiles = await _sales_profiles_by_customer(db, customer_ids=page_ids)
        page_sales_map = await _sales_totals_by_customer(
            db,
            customer_ids=page_ids,
            date_from=date_from,
            date_to=date_to,
        )
        collections_map = await _cashflow_totals_by_counterparty(
            db,
            counterparty_ids=page_ids,
            date_from=date_from,
            date_to=date_to,
            subcategories=_customer_collection_subcategories(),
        )
        gross_turnover_total = await _sales_total_gross(db, date_from=date_from, date_to=date_to)
        collections_total = await _cashflow_total(
            db,
            date_from=date_from,
            date_to=date_to,
            subcategories=_customer_collection_subcategories(),
        )
        for item in paged_rows:
            customer_id = str(item.get('customer_id') or '').strip()
            code = str(item.get('code') or '').strip()
            profile = page_profiles.get(customer_id) or page_profiles.get(code) or {}
            profile_name = str(profile.get('customer_name') or '').strip()
            if profile_name and profile_name not in {customer_id, code}:
                item['name'] = profile_name
            item['afm'] = str(profile.get('afm') or item.get('afm') or '')
            item['address'] = str(profile.get('address') or item.get('address') or '')
            item['city'] = str(profile.get('city') or item.get('city') or '')
            sales_totals = page_sales_map.get(customer_id) or page_sales_map.get(code) or {}
            item['turnover'] = float(sales_totals.get('turnover') or 0)
            item['gross_turnover'] = float(sales_totals.get('gross_turnover') or 0)
            item['collections_total'] = float(collections_map.get(customer_id, 0.0))
        return {
            'summary': {
                'customers': int(total),
                'turnover': gross_turnover_total,
                'collections_total': collections_total,
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
                func.coalesce(func.sum(grouped.c.gross_turnover), 0).label('gross_turnover'),
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
    collection_candidates: dict[str, set[str]] = {}
    customer_names = [str(r.get('customer_name') or '').strip() for r in rows if str(r.get('customer_name') or '').strip()]
    dim_codes_by_name: dict[str, set[str]] = {}
    if customer_names:
        dim_rows = (
            await db.execute(
                select(DimCustomer.name, DimCustomer.external_id).where(DimCustomer.name.in_(customer_names))
            )
        ).all()
        for name_val, external_id_val in dim_rows:
            name_key = str(name_val or '').strip()
            external_id = str(external_id_val or '').strip()
            if name_key and external_id:
                dim_codes_by_name.setdefault(name_key, set()).add(external_id)
    for row in rows:
        customer_id = str(row.get('customer_id') or '').strip()
        if not customer_id:
            continue
        candidates = {customer_id}
        customer_code = str(row.get('customer_code') or '').strip()
        if customer_code:
            candidates.add(customer_code)
        customer_name = str(row.get('customer_name') or '').strip()
        candidates.update(dim_codes_by_name.get(customer_name, set()))
        collection_candidates[customer_id] = {value for value in candidates if value}
    all_collection_ids = sorted({value for values in collection_candidates.values() for value in values})
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
    raw_collections_map = await _cashflow_totals_by_counterparty(
        db,
        counterparty_ids=all_collection_ids,
        date_from=date_from,
        date_to=date_to,
        subcategories=_customer_collection_subcategories(),
    )
    collections_map = {
        customer_id: float(sum(float(raw_collections_map.get(candidate, 0.0)) for candidate in candidates))
        for customer_id, candidates in collection_candidates.items()
    }

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
        collections_total = float(collections_map.get(customer_id, 0.0))
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
                'collections_total': collections_total,
                'sales_docs': int(row.get('sales_docs') or 0),
                'last_sale_date': last_sale.isoformat() if isinstance(last_sale, date) else str(last_sale or ''),
                'updated_at': _raw_scalar(row.get('updated_at')),
            }
        )

    return {
        'summary': {
            'customers': int(totals_row.get('customers') or 0),
            'turnover': float(totals_row.get('gross_turnover') or totals_row.get('turnover') or 0),
            'collections_total': float(sum(collections_map.values()) if collections_map else 0.0),
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
                literal(0).label('purchases_gross'),
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
                    func.coalesce(func.sum(grouped.c.purchases_gross), 0).label('purchases_gross'),
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

        purchase_gross_expr = func.coalesce(FactPurchases.net_value, 0) + _fact_purchases_payload_line_vat_expr()
        gross_supplier_key = func.coalesce(FactPurchases.supplier_ext_id, DimSupplier.external_id, DimSupplier.name, literal(''))
        gross_base = (
            select(
                gross_supplier_key.label('supplier_id'),
                func.coalesce(func.sum(purchase_gross_expr), 0).label('purchases_gross'),
            )
            .select_from(FactPurchases)
            .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
            .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        )
        if branches is not None:
            gross_base = gross_base.where(FactPurchases.branch_ext_id.in_(branches))
        if q_clean:
            like = f'%{q_clean}%'
            gross_base = gross_base.where(
                _sql_normalized_text(FactPurchases.supplier_ext_id).like(like)
                | _sql_normalized_text(DimSupplier.name).like(like)
                | _sql_normalized_text(_fact_purchases_supplier_afm_expr()).like(like)
            )
        gross_grouped = gross_base.group_by(gross_supplier_key).subquery('supplier_gross_rows_agg')
        gross_total = float(
            (
                await db.execute(select(func.coalesce(func.sum(gross_grouped.c.purchases_gross), 0)))
            ).scalar_one_or_none()
            or 0
        )
        gross_map: dict[str, float] = {}
        if supplier_ids:
            gross_rows = (
                await db.execute(
                    select(gross_grouped.c.supplier_id, gross_grouped.c.purchases_gross).where(
                        gross_grouped.c.supplier_id.in_(supplier_ids)
                    )
                )
            ).all()
            gross_map = {str(supplier_id or '').strip(): float(value or 0) for supplier_id, value in gross_rows}

        afm_supplier_key = func.coalesce(FactPurchases.supplier_ext_id, DimSupplier.external_id, DimSupplier.name, literal(''))
        afm_base = (
            select(
                afm_supplier_key.label('supplier_id'),
                func.max(_fact_purchases_supplier_afm_expr()).label('supplier_afm'),
            )
            .select_from(FactPurchases)
            .join(DimSupplier, DimSupplier.external_id == FactPurchases.supplier_ext_id, isouter=True)
            .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        )
        if branches is not None:
            afm_base = afm_base.where(FactPurchases.branch_ext_id.in_(branches))
        if q_clean:
            like = f'%{q_clean}%'
            afm_base = afm_base.where(
                _sql_normalized_text(FactPurchases.supplier_ext_id).like(like)
                | _sql_normalized_text(DimSupplier.name).like(like)
                | _sql_normalized_text(_fact_purchases_supplier_afm_expr()).like(like)
            )
        afm_grouped = afm_base.group_by(afm_supplier_key).subquery('supplier_afm_rows_agg')
        afm_map: dict[str, str] = {}
        if supplier_ids:
            afm_rows = (
                await db.execute(
                    select(afm_grouped.c.supplier_id, afm_grouped.c.supplier_afm).where(
                        afm_grouped.c.supplier_id.in_(supplier_ids)
                    )
                )
            ).all()
            afm_map = {str(supplier_id or '').strip(): str(supplier_afm or '').strip() for supplier_id, supplier_afm in afm_rows}
        balance_afm_map = await _supplier_balance_afm_map(db, supplier_ids=supplier_ids, as_of=date_to, branches=branches)
        for supplier_id, supplier_afm in balance_afm_map.items():
            if supplier_afm and not afm_map.get(supplier_id):
                afm_map[supplier_id] = supplier_afm

        payments_map = await _cashflow_totals_by_counterparty(
            db,
            counterparty_ids=supplier_ids,
            date_from=date_from,
            date_to=date_to,
            subcategories=_supplier_payment_subcategories(),
            branches=branches,
        )

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

        supplier_payments_total = await _cashflow_total(
            db,
            date_from=date_from,
            date_to=date_to,
            subcategories=_supplier_payment_subcategories(),
            branches=branches,
        )

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
                    'afm': afm_map.get(supplier_id, ''),
                    'name': str(row.get('supplier_name') or 'N/A'),
                    'purchases_net': float(row.get('purchases_net') or 0),
                    'purchases_gross': float(gross_map.get(supplier_id, 0.0)),
                    'purchases_cost': float(row.get('purchases_cost') or 0),
                    'payments_total': float(payments_map.get(supplier_id, 0.0)),
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
                'purchases_gross': gross_total,
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
                'purchases_gross': 0.0,
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
    purchase_gross_expr = func.coalesce(FactPurchases.net_value, 0) + _fact_purchases_payload_line_vat_expr()
    base = (
        select(
            supplier_key.label('supplier_id'),
            func.coalesce(func.max(FactPurchases.supplier_ext_id), literal('')).label('supplier_code'),
            func.max(_fact_purchases_supplier_afm_expr()).label('supplier_afm'),
            func.coalesce(func.max(DimSupplier.name), func.max(FactPurchases.supplier_ext_id), literal('N/A')).label(
                'supplier_name'
            ),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_net'),
            func.coalesce(func.sum(purchase_gross_expr), 0).label('purchases_gross'),
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
            | _sql_normalized_text(_fact_purchases_supplier_afm_expr()).like(like)
        )

    grouped = base.group_by(supplier_key).subquery('supplier_rows')
    totals_row = (
        await db.execute(
            select(
                func.coalesce(func.count(), 0).label('suppliers'),
                func.coalesce(func.sum(grouped.c.purchases_net), 0).label('purchases_net'),
                func.coalesce(func.sum(grouped.c.purchases_gross), 0).label('purchases_gross'),
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
        payments_map = await _cashflow_totals_by_counterparty(
            db,
            counterparty_ids=supplier_ids,
            date_from=date_from,
            date_to=date_to,
            subcategories=_supplier_payment_subcategories(),
            branches=branches,
        )
    balance_afm_map = await _supplier_balance_afm_map(db, supplier_ids=supplier_ids, as_of=date_to, branches=branches)
    supplier_payments_total = await _cashflow_total(
        db,
        date_from=date_from,
        date_to=date_to,
        subcategories=_supplier_payment_subcategories(),
        branches=branches,
    )

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
                'afm': str(row.get('supplier_afm') or balance_afm_map.get(supplier_id) or '').strip(),
                'name': str(row.get('supplier_name') or 'N/A'),
                'purchases_net': float(row.get('purchases_net') or 0),
                'purchases_gross': float(row.get('purchases_gross') or 0),
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
            'purchases_gross': float(totals_row.get('purchases_gross') or 0),
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


async def receivables_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    aggregate_only: bool = False,
):
    previous_to = date_from - timedelta(days=1)
    current_snapshot = await _customer_balances_summary_snapshot(
        db,
        as_of=date_to,
        branches=branches,
        include_top=not aggregate_only,
    )

    current_open = float(current_snapshot.get('open_balance') or 0)
    current_overdue = float(current_snapshot.get('overdue_balance') or 0)
    previous_open = 0.0
    if not aggregate_only:
        previous_snapshot = await _customer_balances_summary_snapshot(
            db,
            as_of=previous_to,
            branches=branches,
            include_top=False,
        )
        previous_open = float(previous_snapshot.get('open_balance') or 0)
    growth_value = current_open - previous_open
    growth_pct = ((growth_value / previous_open) * 100.0) if previous_open > 0 else None
    overdue_ratio_pct = ((current_overdue / current_open) * 100.0) if current_open > 0 else 0.0

    top_customer_id = str(current_snapshot.get('top_customer_id') or '')
    top_customer_name = str(current_snapshot.get('top_customer_name') or top_customer_id)
    top_customer_balance = float(current_snapshot.get('top_customer_balance') or 0)
    top_customer_share_pct = ((top_customer_balance / current_open) * 100.0) if current_open > 0 else 0.0

    return {
        'as_of': date_to.isoformat(),
        'summary': {
            'customers': int(current_snapshot.get('customers') or 0),
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
    current_snapshot = await _customer_balances_summary_snapshot(
        db,
        as_of=date_to,
        branches=branches,
        include_top=False,
    )
    bucket_0_30 = float(current_snapshot.get('aging_bucket_0_30') or 0)
    bucket_31_60 = float(current_snapshot.get('aging_bucket_31_60') or 0)
    bucket_61_90 = float(current_snapshot.get('aging_bucket_61_90') or 0)
    bucket_90_plus = float(current_snapshot.get('aging_bucket_90_plus') or 0)
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
    agg_has_rows = (await db.execute(select(AggCustomerBalancesDaily.balance_date).limit(1))).first() is not None
    if agg_has_rows:
        branches = _effective_branch_filter(branches)
        latest_date_stmt = select(func.max(AggCustomerBalancesDaily.balance_date)).where(
            AggCustomerBalancesDaily.balance_date <= date_to
        )
        if branches is not None:
            latest_date_stmt = latest_date_stmt.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
        latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
        if latest_date is not None:
            customer_id_expr = func.nullif(
                func.btrim(cast(func.coalesce(AggCustomerBalancesDaily.customer_ext_id, literal('')), String)),
                '',
            )
            customer_code_expr = func.coalesce(
                func.nullif(func.btrim(cast(func.coalesce(DimCustomer.customer_code, literal('')), String)), ''),
                customer_id_expr,
            )
            customer_name_expr = func.coalesce(
                func.nullif(func.btrim(cast(func.coalesce(DimCustomer.name, literal('')), String)), ''),
                customer_code_expr,
                customer_id_expr,
            )
            grouped = (
                select(
                    customer_id_expr.label('customer_id'),
                    customer_code_expr.label('customer_code'),
                    customer_name_expr.label('customer_name'),
                    AggCustomerBalancesDaily.balance_date.label('balance_date'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.open_balance), 0).label('open_balance'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.overdue_balance), 0).label('overdue_balance'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_0_30), 0).label('aging_bucket_0_30'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_31_60), 0).label('aging_bucket_31_60'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_61_90), 0).label('aging_bucket_61_90'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.aging_bucket_90_plus), 0).label('aging_bucket_90_plus'),
                    func.coalesce(func.sum(AggCustomerBalancesDaily.trend_vs_previous), 0).label('trend_vs_previous'),
                    func.max(AggCustomerBalancesDaily.updated_at).label('updated_at'),
                )
                .select_from(AggCustomerBalancesDaily)
                .join(DimCustomer, DimCustomer.external_id == AggCustomerBalancesDaily.customer_ext_id, isouter=True)
                .where(AggCustomerBalancesDaily.balance_date == latest_date)
                .where(customer_id_expr.is_not(None))
            )
            if branches is not None:
                grouped = grouped.where(AggCustomerBalancesDaily.branch_ext_id.in_(branches))
            q_clean = _normalize_search_term(q)
            if q_clean:
                like = f'%{q_clean}%'
                grouped = grouped.where(
                    _sql_normalized_text(customer_id_expr).like(like)
                    | _sql_normalized_text(customer_code_expr).like(like)
                    | _sql_normalized_text(customer_name_expr).like(like)
                )
            grouped = grouped.group_by(
                customer_id_expr,
                customer_code_expr,
                customer_name_expr,
                AggCustomerBalancesDaily.balance_date,
            ).subquery('receivables_top_customer_rows')
            totals_row = (
                await db.execute(
                    select(
                        func.coalesce(func.count(), 0).label('customers'),
                        func.coalesce(func.sum(grouped.c.open_balance), 0).label('total_open_balance'),
                        func.coalesce(func.sum(grouped.c.overdue_balance), 0).label('total_overdue_balance'),
                    ).select_from(grouped)
                )
            ).mappings().one()
            total_open_balance = float(totals_row.get('total_open_balance') or 0)
            page_rows = (
                await db.execute(
                    select(grouped)
                    .order_by(grouped.c.open_balance.desc(), grouped.c.overdue_balance.desc(), grouped.c.customer_name.asc())
                    .offset(max(0, int(offset)))
                    .limit(max(1, min(int(limit), 500)))
                )
            ).mappings().all()
            page = []
            for row in page_rows:
                open_balance = float(row.get('open_balance') or 0)
                overdue_balance = float(row.get('overdue_balance') or 0)
                balance_date_val = row.get('balance_date')
                page.append(
                    {
                        'customer_id': str(row.get('customer_id') or '').strip(),
                        'code': str(row.get('customer_code') or row.get('customer_id') or '').strip(),
                        'name': str(row.get('customer_name') or row.get('customer_code') or row.get('customer_id') or '').strip(),
                        'open_balance': open_balance,
                        'overdue_balance': overdue_balance,
                        'overdue_ratio_pct': ((overdue_balance / open_balance) * 100.0) if open_balance > 0 else 0.0,
                        'aging_bucket_0_30': float(row.get('aging_bucket_0_30') or 0),
                        'aging_bucket_31_60': float(row.get('aging_bucket_31_60') or 0),
                        'aging_bucket_61_90': float(row.get('aging_bucket_61_90') or 0),
                        'aging_bucket_90_plus': float(row.get('aging_bucket_90_plus') or 0),
                        'last_collection_date': '',
                        'trend_vs_previous': float(row.get('trend_vs_previous') or 0),
                        'balance_date': _raw_scalar(balance_date_val),
                        'updated_at': _raw_scalar(row.get('updated_at')),
                        'share_pct': (open_balance / total_open_balance * 100.0) if total_open_balance > 0 else 0.0,
                    }
                )
            return {
                'as_of': date_to.isoformat(),
                'summary': {
                    'customers': int(totals_row.get('customers') or 0),
                    'total_open_balance': total_open_balance,
                    'total_overdue_balance': float(totals_row.get('total_overdue_balance') or 0),
                },
                'limit': int(limit),
                'offset': int(offset),
                'rows': page,
            }

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

    counterparty_col = func.lower(cast(func.coalesce(FactCashflow.counterparty_id, literal('')), String))
    notes_col = func.lower(cast(func.coalesce(FactCashflow.notes, literal('')), String))
    ref_col = func.lower(cast(func.coalesce(FactCashflow.reference_no, literal('')), String))
    term_filters = [notes_col.like(f'%{term}%') | ref_col.like(f'%{term}%') for term in search_terms]
    identity_filters = [counterparty_col == target.lower()]
    if term_filters:
        identity_filters.append(or_(*term_filters))

    if identity_filters:
        subcategory_expr = _cashflow_subcategory_expr()
        signed_amount_expr = _cashflow_signed_amount_expr()
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
                func.coalesce(func.sum(signed_amount_expr), 0).label('total_value'),
                func.max(FactCashflow.updated_at).label('updated_at'),
            )
            .select_from(FactCashflow)
            .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
            .where(subcategory_expr.in_(_customer_collection_subcategories()))
            .where(or_(*identity_filters))
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
    signed_amount_expr = _cashflow_signed_amount_expr()
    stmt = (
        select(
            func.coalesce(func.count(FactCashflow.id), 0).label('entries'),
            func.coalesce(func.sum(case((signed_amount_expr > 0, signed_amount_expr), else_=literal(0.0))), 0).label('inflows'),
            func.coalesce(func.sum(case((signed_amount_expr < 0, -signed_amount_expr), else_=literal(0.0))), 0).label('outflows'),
            func.coalesce(func.sum(signed_amount_expr), 0).label('net'),
        )
        .select_from(FactCashflow)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
    )
    branches = _effective_branch_filter(branches)
    if branches is not None:
        stmt = stmt.where(FactCashflow.branch_id.in_(select(DimBranch.id).where(DimBranch.external_id.in_(branches))))
    row = (await db.execute(stmt)).mappings().first() or {}
    entries = int(row.get('entries') or 0)
    inflows = float(row.get('inflows') or 0)
    outflows = float(row.get('outflows') or 0)
    net = float(row.get('net') or 0)
    return {
        'entries': entries,
        'inflows': inflows,
        'outflows': outflows,
        'net': net,
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
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return []

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
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    )
    stmt = stmt.where(FactInventory.id.in_(_deduped_snapshot_fact_ids(snapshot_date)))
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
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return []

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
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    )
    stmt = stmt.where(FactInventory.id.in_(_deduped_snapshot_fact_ids(snapshot_date)))
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
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return []

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
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    )
    stmt = stmt.where(FactInventory.id.in_(_deduped_snapshot_fact_ids(snapshot_date)))
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


async def export_filter_options(db: AsyncSession) -> dict[str, list[dict[str, str]]]:
    """Filter options for the Εξαγωγές circuit (Αναφορές + CSV/Excel).

    Returns branch / warehouse / brand dimensions ({value: external_id, label: name})
    plus the three basic item category levels (category_1/2/3) each item belongs to,
    sourced as distinct non-empty text values from dim_items. Shared by the Reports
    and CSV/Excel pages so the two stay in parity.
    """
    async def _dim_options(model) -> list[dict[str, str]]:
        rows = (
            await db.execute(
                select(model.external_id, model.name)
                .where(model.external_id.is_not(None))
                .order_by(func.lower(func.coalesce(model.name, model.external_id)))
            )
        ).all()
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for ext, name in rows:
            value = str(ext or '').strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append({'value': value, 'label': (str(name or '').strip() or value)})
        return out

    async def _item_category_options(column) -> list[dict[str, str]]:
        clean = _softone_clean_dimension_text(column)
        # ORDER BY must reference the selected expression for SELECT DISTINCT.
        rows = (
            await db.execute(
                select(clean.label('v')).distinct().where(clean.is_not(None)).order_by(clean)
            )
        ).scalars().all()
        return [{'value': str(v), 'label': str(v)} for v in rows if str(v or '').strip()]

    # The three item category levels are interdependent: a category_2 belongs to
    # a category_1, and a category_3 to a category_2. Ship the distinct
    # (c1, c2, c3) combinations so the UI can cascade the dropdowns client-side.
    c1 = _softone_clean_dimension_text(DimItem.category_1)
    c2 = _softone_clean_dimension_text(DimItem.category_2)
    c3 = _softone_clean_dimension_text(DimItem.category_3)
    hier_rows = (
        await db.execute(select(c1, c2, c3).distinct().order_by(c1, c2, c3))
    ).all()
    category_hierarchy = [
        {'c1': str(a or ''), 'c2': str(b or ''), 'c3': str(c or '')}
        for a, b, c in hier_rows
    ]

    supplier_rows = (await db.execute(
        select(DimItem.preferred_supplier_ext_id, func.max(DimItem.preferred_supplier_name))
        .where(func.coalesce(func.trim(DimItem.preferred_supplier_ext_id), '') != '')
        .group_by(DimItem.preferred_supplier_ext_id)
    )).all()
    suppliers = sorted(
        ({'value': str(r[0]), 'label': str(r[1] or r[0])} for r in supplier_rows if str(r[0] or '').strip()),
        key=lambda o: o['label'].lower(),
    )
    payment_rows = (await db.execute(
        select(FactSales.payment_method).distinct().where(func.coalesce(func.trim(FactSales.payment_method), '') != '')
    )).all()
    payments = sorted(
        ({'value': str(r[0]), 'label': str(r[0])} for r in payment_rows if str(r[0] or '').strip()),
        key=lambda o: o['label'].lower(),
    )
    channel_rows = (await db.execute(
        select(FactSales.channel_name).distinct().where(func.coalesce(func.trim(FactSales.channel_name), '') != '')
    )).all()
    # Empty channel = physical store; expose it as a selectable value alongside real channels.
    channels = [{'value': '__physical__', 'label': 'Φυσικό κατάστημα'}] + sorted(
        ({'value': str(r[0]), 'label': str(r[0])} for r in channel_rows if str(r[0] or '').strip()),
        key=lambda o: o['label'].lower(),
    )

    return {
        'branches': await _dim_options(DimBranch),
        'warehouses': await _dim_options(DimWarehouse),
        'brands': await _dim_options(DimBrand),
        'group': await _dim_options(DimGroup),
        'category_1': await _item_category_options(DimItem.category_1),
        'category_2': await _item_category_options(DimItem.category_2),
        'category_3': await _item_category_options(DimItem.category_3),
        'category_hierarchy': category_hierarchy,
        'supplier': suppliers,
        'payment': payments,
        'channel': channels,
    }


async def export_item_rows(
    db: AsyncSession,
    *,
    brands: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    groups: list[str] | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    limit: int = 1000,
) -> list[dict[str, str]]:
    """Item rows for the Εξαγωγές report: name, barcode, brand, category 1/2/3,
    plus sold quantity (units) and value for the selected scope.

    Brand/category filters apply to the item master. Sold quantity/value are
    aggregated from fact_sales within the selected branch / warehouse / period,
    using the same behaviour-aware signing as the dashboard (returns subtract).
    Only items with sales activity in that scope are returned, ordered by value.
    """
    qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    val_expr = _fact_sales_signed_net_expr()
    sales = select(
        FactSales.item_id.label('iid'),
        func.coalesce(func.sum(qty_expr), 0).label('sold_qty'),
        func.coalesce(func.sum(val_expr), 0).label('sold_value'),
    ).where(FactSales.item_id.is_not(None))
    if period_from:
        sales = sales.where(FactSales.doc_date >= period_from)
    if period_to:
        sales = sales.where(FactSales.doc_date <= period_to)
    if branches:
        sales = sales.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        sales = sales.where(FactSales.warehouse_ext_id.in_(warehouses))
    sales = sales.group_by(FactSales.item_id).subquery()

    stmt = (
        select(
            DimItem.name,
            DimItem.barcode,
            DimBrand.name.label('brand'),
            DimItem.category_1,
            DimItem.category_2,
            DimItem.category_3,
            sales.c.sold_qty,
            sales.c.sold_value,
        )
        .select_from(DimItem)
        .join(sales, sales.c.iid == DimItem.id)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
    )
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if groups:
        stmt = stmt.join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True).where(
            DimGroup.external_id.in_(groups)
        )
    if category_1:
        stmt = stmt.where(DimItem.category_1.in_(category_1))
    if category_2:
        stmt = stmt.where(DimItem.category_2.in_(category_2))
    if category_3:
        stmt = stmt.where(DimItem.category_3.in_(category_3))
    stmt = stmt.order_by(sales.c.sold_value.desc().nullslast()).limit(limit)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'name': str(r[0] or ''),
            'barcode': str(r[1] or ''),
            'brand': str(r[2] or ''),
            'category_1': str(r[3] or ''),
            'category_2': str(r[4] or ''),
            'category_3': str(r[5] or ''),
            'sold_qty': float(r[6] or 0),
            'sold_value': float(r[7] or 0),
        }
        for r in rows
    ]


async def export_item_totals(
    db: AsyncSession,
    *,
    brands: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    groups: list[str] | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
) -> dict[str, float]:
    """Grand totals (item count, sold quantity, value) over the FULL filtered
    set — independent of the on-screen row cap — for the report's ΣΥΝΟΛΟ line."""
    qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    val_expr = _fact_sales_signed_net_expr()
    sales = select(
        FactSales.item_id.label('iid'),
        func.coalesce(func.sum(qty_expr), 0).label('sold_qty'),
        func.coalesce(func.sum(val_expr), 0).label('sold_value'),
    ).where(FactSales.item_id.is_not(None))
    if period_from:
        sales = sales.where(FactSales.doc_date >= period_from)
    if period_to:
        sales = sales.where(FactSales.doc_date <= period_to)
    if branches:
        sales = sales.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        sales = sales.where(FactSales.warehouse_ext_id.in_(warehouses))
    sales = sales.group_by(FactSales.item_id).subquery()

    stmt = (
        select(
            func.count(),
            func.coalesce(func.sum(sales.c.sold_qty), 0),
            func.coalesce(func.sum(sales.c.sold_value), 0),
        )
        .select_from(DimItem)
        .join(sales, sales.c.iid == DimItem.id)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
    )
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if groups:
        stmt = stmt.join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True).where(
            DimGroup.external_id.in_(groups)
        )
    if category_1:
        stmt = stmt.where(DimItem.category_1.in_(category_1))
    if category_2:
        stmt = stmt.where(DimItem.category_2.in_(category_2))
    if category_3:
        stmt = stmt.where(DimItem.category_3.in_(category_3))
    row = (await db.execute(stmt)).first()
    return {
        'count': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'value': float(row[2] or 0),
    }


async def sales_by_channel(
    db: AsyncSession,
    *,
    brands: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    groups: list[str] | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
) -> tuple[list[dict], dict]:
    """Sales contribution per sales channel (channel_name), with returns signed
    like the dashboard. Blank channel = 'Φυσικό κατάστημα'. Returns (rows, totals)
    where each row has channel, net_value, contribution_pct and margin_pct.

    Margin uses profit_amount (gross profit), because the cost_amount column mirrors
    net_value in this dataset and would yield a zero margin.
    """
    net_expr = _fact_sales_signed_net_expr()
    profit_expr = func.coalesce(FactSales.profit_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    qty_expr = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    channel_label = func.coalesce(
        func.nullif(func.trim(func.coalesce(FactSales.channel_name, '')), ''),
        literal('Φυσικό κατάστημα'),
    )
    stmt = select(
        channel_label.label('channel'),
        func.coalesce(func.sum(net_expr), 0).label('net_value'),
        func.coalesce(func.sum(profit_expr), 0).label('profit_amount'),
        func.coalesce(func.sum(qty_expr), 0).label('qty'),
    ).select_from(FactSales)
    if brands or category_1 or category_2 or category_3 or groups:
        stmt = stmt.join(DimItem, FactSales.item_id == DimItem.id)
        if brands:
            stmt = stmt.join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True).where(
                DimBrand.external_id.in_(brands)
            )
        if groups:
            stmt = stmt.join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True).where(
                DimGroup.external_id.in_(groups)
            )
        if category_1:
            stmt = stmt.where(DimItem.category_1.in_(category_1))
        if category_2:
            stmt = stmt.where(DimItem.category_2.in_(category_2))
        if category_3:
            stmt = stmt.where(DimItem.category_3.in_(category_3))
    if period_from:
        stmt = stmt.where(FactSales.doc_date >= period_from)
    if period_to:
        stmt = stmt.where(FactSales.doc_date <= period_to)
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    stmt = stmt.group_by(channel_label).order_by(func.sum(net_expr).desc())
    raw = (await db.execute(stmt)).all()

    total_net = sum(float(r[1] or 0) for r in raw)
    total_profit = sum(float(r[2] or 0) for r in raw)
    total_qty = sum(float(r[3] or 0) for r in raw)
    rows = []
    for channel, net_raw, profit_raw, qty_raw in raw:
        net = float(net_raw or 0)
        profit = float(profit_raw or 0)
        rows.append({
            'channel': str(channel or 'Φυσικό κατάστημα'),
            'net_value': net,
            'qty': float(qty_raw or 0),
            'contribution_pct': (net / total_net * 100.0) if total_net else 0.0,
            'margin_pct': (profit / net * 100.0) if net else 0.0,
        })
    totals = {
        'count': len(rows),
        'net_value': total_net,
        'qty': total_qty,
        'contribution_pct': 100.0 if total_net else 0.0,
        'margin_pct': (total_profit / total_net * 100.0) if total_net else 0.0,
    }
    return rows, totals


_PIVOT_DIMENSIONS = {
    'item': 'Είδος',
    'channel': 'Κανάλι',
    'group': 'Ομάδα Ειδών',
    'brand': 'Brand',
    'category_1': 'Κατηγορία 1',
    'category_2': 'Κατηγορία 2',
    'category_3': 'Κατηγορία 3',
    'branch': 'Υποκατάστημα',
    'warehouse': 'Αποθηκευτικός χώρος',
    'supplier': 'Προμηθευτής',
    'payment': 'Τρόπος πληρωμής',
}


def _pivot_label_expr(group_by: str):
    """SQL label expression for a pivot dimension (with a sensible fallback)."""
    def _clean(col, fallback):
        return func.coalesce(func.nullif(func.trim(func.coalesce(col, '')), ''), literal(fallback))
    if group_by == 'item':
        return func.coalesce(func.nullif(func.trim(func.coalesce(DimItem.name, '')), ''), DimItem.external_id, literal('—'))
    if group_by == 'group':
        return _clean(DimGroup.name, 'Χωρίς ομάδα')
    if group_by == 'brand':
        return _clean(DimBrand.name, 'Χωρίς brand')
    if group_by in ('category_1', 'category_2', 'category_3'):
        return _clean(getattr(DimItem, group_by), '(κενό)')
    if group_by == 'branch':
        return func.coalesce(func.nullif(func.trim(func.coalesce(DimBranch.name, '')), ''), FactSales.branch_ext_id, literal('N/A'))
    if group_by == 'warehouse':
        return func.coalesce(func.nullif(func.trim(func.coalesce(DimWarehouse.name, '')), ''), FactSales.warehouse_ext_id, literal('N/A'))
    if group_by == 'supplier':
        return _clean(DimItem.preferred_supplier_name, 'Χωρίς προμηθευτή')
    if group_by == 'payment':
        return _clean(FactSales.payment_method, 'Χωρίς τρόπο πληρωμής')
    return _clean(FactSales.channel_name, 'Φυσικό κατάστημα')  # default: channel


async def sales_pivot(
    db: AsyncSession,
    *,
    group_by: str = 'channel',
    mode: str = 'analysis',
    period_from: date | None = None,
    period_to: date | None = None,
    period_b_from: date | None = None,
    period_b_to: date | None = None,
    brands: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    groups: list[str] | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    suppliers: list[str] | None = None,
    payments: list[str] | None = None,
    channels: list[str] | None = None,
) -> tuple[list[dict], dict]:
    """One flexible sales report: group by ANY dimension (channel/group/brand/
    category 1-3/branch/warehouse) in one of two modes.

    mode='analysis'  -> rows {label, net_value, qty, contribution_pct, margin_pct}
    mode='comparison'-> rows {label, turnover_a, cost_a, profit_a, turnover_b,
                              cost_b, profit_b} across period A vs B.

    Turnover = signed net, Profit = signed profit_amount, Cost = Turnover-Profit,
    Qty = signed quantity — all signed like the dashboard (returns subtract).
    """
    if group_by not in _PIVOT_DIMENSIONS:
        group_by = 'channel'
    label_expr = _pivot_label_expr(group_by)
    _amt_sign = _fact_sales_behavior_sign_expr(quantity=False)
    net = _fact_sales_signed_net_expr()
    profit = func.coalesce(FactSales.profit_amount, 0) * _amt_sign
    qty = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    gross = _fact_sales_signed_gross_expr()
    vat = func.coalesce(FactSales.vat_amount, 0) * _amt_sign
    discount = func.coalesce(FactSales.discount_amount, 0) * _amt_sign
    doc_key = _fact_sales_document_key_expr()

    need_item = group_by in {'item', 'group', 'brand', 'category_1', 'category_2', 'category_3', 'supplier'} or bool(
        brands or groups or category_1 or category_2 or category_3 or suppliers
    )
    need_brand = group_by in {'brand', 'item'} or bool(brands)
    need_group = group_by in {'group', 'item'} or bool(groups)

    def _windowed(expr, window):
        return func.coalesce(func.sum(case((window, expr), else_=literal(0.0))), 0) if window is not None else literal(0.0)

    if mode == 'comparison':
        in_a = (FactSales.doc_date >= period_from) & (FactSales.doc_date <= period_to) if (period_from and period_to) else None
        in_b = (FactSales.doc_date >= period_b_from) & (FactSales.doc_date <= period_b_to) if (period_b_from and period_b_to) else None
        stmt = select(
            label_expr.label('label'),
            _windowed(net, in_a).label('turnover_a'),
            _windowed(profit, in_a).label('profit_a'),
            _windowed(net, in_b).label('turnover_b'),
            _windowed(profit, in_b).label('profit_b'),
        ).select_from(FactSales)
    else:
        stmt = select(
            label_expr.label('label'),
            func.coalesce(func.sum(net), 0).label('net_value'),
            func.coalesce(func.sum(qty), 0).label('qty'),
            func.coalesce(func.sum(profit), 0).label('profit'),
            func.coalesce(func.sum(gross), 0).label('gross_value'),
            func.coalesce(func.sum(vat), 0).label('vat'),
            func.coalesce(func.sum(discount), 0).label('discount'),
            func.count(func.distinct(doc_key)).label('doc_count'),
            func.count(func.distinct(FactSales.item_id)).label('item_count'),
        ).select_from(FactSales)
        if group_by == 'item':
            # Item attributes as available columns (constant per grouped item).
            stmt = stmt.add_columns(
                func.max(DimItem.barcode).label('a_barcode'),
                func.max(DimBrand.name).label('a_brand'),
                func.max(DimItem.category_1).label('a_cat1'),
                func.max(DimItem.category_2).label('a_cat2'),
                func.max(DimItem.category_3).label('a_cat3'),
                func.max(DimGroup.name).label('a_group'),
            )

    if need_item:
        stmt = stmt.join(DimItem, FactSales.item_id == DimItem.id, isouter=True)
    if need_brand:
        stmt = stmt.join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
    if need_group:
        stmt = stmt.join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
    if group_by == 'branch':
        stmt = stmt.join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
    if group_by == 'warehouse':
        stmt = stmt.join(DimWarehouse, DimWarehouse.external_id == FactSales.warehouse_ext_id, isouter=True)

    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    if category_1:
        stmt = stmt.where(DimItem.category_1.in_(category_1))
    if category_2:
        stmt = stmt.where(DimItem.category_2.in_(category_2))
    if category_3:
        stmt = stmt.where(DimItem.category_3.in_(category_3))
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if suppliers:
        stmt = stmt.where(DimItem.preferred_supplier_ext_id.in_(suppliers))
    if payments:
        stmt = stmt.where(FactSales.payment_method.in_(payments))
    if channels:
        _real_ch = [c for c in channels if c != '__physical__']
        _ch_conds = []
        if _real_ch:
            _ch_conds.append(FactSales.channel_name.in_(_real_ch))
        if '__physical__' in channels:
            _ch_conds.append(func.coalesce(func.trim(func.coalesce(FactSales.channel_name, '')), '') == '')
        if _ch_conds:
            stmt = stmt.where(or_(*_ch_conds))

    if mode == 'comparison':
        windows = [w for w in (in_a, in_b) if w is not None]
        if windows:
            scope = windows[0]
            for w in windows[1:]:
                scope = scope | w
            stmt = stmt.where(scope)
    else:
        if period_from:
            stmt = stmt.where(FactSales.doc_date >= period_from)
        if period_to:
            stmt = stmt.where(FactSales.doc_date <= period_to)

    # ORDER BY the 2nd select column (the primary value metric) descending.
    # Group items by id (so equally-named items stay distinct); everything else by label.
    group_col = DimItem.id if group_by == 'item' else label_expr
    stmt = stmt.group_by(group_col).order_by(literal_column('2').desc())
    if group_by == 'item':
        stmt = stmt.limit(5000)
    raw = (await db.execute(stmt)).mappings().all()

    rows: list[dict] = []
    if mode == 'comparison':
        tot = {'turnover_a': 0.0, 'cost_a': 0.0, 'profit_a': 0.0, 'turnover_b': 0.0, 'cost_b': 0.0, 'profit_b': 0.0}
        def _pct(cur, base):
            # No % change is definable from a zero baseline (item had no sales in period B);
            # return None so the UI shows nothing instead of a misleading +100%.
            return ((cur - base) / abs(base) * 100.0) if base else None
        for r in raw:
            ta, pa = float(r['turnover_a'] or 0), float(r['profit_a'] or 0)
            tb, pb = float(r['turnover_b'] or 0), float(r['profit_b'] or 0)
            ca, cb = ta - pa, tb - pb
            row = {'label': str(r['label'] or '—'),
                   'turnover_a': ta, 'cost_a': ca, 'profit_a': pa,
                   'turnover_b': tb, 'cost_b': cb, 'profit_b': pb,
                   # Per-metric Δ%: each A metric vs its B counterpart, and each B metric vs A.
                   'delta_pct': _pct(ta, tb),
                   'd_turnover_a': _pct(ta, tb), 'd_cost_a': _pct(ca, cb), 'd_profit_a': _pct(pa, pb),
                   'd_turnover_b': _pct(tb, ta), 'd_cost_b': _pct(cb, ca), 'd_profit_b': _pct(pb, pa)}
            rows.append(row)
            for k in ('turnover_a', 'cost_a', 'profit_a', 'turnover_b', 'cost_b', 'profit_b'):
                tot[k] += row[k]
        tot['count'] = len(rows)
        tot['delta_pct'] = _pct(tot['turnover_a'], tot['turnover_b'])
        tot['d_turnover_a'] = _pct(tot['turnover_a'], tot['turnover_b'])
        tot['d_cost_a'] = _pct(tot['cost_a'], tot['cost_b'])
        tot['d_profit_a'] = _pct(tot['profit_a'], tot['profit_b'])
        tot['d_turnover_b'] = _pct(tot['turnover_b'], tot['turnover_a'])
        tot['d_cost_b'] = _pct(tot['cost_b'], tot['cost_a'])
        tot['d_profit_b'] = _pct(tot['profit_b'], tot['profit_a'])
        totals = tot
    else:
        total_net = sum(float(r['net_value'] or 0) for r in raw)
        total_profit = sum(float(r['profit'] or 0) for r in raw)

        def _metric_row(label, netv, prof, qtyv, grossv, vatv, disc, docs, items):
            cost = netv - prof
            return {
                'label': label,
                'net_value': netv, 'qty': qtyv,
                'contribution_pct': (netv / total_net * 100.0) if total_net else 0.0,
                'margin_pct': (prof / netv * 100.0) if netv else 0.0,
                'cost': cost, 'profit': prof, 'gross_value': grossv,
                'doc_count': docs, 'item_count': items,
                'avg_per_doc': (netv / docs) if docs else 0.0,
                'avg_per_item': (netv / items) if items else 0.0,
                'vat': vatv, 'discount': disc,
            }

        agg = {'net_value': 0.0, 'qty': 0.0, 'profit': 0.0, 'gross_value': 0.0,
               'vat': 0.0, 'discount': 0.0, 'doc_count': 0, 'item_count': 0}
        for r in raw:
            netv = float(r['net_value'] or 0)
            row = _metric_row(
                str(r['label'] or '—'), netv, float(r['profit'] or 0), float(r['qty'] or 0),
                float(r['gross_value'] or 0), float(r['vat'] or 0), float(r['discount'] or 0),
                int(r['doc_count'] or 0), int(r['item_count'] or 0),
            )
            if group_by == 'item':
                row['a_barcode'] = str(r.get('a_barcode') or '')
                row['a_brand'] = str(r.get('a_brand') or '')
                row['a_cat1'] = str(r.get('a_cat1') or '')
                row['a_cat2'] = str(r.get('a_cat2') or '')
                row['a_cat3'] = str(r.get('a_cat3') or '')
                row['a_group'] = str(r.get('a_group') or '')
            rows.append(row)
            for k in agg:
                agg[k] += row[k]
        totals = {
            'count': len(rows),
            'net_value': agg['net_value'], 'qty': agg['qty'],
            'contribution_pct': 100.0 if total_net else 0.0,
            'margin_pct': (total_profit / total_net * 100.0) if total_net else 0.0,
            'cost': agg['net_value'] - agg['profit'], 'profit': agg['profit'],
            'gross_value': agg['gross_value'], 'doc_count': agg['doc_count'], 'item_count': agg['item_count'],
            'avg_per_doc': (agg['net_value'] / agg['doc_count']) if agg['doc_count'] else 0.0,
            'avg_per_item': (agg['net_value'] / agg['item_count']) if agg['item_count'] else 0.0,
            'vat': agg['vat'], 'discount': agg['discount'],
        }
    return rows, totals


async def sales_comparison_by_group(
    db: AsyncSession,
    *,
    period_a_from: date | None = None,
    period_a_to: date | None = None,
    period_b_from: date | None = None,
    period_b_to: date | None = None,
    brands: list[str] | None = None,
    category_1: list[str] | None = None,
    category_2: list[str] | None = None,
    category_3: list[str] | None = None,
    groups: list[str] | None = None,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
) -> tuple[list[dict], dict]:
    """Sales comparison per item group across two periods (A vs B): Turnover, Cost
    and Profit for each. Turnover = signed net_value, Profit = signed profit_amount,
    Cost = Turnover - Profit (cost_amount mirrors net in this dataset). Items with
    no group fall under 'Χωρίς ομάδα'. Returns (rows, totals)."""
    net = _fact_sales_signed_net_expr()
    profit = func.coalesce(FactSales.profit_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    group_label = func.coalesce(
        func.nullif(func.trim(func.coalesce(DimGroup.name, '')), ''),
        literal('Χωρίς ομάδα'),
    )

    def _window(dfrom, dto):
        if dfrom and dto:
            return (FactSales.doc_date >= dfrom) & (FactSales.doc_date <= dto)
        return None

    in_a = _window(period_a_from, period_a_to)
    in_b = _window(period_b_from, period_b_to)
    zero = literal(0.0)
    net_a = func.sum(case((in_a, net), else_=zero)) if in_a is not None else zero
    profit_a = func.sum(case((in_a, profit), else_=zero)) if in_a is not None else zero
    net_b = func.sum(case((in_b, net), else_=zero)) if in_b is not None else zero
    profit_b = func.sum(case((in_b, profit), else_=zero)) if in_b is not None else zero

    stmt = (
        select(
            group_label.label('grp'),
            func.coalesce(net_a, 0).label('turnover_a'),
            func.coalesce(profit_a, 0).label('profit_a'),
            func.coalesce(net_b, 0).label('turnover_b'),
            func.coalesce(profit_b, 0).label('profit_b'),
        )
        .select_from(FactSales)
        .join(DimItem, FactSales.item_id == DimItem.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
    )
    if brands:
        stmt = stmt.join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True).where(
            DimBrand.external_id.in_(brands)
        )
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    if category_1:
        stmt = stmt.where(DimItem.category_1.in_(category_1))
    if category_2:
        stmt = stmt.where(DimItem.category_2.in_(category_2))
    if category_3:
        stmt = stmt.where(DimItem.category_3.in_(category_3))
    # Restrict the scan to rows inside either comparison window.
    windows = [w for w in (in_a, in_b) if w is not None]
    if windows:
        scope = windows[0]
        for w in windows[1:]:
            scope = scope | w
        stmt = stmt.where(scope)
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    stmt = stmt.group_by(group_label).order_by(func.coalesce(net_a, 0).desc())
    raw = (await db.execute(stmt)).all()

    rows = []
    tot = {'turnover_a': 0.0, 'cost_a': 0.0, 'profit_a': 0.0, 'turnover_b': 0.0, 'cost_b': 0.0, 'profit_b': 0.0}
    for grp, ta_raw, pa_raw, tb_raw, pb_raw in raw:
        ta = float(ta_raw or 0)
        pa = float(pa_raw or 0)
        tb = float(tb_raw or 0)
        pb = float(pb_raw or 0)
        row = {
            'group': str(grp or 'Χωρίς ομάδα'),
            'turnover_a': ta, 'cost_a': ta - pa, 'profit_a': pa,
            'turnover_b': tb, 'cost_b': tb - pb, 'profit_b': pb,
        }
        rows.append(row)
        for k in tot:
            tot[k] += row[k]
    tot['count'] = len(rows)
    return rows, tot


def _non_pharmacy_warehouse():
    """EXISTS condition that is true when a fact_inventory row sits in a NON-sellable
    warehouse of the store — the e-shop / Click&Collect space, the damaged/expired
    ('ακατάλληλα') space, an in-transit / reconciliation space — as opposed to the actual
    pharmacy shelf. Every store stock metric (value, dead stock, availability, transfers)
    must count ONLY the pharmacy warehouse, so these are excluded via not_(...).
    Identified by the store's warehouse naming convention; rows with no matching
    dim_warehouses entry are treated as pharmacy stock (kept)."""
    excl = or_(
        DimWarehouse.name.ilike('%eshop%'),
        DimWarehouse.name.ilike('%e-shop%'),
        DimWarehouse.name.ilike('%e shop%'),
        DimWarehouse.name.ilike('%ακαταλλ%'),      # ακατάλληλα (unfit)
        DimWarehouse.name.ilike('%ακατάλληλ%'),
        DimWarehouse.name.ilike('%κατεστραμμ%'),   # Κατεστραμμένα (destroyed)
        DimWarehouse.name.ilike('%ληγμέν%'),       # ληγμένα (expired)
        DimWarehouse.name.ilike('%transit%'),
        DimWarehouse.name.ilike('%ενδιάμεσ%'),     # ενδιάμεσος (in transit)
        DimWarehouse.name.ilike('%διαφορ%'),       # Διαφορές (reconciliation)
    )
    return exists().where(and_(DimWarehouse.external_id == FactInventory.warehouse_ext_id, excl))


async def store_dashboard(
    db: AsyncSession,
    *,
    branch_ext: str,
    date_from: date,
    date_to: date,
    dead_days: int = 90,
    top_n: int = 15,
) -> dict:
    """Prescriptive per-store cockpit: KPIs + health, best-sellers now out of
    stock (lost sales), dead stock (tied capital), and declining categories vs
    last year. All sales signed like the dashboard."""
    days = max(1, (date_to - date_from).days + 1)
    snapshot = await _latest_inventory_snapshot_date(db, date_to)
    net = _fact_sales_signed_net_expr()
    profit = func.coalesce(FactSales.profit_amount, 0) * _fact_sales_behavior_sign_expr(quantity=False)
    qty = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)

    srow = (await db.execute(
        select(func.coalesce(func.sum(net), 0), func.coalesce(func.sum(profit), 0), func.coalesce(func.sum(qty), 0))
        .where(FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from, FactSales.doc_date <= date_to)
    )).first()
    sales_net, sales_profit, sales_qty = float(srow[0] or 0), float(srow[1] or 0), float(srow[2] or 0)

    expenses = float((await db.execute(
        select(func.coalesce(func.sum(FactExpense.amount_net), 0))
        .where(FactExpense.branch_ext_id == branch_ext, FactExpense.expense_date >= date_from, FactExpense.expense_date <= date_to)
    )).scalar() or 0)

    stock_val = stock_cost = 0.0
    if snapshot:
        strow = (await db.execute(
            select(func.coalesce(func.sum(FactInventory.value_amount), 0), func.coalesce(func.sum(FactInventory.cost_amount), 0))
            .where(FactInventory.branch_ext_id == branch_ext, FactInventory.doc_date == snapshot, FactInventory.movement_type == 'snapshot', not_(_non_pharmacy_warehouse()))
        )).first()
        stock_val, stock_cost = float(strow[0] or 0), float(strow[1] or 0)

    cogs = sales_net - sales_profit
    margin_pct = (sales_profit / sales_net * 100.0) if sales_net else 0.0
    dio = (stock_cost / (cogs / days)) if cogs > 0 else 0.0
    kpis = {
        'net': sales_net, 'profit': sales_profit, 'margin_pct': margin_pct, 'qty': sales_qty,
        'expenses': expenses, 'net_result': sales_profit - expenses,
        'stock_value': stock_val, 'stock_cost': stock_cost, 'dio': dio,
    }

    # Lost sales — sold in the period but 0 stock now (best-sellers gone missing).
    sold = select(
        FactSales.item_id.label('iid'),
        func.coalesce(func.sum(net), 0).label('sv'),
        func.coalesce(func.sum(qty), 0).label('sq'),
    ).where(
        FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from,
        FactSales.doc_date <= date_to, FactSales.item_id.is_not(None),
    ).group_by(FactSales.item_id).subquery()
    lost = []
    if snapshot:
        stock_sub = select(
            FactInventory.item_id.label('iid'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('soh'),
        ).where(
            FactInventory.branch_ext_id == branch_ext, FactInventory.doc_date == snapshot,
            FactInventory.movement_type == 'snapshot', not_(_non_pharmacy_warehouse()),
        ).group_by(FactInventory.item_id).subquery()
        lstmt = (
            select(DimItem.name, DimItem.barcode, sold.c.sv, sold.c.sq, func.coalesce(stock_sub.c.soh, 0).label('soh'))
            .select_from(sold).join(DimItem, sold.c.iid == DimItem.id)
            .join(stock_sub, stock_sub.c.iid == sold.c.iid, isouter=True)
            .where(
                sold.c.sv > 0,
                func.coalesce(stock_sub.c.soh, 0) <= 0,
                func.coalesce(func.trim(DimItem.barcode), '') != '',  # real stockable products only
            )
            .order_by(sold.c.sv.desc()).limit(top_n)
        )
        lost = [{'name': str(r[0] or ''), 'barcode': str(r[1] or ''), 'sold_value': float(r[2] or 0),
                 'sold_qty': float(r[3] or 0), 'lost_daily': float(r[2] or 0) / days}
                for r in (await db.execute(lstmt)).all()]

    # Dead stock — has stock now, but no sales at this store in `dead_days`.
    dead = []
    if snapshot:
        cutoff = date_to - timedelta(days=dead_days)
        recent = select(FactSales.item_id).where(
            FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= cutoff, FactSales.item_id.is_not(None),
        ).distinct()
        dstock = select(
            FactInventory.item_id.label('iid'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('soh'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('sv'),
        ).where(
            FactInventory.branch_ext_id == branch_ext, FactInventory.doc_date == snapshot,
            FactInventory.movement_type == 'snapshot', not_(_non_pharmacy_warehouse()),
        ).group_by(FactInventory.item_id).having(func.sum(FactInventory.qty_on_hand) > 0).subquery()
        dstmt = (
            select(DimItem.name, DimItem.barcode, dstock.c.soh, dstock.c.sv)
            .select_from(dstock).join(DimItem, dstock.c.iid == DimItem.id)
            .where(dstock.c.iid.notin_(recent))
            .order_by(dstock.c.sv.desc().nullslast()).limit(top_n)
        )
        dead = [{'name': str(r[0] or ''), 'barcode': str(r[1] or ''), 'stock_qty': float(r[2] or 0), 'tied_value': float(r[3] or 0)}
                for r in (await db.execute(dstmt)).all()]

    # Category decline vs last year (same period).
    def _shift(d):
        try:
            return d.replace(year=d.year - 1)
        except ValueError:
            return d.replace(year=d.year - 1, day=28)
    ly_from, ly_to = _shift(date_from), _shift(date_to)
    in_now = (FactSales.doc_date >= date_from) & (FactSales.doc_date <= date_to)
    in_ly = (FactSales.doc_date >= ly_from) & (FactSales.doc_date <= ly_to)
    cat = func.coalesce(func.nullif(func.trim(func.coalesce(DimItem.category_1, '')), ''), literal('(κενό)'))
    cstmt = (
        select(
            cat.label('c'),
            func.coalesce(func.sum(case((in_now, net), else_=literal(0.0))), 0).label('now'),
            func.coalesce(func.sum(case((in_ly, net), else_=literal(0.0))), 0).label('ly'),
        )
        .select_from(FactSales).join(DimItem, FactSales.item_id == DimItem.id)
        .where(FactSales.branch_ext_id == branch_ext, in_now | in_ly).group_by(cat)
    )
    decline = []
    for r in (await db.execute(cstmt)).all():
        now_v, ly_v = float(r[1] or 0), float(r[2] or 0)
        if ly_v > 0 and now_v < ly_v:
            decline.append({'category': str(r[0] or ''), 'now': now_v, 'ly': ly_v,
                            'delta': now_v - ly_v, 'delta_pct': (now_v - ly_v) / ly_v * 100.0})
    decline.sort(key=lambda x: x['delta'])
    decline = decline[:top_n]

    # Revenue by sales channel and by item group (this store, this period).
    chan_label = func.coalesce(func.nullif(func.trim(func.coalesce(FactSales.channel_name, '')), ''), literal('Φυσικό κατάστημα'))
    chan_rows = (await db.execute(
        select(chan_label, func.coalesce(func.sum(net), 0))
        .where(FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from, FactSales.doc_date <= date_to)
        .group_by(chan_label).order_by(func.sum(net).desc())
    )).all()
    tot_ch = sum(float(r[1] or 0) for r in chan_rows) or 0.0
    by_channel = [{'label': str(r[0] or ''), 'net': float(r[1] or 0),
                   'pct': (float(r[1] or 0) / tot_ch * 100.0) if tot_ch else 0.0} for r in chan_rows]

    grp_label = func.coalesce(func.nullif(func.trim(func.coalesce(DimGroup.name, '')), ''), literal('Χωρίς ομάδα'))
    grp_rows = (await db.execute(
        select(grp_label, func.coalesce(func.sum(net), 0))
        .select_from(FactSales)
        .join(DimItem, FactSales.item_id == DimItem.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from, FactSales.doc_date <= date_to)
        .group_by(grp_label).order_by(func.sum(net).desc())
    )).all()
    tot_g = sum(float(r[1] or 0) for r in grp_rows) or 0.0
    by_group = [{'label': str(r[0] or ''), 'net': float(r[1] or 0),
                 'pct': (float(r[1] or 0) / tot_g * 100.0) if tot_g else 0.0} for r in grp_rows]

    dead_val = sum(d['tied_value'] for d in dead)
    lost_val = sum(l['sold_value'] for l in lost)
    dead_ratio = (dead_val / stock_val) if stock_val else 0.0
    avail_pen = min(30.0, (lost_val / sales_net * 100.0) if sales_net else 0.0)
    kpis['health'] = int(max(0.0, min(100.0, 100 - dead_ratio * 50 - avail_pen + (margin_pct - 12))))
    kpis['lost_value'] = lost_val
    kpis['dead_value'] = dead_val

    return {
        'kpis': kpis,
        'snapshot': snapshot.isoformat() if snapshot else None,
        'lost_sales': lost,
        'dead_stock': dead,
        'category_decline': decline,
        'by_channel': by_channel,
        'by_group': by_group,
        'dead_days': dead_days,
    }


async def store_transfer_suggestions(
    db: AsyncSession,
    *,
    branch_ext: str,
    date_from: date,
    date_to: date,
    target_days: int = 14,
    top_n: int = 30,
) -> dict:
    """For the given store, propose stock TRANSFERS from other stores and an ORDER
    list for what no store can cover.

    Need at a store ≈ velocity(store) × target_days − current stock. A store with a
    positive need that is short receives; stores whose stock exceeds their own
    velocity-based need are the donors (their slow-moving surplus feeds where it
    sells). Quantities scale with each store's movement speed of the item.
    """
    days = max(1, (date_to - date_from).days + 1)
    snapshot = await _latest_inventory_snapshot_date(db, date_to)
    if snapshot is None:
        return {'transfers': [], 'to_order': [], 'target_days': target_days}

    qty = func.coalesce(FactSales.qty, 0) * _fact_sales_behavior_sign_expr(quantity=True)
    net = _fact_sales_signed_net_expr()

    _real = func.coalesce(func.trim(DimItem.barcode), '') != ''  # real stockable products only

    # Items this store actually sells in the period (bounds everything).
    items_here_sub = (
        select(func.distinct(FactSales.item_id))
        .select_from(FactSales).join(DimItem, FactSales.item_id == DimItem.id)
        .where(FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from,
               FactSales.doc_date <= date_to, FactSales.item_id.is_not(None), _real)
    ).scalar_subquery()

    # This store's velocity + value per item.
    here_rows = (await db.execute(
        select(FactSales.item_id, func.coalesce(func.sum(qty), 0), func.coalesce(func.sum(net), 0))
        .select_from(FactSales).join(DimItem, FactSales.item_id == DimItem.id)
        .where(FactSales.branch_ext_id == branch_ext, FactSales.doc_date >= date_from,
               FactSales.doc_date <= date_to, FactSales.item_id.is_not(None), _real)
        .group_by(FactSales.item_id)
    )).all()
    here = {r[0]: {'sq': float(r[1] or 0), 'sv': float(r[2] or 0)} for r in here_rows}

    # Velocity per item per branch (period) and stock per item per branch (snapshot).
    vel_rows = (await db.execute(
        select(FactSales.item_id, FactSales.branch_ext_id, func.coalesce(func.sum(qty), 0))
        .where(FactSales.doc_date >= date_from, FactSales.doc_date <= date_to,
               FactSales.item_id.in_(items_here_sub))
        .group_by(FactSales.item_id, FactSales.branch_ext_id)
    )).all()
    stk_rows = (await db.execute(
        select(FactInventory.item_id, FactInventory.branch_ext_id, func.coalesce(func.sum(FactInventory.qty_on_hand), 0))
        .where(FactInventory.doc_date == snapshot, FactInventory.movement_type == 'snapshot',
               not_(_non_pharmacy_warehouse()), FactInventory.item_id.in_(items_here_sub))
        .group_by(FactInventory.item_id, FactInventory.branch_ext_id)
    )).all()
    vel: dict = {}
    for iid, br, q in vel_rows:
        vel.setdefault(iid, {})[str(br)] = float(q or 0)
    stock: dict = {}
    for iid, br, q in stk_rows:
        stock.setdefault(iid, {})[str(br)] = float(q or 0)

    # Only real operating stores (with sales) can be donors — excludes junk/logistics
    # branches that hold inventory but never sell.
    valid_stores = {
        str(r[0]) for r in (await db.execute(
            select(func.distinct(FactSales.branch_ext_id)).where(FactSales.branch_ext_id.is_not(None))
        )).all()
    }

    transfers: list[dict] = []
    to_order: list[dict] = []
    for iid, hv in here.items():
        vel_here = hv['sq'] / days
        stock_here = stock.get(iid, {}).get(branch_ext, 0.0)
        # Only reinforce what is MISSING and still selling — i.e. the lost-sales
        # items (out of stock at this store but with demand), not a generic top-up.
        if vel_here <= 0 or stock_here >= 1:
            continue
        need = vel_here * target_days
        price = (hv['sv'] / hv['sq']) if hv['sq'] else 0.0
        donors = []
        for br, st in stock.get(iid, {}).items():
            if br == branch_ext or br not in valid_stores:
                continue
            surplus = st - (vel.get(iid, {}).get(br, 0.0) / days) * target_days
            if surplus >= 1:
                donors.append((br, surplus))
        donors.sort(key=lambda x: x[1], reverse=True)
        remaining = need
        for br, surplus in donors:
            take = min(remaining, surplus)
            if take < 1:
                continue
            transfers.append({'item_id': iid, 'from_branch': br, 'qty': take, 'value': take * price, 'lost_value': hv['sv']})
            remaining -= take
            if remaining < 1:
                break
        if remaining >= 1:
            to_order.append({'item_id': iid, 'qty': remaining, 'value': remaining * price})

    # Resolve item + branch labels.
    ids = {t['item_id'] for t in transfers} | {o['item_id'] for o in to_order}
    names = {}
    if ids:
        for r in (await db.execute(select(DimItem.id, DimItem.name, DimItem.barcode).where(DimItem.id.in_(ids)))).all():
            names[r[0]] = {'name': str(r[1] or ''), 'barcode': str(r[2] or '')}
    branch_names = {str(r[0]): str(r[1] or r[0]) for r in (await db.execute(select(DimBranch.external_id, DimBranch.name))).all()}

    def _pack_t(t):
        n = names.get(t['item_id'], {})
        return {'name': n.get('name', ''), 'barcode': n.get('barcode', ''),
                'from_branch': branch_names.get(t['from_branch'], t['from_branch']),
                'qty': round(t['qty']), 'value': t['value'], 'lost_value': t.get('lost_value', 0.0)}

    def _pack_o(o):
        n = names.get(o['item_id'], {})
        return {'name': n.get('name', ''), 'barcode': n.get('barcode', ''),
                'qty': round(o['qty']), 'value': o['value']}

    transfers.sort(key=lambda x: x.get('lost_value', 0.0), reverse=True)
    to_order.sort(key=lambda x: x['value'], reverse=True)
    return {
        'transfers': [_pack_t(t) for t in transfers[:top_n]],
        'to_order': [_pack_o(o) for o in to_order[:top_n]],
        'transfer_value': sum(t['value'] for t in transfers),
        'order_value': sum(o['value'] for o in to_order),
        'target_days': target_days,
    }


async def inventory_filter_options(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    snapshot_date = await _latest_inventory_snapshot_date(db, as_of)
    if snapshot_date is None:
        return {
            'branches': [],
            'warehouses': [],
            'brands': [],
            'categories': [],
            'groups': [],
            'labels': {'branches': {}, 'warehouses': {}, 'brands': {}, 'categories': {}, 'groups': {}},
        }
    category_path_expr = func.coalesce(
        _inventory_category_path_expr(
            func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
            func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
            func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        ),
        _inventory_category_path_expr(DimItem.category_1, DimItem.category_2, DimItem.category_3),
    )
    brand_value_expr = func.coalesce(
        _softone_clean_dimension_text(DimBrand.external_id),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'brand_external_id')),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'brand_name')),
    )
    brand_label_expr = func.coalesce(
        _softone_clean_dimension_text(DimBrand.name),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'brand_name')),
        brand_value_expr,
    )
    group_value_expr = func.coalesce(
        _softone_clean_dimension_text(DimGroup.external_id),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'group_external_id')),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'group_name')),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'commercial_category')),
    )
    group_label_expr = func.coalesce(
        _softone_clean_dimension_text(DimGroup.name),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'group_name')),
        _softone_clean_dimension_text(_json_text(FactInventory.source_payload_json, 'commercial_category')),
        group_value_expr,
    )
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': {},
        'categories': {},
        'groups': {},
    }
    base = (
        select(
            func.coalesce(DimBranch.external_id, FactInventory.branch_ext_id).label('branch'),
            func.coalesce(DimWarehouse.external_id, FactInventory.warehouse_ext_id).label('warehouse'),
            brand_value_expr.label('brand'),
            brand_label_expr.label('brand_label'),
            category_path_expr.label('category'),
            group_value_expr.label('group'),
            group_label_expr.label('group_label'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(
            FactInventory.doc_date == snapshot_date,
            FactInventory.movement_type == 'snapshot',
        )
    )
    base = _apply_inventory_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=FactInventory.branch_ext_id,
        warehouse_ext_col=FactInventory.warehouse_ext_id,
        brand_ext_col=_json_text(FactInventory.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(FactInventory.source_payload_json, 'brand_name'),
        category_1_col=func.coalesce(DimItem.category_1, _json_text(FactInventory.source_payload_json, 'category_1')),
        category_2_col=func.coalesce(DimItem.category_2, _json_text(FactInventory.source_payload_json, 'category_2')),
        category_3_col=func.coalesce(DimItem.category_3, _json_text(FactInventory.source_payload_json, 'category_3')),
        group_ext_col=_json_text(FactInventory.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(FactInventory.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(FactInventory.source_payload_json, 'commercial_category'),
    ).subquery('inv_dims')

    async def _distinct(col_name: str) -> list[str]:
        col = getattr(base.c, col_name)
        rows = (await db.execute(select(col).where(col.is_not(None)).distinct().order_by(col))).scalars().all()
        return [str(x) for x in rows if x]

    categories_out = await _distinct('category')
    labels['categories'] = {value: value for value in categories_out}
    for key in ('brand', 'group'):
        value_col = getattr(base.c, key)
        label_col = getattr(base.c, f'{key}_label')
        rows = (await db.execute(select(value_col, label_col).where(value_col.is_not(None)).distinct())).all()
        labels[f'{key}s'] = {str(value): str(label or value) for value, label in rows if value}

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
    prev_period_from = _safe_same_day(prev1_year, date_from.month, date_from.day)
    prev_period_to = _safe_same_day(prev1_year, date_to.month, date_to.day)
    prev_year_full_from = date(prev1_year, 1, 1)
    prev_year_full_to = date(prev1_year, 12, 31)
    prev2_year_full_from = date(prev2_year, 1, 1)
    prev2_year_full_to = date(prev2_year, 12, 31)
    today = date.today()
    completed_anchor_date = min(anchor_date, today - timedelta(days=1)) if anchor_date >= today else anchor_date
    if completed_anchor_date < month_from:
        # On the first day of a month there is no completed day inside the
        # current month yet; fall back to the requested anchor to avoid an
        # invalid empty date window.
        completed_anchor_date = anchor_date
    comparison_period_to = completed_anchor_date if date_from <= completed_anchor_date <= date_to else date_to
    comparison_year_from = date(comparison_period_to.year, 1, 1)
    comparison_month_from = date(comparison_period_to.year, comparison_period_to.month, 1)

    prev_month_date = month_from - timedelta(days=1)
    prev_month_from = prev_month_date.replace(day=1)
    prev_month_to = _safe_same_day(prev_month_date.year, prev_month_date.month, anchor_date.day)
    prev_year_month_from = date(prev1_year, anchor_date.month, 1)
    prev_year_month_to = _safe_same_day(prev1_year, anchor_date.month, anchor_date.day)
    # Prior-year window aligned to the last *completed* day (yesterday by default),
    # so the monthly branch comparison is full-day vs full-day on both sides.
    prev_year_month_completed_from = date(prev1_year, completed_anchor_date.month, 1)
    prev_year_month_completed_to = _safe_same_day(prev1_year, completed_anchor_date.month, completed_anchor_date.day)
    prev_period_cmp_from = _safe_same_day(prev1_year, date_from.month, date_from.day)
    prev_period_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)
    prev_ytd_cmp_from = date(prev1_year, 1, 1)
    prev_ytd_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)
    prev_year_month_cmp_from = date(prev1_year, comparison_period_to.month, 1)
    prev_year_month_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)

    sales_windows = {
        'day': (day_from, day_anchor_date),
        'week': (week_from, anchor_date),
        'month': (month_from, anchor_date),
        'month_cmp': (comparison_month_from, comparison_period_to),
        'year': (year_from, anchor_date),
        'year_cmp': (comparison_year_from, comparison_period_to),
        'prev_year_cmp': (prev_ytd_cmp_from, prev_ytd_cmp_to),
        'prev_year': (prev_ytd_from, prev_ytd_to),
        'prev_year_full': (prev_year_full_from, prev_year_full_to),
        'prev2_year_full': (prev2_year_full_from, prev2_year_full_to),
        'period_sales': (date_from, date_to),
        'period_sales_cmp': (date_from, comparison_period_to),
        'period_sales_prev': (prev_period_from, prev_period_to),
        'period_sales_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
        'month_prev_year': (prev_year_month_from, prev_year_month_to),
        'month_prev_year_cmp': (prev_year_month_cmp_from, prev_year_month_cmp_to),
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
    month_cmp = sales_windows_data.get('month_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    year = sales_windows_data.get('year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    year_cmp = sales_windows_data.get('year_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    prev_year_cmp = sales_windows_data.get('prev_year_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    prev_year = sales_windows_data.get('prev_year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    prev_year_full = sales_windows_data.get(
        'prev_year_full', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0}
    )
    prev2_year_full = sales_windows_data.get(
        'prev2_year_full', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0}
    )
    period_sales = sales_windows_data.get('period_sales', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    period_sales_cmp = sales_windows_data.get('period_sales_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    period_sales_prev = sales_windows_data.get('period_sales_prev', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    period_sales_prev_cmp = sales_windows_data.get('period_sales_prev_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    month_prev_year = sales_windows_data.get('month_prev_year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})
    month_prev_year_cmp = sales_windows_data.get('month_prev_year_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0})

    branch_windows = await _sales_by_branch_windows(
        db,
        windows={
            'day': (day_from, day_anchor_date),
            'month': (month_from, anchor_date),
            'month_completed': (month_from, completed_anchor_date),
            'year': (year_from, anchor_date),
            'prev_year': (prev_ytd_from, prev_ytd_to),
            'prev_year_month': (prev_year_month_from, prev_year_month_to),
            'prev_year_month_completed': (prev_year_month_completed_from, prev_year_month_completed_to),
            'period_sales_cmp': (date_from, comparison_period_to),
            'period_sales_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    day_by_branch = branch_windows.get('day', [])
    month_by_branch = branch_windows.get('month', [])
    month_completed_by_branch = branch_windows.get('month_completed', [])
    year_by_branch = branch_windows.get('year', [])
    prev_year_by_branch = branch_windows.get('prev_year', [])
    prev_year_month_by_branch = branch_windows.get('prev_year_month', [])
    prev_year_month_completed_by_branch = branch_windows.get('prev_year_month_completed', [])
    period_sales_by_branch = branch_windows.get('period_sales_cmp', [])
    period_sales_prev_by_branch = branch_windows.get('period_sales_prev_cmp', [])
    warehouse_windows = await _sales_by_warehouse_windows(
        db,
        windows={
            'year': (year_from, anchor_date),
            'prev_year': (prev_ytd_cmp_from, prev_ytd_cmp_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )

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
            'purchases_period_cmp': (date_from, comparison_period_to),
            'purchases_period_prev': (prev_period_from, prev_period_to),
            'purchases_period_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
            'purchases_year': (year_from, anchor_date),
            'purchases_year_cmp': (comparison_year_from, comparison_period_to),
            'purchases_prev_year': (prev_ytd_from, prev_ytd_to),
            'purchases_prev_year_cmp': (prev_ytd_cmp_from, prev_ytd_cmp_to),
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
    purchases_period_prev = purchase_windows_data.get(
        'purchases_period_prev', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_period_cmp = purchase_windows_data.get(
        'purchases_period_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_period_prev_cmp = purchase_windows_data.get(
        'purchases_period_prev_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_year_cmp = purchase_windows_data.get(
        'purchases_year_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_prev_year = purchase_windows_data.get(
        'purchases_prev_year', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    purchases_prev_year_cmp = purchase_windows_data.get(
        'purchases_prev_year_cmp', {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    )
    kpi_sales_monthly = await sales_monthly_trend(
        db,
        date_from=date_from,
        date_to=comparison_period_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    kpi_sales_monthly_prev = await sales_monthly_trend(
        db,
        date_from=prev_period_cmp_from,
        date_to=prev_period_cmp_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    kpi_purchases_monthly = await purchases_monthly_trend(
        db,
        date_from=date_from,
        date_to=comparison_period_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    kpi_purchases_monthly_prev = await purchases_monthly_trend(
        db,
        date_from=prev_period_cmp_from,
        date_to=prev_period_cmp_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    key_alerts = await list_recent_insights(db, limit=max(1, min(int(insights_limit), 20)), statuses=['open'])
    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
        'anchors': {
            'day_from': day_from.isoformat(),
            'week_from': week_from.isoformat(),
            'month_from': month_from.isoformat(),
            'month_to': anchor_date.isoformat(),
            'month_completed_from': month_from.isoformat(),
            'month_completed_to': completed_anchor_date.isoformat(),
            'comparison_period_from': date_from.isoformat(),
            'comparison_period_to': comparison_period_to.isoformat(),
            'year_from': year_from.isoformat(),
            'comparison_year_from': comparison_year_from.isoformat(),
            'comparison_month_from': comparison_month_from.isoformat(),
            'prev_ytd_from': prev_ytd_from.isoformat(),
            'prev_ytd_to': prev_ytd_to.isoformat(),
            'prev_ytd_cmp_from': prev_ytd_cmp_from.isoformat(),
            'prev_ytd_cmp_to': prev_ytd_cmp_to.isoformat(),
            'prev_period_from': prev_period_from.isoformat(),
            'prev_period_to': prev_period_to.isoformat(),
            'prev_period_cmp_from': prev_period_cmp_from.isoformat(),
            'prev_period_cmp_to': prev_period_cmp_to.isoformat(),
            'prev_year_full_from': prev_year_full_from.isoformat(),
            'prev_year_full_to': prev_year_full_to.isoformat(),
            'prev2_year_full_from': prev2_year_full_from.isoformat(),
            'prev2_year_full_to': prev2_year_full_to.isoformat(),
            'prev_month_from': prev_month_from.isoformat(),
            'prev_month_to': prev_month_to.isoformat(),
            'prev_year_month_from': prev_year_month_from.isoformat(),
            'prev_year_month_to': prev_year_month_to.isoformat(),
            'prev_year_month_completed_from': prev_year_month_completed_from.isoformat(),
            'prev_year_month_completed_to': prev_year_month_completed_to.isoformat(),
            'current_year': current_year,
            'prev1_year': prev1_year,
            'prev2_year': prev2_year,
        },
        'cards': {
            'day': day,
            'week': week,
            'month': month,
            'month_cmp': month_cmp,
            'year': year,
            'year_cmp': year_cmp,
            'prev_year_cmp': prev_year_cmp,
            'prev_year': prev_year,
            'prev_year_full': prev_year_full,
            'prev2_year_full': prev2_year_full,
            'period_sales': period_sales,
            'period_sales_cmp': period_sales_cmp,
            'period_sales_prev': period_sales_prev,
            'period_sales_prev_cmp': period_sales_prev_cmp,
            'month_prev_year': month_prev_year,
            'month_prev_year_cmp': month_prev_year_cmp,
            'purchases_period': purchases_period,
            'purchases_period_cmp': purchases_period_cmp,
            'purchases_period_prev': purchases_period_prev,
            'purchases_period_prev_cmp': purchases_period_prev_cmp,
            'purchases_year': purchases_year,
            'purchases_year_cmp': purchases_year_cmp,
            'purchases_prev_year': purchases_prev_year,
            'purchases_prev_year_cmp': purchases_prev_year_cmp,
        },
        'comparisons': {
            'same_period_prev_year': {
                'from': prev_period_from.isoformat(),
                'to': prev_period_to.isoformat(),
            },
            'ytd_prev_year': {
                'from': prev_ytd_from.isoformat(),
                'to': prev_ytd_to.isoformat(),
            },
            'month_prev_year': {
                'from': prev_year_month_from.isoformat(),
                'to': prev_year_month_to.isoformat(),
            },
        },
        'branch_breakdown': {
            'day': day_by_branch,
            'month': month_by_branch,
            'month_completed': month_completed_by_branch,
            'year': year_by_branch,
            'prev_year': prev_year_by_branch,
            'prev_year_month': prev_year_month_by_branch,
            'prev_year_month_completed': prev_year_month_completed_by_branch,
            'period_sales': period_sales_by_branch,
            'period_sales_prev': period_sales_prev_by_branch,
            'warehouse_year': warehouse_windows.get('year', []),
            'warehouse_prev_year': warehouse_windows.get('prev_year', []),
        },
        'trend': {
            'y0': {'year': current_year, 'rows': trend_y0},
            'y1': {'year': prev1_year, 'rows': trend_y1},
            'y2': {'year': prev2_year, 'rows': trend_y2},
            'kpi_sales': {'year': current_year, 'rows': kpi_sales_monthly},
            'kpi_sales_prev': {'year': prev1_year, 'rows': kpi_sales_monthly_prev},
            'kpi_purchases': {'year': current_year, 'rows': kpi_purchases_monthly},
            'kpi_purchases_prev': {'year': prev1_year, 'rows': kpi_purchases_monthly_prev},
        },
        'key_alerts': key_alerts,
    }


async def executive_dashboard_cards_summary(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    include_warehouse_breakdown: bool = True,
) -> dict:
    anchor_date = date_to
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
    prev_period_from = _safe_same_day(prev1_year, date_from.month, date_from.day)
    prev_period_to = _safe_same_day(prev1_year, date_to.month, date_to.day)
    prev_year_full_from = date(prev1_year, 1, 1)
    prev_year_full_to = date(prev1_year, 12, 31)
    prev2_year_full_from = date(prev2_year, 1, 1)
    prev2_year_full_to = date(prev2_year, 12, 31)
    today = date.today()
    completed_anchor_date = min(anchor_date, today - timedelta(days=1)) if anchor_date >= today else anchor_date
    if completed_anchor_date < month_from:
        completed_anchor_date = anchor_date
    comparison_period_to = completed_anchor_date if date_from <= completed_anchor_date <= date_to else date_to
    comparison_year_from = date(comparison_period_to.year, 1, 1)
    comparison_month_from = date(comparison_period_to.year, comparison_period_to.month, 1)
    prev_month_date = month_from - timedelta(days=1)
    prev_month_from = prev_month_date.replace(day=1)
    prev_month_to = _safe_same_day(prev_month_date.year, prev_month_date.month, anchor_date.day)
    prev_year_month_from = date(prev1_year, anchor_date.month, 1)
    prev_year_month_to = _safe_same_day(prev1_year, anchor_date.month, anchor_date.day)
    prev_period_cmp_from = _safe_same_day(prev1_year, date_from.month, date_from.day)
    prev_period_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)
    prev_ytd_cmp_from = date(prev1_year, 1, 1)
    prev_ytd_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)
    prev_year_month_cmp_from = date(prev1_year, comparison_period_to.month, 1)
    prev_year_month_cmp_to = _safe_same_day(prev1_year, comparison_period_to.month, comparison_period_to.day)

    sales_windows_data = await _sales_summaries_by_windows(
        db,
        windows={
            'day': (day_from, day_anchor_date),
            'week': (week_from, anchor_date),
            'month': (month_from, anchor_date),
            'month_cmp': (comparison_month_from, comparison_period_to),
            'year': (year_from, anchor_date),
            'year_cmp': (comparison_year_from, comparison_period_to),
            'prev_year_cmp': (prev_ytd_cmp_from, prev_ytd_cmp_to),
            'prev_year': (prev_ytd_from, prev_ytd_to),
            'prev_year_full': (prev_year_full_from, prev_year_full_to),
            'prev2_year_full': (prev2_year_full_from, prev2_year_full_to),
            'period_sales': (date_from, date_to),
            'period_sales_cmp': (date_from, comparison_period_to),
            'period_sales_prev': (prev_period_from, prev_period_to),
            'period_sales_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
            'month_prev_year': (prev_year_month_from, prev_year_month_to),
            'month_prev_year_cmp': (prev_year_month_cmp_from, prev_year_month_cmp_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    purchase_windows_data = await _purchases_summaries_by_windows(
        db,
        windows={
            'purchases_period': (date_from, date_to),
            'purchases_period_cmp': (date_from, comparison_period_to),
            'purchases_period_prev': (prev_period_from, prev_period_to),
            'purchases_period_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
            'purchases_year': (year_from, anchor_date),
            'purchases_year_cmp': (comparison_year_from, comparison_period_to),
            'purchases_prev_year': (prev_ytd_from, prev_ytd_to),
            'purchases_prev_year_cmp': (prev_ytd_cmp_from, prev_ytd_cmp_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    branch_windows = await _sales_by_branch_windows(
        db,
        windows={
            'year': (year_from, anchor_date),
            'prev_year': (prev_ytd_cmp_from, prev_ytd_cmp_to),
            'period_sales_cmp': (date_from, comparison_period_to),
            'period_sales_prev_cmp': (prev_period_cmp_from, prev_period_cmp_to),
        },
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    warehouse_windows = {}
    if include_warehouse_breakdown:
        warehouse_windows = await _sales_by_warehouse_windows(
            db,
            windows={
                'year': (year_from, anchor_date),
                'prev_year': (prev_ytd_cmp_from, prev_ytd_cmp_to),
            },
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )

    empty_sales = {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'gross_value': 0.0}
    empty_purchase = {'records': 0, 'qty': 0.0, 'net_value': 0.0, 'cost_amount': 0.0}
    return {
        'period': {'from': date_from.isoformat(), 'to': date_to.isoformat()},
        'anchors': {
            'day_from': day_from.isoformat(),
            'week_from': week_from.isoformat(),
            'month_from': month_from.isoformat(),
            'year_from': year_from.isoformat(),
            'comparison_period_from': date_from.isoformat(),
            'comparison_period_to': comparison_period_to.isoformat(),
            'comparison_year_from': comparison_year_from.isoformat(),
            'comparison_month_from': comparison_month_from.isoformat(),
            'prev_ytd_from': prev_ytd_from.isoformat(),
            'prev_ytd_to': prev_ytd_to.isoformat(),
            'prev_ytd_cmp_from': prev_ytd_cmp_from.isoformat(),
            'prev_ytd_cmp_to': prev_ytd_cmp_to.isoformat(),
            'prev_period_from': prev_period_from.isoformat(),
            'prev_period_to': prev_period_to.isoformat(),
            'prev_period_cmp_from': prev_period_cmp_from.isoformat(),
            'prev_period_cmp_to': prev_period_cmp_to.isoformat(),
            'prev_year_full_from': prev_year_full_from.isoformat(),
            'prev_year_full_to': prev_year_full_to.isoformat(),
            'prev2_year_full_from': prev2_year_full_from.isoformat(),
            'prev2_year_full_to': prev2_year_full_to.isoformat(),
            'prev_month_from': prev_month_from.isoformat(),
            'prev_month_to': prev_month_to.isoformat(),
            'prev_year_month_from': prev_year_month_from.isoformat(),
            'prev_year_month_to': prev_year_month_to.isoformat(),
            'current_year': current_year,
            'prev1_year': prev1_year,
            'prev2_year': prev2_year,
        },
        'cards': {
            'day': sales_windows_data.get('day', empty_sales),
            'week': sales_windows_data.get('week', empty_sales),
            'month': sales_windows_data.get('month', empty_sales),
            'month_cmp': sales_windows_data.get('month_cmp', empty_sales),
            'year': sales_windows_data.get('year', empty_sales),
            'year_cmp': sales_windows_data.get('year_cmp', empty_sales),
            'prev_year_cmp': sales_windows_data.get('prev_year_cmp', empty_sales),
            'prev_year': sales_windows_data.get('prev_year', empty_sales),
            'prev_year_full': sales_windows_data.get('prev_year_full', empty_sales),
            'prev2_year_full': sales_windows_data.get('prev2_year_full', empty_sales),
            'period_sales': sales_windows_data.get('period_sales', empty_sales),
            'period_sales_cmp': sales_windows_data.get('period_sales_cmp', empty_sales),
            'period_sales_prev': sales_windows_data.get('period_sales_prev', empty_sales),
            'period_sales_prev_cmp': sales_windows_data.get('period_sales_prev_cmp', empty_sales),
            'month_prev_year': sales_windows_data.get('month_prev_year', empty_sales),
            'month_prev_year_cmp': sales_windows_data.get('month_prev_year_cmp', empty_sales),
            'purchases_period': purchase_windows_data.get('purchases_period', empty_purchase),
            'purchases_period_cmp': purchase_windows_data.get('purchases_period_cmp', empty_purchase),
            'purchases_period_prev': purchase_windows_data.get('purchases_period_prev', empty_purchase),
            'purchases_period_prev_cmp': purchase_windows_data.get('purchases_period_prev_cmp', empty_purchase),
            'purchases_year': purchase_windows_data.get('purchases_year', empty_purchase),
            'purchases_year_cmp': purchase_windows_data.get('purchases_year_cmp', empty_purchase),
            'purchases_prev_year': purchase_windows_data.get('purchases_prev_year', empty_purchase),
            'purchases_prev_year_cmp': purchase_windows_data.get('purchases_prev_year_cmp', empty_purchase),
        },
        'comparisons': {
            'same_period_prev_year': {
                'from': prev_period_from.isoformat(),
                'to': prev_period_to.isoformat(),
            },
            'ytd_prev_year': {
                'from': prev_ytd_from.isoformat(),
                'to': prev_ytd_to.isoformat(),
            },
            'month_prev_year': {
                'from': prev_year_month_from.isoformat(),
                'to': prev_year_month_to.isoformat(),
            },
        },
        'branch_breakdown': {
            'year': branch_windows.get('year', []),
            'prev_year': branch_windows.get('prev_year', []),
            'period_sales': branch_windows.get('period_sales_cmp', []),
            'period_sales_prev': branch_windows.get('period_sales_prev_cmp', []),
            'warehouse_year': warehouse_windows.get('year', []),
            'warehouse_prev_year': warehouse_windows.get('prev_year', []),
        },
        'trend': {},
        'key_alerts': [],
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
    previous_to = date_from - timedelta(days=1)
    current_receivables = await _customer_balances_summary_snapshot(db, as_of=date_to, branches=branches)
    previous_receivables = await _customer_balances_summary_snapshot(
        db,
        as_of=previous_to,
        branches=branches,
        include_top=False,
    )
    current_open = float(current_receivables.get('open_balance') or 0)
    current_overdue = float(current_receivables.get('overdue_balance') or 0)
    previous_open = float(previous_receivables.get('open_balance') or 0)
    growth_value = current_open - previous_open
    growth_pct = ((growth_value / previous_open) * 100.0) if previous_open > 0 else None
    overdue_ratio_pct = ((current_overdue / current_open) * 100.0) if current_open > 0 else 0.0
    bucket_0_30 = float(current_receivables.get('aging_bucket_0_30') or 0)
    bucket_31_60 = float(current_receivables.get('aging_bucket_31_60') or 0)
    bucket_61_90 = float(current_receivables.get('aging_bucket_61_90') or 0)
    bucket_90_plus = float(current_receivables.get('aging_bucket_90_plus') or 0)
    aging_total = bucket_0_30 + bucket_31_60 + bucket_61_90 + bucket_90_plus
    top_customer_id = str(current_receivables.get('top_customer_id') or '')
    top_customer_name = str(current_receivables.get('top_customer_name') or top_customer_id)
    top_customer_balance = float(current_receivables.get('top_customer_balance') or 0)

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
                'customers': int(current_receivables.get('customers') or 0),
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
        net_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_net, 0))
        tax_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_tax, 0))
        gross_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_gross, 0))
        stmt = (
            select(
                func.coalesce(func.sum(net_amount_expr), 0).label('amount_net'),
                func.coalesce(func.sum(tax_amount_expr), 0).label('amount_tax'),
                func.coalesce(func.sum(gross_amount_expr), 0).label('amount_gross'),
                func.count(FactExpense.id).label('entries'),
            )
            .where(*_date_range(FactExpense.expense_date, date_from, date_to))
        )
        stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
        row = (await db.execute(stmt)).mappings().first() or {}
        total_expenses = float(row.get('amount_net') or 0)
        total_tax = float(row.get('amount_tax') or 0)
        total_gross = float(row.get('amount_gross') or 0)
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
        net_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_net, 0))
        stmt = (
            select(
                FactExpense.expense_category_code.label('category_code'),
                func.coalesce(func.sum(net_amount_expr), 0).label('amount_net'),
                func.count(FactExpense.id).label('entries'),
            )
            .where(*_date_range(FactExpense.expense_date, date_from, date_to))
        )
        stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
        stmt = stmt.group_by(FactExpense.expense_category_code)

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
            amount_net = float(row.get('amount_net') or 0)
            out.append(
                {
                    'category_code': code if code != '-' else None,
                    'category_name': name_map.get(code, code if code != '-' else 'N/A'),
                    'amount_net': amount_net,
                    'entries': int(row.get('entries') or 0),
                    'share_pct': (abs(amount_net) / total * 100.0) if total > 0 else 0.0,
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
        net_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_net, 0))
        stmt = (
            select(
                FactExpense.branch_ext_id.label('branch_ext_id'),
                func.coalesce(func.sum(net_amount_expr), 0).label('amount_net'),
                func.count(FactExpense.id).label('entries'),
            )
            .where(*_date_range(FactExpense.expense_date, date_from, date_to))
        )
        stmt = _apply_fact_expense_filters(stmt, branches=branches, categories=categories)
        stmt = stmt.group_by(FactExpense.branch_ext_id)

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
                    'amount_net': float(row.get('amount_net') or 0),
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
        net_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_net, 0))
        gross_amount_expr = _fact_expenses_signed_amount_expr(func.coalesce(FactExpense.amount_gross, 0))
        stmt = (
            select(
                FactExpense.expense_date.label('expense_date'),
                func.coalesce(func.sum(net_amount_expr), 0).label('amount_net'),
                func.coalesce(func.sum(gross_amount_expr), 0).label('amount_gross'),
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
                'amount_net': float(row.get('amount_net') or 0),
                'amount_gross': float(row.get('amount_gross') or 0),
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
            func.coalesce(func.sum(_fact_purchases_analysis_qty_expr()), 0).label('qty'),
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
    abc_categories: list[str] | None = None,
    commercial_statuses: list[str] | None = None,
    status: str = 'all',
    movement: str = 'all',
    q: str | None = None,
    limit: int = 200,
    offset: int = 0,
    classification_config: dict | None = None,
    scope: str | None = None,
):
    resolved_classification = normalize_inventory_item_classification_config(classification_config)
    movement_window_days = int(
        resolved_classification.get('movement_window_days')
        or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['movement_window_days']
    )
    # 'commercial' status source: active/inactive comes from the SoftOne commercial_status
    # lifecycle (inactive = Καταργημένο/Ληγμένο). Scope modes:
    #  - 'master'      : every item that carries a commercial_status (managed catalog), no-stock incl.
    #  - 'stock_sold'  : items in stock OR sold within inventory_scope_sold_days (warehouse view).
    #  - 'stock'       : legacy — only items in the latest snapshot.
    _status_source = str(resolved_classification.get('status_source') or 'softone').strip().lower()
    inventory_scope_sold_days = int(
        resolved_classification.get('inventory_scope_sold_days')
        or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['inventory_scope_sold_days']
    )
    scope_mode = str(scope or '').strip().lower()
    if not scope_mode:
        if _status_source in ('commercial', 'active_status12'):
            scope_mode = 'master'
        elif _status_source in ('active_available', 'active_stock_sales', 'softone_available'):
            # Warehouse working set: items in stock OR sold within inventory_scope_sold_days.
            # 'active' among them = ISACTIVE (ENERGO) items — matches SoftOne's active count.
            scope_mode = 'stock_sold'
        else:
            scope_mode = 'stock'
    master_scope = scope_mode == 'master'
    stock_sold_scope = scope_mode == 'stock_sold'
    latest_date = await _latest_inventory_snapshot_date(db, as_of)
    if latest_date is None:
        return {'snapshot_date': None, 'summary': {}, 'rows': []}

    latest_inventory_rows = (
        select(
            FactInventory.id.label('fact_id'),
            FactInventory.branch_id.label('branch_id'),
            FactInventory.branch_ext_id.label('branch_ext_id'),
            FactInventory.warehouse_id.label('warehouse_id'),
            FactInventory.warehouse_ext_id.label('warehouse_ext_id'),
            FactInventory.item_id.label('item_id'),
            FactInventory.item_code.label('item_code'),
            FactInventory.source_payload_json['item_name'].astext.label('payload_item_name'),
            FactInventory.source_payload_json['barcode'].astext.label('payload_barcode'),
            FactInventory.source_payload_json['alternate_barcodes'].astext.label('payload_alternate_barcodes'),
            FactInventory.source_payload_json['brand_external_id'].astext.label('payload_brand_external_id'),
            FactInventory.source_payload_json['brand_name'].astext.label('payload_brand_name'),
            FactInventory.source_payload_json['manufacturer_code'].astext.label('payload_manufacturer_code'),
            FactInventory.source_payload_json['manufacturer_name'].astext.label('payload_manufacturer_name'),
            FactInventory.source_payload_json['group_external_id'].astext.label('payload_group_external_id'),
            FactInventory.source_payload_json['group_name'].astext.label('payload_group_name'),
            FactInventory.source_payload_json['commercial_category'].astext.label('payload_commercial_category'),
            FactInventory.source_payload_json['category_1'].astext.label('payload_category_1'),
            FactInventory.source_payload_json['category_2'].astext.label('payload_category_2'),
            FactInventory.source_payload_json['category_3'].astext.label('payload_category_3'),
            FactInventory.source_payload_json['is_active'].astext.label('payload_is_active'),
            FactInventory.source_payload_json['is_active_source'].astext.label('payload_is_active_source'),
            FactInventory.qty_on_hand.label('qty_on_hand'),
            FactInventory.value_amount.label('value_amount'),
            # Keep one row per (branch, warehouse, item) when more than one snapshot set
            # lands on a day (nightly STKSNAP refresh + a full-sync pull) so the per-item
            # SUM below does not double-count. item_code is the stable cross-source key.
            func.row_number()
            .over(
                partition_by=(
                    func.coalesce(FactInventory.branch_ext_id, literal('')),
                    func.coalesce(FactInventory.warehouse_ext_id, literal('')),
                    func.coalesce(func.nullif(FactInventory.item_code, ''), cast(FactInventory.item_id, String), literal('')),
                ),
                order_by=FactInventory.updated_at.desc(),
            )
            .label('rn'),
        )
        .where(
            FactInventory.doc_date == latest_date,
            FactInventory.movement_type == 'snapshot',
        )
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
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.manual_order_category), '')),
                func.max(func.nullif(func.btrim(DimItem.abc_category), '')),
                literal('Χωρίς ABC'),
            ).label('abc_category'),
            func.coalesce(func.max(func.nullif(func.btrim(DimItem.manual_order_category), '')), literal('')).label('manual_order_category'),
            func.coalesce(
                func.max(func.nullif(func.btrim(DimItem.commercial_status), '')),
                literal(''),
            ).label('commercial_status'),
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
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        branch_ext_col=latest_inventory_rows.c.branch_ext_id,
        warehouse_ext_col=latest_inventory_rows.c.warehouse_ext_id,
        brand_ext_col=latest_inventory_rows.c.payload_brand_external_id,
        brand_label_col=latest_inventory_rows.c.payload_brand_name,
        category_1_col=latest_inventory_rows.c.payload_category_1,
        category_2_col=latest_inventory_rows.c.payload_category_2,
        category_3_col=latest_inventory_rows.c.payload_category_3,
        group_ext_col=latest_inventory_rows.c.payload_group_external_id,
        group_label_col=latest_inventory_rows.c.payload_group_name,
        commercial_category_col=latest_inventory_rows.c.payload_commercial_category,
    )
    inv_base = inv_base.where(latest_inventory_rows.c.rn == 1).group_by(item_code_expr).subquery('inv_base')

    # last_sale_date must reach back far enough for the 'active_available' sold-window
    # (inventory_scope_sold_days), while sales_qty_30 stays bounded to movement_window_days.
    _sale_window_days = max(int(movement_window_days), int(inventory_scope_sold_days))
    if not any([branches, warehouses, brands, categories, groups]) and _effective_branch_filter(None) is None:
        sales_30 = (
            select(
                AggSalesItemDaily.item_external_id.label('item_code'),
                func.coalesce(
                    func.sum(AggSalesItemDaily.qty).filter(
                        AggSalesItemDaily.doc_date >= (as_of - timedelta(days=movement_window_days))
                    ),
                    0,
                ).label('sales_qty_30'),
                func.max(AggSalesItemDaily.doc_date).label('last_sale_date'),
            )
            .where(AggSalesItemDaily.doc_date >= (as_of - timedelta(days=_sale_window_days)))
            .where(AggSalesItemDaily.doc_date <= as_of)
            .group_by(AggSalesItemDaily.item_external_id)
            .subquery('sales_30')
        )
    else:
        sales_30 = (
            select(
                FactSales.item_code.label('item_code'),
                func.coalesce(
                    func.sum(FactSales.qty).filter(
                        FactSales.doc_date >= (as_of - timedelta(days=movement_window_days))
                    ),
                    0,
                ).label('sales_qty_30'),
                func.max(FactSales.doc_date).label('last_sale_date'),
            )
            .where(FactSales.doc_date >= (as_of - timedelta(days=_sale_window_days)))
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
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=movement_window_days)))
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
        select(func.min(FactInventory.doc_date).label('first_seen'))
        .where(
            FactInventory.movement_type == 'snapshot',
            FactInventory.item_code == inv_base.c.item_code,
        )
        .lateral('first_seen')
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
            inv_base.c.abc_category,
            inv_base.c.commercial_status,
            inv_base.c.manual_order_category,
        )
        .select_from(inv_base)
        .join(sales_30, sales_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(purch_30, purch_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(first_seen, true(), isouter=True)
    )

    q_clean = (q or '').strip().lower()
    if q_clean:
        stmt = stmt.where(
            func.lower(cast(inv_base.c.item_name, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.item_code, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.barcode, String)).like(f'%{q_clean}%')
        )

    clean_abc_categories = {str(x).strip() for x in (abc_categories or []) if str(x).strip()}
    clean_commercial_statuses = {str(x).strip() for x in (commercial_statuses or []) if str(x).strip()}
    safe_limit = max(1, min(int(limit), 500))
    safe_offset = max(0, int(offset))
    simple_sql_page = (
        not q_clean
        and status not in {'active', 'inactive'}
        and movement not in {'fast', 'slow', 'normal'}
        and not clean_abc_categories
        and not clean_commercial_statuses
    )

    if simple_sql_page:
        base_inventory = inv_base
        if master_scope:
            # Master-scoped view: every item that carries a SoftOne commercial_status,
            # regardless of current stock. Stock qty/value LEFT-joined from the latest snapshot.
            snap_q = (
                select(
                    AggInventorySnapshotDaily.item_external_id.label('item_code'),
                    AggInventorySnapshotDaily.qty_on_hand.label('qty_on_hand'),
                    AggInventorySnapshotDaily.value_amount.label('value_amount'),
                )
                .where(AggInventorySnapshotDaily.snapshot_date == latest_date)
                .subquery('snap_q')
            )
            base_inventory = (
                select(
                    DimItem.external_id.label('item_code'),
                    func.coalesce(func.nullif(func.btrim(DimItem.name), ''), DimItem.external_id).label('item_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.barcode), ''), literal('')).label('barcode'),
                    func.coalesce(func.nullif(func.btrim(DimBrand.name), ''), literal('N/A')).label('brand'),
                    func.coalesce(
                        func.nullif(func.btrim(DimItem.manufacturer_name), ''),
                        func.nullif(func.btrim(DimItem.manufacturer_code), ''),
                        literal('N/A'),
                    ).label('manufacturer'),
                    func.coalesce(func.nullif(func.btrim(DimCategory.name), ''), literal('N/A')).label('category'),
                    func.coalesce(func.nullif(func.btrim(DimGroup.name), ''), literal('N/A')).label('group_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.category_1), ''), literal('')).label('category_1'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_category), ''), literal('')).label('commercial_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), func.nullif(func.btrim(DimItem.abc_category), ''), literal('Χωρίς ABC')).label('abc_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), literal('')).label('manual_order_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_status), ''), literal('')).label('commercial_status'),
                    DimItem.is_active_source.label('is_active_source'),
                    func.coalesce(snap_q.c.qty_on_hand, 0).label('qty_on_hand'),
                    func.coalesce(snap_q.c.value_amount, 0).label('stock_value'),
                )
                .select_from(DimItem)
                .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
                .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
                .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
                .join(snap_q, snap_q.c.item_code == DimItem.external_id, isouter=True)
                .where(func.coalesce(DimItem.softone_sotype, 51) == 51)
                .where(func.btrim(func.coalesce(DimItem.commercial_status, literal(''))) != literal(''))
                .subquery('agg_inventory_base')
            )
        elif stock_sold_scope:
            # Warehouse view: items currently in stock OR sold within the configured window.
            snap_q = (
                select(
                    AggInventorySnapshotDaily.item_external_id.label('item_code'),
                    AggInventorySnapshotDaily.qty_on_hand.label('qty_on_hand'),
                    AggInventorySnapshotDaily.value_amount.label('value_amount'),
                )
                .where(AggInventorySnapshotDaily.snapshot_date == latest_date)
                .subquery('snap_q')
            )
            scope_codes = (
                select(AggInventorySnapshotDaily.item_external_id.label('item_code'))
                .where(AggInventorySnapshotDaily.snapshot_date == latest_date)
                .union(
                    select(AggSalesItemDaily.item_external_id.label('item_code'))
                    .where(AggSalesItemDaily.doc_date >= (as_of - timedelta(days=inventory_scope_sold_days)))
                    .where(AggSalesItemDaily.doc_date <= as_of)
                )
                .subquery('scope_codes')
            )
            base_inventory = (
                select(
                    scope_codes.c.item_code.label('item_code'),
                    func.coalesce(func.nullif(func.btrim(DimItem.name), ''), scope_codes.c.item_code).label('item_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.barcode), ''), literal('')).label('barcode'),
                    func.coalesce(func.nullif(func.btrim(DimBrand.name), ''), literal('N/A')).label('brand'),
                    func.coalesce(
                        func.nullif(func.btrim(DimItem.manufacturer_name), ''),
                        func.nullif(func.btrim(DimItem.manufacturer_code), ''),
                        literal('N/A'),
                    ).label('manufacturer'),
                    func.coalesce(func.nullif(func.btrim(DimCategory.name), ''), literal('N/A')).label('category'),
                    func.coalesce(func.nullif(func.btrim(DimGroup.name), ''), literal('N/A')).label('group_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.category_1), ''), literal('')).label('category_1'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_category), ''), literal('')).label('commercial_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), func.nullif(func.btrim(DimItem.abc_category), ''), literal('Χωρίς ABC')).label('abc_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), literal('')).label('manual_order_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_status), ''), literal('')).label('commercial_status'),
                    DimItem.is_active_source.label('is_active_source'),
                    func.coalesce(snap_q.c.qty_on_hand, 0).label('qty_on_hand'),
                    func.coalesce(snap_q.c.value_amount, 0).label('stock_value'),
                )
                .select_from(scope_codes)
                .join(DimItem, DimItem.external_id == scope_codes.c.item_code, isouter=True)
                .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
                .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
                .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
                .join(snap_q, snap_q.c.item_code == scope_codes.c.item_code, isouter=True)
                .subquery('agg_inventory_base')
            )
        elif not any([branches, warehouses, brands, categories, groups]):
            base_inventory = (
                select(
                    AggInventorySnapshotDaily.item_external_id.label('item_code'),
                    func.coalesce(func.nullif(func.btrim(DimItem.name), ''), AggInventorySnapshotDaily.item_external_id).label('item_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.barcode), ''), literal('')).label('barcode'),
                    func.coalesce(func.nullif(func.btrim(DimBrand.name), ''), literal('N/A')).label('brand'),
                    func.coalesce(
                        func.nullif(func.btrim(DimItem.manufacturer_name), ''),
                        func.nullif(func.btrim(DimItem.manufacturer_code), ''),
                        literal('N/A'),
                    ).label('manufacturer'),
                    func.coalesce(func.nullif(func.btrim(DimCategory.name), ''), literal('N/A')).label('category'),
                    func.coalesce(func.nullif(func.btrim(DimGroup.name), ''), literal('N/A')).label('group_name'),
                    func.coalesce(func.nullif(func.btrim(DimItem.category_1), ''), literal('')).label('category_1'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_category), ''), literal('')).label('commercial_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), func.nullif(func.btrim(DimItem.abc_category), ''), literal('Χωρίς ABC')).label('abc_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.manual_order_category), ''), literal('')).label('manual_order_category'),
                    func.coalesce(func.nullif(func.btrim(DimItem.commercial_status), ''), literal('')).label('commercial_status'),
                    DimItem.is_active_source.label('is_active_source'),
                    func.coalesce(AggInventorySnapshotDaily.qty_on_hand, 0).label('qty_on_hand'),
                    func.coalesce(AggInventorySnapshotDaily.value_amount, 0).label('stock_value'),
                )
                .select_from(AggInventorySnapshotDaily)
                .join(DimItem, DimItem.external_id == AggInventorySnapshotDaily.item_external_id, isouter=True)
                .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
                .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
                .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
                .where(AggInventorySnapshotDaily.snapshot_date == latest_date)
                .subquery('agg_inventory_base')
            )
        sales_qty_expr = func.coalesce(sales_30.c.sales_qty_30, 0)
        active_days = int(
            resolved_classification.get('active_last_sale_days')
            or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['active_last_sale_days']
        )
        fast_min = int(
            resolved_classification.get('fast_sales_qty_30d_min')
            or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['fast_sales_qty_30d_min']
        )
        slow_max = int(
            resolved_classification.get('slow_sales_qty_30d_max')
            or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['slow_sales_qty_30d_max']
        )
        status_source = _status_source
        _commercial_norm = func.lower(func.btrim(func.coalesce(base_inventory.c.commercial_status, literal(''))))
        if status_source == 'commercial':
            active_sql = case(
                (
                    (_commercial_norm != literal('')) & _commercial_norm.notin_(list(_INACTIVE_COMMERCIAL_STATUSES)),
                    1,
                ),
                else_=0,
            )
        elif status_source == 'active_status12':
            # ENERGO AND non-empty status_1 (manual_order_category) AND status_2 (commercial_status).
            active_sql = case(
                (
                    base_inventory.c.is_active_source.is_(True)
                    & (func.btrim(func.coalesce(base_inventory.c.manual_order_category, literal(''))) != literal(''))
                    & (func.btrim(func.coalesce(base_inventory.c.commercial_status, literal(''))) != literal('')),
                    1,
                ),
                else_=0,
            )
        elif status_source == 'softone':
            active_sql = case((base_inventory.c.is_active_source.is_(True), 1), else_=0)
        elif status_source in ('active_available', 'active_stock_sales', 'softone_available'):
            # ENERGO (ISACTIVE) AND availability: net stock on hand OR a sale within
            # inventory_scope_sold_days. last_sale_date reaches back over this window
            # (sales_30 CTE is widened to max(movement_window_days, inventory_scope_sold_days)).
            active_sql = case(
                (
                    base_inventory.c.is_active_source.is_(True)
                    & (
                        (base_inventory.c.qty_on_hand != 0)
                        | (sales_30.c.last_sale_date >= (as_of - timedelta(days=inventory_scope_sold_days)))
                    ),
                    1,
                ),
                else_=0,
            )
        else:
            active_sql = case(
                (sales_30.c.last_sale_date >= (as_of - timedelta(days=active_days)), 1),
                else_=0,
            )
        fast_sql = case((sales_qty_expr >= fast_min, 1), else_=0)
        slow_sql = case((sales_qty_expr <= slow_max, 1), else_=0)
        sold_period_sql = case((sales_30.c.last_sale_date.isnot(None), 1), else_=0)

        summary_stmt = (
            select(
                func.count().label('total_items'),
                func.coalesce(func.sum(active_sql), 0).label('active_items'),
                (func.count() - func.coalesce(func.sum(active_sql), 0)).label('inactive_items'),
                func.coalesce(func.sum(fast_sql), 0).label('fast_items'),
                func.coalesce(func.sum(slow_sql), 0).label('slow_items'),
                func.coalesce(func.sum(sold_period_sql), 0).label('sold_period_items'),
                func.coalesce(func.sum(base_inventory.c.stock_value), 0).label('stock_value'),
            )
            .select_from(base_inventory)
            .join(sales_30, sales_30.c.item_code == base_inventory.c.item_code, isouter=True)
        )
        summary_row = (await db.execute(summary_stmt)).mappings().one()

        abc_rows = (
            await db.execute(
                select(
                    base_inventory.c.abc_category.label('category'),
                    func.count().label('count'),
                    func.coalesce(func.sum(base_inventory.c.stock_value), 0).label('stock_value'),
                )
                .select_from(base_inventory)
                .group_by(base_inventory.c.abc_category)
            )
        ).mappings().all()
        commercial_rows = (
            await db.execute(
                select(
                    base_inventory.c.commercial_status.label('status'),
                    func.count().label('count'),
                    func.coalesce(func.sum(base_inventory.c.stock_value), 0).label('stock_value'),
                )
                .select_from(base_inventory)
                .group_by(base_inventory.c.commercial_status)
            )
        ).mappings().all()

        def _abc_sort_key(value: str) -> tuple[int, int, int, str]:
            text = str(value or '').strip() or 'Χωρίς ABC'
            upper = text.upper()
            if 'ΧΩΡΙΣ' in upper or 'WITHOUT' in upper:
                return (999, 999, 0, upper)
            if 'FIRST-A' in upper:
                return (0, 50, 0, upper)
            if upper == 'CONS':
                return (998, 0, 0, upper)
            match = re.search(r'[A-Z]', upper)
            letter_rank = ord(match.group(0)) - ord('A') if match else 900
            suffix = re.search(r'\d+', upper)
            number_rank = int(suffix.group(0)) if suffix else 0
            secondary_rank = 0 if match and upper.startswith(match.group(0)) else 1
            return (letter_rank, number_rank, secondary_rank, upper)

        page_base = (
            select(
                base_inventory.c.item_code.label('item_code'),
                base_inventory.c.item_name.label('item_name'),
                base_inventory.c.barcode.label('barcode'),
                base_inventory.c.brand.label('brand'),
                base_inventory.c.manufacturer.label('manufacturer'),
                base_inventory.c.category.label('category'),
                base_inventory.c.group_name.label('group_name'),
                base_inventory.c.category_1.label('category_1'),
                base_inventory.c.commercial_category.label('commercial_category'),
                base_inventory.c.qty_on_hand.label('qty_on_hand'),
                base_inventory.c.stock_value.label('stock_value'),
                func.coalesce(sales_30.c.sales_qty_30, 0).label('sales_qty_30'),
                sales_30.c.last_sale_date.label('last_sale_date'),
                func.coalesce(purch_30.c.purchases_qty_30, 0).label('purchases_qty_30'),
                base_inventory.c.is_active_source.label('is_active_source'),
                base_inventory.c.abc_category.label('abc_category'),
                base_inventory.c.manual_order_category.label('manual_order_category'),
                base_inventory.c.commercial_status.label('commercial_status'),
            )
            .select_from(base_inventory)
            .join(sales_30, sales_30.c.item_code == base_inventory.c.item_code, isouter=True)
            .join(purch_30, purch_30.c.item_code == base_inventory.c.item_code, isouter=True)
            .order_by(base_inventory.c.stock_value.desc(), base_inventory.c.qty_on_hand.desc())
            .offset(safe_offset)
            .limit(safe_limit)
            .subquery('inventory_page_base')
        )
        page_first_seen = (
            select(func.min(FactInventory.doc_date).label('first_seen'))
            .where(
                FactInventory.movement_type == 'snapshot',
                FactInventory.item_code == page_base.c.item_code,
            )
            .lateral('page_first_seen')
        )
        page_stmt = (
            select(page_base, page_first_seen.c.first_seen)
            .select_from(page_base)
            .join(page_first_seen, true(), isouter=True)
        )
        page_rows = (await db.execute(page_stmt)).mappings().all()
        mapped = []
        for r in page_rows:
            sales_qty_30 = float(r['sales_qty_30'] or 0)
            last_sale_date = r['last_sale_date']
            is_active_source = r['is_active_source']
            abc_category = str(r['abc_category'] or 'Χωρίς ABC').strip() or 'Χωρίς ABC'
            commercial_status = str(r['commercial_status'] or '').strip()
            category_value = str(r['category'] or 'N/A')
            if category_value == 'N/A':
                category_value = str(r['category_1'] or r['commercial_category'] or 'N/A')
            status_value, movement_level = _classify_inventory_item(
                as_of=as_of,
                last_sale_date=last_sale_date,
                sales_qty=sales_qty_30,
                config=resolved_classification,
                is_active_source=(bool(is_active_source) if is_active_source is not None else None),
                commercial_status=commercial_status,
                manual_order_category=str(r['manual_order_category'] or ''),
                qty_on_hand=float(r['qty_on_hand'] or 0),
            )
            mapped.append(
                {
                    'item_code': str(r['item_code'] or 'N/A'),
                    'item_name': _clean_item_name(r['item_name'], r['item_code'] or 'N/A'),
                    'barcode': str(r['barcode'] or ''),
                    'brand': str(r['brand'] or 'N/A'),
                    'manufacturer': str(r['manufacturer'] or 'N/A'),
                    'category': category_value,
                    'group': str(r['group_name'] or 'N/A'),
                    'qty_on_hand': float(r['qty_on_hand'] or 0),
                    'stock_value': float(r['stock_value'] or 0),
                    'sales_qty_30': sales_qty_30,
                    'last_sale_date': str(last_sale_date) if last_sale_date else None,
                    'purchases_qty_30': float(r['purchases_qty_30'] or 0),
                    'first_seen': str(r['first_seen']) if r['first_seen'] else None,
                    'is_active_source': (bool(is_active_source) if is_active_source is not None else None),
                    'abc_category': abc_category,
                    'commercial_status': commercial_status,
                    'status': status_value,
                    'movement': movement_level,
                }
            )

        summary = {
            'total_items': int(summary_row['total_items'] or 0),
            'active_items': int(summary_row['active_items'] or 0),
            'inactive_items': int(summary_row['inactive_items'] or 0),
            'fast_items': int(summary_row['fast_items'] or 0),
            'slow_items': int(summary_row['slow_items'] or 0),
            'sold_period_items': int(summary_row['sold_period_items'] or 0),
            'movement_window_days': movement_window_days,
            'stock_value': float(summary_row['stock_value'] or 0),
            'abc_counts': [
                {
                    'category': str(r['category'] or 'Χωρίς ABC').strip() or 'Χωρίς ABC',
                    'count': int(r['count'] or 0),
                    'stock_value': float(r['stock_value'] or 0),
                }
                for r in sorted(abc_rows, key=lambda row: _abc_sort_key(str(row['category'] or 'Χωρίς ABC')))
            ],
            'commercial_status_counts': [
                {
                    'status': str(r['status'] or '').strip() or 'Χωρίς Εμπορικό Status',
                    'count': int(r['count'] or 0),
                    'stock_value': float(r['stock_value'] or 0),
                }
                for r in sorted(
                    commercial_rows,
                    key=lambda row: (
                        1 if (str(row['status'] or '').strip() or 'Χωρίς Εμπορικό Status') == 'Χωρίς Εμπορικό Status' else 0,
                        (str(row['status'] or '').strip() or 'Χωρίς Εμπορικό Status').casefold(),
                    ),
                )
            ],
        }
        return {
            'snapshot_date': str(latest_date),
            'classification_config': resolved_classification,
            'summary': summary,
            'rows': mapped,
            'total': int(summary_row['total_items'] or 0),
            'limit': safe_limit,
            'offset': safe_offset,
        }

    rows = (await db.execute(stmt)).all()
    mapped = []
    for r in rows:
        sales_qty_30 = float(r[11] or 0)
        last_sale_date = r[12]
        is_active_source = r[15]
        abc_category = str(r[16] or 'Χωρίς ABC').strip() or 'Χωρίς ABC'
        commercial_status = str(r[17] or '').strip()
        category_value = str(r[5] or 'N/A')
        if category_value == 'N/A':
            category_value = str(r[7] or r[8] or 'N/A')
        status_value, movement_level = _classify_inventory_item(
            as_of=as_of,
            last_sale_date=last_sale_date,
            sales_qty=sales_qty_30,
            config=resolved_classification,
            is_active_source=(bool(is_active_source) if is_active_source is not None else None),
            commercial_status=commercial_status,
            manual_order_category=str(r[18] or ''),
            qty_on_hand=float(r[9] or 0),
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
                'abc_category': abc_category,
                'commercial_status': commercial_status,
                'status': status_value,
                'movement': movement_level,
            }
        )

    if status in {'active', 'inactive'}:
        mapped = [x for x in mapped if x['status'] == status]
    if movement in {'fast', 'slow', 'normal'}:
        mapped = [x for x in mapped if x['movement'] == movement]
    if clean_abc_categories:
        mapped = [x for x in mapped if str(x.get('abc_category') or 'Χωρίς ABC').strip() in clean_abc_categories]
    if clean_commercial_statuses:
        mapped = [
            x
            for x in mapped
            if (str(x.get('commercial_status') or '').strip() or 'Χωρίς Εμπορικό Status')
            in clean_commercial_statuses
        ]

    # Summary cards must reflect active filters (status/movement/search/etc.),
    # not the unfiltered snapshot.
    summary_source = list(mapped)
    abc_counts: dict[str, dict[str, float | int]] = {}
    commercial_status_counts: dict[str, dict[str, float | int]] = {}
    for item in summary_source:
        key = str(item.get('abc_category') or 'Χωρίς ABC').strip() or 'Χωρίς ABC'
        abc_bucket = abc_counts.setdefault(key, {'count': 0, 'stock_value': 0.0})
        abc_bucket['count'] = int(abc_bucket.get('count') or 0) + 1
        abc_bucket['stock_value'] = float(abc_bucket.get('stock_value') or 0) + float(item.get('stock_value') or 0)
        status_key = str(item.get('commercial_status') or '').strip() or 'Χωρίς Εμπορικό Status'
        status_bucket = commercial_status_counts.setdefault(status_key, {'count': 0, 'stock_value': 0.0})
        status_bucket['count'] = int(status_bucket.get('count') or 0) + 1
        status_bucket['stock_value'] = float(status_bucket.get('stock_value') or 0) + float(item.get('stock_value') or 0)

    def _abc_sort_key(value: str) -> tuple[int, int, int, str]:
        text = str(value or '').strip() or 'Χωρίς ABC'
        upper = text.upper()
        if 'ΧΩΡΙΣ' in upper or 'WITHOUT' in upper:
            return (999, 999, 0, upper)
        if 'FIRST-A' in upper:
            return (0, 50, 0, upper)
        if upper == 'CONS':
            return (998, 0, 0, upper)
        match = re.search(r'[A-Z]', upper)
        letter_rank = ord(match.group(0)) - ord('A') if match else 900
        suffix = re.search(r'\d+', upper)
        number_rank = int(suffix.group(0)) if suffix else 0
        secondary_rank = 0 if match and upper.startswith(match.group(0)) else 1
        return (letter_rank, number_rank, secondary_rank, upper)

    mapped.sort(key=lambda x: (x['stock_value'], x['qty_on_hand']), reverse=True)
    total = len(mapped)
    mapped = mapped[safe_offset : safe_offset + safe_limit]

    summary = {
        'total_items': len(summary_source),
        'active_items': sum(1 for x in summary_source if x.get('status') == 'active'),
        'inactive_items': sum(1 for x in summary_source if x.get('status') == 'inactive'),
        'fast_items': sum(1 for x in summary_source if x.get('movement') == 'fast'),
        'slow_items': sum(1 for x in summary_source if x.get('movement') == 'slow'),
        'sold_period_items': sum(1 for x in summary_source if x.get('last_sale_date')),
        'movement_window_days': movement_window_days,
        'stock_value': float(sum(float(x.get('stock_value') or 0) for x in summary_source)),
        'abc_counts': [
            {
                'category': key,
                'count': int(value.get('count') or 0),
                'stock_value': float(value.get('stock_value') or 0),
            }
            for key, value in sorted(abc_counts.items(), key=lambda kv: _abc_sort_key(kv[0]))
        ],
        'commercial_status_counts': [
            {
                'status': key,
                'count': int(value.get('count') or 0),
                'stock_value': float(value.get('stock_value') or 0),
            }
            for key, value in sorted(
                commercial_status_counts.items(),
                key=lambda kv: (1 if kv[0] == 'Χωρίς Εμπορικό Status' else 0, kv[0].casefold()),
            )
        ],
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
    movement_window_days = int(
        resolved_classification.get('movement_window_days')
        or _DEFAULT_INVENTORY_ITEM_CLASSIFICATION['movement_window_days']
    )
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
            FactInventory.warehouse_ext_id.label('warehouse_ext_id'),
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
                    func.coalesce(FactInventory.branch_ext_id, literal('')),
                    func.coalesce(FactInventory.warehouse_ext_id, literal('')),
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
        warehouse_ext_col=latest_item_rows.c.warehouse_ext_id,
        brand_ext_col=_json_text(latest_item_rows.c.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(latest_item_rows.c.source_payload_json, 'brand_name'),
        category_1_col=_json_text(latest_item_rows.c.source_payload_json, 'category_1'),
        category_2_col=_json_text(latest_item_rows.c.source_payload_json, 'category_2'),
        category_3_col=_json_text(latest_item_rows.c.source_payload_json, 'category_3'),
        group_ext_col=_json_text(latest_item_rows.c.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(latest_item_rows.c.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(latest_item_rows.c.source_payload_json, 'commercial_category'),
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
        warehouse_ext_col=latest_item_rows.c.warehouse_ext_id,
        brand_ext_col=_json_text(latest_item_rows.c.source_payload_json, 'brand_external_id'),
        brand_label_col=_json_text(latest_item_rows.c.source_payload_json, 'brand_name'),
        category_1_col=_json_text(latest_item_rows.c.source_payload_json, 'category_1'),
        category_2_col=_json_text(latest_item_rows.c.source_payload_json, 'category_2'),
        category_3_col=_json_text(latest_item_rows.c.source_payload_json, 'category_3'),
        group_ext_col=_json_text(latest_item_rows.c.source_payload_json, 'group_external_id'),
        group_label_col=_json_text(latest_item_rows.c.source_payload_json, 'group_name'),
        commercial_category_col=_json_text(latest_item_rows.c.source_payload_json, 'commercial_category'),
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
        .where(FactSales.doc_date >= (as_of - timedelta(days=movement_window_days)))
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
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=movement_window_days)))
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
        sales_qty=sales_qty_30,
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
        sales_qty=sales_qty_30,
        config=resolved_classification,
        is_active_source=effective_active_source,
        manual_order_category=(getattr(dim_item, 'manual_order_category', '') or '') if dim_item else '',
        commercial_status=(getattr(dim_item, 'commercial_status', '') or '') if dim_item else '',
        qty_on_hand=qty_on_hand,
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
    _append_raw_field(raw_fields, 'classification.movement_window_days', movement_window_days)
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
        'commercial_status': str(dim_item.commercial_status or '') if dim_item else '',
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
    group_labels = await _dimension_label_map(db, DimGroup)

    suppliers_rows = (
        await db.execute(
            select(DimSupplier.external_id, DimSupplier.name)
            .where(DimSupplier.external_id.is_not(None))
            .order_by(DimSupplier.name.asc())
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
    category_path_expr = _item_category_path_expr()

    categories_stmt = (
        select(category_path_expr.label('category_path'))
        .select_from(FactPurchases)
        .join(DimItem, DimItem.external_id == FactPurchases.item_code)
        .where(FactPurchases.item_code.is_not(None))
        .where(category_path_expr.is_not(None))
        .where(category_path_expr != 'N/A > N/A > N/A')
    )
    if effective_supplier:
        categories_stmt = categories_stmt.where(FactPurchases.supplier_ext_id == effective_supplier)
    if target_item_codes:
        categories_stmt = categories_stmt.where(FactPurchases.item_code.in_(list(target_item_codes)))
    categories_rows = (
        await db.execute(categories_stmt.group_by(category_path_expr).order_by(category_path_expr.asc()).limit(1000))
    ).all()

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
        {'value': str(r[0]), 'label': str(r[0])}
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
    price_position: str | None = None,
    limit: int = 500,
    price_margin_targets: dict | None = None,
):
    def _vat_pct_from_label(value: object) -> float:
        text = str(value or '')
        match = re.search(r'(\d+(?:[,.]\d+)?)\s*%', text)
        if not match:
            return 0.0
        try:
            return max(0.0, min(100.0, float(match.group(1).replace(',', '.'))))
        except ValueError:
            return 0.0

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
    category_item_codes = None
    if categories:
        category_path_expr = _item_category_path_expr()
        category_item_codes = (
            select(DimItem.external_id)
            .select_from(DimItem)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .where(DimItem.external_id.is_not(None))
            .where(
                or_(
                    _item_category_path_expr().in_(categories),
                    DimCategory.external_id.in_(categories),
                    DimCategory.name.in_(categories),
                )
            )
            .distinct()
        )

    purchases_stmt = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value'),
            func.coalesce(
                func.sum(
                    case(
                        (func.abs(func.coalesce(FactPurchases.discount_amount, 0)) > 0, func.abs(func.coalesce(FactPurchases.discount_amount, 0))),
                        else_=(
                            func.abs(func.coalesce(FactPurchases.discount1_amount, 0))
                            + func.abs(func.coalesce(FactPurchases.discount2_amount, 0))
                            + func.abs(func.coalesce(FactPurchases.discount3_amount, 0))
                        ),
                    )
                ),
                0,
            ).label('purchases_discount'),
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
        purchases_stmt = purchases_stmt.where(
            or_(FactPurchases.category_ext_id.in_(categories), FactPurchases.item_code.in_(category_item_codes))
        )
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
        sales_stmt = sales_stmt.where(or_(FactSales.category_ext_id.in_(categories), FactSales.item_code.in_(category_item_codes)))
    if groups:
        sales_stmt = sales_stmt.where(FactSales.group_ext_id.in_(groups))
    if target_item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(list(target_item_codes)))
    if item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(item_codes))
    sales_stmt = sales_stmt.group_by(FactSales.item_code).subquery('pc_sales')

    item_codes_rows = (await db.execute(select(purchases_stmt.c.item_code))).scalars().all()
    sales_codes = (await db.execute(select(sales_stmt.c.item_code))).scalars().all()
    # Price control needs both acquisition cost and sale price. Exclude synthetic
    # sales-only lines such as VAT summary codes because they cannot produce a
    # meaningful purchase cost or discount recommendation.
    code_set = {str(x) for x in item_codes_rows if x}.intersection({str(x) for x in sales_codes if x})
    if not code_set:
        return {
            'summary': {'items': 0, 'avg_margin_pct': 0.0, 'target_margin_pct': target_margin_pct},
            'rows': [],
            'effective_discount_pct': effective_discount_pct,
        }

    latest_inventory_date = (
        await db.execute(
            select(func.max(FactInventory.doc_date)).where(
                FactInventory.doc_date <= date_to,
                FactInventory.movement_type == 'snapshot',
            )
        )
    ).scalar_one_or_none()
    retail_unit_map: dict[str, dict[str, float]] = {}
    if latest_inventory_date:
        retail_value_expr = cast(
            func.nullif(FactInventory.source_payload_json['retail_value_amount'].astext, ''),
            Numeric,
        )
        inventory_rows = (
            await db.execute(
                select(
                    FactInventory.item_code,
                    func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
                    func.coalesce(func.sum(retail_value_expr), 0).label('retail_value'),
                    func.max(FactInventory.source_payload_json['vat_label'].astext).label('vat_label'),
                )
                .where(
                    FactInventory.doc_date == latest_inventory_date,
                    FactInventory.movement_type == 'snapshot',
                    FactInventory.item_code.in_(list(code_set)),
                )
                .group_by(FactInventory.item_code)
            )
        ).all()
        retail_unit_map = {
            str(r[0]): {
                'gross': float(r[2] or 0) / float(r[1] or 0),
                'vat_pct': _vat_pct_from_label(r[3]),
            }
            for r in inventory_rows
            if r[0] and float(r[1] or 0) > 0 and float(r[2] or 0) > 0
        }

    meta_rows = (
        await db.execute(
            select(
                DimItem.external_id,
                func.coalesce(DimItem.name, DimItem.external_id).label('item_name'),
                func.coalesce(DimItem.barcode, literal('')).label('barcode'),
                func.coalesce(DimBrand.name, literal('N/A')).label('brand'),
                func.coalesce(
                    DimCategory.name,
                    func.nullif(_item_category_path_expr(), literal('N/A > N/A > N/A')),
                    literal('N/A'),
                ).label('category'),
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
            'discount': float(r[3] or 0),
            'cost': float(r[4] or 0),
            'supplier_ext_id': str(r[5] or ''),
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
        if p['qty'] <= 0 or s['qty'] <= 0:
            continue

        purchase_after_invoice_discount = p['cost'] if p['cost'] > 0 else p['value']
        purchase_before_invoice_discount = purchase_after_invoice_discount + max(0.0, float(p.get('discount') or 0.0))
        wholesale_unit = (purchase_before_invoice_discount / p['qty']) if p['qty'] > 0 else 0.0
        invoice_discount_pct = (
            (float(p.get('discount') or 0.0) / purchase_before_invoice_discount) * 100.0
            if purchase_before_invoice_discount > 0
            else 0.0
        )
        acquisition_unit = (purchase_after_invoice_discount / p['qty']) if p['qty'] > 0 else 0.0
        simulated_acquisition_unit = acquisition_unit * (1 - effective_discount_pct / 100.0)
        sale_unit = (s['value'] / s['qty']) if s['qty'] > 0 else 0.0
        retail_info = retail_unit_map.get(code) or {'gross': 0.0, 'vat_pct': 0.0}
        retail_unit = float(retail_info.get('gross') or 0.0)
        vat_pct = float(retail_info.get('vat_pct') or 0.0)
        vat_factor = 1 + (vat_pct / 100.0)
        retail_unit_net = (retail_unit / vat_factor) if retail_unit > 0 and vat_factor > 0 else 0.0
        m = meta.get(code, {'item_name': code, 'barcode': '', 'brand': 'N/A', 'category': 'N/A', 'group': 'N/A'})
        row_target_margin_pct, row_target_source = resolve_price_margin_target(
            price_margin_targets,
            category=m.get('category'),
            group=m.get('group'),
            fallback_pct=target_margin_pct,
        )
        target_sale_unit_net = (
            acquisition_unit / (1 - row_target_margin_pct / 100.0)
            if acquisition_unit > 0 and row_target_margin_pct < 100
            else 0.0
        )
        target_sale_unit = target_sale_unit_net * vat_factor if target_sale_unit_net > 0 else 0.0
        margin_base_unit = retail_unit_net if retail_unit_net > 0 else sale_unit
        unit_profit = margin_base_unit - acquisition_unit
        margin_pct = ((unit_profit / margin_base_unit) * 100.0) if margin_base_unit > 0 else 0.0
        price_gap_value = retail_unit - target_sale_unit
        price_gap_pct = (price_gap_value / target_sale_unit * 100.0) if target_sale_unit > 0 else 0.0

        required_total_discount_pct = None
        recommended_extra_discount_pct = None
        if retail_unit > 0 and target_sale_unit > 0:
            retail_discount = max(0.0, min(99.0, (1 - (target_sale_unit / retail_unit)) * 100.0))
            required_total_discount_pct = retail_discount
            recommended_extra_discount_pct = retail_discount

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
                'discount_pct': invoice_discount_pct + effective_discount_pct,
                'acquisition_after_discount': acquisition_unit,
                'simulated_acquisition_unit': simulated_acquisition_unit,
                'vat_pct': vat_pct,
                'retail_unit': retail_unit,
                'retail_unit_net': retail_unit_net,
                'sale_unit': sale_unit,
                'target_sale_unit': target_sale_unit,
                'target_sale_unit_net': target_sale_unit_net,
                'predicted_sale_unit': target_sale_unit,
                'price_gap_value': price_gap_value,
                'price_gap_pct': price_gap_pct,
                'unit_profit': unit_profit,
                'margin_pct': margin_pct,
                'target_margin_pct': row_target_margin_pct,
                'target_margin_source': row_target_source,
                'required_total_discount_pct': required_total_discount_pct,
                'recommended_extra_discount_pct': recommended_extra_discount_pct,
            }
        )

    price_position_norm = (price_position or '').strip().lower()
    if price_position_norm == 'below':
        rows = [row for row in rows if float(row.get('price_gap_value') or 0) < 0]
    elif price_position_norm == 'above':
        rows = [row for row in rows if float(row.get('price_gap_value') or 0) >= 0]

    rows.sort(key=lambda x: float(x.get('sales_value', 0) or 0), reverse=True)
    rows = rows[: max(1, min(limit, 2000))]
    avg_margin = (sum(float(r['margin_pct']) for r in rows) / len(rows)) if rows else 0.0
    avg_discount = (sum(float(r['discount_pct']) for r in rows) / len(rows)) if rows else 0.0
    avg_target_margin = (sum(float(r['target_margin_pct']) for r in rows) / len(rows)) if rows else target_margin_pct

    return {
        'summary': {
            'items': len(rows),
            'avg_margin_pct': round(avg_margin, 2),
            'avg_discount_pct': round(avg_discount, 2),
            'target_margin_pct': round(avg_target_margin, 2),
            'default_target_margin_pct': round(target_margin_pct, 2),
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
    action_limit: int = 20,
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

    row_limit = max(1, min(int(limit), 20000))
    action_row_limit = max(1, min(int(action_limit), 20000))
    row_offset = max(0, int(offset))
    all_item_code_rows = (
        await db.execute(
            select(grouped.c.item_code)
            .order_by(grouped.c.sales_value.desc(), grouped.c.sales_qty.desc(), grouped.c.item_code.asc())
            .limit(max(5000, row_limit))
        )
    ).scalars().all()
    all_item_codes = [str(code) for code in all_item_code_rows if code]
    all_inventory_map = await _sellout_latest_inventory_rows(
        db,
        all_item_codes,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
    )

    sales_rows = (
        await db.execute(
            select(grouped)
            .order_by(grouped.c.sales_value.desc(), grouped.c.sales_qty.desc(), grouped.c.item_code.asc())
            .offset(row_offset)
            .limit(row_limit)
        )
    ).mappings().all()
    item_codes = [str(r['item_code']) for r in sales_rows if r.get('item_code')]

    inventory_map = {code: all_inventory_map.get(code, {}) for code in item_codes}

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
    stock_total = sum(float(v.get('stock_qty') or 0) for v in all_inventory_map.values())
    stock_value_total = sum(float(v.get('stock_value') or 0) for v in all_inventory_map.values())
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
        avg_daily_qty = sales_qty / max(days, 1)
        days_of_supply = (stock_qty / avg_daily_qty) if avg_daily_qty > 0 else None
        sell_through_pct = (sales_qty / (sales_qty + stock_qty) * 100.0) if (sales_qty + stock_qty) > 0 else 0.0
        gmroi = (gross_profit_value / stock_value) if stock_value > 0 else 0.0
        stock_to_sales = (stock_value / sales_value) if sales_value > 0 else 0.0
        reorder_qty = max(0.0, (avg_daily_qty * 30.0) - stock_qty) if avg_daily_qty > 0 else 0.0
        avg_unit_sales_value = (sales_value / sales_qty) if sales_qty else 0.0
        lost_sales_value = (avg_daily_qty * min(days, 14) * avg_unit_sales_value) if sales_qty > 0 and stock_qty <= 0 else 0.0
        status = 'normal'
        if sales_qty > 0 and stock_qty <= 0:
            status = 'stockout'
        elif days_of_supply is not None and days_of_supply < 20 and profit_pct >= 15:
            status = 'reorder'
        elif days_of_supply is not None and days_of_supply > 120:
            status = 'overstock'
        elif sales_value > 0 and profit_pct < 15:
            status = 'low_margin'
        elif gmroi >= 1.5 and sales_qty > 0:
            status = 'winner'
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
                'days_of_supply': round(days_of_supply, 1) if days_of_supply is not None else None,
                'sell_through_pct': round(sell_through_pct, 2),
                'gmroi': round(gmroi, 2),
                'stock_to_sales': round(stock_to_sales, 2),
                'reorder_qty': round(reorder_qty, 2),
                'lost_sales_value': round(lost_sales_value, 2),
                'status': status,
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
    total_sales_qty = float(totals.get('sales_qty') or 0)
    avg_daily_qty_total = total_sales_qty / max(days, 1)
    days_of_supply_total = (stock_total / avg_daily_qty_total) if avg_daily_qty_total > 0 else 0.0
    sell_through_total = (total_sales_qty / (total_sales_qty + stock_total) * 100.0) if (total_sales_qty + stock_total) > 0 else 0.0
    gmroi_total = (total_profit / stock_value_total) if stock_value_total > 0 else 0.0
    stock_to_sales_total = (stock_value_total / total_sales) if total_sales > 0 else 0.0

    def _action_row(row: dict[str, object]) -> dict[str, object]:
        return {
            'item_code': row.get('item_code'),
            'product': row.get('product'),
            'barcode': row.get('barcode'),
            'supplier_name': row.get('supplier_name'),
            'brand': row.get('brand'),
            'category': row.get('category'),
            'sales_value': row.get('sales_value'),
            'sales_qty': row.get('sales_qty'),
            'gross_profit_pct': row.get('gross_profit_pct'),
            'gross_profit_value': row.get('gross_profit_value'),
            'stock_qty': row.get('stock_qty'),
            'stock_value': row.get('stock_value'),
            'days_of_supply': row.get('days_of_supply'),
            'sell_through_pct': row.get('sell_through_pct'),
            'gmroi': row.get('gmroi'),
            'reorder_qty': row.get('reorder_qty'),
            'lost_sales_value': row.get('lost_sales_value'),
            'action': row.get('action'),
            'status': row.get('status'),
        }

    stockout_risk = [_action_row(r) for r in rows if r.get('status') == 'stockout'][:action_row_limit]
    reorder_candidates = sorted(
        [_action_row(r) for r in rows if r.get('status') == 'reorder'],
        key=lambda x: (float(x.get('days_of_supply') or 9999), -float(x.get('gross_profit_value') or 0)),
    )[:action_row_limit]
    overstock_risk = sorted(
        [_action_row(r) for r in rows if r.get('status') == 'overstock'],
        key=lambda x: float(x.get('stock_value') or 0),
        reverse=True,
    )[:action_row_limit]
    low_margin = sorted(
        [_action_row(r) for r in rows if r.get('status') == 'low_margin'],
        key=lambda x: float(x.get('sales_value') or 0),
        reverse=True,
    )[:action_row_limit]
    top_gmroi = sorted(
        [_action_row(r) for r in rows if float(r.get('gmroi') or 0) > 0],
        key=lambda x: float(x.get('gmroi') or 0),
        reverse=True,
    )[:action_row_limit]
    transfer_candidates = sorted(
        [
            _action_row(r) for r in rows
            if float(r.get('stock_qty') or 0) > max(float(r.get('sales_qty') or 0) * 2.0, 10.0)
            and float(r.get('sales_qty') or 0) > 0
        ],
        key=lambda x: float(x.get('stock_value') or 0),
        reverse=True,
    )[:action_row_limit]
    lost_sales = sorted(
        [_action_row(r) for r in rows if float(r.get('lost_sales_value') or 0) > 0],
        key=lambda x: float(x.get('lost_sales_value') or 0),
        reverse=True,
    )[:action_row_limit]

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
            'stock_value': round(stock_value_total, 2),
            'sell_through_pct': round(sell_through_total, 2),
            'days_of_supply': round(days_of_supply_total, 1),
            'gmroi': round(gmroi_total, 2),
            'stock_to_sales': round(stock_to_sales_total, 2),
            'stockout_items': len(stockout_risk),
            'reorder_items': len(reorder_candidates),
            'overstock_items': len(overstock_risk),
            'low_margin_items': len(low_margin),
            'lost_sales_value': round(sum(float(r.get('lost_sales_value') or 0) for r in rows), 2),
        },
        'actions': {
            'stockout_risk': stockout_risk,
            'reorder_candidates': reorder_candidates,
            'overstock_risk': overstock_risk,
            'low_margin': low_margin,
            'top_gmroi': top_gmroi,
            'transfer_candidates': transfer_candidates,
            'lost_sales': lost_sales,
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
    # Use the ERP document timestamp, not the BI ingest/download timestamp.
    # SoftOne installations do not always expose the original insert time, so
    # fall back to the ERP update/posting timestamp before the visual noon bucket.
    erp_time_expr = func.coalesce(FactSales.source_created_at, FactSales.source_updated_at)

    stmt = (
        select(
            func.extract('hour', erp_time_expr).label('hour'),
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.sum(FactSales.net_value).label('net_value'),
        )
        .outerjoin(DimBranch, DimBranch.external_id == FactSales.branch_ext_id)
        .where(FactSales.doc_date == day)
        .where(erp_time_expr.is_not(None))
        .group_by(
            func.extract('hour', erp_time_expr),
            FactSales.branch_ext_id,
        )
        .order_by(func.extract('hour', erp_time_expr))
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
        display_branch = str(branch_name or '').strip()
        if not display_branch or display_branch.upper() == 'N/A':
            display_branch = _strip_tenant_prefix(str(branch_ext_id or '').strip()) or str(branch_ext_id)
        branch_key = _normalize_search_term(display_branch) or str(branch_ext_id)
        branches_map.setdefault(branch_key, display_branch)
        hour_values = data.setdefault(h, {})
        hour_values[branch_key] = round(float(hour_values.get(branch_key) or 0) + float(net_value or 0), 2)

    # Fallback: if no ERP timestamp exists, return daily totals at hour 12.
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
            display_branch = str(branch_name or '').strip()
            if not display_branch or display_branch.upper() == 'N/A':
                display_branch = _strip_tenant_prefix(str(branch_ext_id or '').strip()) or str(branch_ext_id)
            branch_key = _normalize_search_term(display_branch) or str(branch_ext_id)
            branches_map.setdefault(branch_key, display_branch)
            noon_values = data.setdefault(12, {})
            noon_values[branch_key] = round(float(noon_values.get(branch_key) or 0) + float(net_value or 0), 2)

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
        display_branch = str(branch_name or branch_ext_id).strip() or str(branch_ext_id)
        branch_key = _normalize_search_term(display_branch) or str(branch_ext_id)
        branches_map.setdefault(branch_key, display_branch)
        day_values = data.setdefault(d, {})
        day_values[branch_key] = round(float(day_values.get(branch_key) or 0) + float(net_value or 0), 2)

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
        display_name = str(branch_name or '').strip()
        if not display_name or display_name.upper() == 'N/A':
            display_name = _strip_tenant_prefix(str(branch_ext_id or '').strip()) or str(branch_ext_id)
        branch_key = _normalize_search_term(display_name) or str(branch_ext_id)
        branches_map.setdefault(branch_key, display_name)
        month_values = data.setdefault(m, {})
        month_values[branch_key] = round(float(month_values.get(branch_key, 0) or 0) + float(net_value or 0), 2)

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
