from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from uuid import UUID

from sqlalchemy import Date, Integer, String, case, cast, func, literal, literal_column, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import over

from app.services.intelligence_service import list_recent_insights
from app.models.tenant import (
    AggPurchasesDaily,
    AggPurchasesMonthly,
    AggSalesDaily,
    AggSalesDailyBranch,
    AggSalesMonthly,
    AggInventorySnapshotDaily,
    AggStockAging,
    AggCashDaily,
    AggCashByType,
    AggCashAccounts,
    AggCustomerBalancesDaily,
    DimBranch,
    DimBrand,
    DimCategory,
    DimGroup,
    DimItem,
    DimSupplier,
    DimWarehouse,
    AggSupplierBalancesDaily,
    FactCashflow,
    FactCustomerBalance,
    FactInventory,
    FactPurchases,
    FactSales,
    FactSupplierBalance,
    SupplierTarget,
    SupplierTargetItem,
)


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


def _apply_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
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
    if branches:
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


def _apply_sales_monthly_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
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
    if branches:
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


def _apply_fact_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactSales.group_ext_id.in_(groups))
    return stmt


def _apply_fact_purchases_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactPurchases.group_ext_id.in_(groups))
    return stmt


def _fact_purchases_document_key_expr():
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
    return func.coalesce(ext_token, event_token, coarse_key)


def _fact_purchases_document_no_expr(doc_key_expr):
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
    return func.coalesce(event_doc_no, external_doc_no, item_doc_no, doc_key_expr)


def _purchase_document_no_from_fact(fact: FactPurchases, doc_id: str) -> str:
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
        func.coalesce(cast(FactInventory.branch_id, String), literal('')),
        '|',
        func.coalesce(cast(FactInventory.warehouse_id, String), literal('')),
    )
    return func.coalesce(ext_token, coarse_key)


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
        if branches:
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
            out: dict[str, dict[str, object]] = {}
            for row in agg_latest_rows:
                customer_id = str(row.get('customer_id') or '').strip()
                if not customer_id:
                    continue
                out[customer_id] = dict(row)
            return out

    if aggregate_only:
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
    if branches:
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


def _apply_inventory_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    if warehouses:
        stmt = stmt.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if categories:
        stmt = stmt.where(DimCategory.external_id.in_(categories))
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    return stmt


def _month_floor(value: date) -> date:
    return value.replace(day=1)


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
    cols = []
    for key, (window_from, window_to) in windows.items():
        cond = AggPurchasesDaily.doc_date.between(window_from, window_to)
        cols.extend(
            [
                func.count(AggPurchasesDaily.id).filter(cond).label(f'{key}_records'),
                func.coalesce(func.sum(AggPurchasesDaily.qty).filter(cond), 0).label(f'{key}_qty'),
                func.coalesce(func.sum(AggPurchasesDaily.net_value).filter(cond), 0).label(f'{key}_net_value'),
                func.coalesce(func.sum(AggPurchasesDaily.cost_amount).filter(cond), 0).label(f'{key}_cost_amount'),
            ]
        )

    stmt = select(*cols).where(*_date_range(AggPurchasesDaily.doc_date, global_from, global_to))
    stmt = _apply_purchase_filters(
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
        }
    return out


def _map_branch_window_rows(
    raw_rows: list[dict],
    *,
    key_prefix: str,
) -> list[dict]:
    prepped = []
    for row in raw_rows:
        net_value = float(row.get(f'{key_prefix}_net') or 0)
        gross_value = float(row.get(f'{key_prefix}_gross') or 0)
        cost_amount = float(row.get(f'{key_prefix}_cost') or 0)
        if net_value == 0 and gross_value == 0 and cost_amount == 0:
            continue
        prepped.append(
            {
                'branch': row.get('branch_name') or row.get('branch_ext_id') or 'N/A',
                'branch_code': row.get('branch_ext_id') or 'N/A',
                'net_value': net_value,
                'gross_value': gross_value,
                'cost_amount': cost_amount,
            }
        )

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

    use_branch_daily = not any([warehouses, brands, categories, groups])
    global_from, global_to = _window_bounds(windows)
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

    if use_branch_daily:
        stmt = (
            select(*cols)
            .select_from(AggSalesDailyBranch)
            .join(DimBranch, DimBranch.external_id == AggSalesDailyBranch.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDailyBranch.doc_date, global_from, global_to))
        )
        if branches:
            stmt = stmt.where(AggSalesDailyBranch.branch_ext_id.in_(branches))
        rows = (await db.execute(stmt.group_by(AggSalesDailyBranch.branch_ext_id))).mappings().all()
    else:
        stmt = (
            select(*cols)
            .select_from(AggSalesDaily)
            .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
            .where(*_date_range(AggSalesDaily.doc_date, global_from, global_to))
        )
        stmt = _apply_sales_filters(
            stmt,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
        rows = (await db.execute(stmt.group_by(AggSalesDaily.branch_ext_id))).mappings().all()

    out: dict[str, list[dict]] = {}
    for key in windows:
        out[key] = _map_branch_window_rows(rows, key_prefix=key)
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
            if branches:
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

    total_net = sum(float(r[2] or 0) for r in rows)
    avg_net = (total_net / len(rows)) if rows else 0.0
    out = []
    for r in rows:
        net_value = float(r[2] or 0)
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
    return out


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
    stmt = (
        select(
            func.count(AggPurchasesDaily.id),
            func.coalesce(func.sum(AggPurchasesDaily.qty), 0),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0),
        )
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    row = (await db.execute(stmt)).one()
    return {
        'records': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'net_value': float(row[2] or 0),
        'cost_amount': float(row[3] or 0),
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
            AggPurchasesDaily.supplier_ext_id,
            func.coalesce(func.max(DimSupplier.name), AggPurchasesDaily.supplier_ext_id).label('supplier_name'),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0).label('cost_amount'),
        )
        .join(DimSupplier, DimSupplier.external_id == AggPurchasesDaily.supplier_ext_id, isouter=True)
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggPurchasesDaily.supplier_ext_id).order_by(func.sum(AggPurchasesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'supplier': r[1] or r[0] or 'N/A',
            'supplier_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'cost_amount': float(r[3] or 0),
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
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), AggPurchasesDaily.doc_date), Date)
    stmt = (
        select(
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(AggPurchasesDaily.qty), 0).label('qty'),
        )
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
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
        net_value = float(r['net_value'])
        cost_amount = float(r['cost_amount'])
        margin_value = net_value - cost_amount
        margin_pct = (margin_value / net_value * 100.0) if net_value > 0 else 0.0
        enriched.append(
            {
                'supplier': r['supplier'],
                'net_value': net_value,
                'cost_amount': cost_amount,
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
    return {
        'summary': summary,
        'purchase_trend': trend,
        'supplier_distribution': supplier_distribution,
        'margin_by_supplier': margin_by_supplier,
        'cost_change_detection': cost_change,
        'seasonality': seasonal,
        'new_codes': new_codes,
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

    rows = (await db.execute(select(model.external_id, model.name).where(model.external_id.is_not(None)))).all()
    return {str(ext): str(name or ext) for ext, name in rows if ext}


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

    return {**options, 'labels': labels}


async def sales_documents_overview(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
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
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value'),
            func.count(FactSales.id).label('line_count'),
            func.coalesce(func.max(FactSales.origin_ref), literal('')).label('origin_ref'),
            func.coalesce(func.max(FactSales.destination_ref), literal('')).label('destination_ref'),
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

    gross_total_expr = func.coalesce(func.sum(FactSales.gross_value), 0)
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
            'description',
            'product_name',
            'name',
            fallback='',
        )
        item_code = str(payload_item_code or fact.item_code or '').strip()
        item_name = _clean_item_name(row[1] or payload_item_name or '', None)
        if item_name == 'N/A':
            item_name = ''

        qty = float(fact.qty or 0)
        qty_exec = float(fact.qty_executed if fact.qty_executed is not None else qty)
        net_value = float(fact.net_value or 0)
        gross_value = float(fact.gross_value or 0)
        vat_value = float(fact.vat_amount if fact.vat_amount is not None else max(0.0, gross_value - net_value))
        unit_price = float(fact.unit_price) if fact.unit_price is not None else (net_value / qty if qty else 0.0)
        discount_pct = float(fact.discount_pct) if fact.discount_pct is not None else 0.0
        discount_amount = float(fact.discount_amount) if fact.discount_amount is not None else 0.0

        total_qty += qty
        total_exec += qty_exec
        total_net += net_value
        total_vat += vat_value
        total_gross += gross_value

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
        },
        'delivery': {
            'customer_branch': customer_branch,
            'address': str(first_fact.delivery_address or ''),
            'zip': str(first_fact.delivery_zip or ''),
            'city': str(first_fact.delivery_city or ''),
            'area': str(first_fact.delivery_area or ''),
            'movement_type': movement_text,
            'carrier_name': str(first_fact.carrier_name or ''),
            'transport_medium': str(first_fact.transport_medium or ''),
            'transport_no': str(first_fact.transport_no or ''),
            'route_name': str(first_fact.route_name or ''),
            'loading_date': first_fact.loading_date.isoformat() if first_fact.loading_date else '',
            'delivery_date': first_fact.delivery_date.isoformat() if first_fact.delivery_date else '',
        },
        'notes': {
            'notes_1': str(first_fact.notes or ''),
            'notes_2': str(first_fact.notes_2 or ''),
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
            literal('Αγορές').label('series_label'),
            literal('').label('status_label'),
            literal('Παραστατικό Αγορών').label('document_type'),
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
            | func.lower(cast(func.coalesce(DimSupplier.name, FactPurchases.supplier_ext_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.supplier_ext_id, literal('')), String)).like(like)
            | func.lower(cast(func.coalesce(FactPurchases.item_code, literal('')), String)).like(like)
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
                'series': str(r.get('series_label') or 'Αγορές'),
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
    total_cost = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactPurchases = row[0]
        item_name = _clean_item_name(row[1], fact.item_code)
        qty = float(fact.qty or 0)
        net_value = float(fact.net_value or 0)
        cost_value = float(fact.cost_amount or 0)
        unit_price = net_value / qty if qty else 0.0

        total_qty += qty
        total_net += net_value
        total_cost += cost_value

        line_rows.append(
            {
                'row_no': idx,
                'line_no': idx,
                'item_code': str(fact.item_code or ''),
                'item_name': item_name,
                'qty': qty,
                'qty_executed': qty,
                'unit_price': unit_price,
                'discount_pct': 0.0,
                'discount_amount': 0.0,
                'vat_amount': 0.0,
                'line_total': net_value,
                'line_net': net_value,
                'line_external_id': str(fact.external_id or ''),
            }
        )

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'fact_purchases.header', first_fact)
    _append_raw_field(raw_fields, 'dim_branches.name', branch_name)
    _append_raw_field(raw_fields, 'dim_warehouses.name', warehouse_name)
    _append_raw_field(raw_fields, 'dim_suppliers.name', supplier_name)

    return {
        'document_id': doc_id,
        'document_no': document_no,
        'document_date': first_fact.doc_date.isoformat() if first_fact.doc_date else '',
        'header': {
            'branch_code': '',
            'branch_name': branch_name,
            'warehouse_code': '',
            'warehouse_name': warehouse_name,
            'series': 'Αγορές',
            'document_type': 'Παραστατικό Αγορών',
            'status': '',
            'supplier_code': '',
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
            'gross_value': total_net,
            'net_value': total_net,
            'vat_value': 0.0,
            'expenses_value': 0.0,
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
            func.coalesce(func.max(doc_key), literal('')).label('document_no'),
            func.max(FactInventory.doc_date).label('document_date'),
            func.coalesce(func.max(DimBranch.name), literal('N/A')).label('branch_name'),
            func.coalesce(func.max(DimBranch.external_id), literal('')).label('branch_code'),
            func.coalesce(func.max(DimWarehouse.name), literal('N/A')).label('warehouse_name'),
            func.coalesce(func.max(DimWarehouse.external_id), literal('')).label('warehouse_code'),
            literal('Δελτίο Ενδοδιακίνησης').label('series_label'),
            literal('Δελτίο Ενδοδιακίνησης').label('document_type'),
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
    )
    if branches:
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
                'series': str(r.get('series_label') or 'Δελτίο Ενδοδιακίνησης'),
                'document_type': str(r.get('document_type') or 'Δελτίο Ενδοδιακίνησης'),
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
    )
    if date_from is not None:
        stmt = stmt.where(FactInventory.doc_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(FactInventory.doc_date <= date_to)
    if branches:
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
    branch_code = str(rows[0][3] or '')
    branch_name = str(rows[0][4] or 'N/A')
    warehouse_code = str(rows[0][5] or '')
    warehouse_name = str(rows[0][6] or 'N/A')

    line_rows = []
    total_qty = 0.0
    total_value = 0.0
    total_cost = 0.0
    for idx, row in enumerate(rows, start=1):
        fact: FactInventory = row[0]
        item_code = str(row[1] or '')
        item_name = _clean_item_name(row[2], item_code)
        qty = float(fact.qty_on_hand or 0)
        value_amount = float(fact.value_amount or 0)
        cost_amount = float(fact.cost_amount or 0)
        unit_price = value_amount / qty if qty else 0.0

        total_qty += qty
        total_value += value_amount
        total_cost += cost_amount

        line_rows.append(
            {
                'row_no': idx,
                'line_no': idx,
                'item_code': item_code,
                'item_name': item_name,
                'qty': qty,
                'qty_executed': 0.0,
                'unit_price': unit_price,
                'discount_pct': 0.0,
                'discount_amount': 0.0,
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

    return {
        'document_id': doc_id,
        'document_no': doc_id,
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
            'series': 'Δελτίο Ενδοδιακίνησης',
            'document_type': 'Δελτίο Ενδοδιακίνησης',
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
            'expenses_value': 0.0,
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
        'brands': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.brand_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=None,
            categories=categories,
            groups=groups,
        ),
        'categories': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.category_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=None,
            groups=groups,
        ),
        'groups': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.group_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=None,
        ),
        'labels': labels,
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
    if branches:
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
    if branches:
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
        if branches:
            purchases_stmt = purchases_stmt.where(FactPurchases.branch_ext_id.in_(branches))
        if warehouses:
            purchases_stmt = purchases_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
        if brands:
            purchases_stmt = purchases_stmt.where(FactPurchases.brand_ext_id.in_(brands))
        if categories:
            purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
        if groups:
            purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
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
    stmt = _apply_sales_filters(stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups)
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
    if branches:
        stmt = stmt.where(AggStockAging.branch_ext_id.in_(branches))
    snapshot_date = (await db.execute(stmt)).scalar_one_or_none()
    if isinstance(snapshot_date, date):
        return snapshot_date
    return None


def _apply_stock_aging_dimension_filters(stmt, *, brands=None, categories=None, groups=None):
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
    if branches:
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
    if branches:
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
    if branches:
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
    if branches:
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
    if branches:
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

    if branches:
        base = base.where(DimBranch.external_id.in_(branches))
    if normalized_category:
        base = base.where(
            (subcategory_col == normalized_category)
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
    if branches:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    if normalized_category:
        stmt = stmt.where(
            (subcategory_col == normalized_category)
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
        if branches:
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
    if branches:
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
    if branches:
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

        if branches:
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
            if branches:
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
        if branches:
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

    if branches:
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
        if branches:
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
        if branches:
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
        if branches:
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
    if branches:
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
    supplier_ranked = (
        select(
            FactPurchases.item_code.label('item_code'),
            FactPurchases.supplier_ext_id.label('supplier_ext_id'),
            over(
                func.row_number(),
                partition_by=FactPurchases.item_code,
                order_by=(FactPurchases.doc_date.desc(), FactPurchases.updated_at.desc()),
            ).label('rn'),
        )
        .where(FactPurchases.item_code.is_not(None))
        .where(FactPurchases.doc_date <= as_of)
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=max(30, lookback_days))))
        .subquery('supplier_ranked')
    )
    latest_supplier = (
        select(supplier_ranked.c.item_code, supplier_ranked.c.supplier_ext_id)
        .where(supplier_ranked.c.rn == 1)
        .subquery('latest_supplier')
    )

    stmt = (
        select(
            func.coalesce(DimSupplier.name, latest_supplier.c.supplier_ext_id, literal('N/A')).label('manufacturer'),
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
        .join(latest_supplier, DimItem.external_id == latest_supplier.c.item_code, isouter=True)
        .join(DimSupplier, DimSupplier.external_id == latest_supplier.c.supplier_ext_id, isouter=True)
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
        stmt.group_by(DimSupplier.name, latest_supplier.c.supplier_ext_id)
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
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    base = (
        select(
            DimBranch.external_id.label('branch'),
            DimWarehouse.external_id.label('warehouse'),
            DimBrand.external_id.label('brand'),
            DimCategory.external_id.label('category'),
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

    return {
        'branches': await _distinct('branch'),
        'warehouses': await _distinct('warehouse'),
        'brands': await _distinct('brand'),
        'categories': await _distinct('category'),
        'groups': await _distinct('group'),
        'labels': labels,
    }


async def cashflow_by_entry_type(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    if branches:
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
    if branches:
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
    anchor_date = date_to
    day_from = anchor_date
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
        'day': (day_from, anchor_date),
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
            'day': (day_from, anchor_date),
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

    trend_all = await sales_monthly_trend_from_monthly_agg(
        db,
        date_from=date(prev2_year, 1, 1),
        date_to=date(current_year, 12, 31),
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
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
    if branches:
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
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
) -> dict:
    snapshot_date = await _latest_stock_aging_snapshot_date(db, as_of=as_of, branches=branches)
    snapshot = await inventory_snapshot_from_aggregates(
        db,
        as_of=as_of,
        snapshot_date=snapshot_date,
        branches=branches,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    aging = await stock_aging_from_aggregates(
        db,
        as_of=as_of,
        snapshot_date=snapshot_date,
        branches=branches,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_brand = await inventory_by_brand_from_aggregates(
        db,
        as_of=as_of,
        snapshot_date=snapshot_date,
        branches=branches,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_commercial = await inventory_by_group_from_aggregates(
        db,
        as_of=as_of,
        snapshot_date=snapshot_date,
        branches=branches,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    return {
        'snapshot': snapshot,
        'aging': aging,
        'by_brand': by_brand,
        'by_commercial_category': by_commercial,
        'by_manufacturer': [],
    }


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
    if branches:
        stmt = stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactPurchases.group_ext_id.in_(groups))
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
    if branches:
        purchases_stmt = purchases_stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purchases_stmt = purchases_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purchases_stmt = purchases_stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
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
):
    latest_date_stmt = select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of)
    latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
    if latest_date is None:
        return {'snapshot_date': None, 'summary': {}, 'rows': []}

    inv_base = (
        select(
            DimItem.external_id.label('item_code'),
            func.coalesce(func.max(DimItem.name), DimItem.external_id).label('item_name'),
            func.coalesce(func.max(DimItem.barcode), func.max(DimItem.sku), literal('')).label('barcode'),
            func.coalesce(func.max(DimBrand.name), literal('N/A')).label('brand'),
            func.coalesce(func.max(DimCategory.name), literal('N/A')).label('category'),
            func.coalesce(func.max(DimGroup.name), literal('N/A')).label('group_name'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('stock_value'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == latest_date)
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    inv_base = inv_base.group_by(DimItem.external_id).subquery('inv_base')

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
    if branches:
        purch_30 = purch_30.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purch_30 = purch_30.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purch_30 = purch_30.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purch_30 = purch_30.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purch_30 = purch_30.where(FactPurchases.group_ext_id.in_(groups))
    purch_30 = purch_30.group_by(FactPurchases.item_code).subquery('purch_30')

    first_seen = (
        select(
            DimItem.external_id.label('item_code'),
            func.min(FactInventory.doc_date).label('first_seen'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .group_by(DimItem.external_id)
        .subquery('first_seen')
    )

    stmt = (
        select(
            inv_base.c.item_code,
            inv_base.c.item_name,
            inv_base.c.barcode,
            inv_base.c.brand,
            inv_base.c.category,
            inv_base.c.group_name,
            inv_base.c.qty_on_hand,
            inv_base.c.stock_value,
            func.coalesce(sales_30.c.sales_qty_30, 0).label('sales_qty_30'),
            sales_30.c.last_sale_date,
            func.coalesce(purch_30.c.purchases_qty_30, 0).label('purchases_qty_30'),
            first_seen.c.first_seen,
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
        sales_qty_30 = float(r[8] or 0)
        last_sale_date = r[9]
        is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=60)))
        movement_level = 'fast' if sales_qty_30 >= 50 else ('slow' if sales_qty_30 <= 5 else 'normal')
        mapped.append(
            {
                'item_code': str(r[0] or 'N/A'),
                'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
                'barcode': str(r[2] or ''),
                'brand': str(r[3] or 'N/A'),
                'category': str(r[4] or 'N/A'),
                'group': str(r[5] or 'N/A'),
                'qty_on_hand': float(r[6] or 0),
                'stock_value': float(r[7] or 0),
                'sales_qty_30': sales_qty_30,
                'last_sale_date': str(last_sale_date) if last_sale_date else None,
                'purchases_qty_30': float(r[10] or 0),
                'first_seen': str(r[11]) if r[11] else None,
                'status': 'active' if is_active else 'inactive',
                'movement': movement_level,
            }
        )

    if status in {'active', 'inactive'}:
        mapped = [x for x in mapped if x['status'] == status]
    if movement in {'fast', 'slow', 'normal'}:
        mapped = [x for x in mapped if x['movement'] == movement]
    mapped.sort(key=lambda x: (x['stock_value'], x['qty_on_hand']), reverse=True)
    safe_limit = max(1, min(int(limit), 500))
    safe_offset = max(0, int(offset))
    total = len(mapped)
    mapped = mapped[safe_offset : safe_offset + safe_limit]

    summary = {
        'total_items': len(rows),
        'active_items': sum(1 for x in rows if x[9] and x[9] >= (as_of - timedelta(days=60))),
        'inactive_items': sum(1 for x in rows if not (x[9] and x[9] >= (as_of - timedelta(days=60)))),
        'fast_items': sum(1 for x in rows if float(x[8] or 0) >= 50),
        'slow_items': sum(1 for x in rows if float(x[8] or 0) <= 5),
        'stock_value': float(sum(float(x[7] or 0) for x in rows)),
    }
    return {
        'snapshot_date': str(latest_date),
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
):
    code = (item_code or '').strip()
    if not code:
        raise ValueError('Missing item code')

    latest_date = (await db.execute(select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of))).scalar_one_or_none()
    if latest_date is None:
        raise ValueError('No inventory snapshot found')

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
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.qty_reserved), 0).label('qty_reserved'),
            func.coalesce(func.sum(FactInventory.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('stock_value'),
            func.max(FactInventory.updated_at).label('inv_updated_at'),
            func.min(FactInventory.created_at).label('inv_created_at'),
            func.count(FactInventory.id).label('inv_rows'),
            func.count(func.distinct(FactInventory.branch_id)).label('branch_count'),
            func.count(func.distinct(FactInventory.warehouse_id)).label('warehouse_count'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == latest_date)
        .where(DimItem.external_id == code)
    )
    inv_stmt = _apply_inventory_filters(
        inv_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
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
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == latest_date)
        .where(DimItem.external_id == code)
        .order_by(DimBranch.name.asc(), DimWarehouse.name.asc(), FactInventory.external_id.asc())
    )
    inv_detail_stmt = _apply_inventory_filters(
        inv_detail_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
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
    if branches:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.group_ext_id.in_(groups))
    purch_30_row = (await db.execute(purch_30_stmt)).mappings().one()

    first_seen_row = (
        await db.execute(
            select(
                func.min(FactInventory.doc_date).label('first_seen'),
                func.max(FactInventory.doc_date).label('last_inventory_date'),
            )
            .select_from(FactInventory)
            .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
            .where(DimItem.external_id == code)
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

    is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=60)))
    movement_level = 'fast' if sales_qty_30 >= 50 else ('slow' if sales_qty_30 <= 5 else 'normal')

    item_name = _clean_item_name(dim_item.name if dim_item else None, code)
    barcode = str(dim_item.barcode or dim_item.sku or '') if dim_item else ''
    brand_name = str(dim_brand.name or 'N/A') if dim_brand and dim_brand.name else 'N/A'
    category_name = str(dim_category.name or 'N/A') if dim_category and dim_category.name else 'N/A'
    group_name = str(dim_group.name or 'N/A') if dim_group and dim_group.name else 'N/A'

    raw_fields = []
    _append_model_raw_fields(raw_fields, 'dim_items', dim_item)
    if dim_item is None:
        _append_raw_field(raw_fields, 'dim_items.external_id', code)
    _append_model_raw_fields(raw_fields, 'dim_brands', dim_brand)
    _append_model_raw_fields(raw_fields, 'dim_categories', dim_category)
    _append_model_raw_fields(raw_fields, 'dim_groups', dim_group)

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
    _append_raw_field(raw_fields, 'classification.status', 'active' if is_active else 'inactive')
    _append_raw_field(raw_fields, 'classification.movement', movement_level)

    return {
        'item_code': code,
        'item_name': item_name,
        'barcode': barcode,
        'brand': brand_name,
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
        'status': 'active' if is_active else 'inactive',
        'movement': movement_level,
        'snapshot_date': str(latest_date),
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
                func.coalesce(DimItem.barcode, DimItem.sku, literal('')).label('barcode'),
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
