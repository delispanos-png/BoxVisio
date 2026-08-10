from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tenant_manager import get_tenant_db_session
from app.models.control import Tenant
from app.models.tenant import DimBranch, FactSupplierOrder


DEFAULT_SUPPLIER_ORDER_LOOKBACK_DAYS = 30


@dataclass(frozen=True)
class SupplierOrdersFilters:
    from_date: date
    to_date: date
    supplier: str = ''
    supplier_names: tuple = ()
    only_open: bool = True
    limit: int = 500


def normalize_supplier_order_settings(raw: dict | None) -> dict[str, int]:
    source = raw if isinstance(raw, dict) else {}
    try:
        lookback_days = int(str(source.get('lookback_days') or DEFAULT_SUPPLIER_ORDER_LOOKBACK_DAYS).strip())
    except Exception:
        lookback_days = DEFAULT_SUPPLIER_ORDER_LOOKBACK_DAYS
    return {'lookback_days': max(1, min(365, lookback_days))}


async def supplier_order_settings_for_tenant(tenant: Tenant | None) -> dict[str, int]:
    flags = tenant.feature_flags if tenant is not None else None
    source: dict[str, Any] = {}
    if isinstance(flags, dict) and isinstance(flags.get('supplier_orders'), dict):
        source = flags.get('supplier_orders') or {}
    return normalize_supplier_order_settings(source)


async def build_supplier_orders_dashboard(
    control_db: AsyncSession,
    tenant: Tenant,
    filters: SupplierOrdersFilters,
) -> dict[str, Any]:
    del control_db
    try:
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            return await _query_supplier_orders_from_facts(tenant_db, filters)
    except Exception as exc:
        return _empty_payload(filters, str(exc))
    return _empty_payload(filters, 'Δεν ήταν διαθέσιμη tenant βάση για ανάγνωση παραγγελιών προμηθευτών.')


def _empty_payload(filters: SupplierOrdersFilters, error: str | None = None) -> dict[str, Any]:
    return {
        'filters': filters,
        'summary': {
            'documents': 0,
            'open_documents': 0,
            'closed_documents': 0,
            'lines': 0,
            'open_qty': 0.0,
            'open_value': 0.0,
            'suppliers': 0,
        },
        'supplier_rows': [],
        'document_rows': [],
        'line_rows': [],
        'all_supplier_names': [],
        'error': error,
    }


def _as_date(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.strftime('%Y-%m-%d')
    return str(value or '')[:10]


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _row_dict(columns: list[str], row: Any) -> dict[str, Any]:
    return {columns[idx].lower(): row[idx] for idx in range(len(columns))}


async def _query_supplier_orders_from_facts(tenant_db: AsyncSession, filters: SupplierOrdersFilters) -> dict[str, Any]:
    term = str(filters.supplier or '').strip()
    stmt = (
        select(FactSupplierOrder, DimBranch.name.label('branch_name'))
        .outerjoin(DimBranch, DimBranch.id == FactSupplierOrder.branch_id)
        .where(FactSupplierOrder.doc_date >= filters.from_date, FactSupplierOrder.doc_date <= filters.to_date)
        .order_by(FactSupplierOrder.doc_date.desc(), FactSupplierOrder.document_id.desc(), FactSupplierOrder.external_id.asc())
        # High cap so whole orders are never silently dropped from the unfiltered view
        # (the previous 5000-line cap hid older orders once a period had more lines than that).
        .limit(50000)
    )
    if filters.supplier_names:
        # Exact supplier picks from the multiselect (unified dropdown).
        names = [str(n) for n in filters.supplier_names if str(n or '').strip()]
        if names:
            stmt = stmt.where(FactSupplierOrder.supplier_name.in_(names))
    elif term:
        like = f'%{term}%'
        stmt = stmt.where(
            or_(
                FactSupplierOrder.supplier_name.ilike(like),
                FactSupplierOrder.supplier_ext_id.ilike(like),
                FactSupplierOrder.document_no.ilike(like),
            )
        )
    result = await tenant_db.execute(stmt)
    raw_rows = [_fact_supplier_order_row_dict(fact, branch_name) for fact, branch_name in result.all()]
    payload = _build_payload_from_rows(raw_rows, filters)
    # All supplier names (independent of the current supplier filter) for the search typeahead.
    name_rows = (
        await tenant_db.execute(
            select(FactSupplierOrder.supplier_name)
            .where(func.coalesce(func.trim(FactSupplierOrder.supplier_name), '') != '')
            .distinct()
            .order_by(FactSupplierOrder.supplier_name)
            .limit(3000)
        )
    ).scalars().all()
    payload['all_supplier_names'] = [str(n) for n in name_rows if str(n or '').strip()]
    return payload


def _fact_supplier_order_row_dict(fact: FactSupplierOrder, branch_name: str | None) -> dict[str, Any]:
    return {
        'document_id': fact.document_id or fact.event_id or fact.external_id,
        'doc_date': fact.doc_date,
        'document_no': fact.document_no or fact.document_id or fact.external_id,
        'series_id': fact.document_series,
        'series_name': fact.document_series_name or fact.document_series or '',
        'behavior_id': fact.document_behavior_code,
        'branch_ext_id': fact.branch_ext_id,
        'branch_name': branch_name or fact.branch_ext_id or '',
        'supplier_code': fact.supplier_ext_id or '',
        'supplier_name': fact.supplier_name or fact.supplier_ext_id or '',
        'supplier_afm': fact.supplier_afm or '',
        'item_code': fact.item_code or '',
        'item_name': fact.item_name or '',
        'order_qty': fact.order_qty,
        'covered_qty': fact.covered_qty,
        'cancelled_qty': fact.cancelled_qty,
        'line_value': fact.line_value,
        'has_transformation': fact.has_transformation or str(fact.order_status or '').lower() != 'open',
    }


def _build_payload_from_rows(raw_rows: list[dict[str, Any]], filters: SupplierOrdersFilters) -> dict[str, Any]:
    documents: dict[str, dict[str, Any]] = {}
    suppliers: dict[str, dict[str, Any]] = defaultdict(
        lambda: {'supplier_code': '', 'supplier_name': '', 'documents': 0, 'open_documents': 0, 'open_qty': 0.0, 'open_value': 0.0}
    )
    line_rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        document_id = str(raw.get('document_id') or '')
        transformed = bool(int(raw.get('has_transformation') or 0))
        is_open = not transformed
        line_qty = _as_float(raw.get('order_qty'))
        line_value = _as_float(raw.get('line_value'))
        supplier_code = str(raw.get('supplier_code') or '')
        supplier_name = str(raw.get('supplier_name') or supplier_code or '-')
        doc = documents.setdefault(
            document_id,
            {
                'document_id': document_id,
                'doc_date': _as_date(raw.get('doc_date')),
                'document_no': str(raw.get('document_no') or document_id),
                'series_id': raw.get('series_id'),
                'series_name': str(raw.get('series_name') or ''),
                'supplier_code': supplier_code,
                'supplier_name': supplier_name,
                'supplier_afm': str(raw.get('supplier_afm') or ''),
                'branch_name': str(raw.get('branch_name') or raw.get('branch_ext_id') or ''),
                'is_open': is_open,
                'lines': 0,
                'order_qty': 0.0,
                'open_qty': 0.0,
                'value': 0.0,
                'open_value': 0.0,
            },
        )
        doc['lines'] += 1
        doc['order_qty'] += line_qty
        doc['value'] += line_value
        if is_open:
            doc['open_qty'] += line_qty
            doc['open_value'] += line_value
        line = {
            'doc_date': doc['doc_date'],
            'document_no': doc['document_no'],
            'series_name': doc['series_name'],
            'supplier_code': supplier_code,
            'supplier_name': supplier_name,
            'item_code': str(raw.get('item_code') or ''),
            'item_name': str(raw.get('item_name') or ''),
            'order_qty': line_qty,
            'covered_qty': _as_float(raw.get('covered_qty')),
            'cancelled_qty': _as_float(raw.get('cancelled_qty')),
            'line_value': line_value,
            'is_open': is_open,
        }
        if is_open or not filters.only_open:
            line_rows.append(line)

    for doc in documents.values():
        supplier_key = doc['supplier_code'] or doc['supplier_name']
        sup = suppliers[supplier_key]
        sup['supplier_code'] = doc['supplier_code']
        sup['supplier_name'] = doc['supplier_name']
        sup['documents'] += 1
        if doc['is_open']:
            sup['open_documents'] += 1
            sup['open_qty'] += doc['open_qty']
            sup['open_value'] += doc['open_value']

    document_rows = [doc for doc in documents.values() if doc['is_open'] or not filters.only_open]
    document_rows.sort(key=lambda row: (row['doc_date'], row['document_id']), reverse=True)
    supplier_rows = [row for row in suppliers.values() if row['open_documents'] > 0 or not filters.only_open]
    supplier_rows.sort(key=lambda row: row['open_value'], reverse=True)
    line_rows = line_rows[: max(1, min(filters.limit, 2000))]
    summary = {
        'documents': len(documents),
        'open_documents': sum(1 for doc in documents.values() if doc['is_open']),
        'closed_documents': sum(1 for doc in documents.values() if not doc['is_open']),
        'lines': len(raw_rows),
        'open_qty': sum(doc['open_qty'] for doc in documents.values() if doc['is_open']),
        'open_value': sum(doc['open_value'] for doc in documents.values() if doc['is_open']),
        'suppliers': len({doc['supplier_code'] or doc['supplier_name'] for doc in documents.values()}),
    }
    return {
        'filters': filters,
        'summary': summary,
        'supplier_rows': supplier_rows[:50],
        'document_rows': document_rows[:1000],
        'line_rows': line_rows,
        'all_supplier_names': [],
        'error': None,
    }
