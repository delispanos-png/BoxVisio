from __future__ import annotations

import asyncio
import json
import logging
from datetime import date as date_cls, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.control_session import ControlSessionLocal
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import OperationalStream, Tenant, TenantConnection
from app.models.tenant import (
    DimAccount,
    DimBranch,
    DimBrand,
    DimCategory,
    DimCustomer,
    DimDocumentType,
    DimExpenseCategory,
    DimGroup,
    DimItem,
    DimPaymentMethod,
    DimSupplier,
    DimWarehouse,
    FactCashflow,
    FactCustomerBalance,
    FactExpense,
    FactInventory,
    FactPurchases,
    FactSales,
    FactSalesDocumentCharge,
    FactSupplierBalance,
    FactSupplierOrder,
    IngestDeadLetter,
    StgCashTransaction,
    StgCustomerBalance,
    StgExpenseDocument,
    StgInventoryDocument,
    StgPurchaseDocument,
    StgSalesDocument,
    StgSupplierBalance,
    StagingIngestEvent,
    SyncState,
)
from app.services.connection_secrets import build_odbc_connection_string, decrypt_json_secret, decrypt_sqlserver_secret
from app.services.ingestion.base import (
    ALL_OPERATIONAL_STREAMS,
    ENTITY_TO_STREAM,
    STREAM_TO_ENTITY,
    ConnectorContext,
    IncrementalState,
    IngestEntity,
    OperationalIngestStream,
    normalize_stream_name,
    normalize_stream_values,
)
from app.services.ingestion.external_api_connector import ExternalApiIngestConnector
from app.services.ingestion.file_import_connector import FileImportConnector
from app.services.ingestion.pharmacyone_connector import GenericSqlConnector, PharmacyOneSqlConnector
from app.services.ingestion.progress import update_ingest_progress
from app.services.rule_config import resolve_source_query_template

logger = logging.getLogger(__name__)


async def _relink_fact_dimensions_after_bulk_ingest(tenant_db: AsyncSession, entity: str) -> dict[str, int]:
    """Bulk/backfill mode skips per-row dimension lookups; relink rows after flush."""
    statements: dict[str, str] = {
        'sales': """
            UPDATE fact_sales fs
            SET item_id = di.id, updated_at = NOW()
            FROM dim_items di
            WHERE fs.item_id IS NULL
              AND fs.item_code IS NOT NULL
              AND di.external_id = fs.item_code
        """,
        'purchases': """
            UPDATE fact_purchases fp
            SET item_id = di.id, updated_at = NOW()
            FROM dim_items di
            WHERE fp.item_id IS NULL
              AND fp.item_code IS NOT NULL
              AND di.external_id = fp.item_code
        """,
        'inventory': """
            UPDATE fact_inventory fi
            SET item_id = di.id, updated_at = NOW()
            FROM dim_items di
            WHERE fi.item_id IS NULL
              AND fi.item_code IS NOT NULL
              AND di.external_id = fi.item_code
        """,
        'supplier_orders': """
            UPDATE fact_supplier_orders fso
            SET item_id = di.id, updated_at = NOW()
            FROM dim_items di
            WHERE fso.item_id IS NULL
              AND fso.item_code IS NOT NULL
              AND di.external_id = fso.item_code
        """,
    }
    sql = statements.get(entity)
    if not sql:
        return {}
    result = await tenant_db.execute(text(sql))
    return {f'{entity}_item_id_relinked': int(result.rowcount or 0)}


async def _backfill_sales_cost_from_purchases(tenant_db: AsyncSession, *, recent_days: int = 3) -> int:
    """Fill sales COGS from the item's weighted-average acquisition cost (SUM purchase
    net / SUM purchase qty over purchase invoices) wherever the source gave no usable
    cost (cost >= net, i.e. the net fallback -> 0 margin). This is the SQL-connector
    counterpart of the SoftOne bridge's PURMTK cost: tenants whose SoftOne exposes no
    per-line cost (empty SALESCVAL) and no FMATTOTALS still get a real margin from their
    own purchase history. Scoped to recently-updated rows for efficiency; a plausibility
    guard keeps the derived cost below the selling price (never a negative margin)."""
    sql = text(
        """
        WITH wac AS (
            SELECT item_id, SUM(net_value) / NULLIF(SUM(qty), 0) AS unit_cost
            FROM fact_purchases
            WHERE qty > 0 AND net_value > 0 AND item_id IS NOT NULL
            GROUP BY item_id
        )
        UPDATE fact_sales fs
        SET cost_amount = round((wac.unit_cost * fs.qty)::numeric, 4),
            profit_amount = fs.net_value - round((wac.unit_cost * fs.qty)::numeric, 4),
            updated_at = NOW()
        FROM wac
        WHERE fs.item_id = wac.item_id
          AND fs.net_value > 0 AND fs.qty > 0
          AND fs.cost_amount >= fs.net_value
          AND wac.unit_cost > 0
          AND wac.unit_cost * fs.qty < fs.net_value
          AND fs.updated_at >= NOW() - make_interval(days => :d)
        """
    )
    result = await tenant_db.execute(sql, {'d': max(1, int(recent_days))})
    return int(result.rowcount or 0)

CONNECTORS = {
    'sql_connector': GenericSqlConnector(),
    'pharmacyone_sql': PharmacyOneSqlConnector(),
    'external_api': ExternalApiIngestConnector(),
    'file_import': FileImportConnector(),
}

SQL_CONNECTOR_ALIASES = ('sql_connector', 'pharmacyone_sql')
POSTGRES_TRANSIENT_SQLSTATES = {'40001', '40P01', '55P03'}


def _is_transient_postgres_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        code = (
            getattr(current, 'sqlstate', None)
            or getattr(current, 'pgcode', None)
            or getattr(current, 'code', None)
        )
        if str(code or '').upper() in POSTGRES_TRANSIENT_SQLSTATES:
            return True
        text = f'{current.__class__.__name__} {current}'.lower()
        if (
            'deadlock detected' in text
            or 'could not serialize access' in text
            or 'serialization failure' in text
            or 'lock not available' in text
        ):
            return True
        current = current.__cause__ or current.__context__
    if isinstance(exc, DBAPIError):
        return _is_transient_postgres_error(exc.orig)
    return False


async def _load_connection_for_connector(
    control_db: AsyncSession,
    *,
    tenant_id: int,
    connector_type: str,
) -> TenantConnection | None:
    connector_value = str(connector_type or '').strip().lower()
    if connector_value in SQL_CONNECTOR_ALIASES:
        rows = (
            await control_db.execute(
                select(TenantConnection).where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.connector_type.in_(SQL_CONNECTOR_ALIASES),
                    TenantConnection.is_active.is_(True),
                )
            )
        ).scalars().all()
        by_type = {str(row.connector_type or '').strip().lower(): row for row in rows}
        return by_type.get(connector_value) or by_type.get('sql_connector') or by_type.get('pharmacyone_sql')

    return (
        await control_db.execute(
            select(TenantConnection).where(
                TenantConnection.tenant_id == tenant_id,
                TenantConnection.connector_type == connector_type,
                TenantConnection.is_active.is_(True),
            )
        )
    ).scalar_one_or_none()


def _resolve_source_type(connector_type: str, connection: TenantConnection | None, connector) -> str:
    if connection and (connection.source_type or '').strip():
        return str(connection.source_type).strip().lower()
    declared = str(getattr(connector, 'source_type', '') or '').strip().lower()
    if declared in {'sql', 'api', 'file'}:
        return declared
    connector_key = connector_type.lower()
    if 'api' in connector_key:
        return 'api'
    if 'file' in connector_key or 'csv' in connector_key or 'excel' in connector_key or 'sftp' in connector_key:
        return 'file'
    return 'sql'


def _default_supported_streams_for_connector(connector, connector_type: str) -> list[OperationalIngestStream]:
    declared = list(getattr(connector, 'supported_streams', ()) or ())
    normalized = normalize_stream_values([str(v) for v in declared])
    if normalized:
        return normalized
    if connector_type == 'external_api':
        return list(ALL_OPERATIONAL_STREAMS)
    return list(ALL_OPERATIONAL_STREAMS)


def _resolve_supported_streams(
    connection: TenantConnection | None,
    connector,
    connector_type: str,
) -> list[OperationalIngestStream]:
    if connection and isinstance(connection.supported_streams, list):
        configured = normalize_stream_values([str(v) for v in connection.supported_streams])
        if configured:
            return configured
    return _default_supported_streams_for_connector(connector, connector_type)


def _resolve_enabled_streams(
    connection: TenantConnection | None,
    supported_streams: list[OperationalIngestStream],
) -> list[OperationalIngestStream]:
    if connection and isinstance(connection.enabled_streams, list):
        configured = normalize_stream_values([str(v) for v in connection.enabled_streams])
        if configured:
            return [stream for stream in configured if stream in supported_streams]
    return list(supported_streams)


def _as_datetime(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _as_doc_date(raw: Any) -> datetime.date:
    if isinstance(raw, datetime):
        return raw.date()
    if hasattr(raw, 'year') and hasattr(raw, 'month') and hasattr(raw, 'day'):
        return raw
    if isinstance(raw, str) and raw.strip():
        return datetime.fromisoformat(raw).date()
    return datetime.utcnow().date()


def _as_optional_doc_date(raw: Any):
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date_cls):
        return raw
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return None
        for candidate in (txt, txt.replace(' ', 'T')):
            try:
                return datetime.fromisoformat(candidate).date()
            except ValueError:
                continue
        for fmt in ('%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(txt, fmt).date()
            except ValueError:
                continue
    return None


def _update_incremental_state(last_ts: datetime | None, last_id: str | None, incremental_val: Any) -> tuple[datetime | None, str | None]:
    parsed_ts = _as_datetime(incremental_val)
    if parsed_ts is not None:
        if last_ts is None or parsed_ts > last_ts:
            return parsed_ts, last_id
        return last_ts, last_id

    if incremental_val is not None:
        new_id = str(incremental_val)
        if last_id is None or new_id > last_id:
            return last_ts, new_id
    return last_ts, last_id


def _as_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _as_optional_float(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _as_optional_int(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return int(raw)
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return None
        try:
            return int(float(txt))
        except ValueError:
            return None
    return None


def _as_optional_bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if not normalized:
            return None
        if normalized in {'1', 'true', 'yes', 'y', 'on', 'ναι'}:
            return True
        if normalized in {'0', 'false', 'no', 'n', 'off', 'οχι', 'όχι'}:
            return False
    return None


def _as_optional_text(raw: Any, max_len: int) -> str | None:
    if raw is None:
        return None
    txt = str(raw).strip()
    if not txt:
        return None
    return txt[:max_len]


def _as_optional_softone_text(raw: Any, max_len: int) -> str | None:
    txt = _as_optional_text(raw, max_len)
    if txt is None:
        return None
    if txt.strip().lower() in {'0', '-', 'null', 'n/a', 'na'}:
        return None
    return txt


def _as_optional_softone_text_list(raw: Any, max_len: int) -> str | None:
    txt = _as_optional_text(raw, max_len * 4)
    if txt is None:
        return None
    parts: list[str] = []
    seen: set[str] = set()
    for token in str(txt).replace('\n', ',').replace(';', ',').split(','):
        cleaned = _as_optional_softone_text(token, max_len)
        if cleaned is None:
            continue
        normalized = cleaned.strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        parts.append(cleaned)
    if not parts:
        return None
    joined = ', '.join(parts)
    return joined[: max_len * 4]


def _normalize_cashflow_subcategory(value: str | None) -> str:
    raw = str(value or '').strip().lower()
    if not raw:
        return ''
    raw = raw.replace('-', '_').replace(' ', '_')
    aliases = {
        'customer_collection': 'customer_collections',
        'debtor_collection': 'customer_collections',
        'debtor_collections': 'customer_collections',
        'other_collection': 'customer_collections',
        'other_collections': 'customer_collections',
        'collections': 'customer_collections',
        'collection': 'customer_collections',
        'in': 'customer_collections',
        'inflow': 'customer_collections',
        'credit': 'customer_collections',
        'income': 'customer_collections',
        'customer_transfer': 'customer_transfers',
        'debtor_transfer': 'customer_transfers',
        'debtor_transfers': 'customer_transfers',
        'other_transfer': 'customer_transfers',
        'other_transfers': 'customer_transfers',
        'customer_bank_transfer': 'customer_transfers',
        'customer_wire_transfer': 'customer_transfers',
        'customer_wire': 'customer_transfers',
        'supplier_payment': 'supplier_payments',
        'creditor_payment': 'supplier_payments',
        'creditor_payments': 'supplier_payments',
        'other_payment': 'supplier_payments',
        'other_payments': 'supplier_payments',
        'payments': 'supplier_payments',
        'payment': 'supplier_payments',
        'out': 'supplier_payments',
        'outflow': 'supplier_payments',
        'debit': 'supplier_payments',
        'expense': 'supplier_payments',
        'supplier_transfer': 'supplier_transfers',
        'creditor_transfer': 'supplier_transfers',
        'creditor_transfers': 'supplier_transfers',
        'other_supplier_transfer': 'supplier_transfers',
        'other_supplier_transfers': 'supplier_transfers',
        'other_transfer_out': 'supplier_transfers',
        'supplier_bank_transfer': 'supplier_transfers',
        'supplier_wire_transfer': 'supplier_transfers',
        'supplier_wire': 'supplier_transfers',
        'financial_account': 'financial_accounts',
        'account_transfer': 'financial_accounts',
        'internal_transfer': 'financial_accounts',
        'transfer': 'financial_accounts',
    }
    return aliases.get(raw, raw)


def _json_primitive(value: Any):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date_cls)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_primitive(v) for v in value]
    return str(value)


def _sanitize_payload_json(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(k): _json_primitive(v) for k, v in payload.items()}


def _build_sales_document_charge_facts(fact: dict[str, Any]) -> list[dict[str, Any]]:
    payload = fact.get('source_payload_json') if isinstance(fact.get('source_payload_json'), dict) else {}
    document_id = _as_optional_text(fact.get('document_id') or fact.get('document_no') or fact.get('external_id'), 128)
    doc_date = fact.get('doc_date')
    if not document_id or doc_date is None:
        return []

    charge_specs = [
        ('shipping', 'Έξοδα αποστολής', 'shipping_charge_net_value', 'shipping_charge_vat_value', 'shipping_charge_gross_value'),
        ('cod', 'Έξοδα αντικαταβολής', 'cod_charge_net_value', 'cod_charge_vat_value', 'cod_charge_gross_value'),
        ('gift', 'Έξοδα συσκευασίας δώρου', 'gift_charge_net_value', 'gift_charge_vat_value', 'gift_charge_gross_value'),
        ('other', 'Λοιπά έξοδα', 'other_charge_net_value', 'other_charge_vat_value', 'other_charge_gross_value'),
    ]

    rows: list[dict[str, Any]] = []
    total_net = _as_optional_float(payload.get('charge_revenue_net_value') or payload.get('shipping_expense_value'))
    known_net = 0.0
    for code, name, net_key, tax_key, gross_key in charge_specs:
        net = _as_float(payload.get(net_key))
        tax = _as_float(payload.get(tax_key))
        gross = _as_float(payload.get(gross_key))
        if not gross and (net or tax):
            gross = net + tax
        known_net += net
        if abs(net) < 0.005 and abs(tax) < 0.005 and abs(gross) < 0.005:
            continue
        rows.append(
            {
                'external_id': f'{document_id}:charge:{code}'[:160],
                'doc_date': doc_date,
                'branch_ext_id': fact.get('branch_ext_id'),
                'document_id': document_id,
                'document_no': fact.get('document_no'),
                'document_series': fact.get('document_series'),
                'document_type': fact.get('document_type'),
                'charge_code': code,
                'charge_name': name,
                'amount_net': net,
                'amount_tax': tax,
                'amount_gross': gross,
                'source_connector_id': fact.get('source_connector_id'),
                'source_payload_json': payload,
                'updated_at': fact.get('updated_at') or datetime.utcnow(),
            }
        )

    if total_net is not None and abs(float(total_net) - known_net) >= 0.005:
        residual_net = float(total_net) - known_net
        other_row = next((row for row in rows if row.get('charge_code') == 'other'), None)
        if other_row is not None:
            other_row['amount_net'] = float(other_row.get('amount_net') or 0) + residual_net
            other_row['amount_gross'] = float(other_row.get('amount_gross') or 0) + residual_net
        else:
            rows.append(
                {
                    'external_id': f'{document_id}:charge:other'[:160],
                    'doc_date': doc_date,
                    'branch_ext_id': fact.get('branch_ext_id'),
                    'document_id': document_id,
                    'document_no': fact.get('document_no'),
                    'document_series': fact.get('document_series'),
                    'document_type': fact.get('document_type'),
                    'charge_code': 'other',
                    'charge_name': 'Λοιπά έξοδα',
                    'amount_net': residual_net,
                    'amount_tax': 0.0,
                    'amount_gross': residual_net,
                    'source_connector_id': fact.get('source_connector_id'),
                    'source_payload_json': payload,
                    'updated_at': fact.get('updated_at') or datetime.utcnow(),
                }
            )

    return _dedup_facts_by_external_id(rows)


def _normalized_text_for_match(raw: Any) -> str:
    return str(raw or '').strip().lower()


def _inventory_financial_hint(document_type: str | None, document_series: str | None) -> bool | None:
    txt = _normalized_text_for_match(f'{document_type or ""} {document_series or ""}')
    if not txt:
        return None

    has_invoice = any(token in txt for token in ('τιμολ', 'τιμολογ', 'invoice'))
    has_delivery = any(token in txt for token in ('δελτι', 'delivery', 'dispatch'))

    if has_invoice and has_delivery:
        return True
    if has_delivery and not has_invoice:
        return False
    return None


def _row_getter(row: dict[str, Any], field_mapping: dict[str, str] | None = None):
    lowered = {str(k).lower(): v for k, v in row.items()}
    compact = {str(k).lower().replace('_', ''): v for k, v in row.items()}
    mapping: dict[str, str] = {}
    if isinstance(field_mapping, dict):
        for canonical, source in field_mapping.items():
            c = str(canonical or '').strip().lower()
            s = str(source or '').strip()
            if c and s:
                mapping[c] = s

    def _get(*keys: str) -> Any:
        for key in keys:
            key_l = str(key).lower()
            mapped = mapping.get(key_l)
            if mapped:
                if mapped in row and row.get(mapped) is not None:
                    return row.get(mapped)
                mapped_l = mapped.lower()
                val = lowered.get(mapped_l)
                if val is not None:
                    return val
                val = compact.get(mapped_l.replace('_', ''))
                if val is not None:
                    return val
            if key in row and row.get(key) is not None:
                return row.get(key)
            val = lowered.get(key_l)
            if val is not None:
                return val
            val = compact.get(key_l.replace('_', ''))
            if val is not None:
                return val
        return None

    return _get


def _expected_company_id(context: ConnectorContext) -> str | None:
    params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
    auth_config = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
    company = (
        params.get('company_id')
        or params.get('company')
        or params.get('COMPANY')
        or auth_config.get('company')
        or auth_config.get('COMPANY')
    )
    return _as_optional_text(company, 64)


def _company_mismatch(row: dict[str, Any], fact: dict[str, Any], context: ConnectorContext) -> bool:
    expected = _expected_company_id(context)
    if not expected:
        return False

    get = _row_getter(row)
    row_company = _as_optional_text(
        get('company_id', 'company', 'COMPANY', 'company_code', 'legal_entity_id', 'organization_id'),
        64,
    )
    if row_company and row_company != expected:
        return True

    branch_ext = _as_optional_text(fact.get('branch_ext_id'), 64)
    if branch_ext and ':' in branch_ext:
        branch_company = branch_ext.split(':', 1)[0].strip()
        if branch_company and branch_company != expected:
            return True

    return False


def _scope_branch_to_company(branch_ext: Any, company_id: Any) -> Any:
    """Qualify a branch identifier with its company, as every stream must.

    Branch codes are only unique within a company, so the canonical form is
    "<company>:<branch>". Most streams already emit it that way, but one
    querypack sent a bare BRANCH and the SoftOne JS bridge did the same whenever
    COMPANY_ID was absent. Each variant created a second dim_branches row per
    store ('1000' next to '1001:1000'), which split every branch in the UI and
    mis-attributed its turnover. Normalising here means no single producer can
    reintroduce the split.
    """
    branch = _as_optional_text(branch_ext, 64)
    if not branch:
        return branch_ext
    company = _as_optional_text(company_id, 64)
    if not company or ':' in branch:
        return branch
    return f'{company}:{branch}'[:64]


def _upsert_sales_stmt(fact: dict):
    ins = insert(FactSales).values(**fact)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'updated_at': ins.excluded.updated_at,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'warehouse_id': ins.excluded.warehouse_id,
            'warehouse_ext_id': ins.excluded.warehouse_ext_id,
            'brand_ext_id': ins.excluded.brand_ext_id,
            'category_ext_id': ins.excluded.category_ext_id,
            'group_ext_id': ins.excluded.group_ext_id,
            'item_id': ins.excluded.item_id,
            'customer_id': ins.excluded.customer_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'document_id': ins.excluded.document_id,
            'document_no': ins.excluded.document_no,
            'document_series': ins.excluded.document_series,
            'document_type': ins.excluded.document_type,
            'document_status': ins.excluded.document_status,
            'eshop_code': ins.excluded.eshop_code,
            'customer_code': ins.excluded.customer_code,
            'customer_name': ins.excluded.customer_name,
            'payment_method': ins.excluded.payment_method,
            'shipping_method': ins.excluded.shipping_method,
            'reason': ins.excluded.reason,
            'origin_ref': ins.excluded.origin_ref,
            'destination_ref': ins.excluded.destination_ref,
            'delivery_address': ins.excluded.delivery_address,
            'delivery_zip': ins.excluded.delivery_zip,
            'delivery_city': ins.excluded.delivery_city,
            'delivery_area': ins.excluded.delivery_area,
            'movement_type': ins.excluded.movement_type,
            'carrier_name': ins.excluded.carrier_name,
            'transport_medium': ins.excluded.transport_medium,
            'transport_no': ins.excluded.transport_no,
            'route_name': ins.excluded.route_name,
            'loading_date': ins.excluded.loading_date,
            'delivery_date': ins.excluded.delivery_date,
            'notes': ins.excluded.notes,
            'notes_2': ins.excluded.notes_2,
            'source_created_at': ins.excluded.source_created_at,
            'source_created_by': ins.excluded.source_created_by,
            'source_updated_at': ins.excluded.source_updated_at,
            'source_updated_by': ins.excluded.source_updated_by,
            'line_no': ins.excluded.line_no,
            'qty_executed': ins.excluded.qty_executed,
            'unit_price': ins.excluded.unit_price,
            'discount_pct': ins.excluded.discount_pct,
            'discount_amount': ins.excluded.discount_amount,
            'vat_amount': ins.excluded.vat_amount,
            'source_payload_json': ins.excluded.source_payload_json,
            'item_code': ins.excluded.item_code,
            'qty': ins.excluded.qty,
            'net_value': ins.excluded.net_value,
            'gross_value': ins.excluded.gross_value,
            'cost_amount': ins.excluded.cost_amount,
            'profit_amount': ins.excluded.profit_amount,
        },
    )


def _upsert_sales_document_charges_stmt(facts: list[dict]):
    if not facts:
        return None
    ins = insert(FactSalesDocumentCharge).values(facts)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'document_id': ins.excluded.document_id,
            'document_no': ins.excluded.document_no,
            'document_series': ins.excluded.document_series,
            'document_type': ins.excluded.document_type,
            'charge_code': ins.excluded.charge_code,
            'charge_name': ins.excluded.charge_name,
            'amount_net': ins.excluded.amount_net,
            'amount_tax': ins.excluded.amount_tax,
            'amount_gross': ins.excluded.amount_gross,
            'source_connector_id': ins.excluded.source_connector_id,
            'source_payload_json': ins.excluded.source_payload_json,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_purchases_stmt(fact: dict):
    ins = insert(FactPurchases).values(**fact)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'updated_at': ins.excluded.updated_at,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'warehouse_id': ins.excluded.warehouse_id,
            'warehouse_ext_id': ins.excluded.warehouse_ext_id,
            'supplier_id': ins.excluded.supplier_id,
            'supplier_ext_id': ins.excluded.supplier_ext_id,
            'brand_ext_id': ins.excluded.brand_ext_id,
            'category_ext_id': ins.excluded.category_ext_id,
            'group_ext_id': ins.excluded.group_ext_id,
            'document_id': ins.excluded.document_id,
            'document_no': ins.excluded.document_no,
            'document_series': ins.excluded.document_series,
            'document_type': ins.excluded.document_type,
            'source_module_id': ins.excluded.source_module_id,
            'redirect_module_id': ins.excluded.redirect_module_id,
            'source_entity_id': ins.excluded.source_entity_id,
            'object_id': ins.excluded.object_id,
            'source_payload_json': ins.excluded.source_payload_json,
            'item_id': ins.excluded.item_id,
            'item_code': ins.excluded.item_code,
            'channel_ext_id': ins.excluded.channel_ext_id,
            'channel_name': ins.excluded.channel_name,
            'qty': ins.excluded.qty,
            'discount1_pct': ins.excluded.discount1_pct,
            'discount2_pct': ins.excluded.discount2_pct,
            'discount3_pct': ins.excluded.discount3_pct,
            'discount1_amount': ins.excluded.discount1_amount,
            'discount2_amount': ins.excluded.discount2_amount,
            'discount3_amount': ins.excluded.discount3_amount,
            'discount_pct': ins.excluded.discount_pct,
            'discount_amount': ins.excluded.discount_amount,
            'net_value': ins.excluded.net_value,
            'cost_amount': ins.excluded.cost_amount,
        },
    )


def _upsert_inventory_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'doc_date': fact.get('doc_date'),
        'event_id': fact.get('event_id'),
        'branch_id': fact.get('branch_id'),
        'branch_ext_id': fact.get('branch_ext_id'),
        'item_id': fact.get('item_id'),
        'item_code': fact.get('item_code'),
        'warehouse_id': fact.get('warehouse_id'),
        'warehouse_ext_id': fact.get('warehouse_ext_id'),
        'document_id': fact.get('document_id'),
        'document_no': fact.get('document_no'),
        'document_series': fact.get('document_series'),
        'document_type': fact.get('document_type'),
        'movement_type': fact.get('movement_type'),
        'source_module_id': fact.get('source_module_id'),
        'redirect_module_id': fact.get('redirect_module_id'),
        'source_entity_id': fact.get('source_entity_id'),
        'object_id': fact.get('object_id'),
        'source_payload_json': fact.get('source_payload_json'),
        'source_connector_id': fact.get('source_connector_id'),
        'qty_on_hand': fact.get('qty_on_hand') or 0,
        'qty_reserved': fact.get('qty_reserved') or 0,
        'qty_expected': fact.get('qty_expected') or 0,
        'qty_available': fact.get('qty_available') or 0,
        'cost_amount': fact.get('cost_amount') or 0,
        'value_amount': fact.get('value_amount') or 0,
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactInventory).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'event_id': ins.excluded.event_id,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'item_id': ins.excluded.item_id,
            'item_code': ins.excluded.item_code,
            'warehouse_id': ins.excluded.warehouse_id,
            'warehouse_ext_id': ins.excluded.warehouse_ext_id,
            'document_id': ins.excluded.document_id,
            'document_no': ins.excluded.document_no,
            'document_series': ins.excluded.document_series,
            'document_type': ins.excluded.document_type,
            'movement_type': ins.excluded.movement_type,
            'source_module_id': ins.excluded.source_module_id,
            'redirect_module_id': ins.excluded.redirect_module_id,
            'source_entity_id': ins.excluded.source_entity_id,
            'object_id': ins.excluded.object_id,
            'source_payload_json': ins.excluded.source_payload_json,
            'source_connector_id': ins.excluded.source_connector_id,
            'qty_on_hand': ins.excluded.qty_on_hand,
            'qty_reserved': ins.excluded.qty_reserved,
            'qty_expected': ins.excluded.qty_expected,
            'qty_available': ins.excluded.qty_available,
            'cost_amount': ins.excluded.cost_amount,
            'value_amount': ins.excluded.value_amount,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_supplier_order_stmt(fact: dict):
    valid_columns = set(FactSupplierOrder.__table__.columns.keys())
    payload = {k: v for k, v in fact.items() if k in valid_columns}
    ins = insert(FactSupplierOrder).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'event_id': ins.excluded.event_id,
            'doc_date': ins.excluded.doc_date,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'supplier_id': ins.excluded.supplier_id,
            'supplier_ext_id': ins.excluded.supplier_ext_id,
            'supplier_name': ins.excluded.supplier_name,
            'supplier_afm': ins.excluded.supplier_afm,
            'item_id': ins.excluded.item_id,
            'item_code': ins.excluded.item_code,
            'item_name': ins.excluded.item_name,
            'document_id': ins.excluded.document_id,
            'document_no': ins.excluded.document_no,
            'document_series': ins.excluded.document_series,
            'document_series_name': ins.excluded.document_series_name,
            'document_behavior_code': ins.excluded.document_behavior_code,
            'order_qty': ins.excluded.order_qty,
            'covered_qty': ins.excluded.covered_qty,
            'cancelled_qty': ins.excluded.cancelled_qty,
            'line_value': ins.excluded.line_value,
            'has_transformation': ins.excluded.has_transformation,
            'order_status': ins.excluded.order_status,
            'source_payload_json': ins.excluded.source_payload_json,
            'source_connector_id': ins.excluded.source_connector_id,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_cashflow_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'transaction_id': fact.get('transaction_id'),
        'doc_date': fact.get('doc_date'),
        'transaction_date': fact.get('transaction_date'),
        'branch_id': fact.get('branch_id'),
        'entry_type': fact.get('entry_type') or 'unknown',
        'transaction_type': fact.get('transaction_type'),
        'subcategory': fact.get('subcategory'),
        'account_id': fact.get('account_id'),
        'source_connector_id': fact.get('source_connector_id'),
        'counterparty_type': fact.get('counterparty_type'),
        'counterparty_id': fact.get('counterparty_id'),
        'amount': fact.get('amount') or 0,
        'currency': fact.get('currency') or 'EUR',
        'reference_no': fact.get('reference_no'),
        'notes': fact.get('notes'),
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactCashflow).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'transaction_id': ins.excluded.transaction_id,
            'transaction_date': ins.excluded.transaction_date,
            'branch_id': ins.excluded.branch_id,
            'entry_type': ins.excluded.entry_type,
            'transaction_type': ins.excluded.transaction_type,
            'subcategory': ins.excluded.subcategory,
            'account_id': ins.excluded.account_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'counterparty_type': ins.excluded.counterparty_type,
            'counterparty_id': ins.excluded.counterparty_id,
            'amount': ins.excluded.amount,
            'currency': ins.excluded.currency,
            'reference_no': ins.excluded.reference_no,
            'notes': ins.excluded.notes,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_supplier_balance_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'supplier_id': fact.get('supplier_id'),
        'supplier_ext_id': fact.get('supplier_ext_id'),
        'supplier_afm': fact.get('supplier_afm'),
        'balance_date': fact.get('balance_date'),
        'branch_id': fact.get('branch_id'),
        'branch_ext_id': fact.get('branch_ext_id'),
        'source_connector_id': fact.get('source_connector_id'),
        'open_balance': fact.get('open_balance') or 0,
        'overdue_balance': fact.get('overdue_balance') or 0,
        'aging_bucket_0_30': fact.get('aging_bucket_0_30') or 0,
        'aging_bucket_31_60': fact.get('aging_bucket_31_60') or 0,
        'aging_bucket_61_90': fact.get('aging_bucket_61_90') or 0,
        'aging_bucket_90_plus': fact.get('aging_bucket_90_plus') or 0,
        'last_payment_date': fact.get('last_payment_date'),
        'trend_vs_previous': fact.get('trend_vs_previous'),
        'currency': fact.get('currency') or 'EUR',
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactSupplierBalance).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'supplier_id': ins.excluded.supplier_id,
            'supplier_ext_id': ins.excluded.supplier_ext_id,
            'supplier_afm': ins.excluded.supplier_afm,
            'balance_date': ins.excluded.balance_date,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'open_balance': ins.excluded.open_balance,
            'overdue_balance': ins.excluded.overdue_balance,
            'aging_bucket_0_30': ins.excluded.aging_bucket_0_30,
            'aging_bucket_31_60': ins.excluded.aging_bucket_31_60,
            'aging_bucket_61_90': ins.excluded.aging_bucket_61_90,
            'aging_bucket_90_plus': ins.excluded.aging_bucket_90_plus,
            'last_payment_date': ins.excluded.last_payment_date,
            'trend_vs_previous': ins.excluded.trend_vs_previous,
            'currency': ins.excluded.currency,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_customer_balance_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'customer_id': fact.get('customer_id'),
        'customer_ext_id': fact.get('customer_ext_id'),
        'customer_name': fact.get('customer_name'),
        'customer_afm': fact.get('customer_afm'),
        'balance_date': fact.get('balance_date'),
        'branch_id': fact.get('branch_id'),
        'branch_ext_id': fact.get('branch_ext_id'),
        'source_connector_id': fact.get('source_connector_id'),
        'open_balance': fact.get('open_balance') or 0,
        'overdue_balance': fact.get('overdue_balance') or 0,
        'aging_bucket_0_30': fact.get('aging_bucket_0_30') or 0,
        'aging_bucket_31_60': fact.get('aging_bucket_31_60') or 0,
        'aging_bucket_61_90': fact.get('aging_bucket_61_90') or 0,
        'aging_bucket_90_plus': fact.get('aging_bucket_90_plus') or 0,
        'last_collection_date': fact.get('last_collection_date'),
        'trend_vs_previous': fact.get('trend_vs_previous'),
        'currency': fact.get('currency') or 'EUR',
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactCustomerBalance).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'customer_ext_id': ins.excluded.customer_ext_id,
            'customer_name': ins.excluded.customer_name,
            'customer_afm': ins.excluded.customer_afm,
            'customer_id': ins.excluded.customer_id,
            'balance_date': ins.excluded.balance_date,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'open_balance': ins.excluded.open_balance,
            'overdue_balance': ins.excluded.overdue_balance,
            'aging_bucket_0_30': ins.excluded.aging_bucket_0_30,
            'aging_bucket_31_60': ins.excluded.aging_bucket_31_60,
            'aging_bucket_61_90': ins.excluded.aging_bucket_61_90,
            'aging_bucket_90_plus': ins.excluded.aging_bucket_90_plus,
            'last_collection_date': ins.excluded.last_collection_date,
            'trend_vs_previous': ins.excluded.trend_vs_previous,
            'currency': ins.excluded.currency,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _upsert_expense_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'expense_date': fact.get('expense_date'),
        'posting_date': fact.get('posting_date'),
        'branch_id': fact.get('branch_id'),
        'branch_ext_id': fact.get('branch_ext_id'),
        'location_id': fact.get('location_id'),
        'category_id': fact.get('category_id'),
        'expense_category_code': fact.get('expense_category_code'),
        'supplier_id': fact.get('supplier_id'),
        'supplier_ext_id': fact.get('supplier_ext_id'),
        'account_id': fact.get('account_id'),
        'account_ext_id': fact.get('account_ext_id'),
        'document_type': fact.get('document_type'),
        'document_no': fact.get('document_no'),
        'cost_center': fact.get('cost_center'),
        'payment_status': fact.get('payment_status'),
        'due_date': fact.get('due_date'),
        'currency_code': fact.get('currency_code') or 'EUR',
        'amount_net': fact.get('amount_net') or 0,
        'amount_tax': fact.get('amount_tax') or 0,
        'amount_gross': fact.get('amount_gross') or 0,
        'source_connector_id': fact.get('source_connector_id'),
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactExpense).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'expense_date': ins.excluded.expense_date,
            'posting_date': ins.excluded.posting_date,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'location_id': ins.excluded.location_id,
            'category_id': ins.excluded.category_id,
            'expense_category_code': ins.excluded.expense_category_code,
            'supplier_id': ins.excluded.supplier_id,
            'supplier_ext_id': ins.excluded.supplier_ext_id,
            'account_id': ins.excluded.account_id,
            'account_ext_id': ins.excluded.account_ext_id,
            'document_type': ins.excluded.document_type,
            'document_no': ins.excluded.document_no,
            'cost_center': ins.excluded.cost_center,
            'payment_status': ins.excluded.payment_status,
            'due_date': ins.excluded.due_date,
            'currency_code': ins.excluded.currency_code,
            'amount_net': ins.excluded.amount_net,
            'amount_tax': ins.excluded.amount_tax,
            'amount_gross': ins.excluded.amount_gross,
            'source_connector_id': ins.excluded.source_connector_id,
            'updated_at': ins.excluded.updated_at,
        },
    )


def _dedup_facts_by_external_id(facts: list[dict]) -> list[dict]:
    # PostgreSQL raises CardinalityViolationError when ON CONFLICT DO UPDATE sees two rows
    # with the same external_id in the same INSERT batch. Keep only the last occurrence
    # (rows come sorted by updated_at ASC, so the last is the freshest).
    seen: dict[str, dict] = {}
    none_id_facts: list[dict] = []
    for fact in facts:
        eid = fact.get('external_id')
        if eid is not None:
            seen[eid] = fact
        else:
            none_id_facts.append(fact)
    deduped_count = len(seen) + len(none_id_facts)
    if deduped_count == len(facts):
        return facts
    return list(seen.values()) + none_id_facts


_FACT_MODEL_BY_ENTITY = {
    'sales': FactSales,
    'purchases': FactPurchases,
    'inventory': FactInventory,
    'expenses': FactExpense,
    'supplier_orders': FactSupplierOrder,
}


def _identity_value(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _duplicate_protection_settings_from_flags(flags: Any) -> dict[str, Any]:
    source = {}
    if isinstance(flags, dict):
        cfg = flags.get('duplicate_protection')
        if isinstance(cfg, dict):
            source = cfg
    enabled_raw = source.get('enabled', True)
    if isinstance(enabled_raw, str):
        enabled = enabled_raw.strip().lower() not in {'0', 'false', 'no', 'off', 'disabled'}
    else:
        enabled = bool(enabled_raw)
    mode_raw = str(source.get('mode') or 'natural_key').strip().lower()
    return {
        'enabled': enabled,
        'mode': mode_raw if mode_raw in {'event_id', 'natural_key'} else 'natural_key',
    }


def _fact_identity_keys(entity: str, fact: dict, *, mode: str = 'natural_key') -> list[tuple[Any, ...]]:
    keys: list[tuple[Any, ...]] = []
    if entity in {'sales', 'purchases', 'inventory', 'expenses', 'supplier_orders'}:
        event_id = _identity_value(fact.get('event_id')) or _identity_value(fact.get('external_id'))
        if event_id:
            keys.append(('event', entity, event_id))

    # Expense documents can contain legitimate repeated lines with the same
    # category, supplier and amount. SoftOne MTRLINES/external_id is the stable
    # line identity, so natural-key collapse would incorrectly drop those lines.
    if entity == 'expenses' and keys:
        return keys

    if mode == 'event_id':
        return keys

    if entity == 'sales':
        doc_date = fact.get('doc_date')
        line_no = fact.get('line_no')
        document_ref = _identity_value(fact.get('document_id')) or _identity_value(fact.get('document_no'))
        item_code = _identity_value(fact.get('item_code'))
        if doc_date and document_ref and item_code and line_no is not None:
            keys.append(
                (
                    'sales_doc_line',
                    entity,
                    str(doc_date),
                    document_ref,
                    _identity_value(fact.get('document_series')),
                    _identity_value(fact.get('branch_ext_id')),
                    item_code,
                    str(line_no),
                )
            )
    elif entity == 'expenses':
        expense_date = fact.get('expense_date')
        document_no = _identity_value(fact.get('document_no'))
        if expense_date and document_no:
            keys.append(
                (
                    'expense_doc',
                    entity,
                    str(expense_date),
                    document_no,
                    _identity_value(fact.get('document_type')),
                    _identity_value(fact.get('branch_ext_id')),
                    _identity_value(fact.get('supplier_ext_id')),
                    _identity_value(fact.get('expense_category_code')),
                    _identity_value(fact.get('amount_net')),
                    _identity_value(fact.get('amount_tax')),
                    _identity_value(fact.get('amount_gross')),
                )
            )
    return keys


async def _find_existing_external_id_by_identity(
    tenant_db: AsyncSession,
    entity: str,
    fact: dict,
    key: tuple[Any, ...],
) -> str | None:
    model = _FACT_MODEL_BY_ENTITY.get(entity)
    if model is None:
        return None

    key_type = str(key[0])
    stmt = None
    if key_type == 'event' and hasattr(model, 'event_id'):
        stmt = select(model.external_id).where(model.event_id == str(key[2])).limit(1)
    elif key_type == 'sales_doc_line' and entity == 'sales':
        doc_ref_clause = (
            FactSales.document_id == fact.get('document_id')
            if fact.get('document_id')
            else FactSales.document_no == fact.get('document_no')
        )
        stmt = (
            select(FactSales.external_id)
            .where(
                FactSales.doc_date == fact.get('doc_date'),
                FactSales.line_no == fact.get('line_no'),
                FactSales.item_code == fact.get('item_code'),
                FactSales.branch_ext_id == fact.get('branch_ext_id'),
                FactSales.document_series == fact.get('document_series'),
                doc_ref_clause,
            )
            .limit(1)
        )
    elif key_type == 'expense_doc' and entity == 'expenses':
        stmt = (
            select(FactExpense.external_id)
            .where(
                FactExpense.expense_date == fact.get('expense_date'),
                FactExpense.document_no == fact.get('document_no'),
                FactExpense.document_type == fact.get('document_type'),
                FactExpense.branch_ext_id == fact.get('branch_ext_id'),
                FactExpense.supplier_ext_id == fact.get('supplier_ext_id'),
                FactExpense.expense_category_code == fact.get('expense_category_code'),
                FactExpense.amount_net == (fact.get('amount_net') or 0),
                FactExpense.amount_tax == (fact.get('amount_tax') or 0),
                FactExpense.amount_gross == (fact.get('amount_gross') or 0),
            )
            .limit(1)
        )

    if stmt is None:
        return None
    found = (await tenant_db.execute(stmt)).scalar_one_or_none()
    return _identity_value(found) or None


async def _canonicalize_fact_external_id(
    tenant_db: AsyncSession,
    entity: str,
    fact: dict,
    identity_cache: dict[tuple[Any, ...], str],
    *,
    mode: str = 'natural_key',
) -> None:
    current_external_id = _identity_value(fact.get('external_id'))
    keys = _fact_identity_keys(entity, fact, mode=mode)
    if not current_external_id or not keys:
        return

    for key in keys:
        cached = identity_cache.get(key)
        if cached:
            if cached != current_external_id:
                logger.warning(
                    'ingest_duplicate_in_batch_resolved entity=%s document_no=%s cached_external_id=%s new_external_id=%s',
                    entity,
                    fact.get('document_no'),
                    cached,
                    current_external_id,
                )
                fact['external_id'] = cached
                fact['event_id'] = fact.get('event_id') or cached
            return

    for key in keys:
        existing = await _find_existing_external_id_by_identity(tenant_db, entity, fact, key)
        if existing:
            if existing != current_external_id:
                logger.warning(
                    'ingest_duplicate_natural_key_resolved entity=%s document_no=%s document_series=%s old_external_id=%s new_external_id=%s',
                    entity,
                    fact.get('document_no'),
                    fact.get('document_series') or fact.get('document_type'),
                    existing,
                    current_external_id,
                )
                fact['external_id'] = existing
                fact['event_id'] = fact.get('event_id') or existing
            for cache_key in keys:
                identity_cache[cache_key] = existing
            return

    for key in keys:
        identity_cache[key] = current_external_id


def _build_fact_upsert_stmt(entity: str, facts: list[dict]):
    if not facts:
        return None
    facts = _dedup_facts_by_external_id(facts)

    if entity == 'sales':
        ins = insert(FactSales).values(facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'doc_date': ins.excluded.doc_date,
                'updated_at': ins.excluded.updated_at,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'warehouse_id': ins.excluded.warehouse_id,
                'warehouse_ext_id': ins.excluded.warehouse_ext_id,
                'brand_ext_id': ins.excluded.brand_ext_id,
                'category_ext_id': ins.excluded.category_ext_id,
                'group_ext_id': ins.excluded.group_ext_id,
                'item_id': ins.excluded.item_id,
                'customer_id': ins.excluded.customer_id,
                'source_connector_id': ins.excluded.source_connector_id,
                'document_id': ins.excluded.document_id,
                'document_no': ins.excluded.document_no,
                'document_series': ins.excluded.document_series,
                'document_type': ins.excluded.document_type,
                'document_status': ins.excluded.document_status,
                'eshop_code': ins.excluded.eshop_code,
                'customer_code': ins.excluded.customer_code,
                'customer_name': ins.excluded.customer_name,
                'payment_method': ins.excluded.payment_method,
                'shipping_method': ins.excluded.shipping_method,
                'reason': ins.excluded.reason,
                'origin_ref': ins.excluded.origin_ref,
                'destination_ref': ins.excluded.destination_ref,
                'delivery_address': ins.excluded.delivery_address,
                'delivery_zip': ins.excluded.delivery_zip,
                'delivery_city': ins.excluded.delivery_city,
                'delivery_area': ins.excluded.delivery_area,
                'movement_type': ins.excluded.movement_type,
                'carrier_name': ins.excluded.carrier_name,
                'transport_medium': ins.excluded.transport_medium,
                'transport_no': ins.excluded.transport_no,
                'route_name': ins.excluded.route_name,
                'loading_date': ins.excluded.loading_date,
                'delivery_date': ins.excluded.delivery_date,
                'notes': ins.excluded.notes,
                'notes_2': ins.excluded.notes_2,
                'source_created_at': ins.excluded.source_created_at,
                'source_created_by': ins.excluded.source_created_by,
                'source_updated_at': ins.excluded.source_updated_at,
                'source_updated_by': ins.excluded.source_updated_by,
                'line_no': ins.excluded.line_no,
                'qty_executed': ins.excluded.qty_executed,
                'unit_price': ins.excluded.unit_price,
                'discount_pct': ins.excluded.discount_pct,
                'discount_amount': ins.excluded.discount_amount,
                'vat_amount': ins.excluded.vat_amount,
                'source_payload_json': ins.excluded.source_payload_json,
                'behavior_code': ins.excluded.behavior_code,
                'item_code': ins.excluded.item_code,
                'channel_ext_id': ins.excluded.channel_ext_id,
                'channel_name': ins.excluded.channel_name,
                'qty': ins.excluded.qty,
                'net_value': ins.excluded.net_value,
                'gross_value': ins.excluded.gross_value,
                'cost_amount': ins.excluded.cost_amount,
                'profit_amount': ins.excluded.profit_amount,
            },
        )

    if entity == 'purchases':
        ins = insert(FactPurchases).values(facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'doc_date': ins.excluded.doc_date,
                'updated_at': ins.excluded.updated_at,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'source_connector_id': ins.excluded.source_connector_id,
                'warehouse_id': ins.excluded.warehouse_id,
                'warehouse_ext_id': ins.excluded.warehouse_ext_id,
                'supplier_id': ins.excluded.supplier_id,
                'supplier_ext_id': ins.excluded.supplier_ext_id,
                'brand_ext_id': ins.excluded.brand_ext_id,
                'category_ext_id': ins.excluded.category_ext_id,
                'group_ext_id': ins.excluded.group_ext_id,
                'document_id': ins.excluded.document_id,
                'document_no': ins.excluded.document_no,
                'document_series': ins.excluded.document_series,
                'document_type': ins.excluded.document_type,
                'source_module_id': ins.excluded.source_module_id,
                'redirect_module_id': ins.excluded.redirect_module_id,
                'source_entity_id': ins.excluded.source_entity_id,
                'object_id': ins.excluded.object_id,
                'source_payload_json': ins.excluded.source_payload_json,
                'item_id': ins.excluded.item_id,
                'item_code': ins.excluded.item_code,
                'qty': ins.excluded.qty,
                'discount1_pct': ins.excluded.discount1_pct,
                'discount2_pct': ins.excluded.discount2_pct,
                'discount3_pct': ins.excluded.discount3_pct,
                'discount1_amount': ins.excluded.discount1_amount,
                'discount2_amount': ins.excluded.discount2_amount,
                'discount3_amount': ins.excluded.discount3_amount,
                'discount_pct': ins.excluded.discount_pct,
                'discount_amount': ins.excluded.discount_amount,
                'net_value': ins.excluded.net_value,
                'cost_amount': ins.excluded.cost_amount,
            },
        )

    if entity == 'inventory':
        ins = insert(FactInventory).values(facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'doc_date': ins.excluded.doc_date,
                'event_id': ins.excluded.event_id,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'item_id': ins.excluded.item_id,
                'item_code': ins.excluded.item_code,
                'warehouse_id': ins.excluded.warehouse_id,
                'warehouse_ext_id': ins.excluded.warehouse_ext_id,
                'document_id': ins.excluded.document_id,
                'document_no': ins.excluded.document_no,
                'document_series': ins.excluded.document_series,
                'document_type': ins.excluded.document_type,
                'movement_type': ins.excluded.movement_type,
                'source_module_id': ins.excluded.source_module_id,
                'redirect_module_id': ins.excluded.redirect_module_id,
                'source_entity_id': ins.excluded.source_entity_id,
                'object_id': ins.excluded.object_id,
                'source_payload_json': ins.excluded.source_payload_json,
                'source_connector_id': ins.excluded.source_connector_id,
                'qty_on_hand': ins.excluded.qty_on_hand,
                'qty_reserved': ins.excluded.qty_reserved,
                'qty_expected': ins.excluded.qty_expected,
                'qty_available': ins.excluded.qty_available,
                'cost_amount': ins.excluded.cost_amount,
                'value_amount': ins.excluded.value_amount,
                'updated_at': ins.excluded.updated_at,
            },
        )

    if entity == 'cashflows':
        valid_columns = set(FactCashflow.__table__.columns.keys())
        sanitized_facts = [{k: v for k, v in fact.items() if k in valid_columns} for fact in facts]
        ins = insert(FactCashflow).values(sanitized_facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'doc_date': ins.excluded.doc_date,
                'transaction_id': ins.excluded.transaction_id,
                'transaction_date': ins.excluded.transaction_date,
                'branch_id': ins.excluded.branch_id,
                'entry_type': ins.excluded.entry_type,
                'transaction_type': ins.excluded.transaction_type,
                'subcategory': ins.excluded.subcategory,
                'account_id': ins.excluded.account_id,
                'source_connector_id': ins.excluded.source_connector_id,
                'counterparty_type': ins.excluded.counterparty_type,
                'counterparty_id': ins.excluded.counterparty_id,
                'amount': ins.excluded.amount,
                'currency': ins.excluded.currency,
                'reference_no': ins.excluded.reference_no,
                'notes': ins.excluded.notes,
                'updated_at': ins.excluded.updated_at,
            },
        )

    if entity == 'supplier_balances':
        ins = insert(FactSupplierBalance).values(facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'supplier_id': ins.excluded.supplier_id,
                'supplier_ext_id': ins.excluded.supplier_ext_id,
                'supplier_afm': ins.excluded.supplier_afm,
                'balance_date': ins.excluded.balance_date,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'source_connector_id': ins.excluded.source_connector_id,
                'open_balance': ins.excluded.open_balance,
                'overdue_balance': ins.excluded.overdue_balance,
                'aging_bucket_0_30': ins.excluded.aging_bucket_0_30,
                'aging_bucket_31_60': ins.excluded.aging_bucket_31_60,
                'aging_bucket_61_90': ins.excluded.aging_bucket_61_90,
                'aging_bucket_90_plus': ins.excluded.aging_bucket_90_plus,
                'last_payment_date': ins.excluded.last_payment_date,
                'trend_vs_previous': ins.excluded.trend_vs_previous,
                'currency': ins.excluded.currency,
                'updated_at': ins.excluded.updated_at,
            },
        )

    if entity == 'supplier_orders':
        valid_columns = set(FactSupplierOrder.__table__.columns.keys())
        sanitized_facts = [{k: v for k, v in fact.items() if k in valid_columns} for fact in facts]
        ins = insert(FactSupplierOrder).values(sanitized_facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'event_id': ins.excluded.event_id,
                'doc_date': ins.excluded.doc_date,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'supplier_id': ins.excluded.supplier_id,
                'supplier_ext_id': ins.excluded.supplier_ext_id,
                'supplier_name': ins.excluded.supplier_name,
                'supplier_afm': ins.excluded.supplier_afm,
                'item_id': ins.excluded.item_id,
                'item_code': ins.excluded.item_code,
                'item_name': ins.excluded.item_name,
                'document_id': ins.excluded.document_id,
                'document_no': ins.excluded.document_no,
                'document_series': ins.excluded.document_series,
                'document_series_name': ins.excluded.document_series_name,
                'document_behavior_code': ins.excluded.document_behavior_code,
                'order_qty': ins.excluded.order_qty,
                'covered_qty': ins.excluded.covered_qty,
                'cancelled_qty': ins.excluded.cancelled_qty,
                'line_value': ins.excluded.line_value,
                'has_transformation': ins.excluded.has_transformation,
                'order_status': ins.excluded.order_status,
                'source_payload_json': ins.excluded.source_payload_json,
                'source_connector_id': ins.excluded.source_connector_id,
                'updated_at': ins.excluded.updated_at,
            },
        )

    if entity == 'expenses':
        valid_columns = set(FactExpense.__table__.columns.keys())
        sanitized_facts = [{k: v for k, v in fact.items() if k in valid_columns} for fact in facts]
        ins = insert(FactExpense).values(sanitized_facts)
        return ins.on_conflict_do_update(
            index_elements=['external_id'],
            set_={
                'expense_date': ins.excluded.expense_date,
                'posting_date': ins.excluded.posting_date,
                'branch_id': ins.excluded.branch_id,
                'branch_ext_id': ins.excluded.branch_ext_id,
                'location_id': ins.excluded.location_id,
                'category_id': ins.excluded.category_id,
                'expense_category_code': ins.excluded.expense_category_code,
                'supplier_id': ins.excluded.supplier_id,
                'supplier_ext_id': ins.excluded.supplier_ext_id,
                'account_id': ins.excluded.account_id,
                'account_ext_id': ins.excluded.account_ext_id,
                'document_type': ins.excluded.document_type,
                'document_no': ins.excluded.document_no,
                'cost_center': ins.excluded.cost_center,
                'payment_status': ins.excluded.payment_status,
                'due_date': ins.excluded.due_date,
                'currency_code': ins.excluded.currency_code,
                'amount_net': ins.excluded.amount_net,
                'amount_tax': ins.excluded.amount_tax,
                'amount_gross': ins.excluded.amount_gross,
                'source_connector_id': ins.excluded.source_connector_id,
                'updated_at': ins.excluded.updated_at,
            },
        )

    ins = insert(FactCustomerBalance).values(facts)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'customer_ext_id': ins.excluded.customer_ext_id,
            'customer_name': ins.excluded.customer_name,
            'customer_afm': ins.excluded.customer_afm,
            'customer_id': ins.excluded.customer_id,
            'balance_date': ins.excluded.balance_date,
            'branch_id': ins.excluded.branch_id,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'open_balance': ins.excluded.open_balance,
            'overdue_balance': ins.excluded.overdue_balance,
            'aging_bucket_0_30': ins.excluded.aging_bucket_0_30,
            'aging_bucket_31_60': ins.excluded.aging_bucket_31_60,
            'aging_bucket_61_90': ins.excluded.aging_bucket_61_90,
            'aging_bucket_90_plus': ins.excluded.aging_bucket_90_plus,
            'last_collection_date': ins.excluded.last_collection_date,
            'trend_vs_previous': ins.excluded.trend_vs_previous,
            'currency': ins.excluded.currency,
            'updated_at': ins.excluded.updated_at,
        },
    )


STAGING_MODEL_BY_STREAM = {
    'sales_documents': StgSalesDocument,
    'purchase_documents': StgPurchaseDocument,
    'inventory_documents': StgInventoryDocument,
    'cash_transactions': StgCashTransaction,
    'supplier_balances': StgSupplierBalance,
    'customer_balances': StgCustomerBalance,
    'operating_expenses': StgExpenseDocument,
    'supplier_orders': StgPurchaseDocument,
}


async def _stage_ingest_row(
    tenant_db: AsyncSession,
    *,
    connector_type: str,
    stream: OperationalIngestStream,
    row: dict[str, Any],
) -> int:
    model = STAGING_MODEL_BY_STREAM.get(stream)
    if model is None:
        raise RuntimeError(f'Missing staging table mapping for stream: {stream}')
    payload_json = _sanitize_payload_json(row)
    event_id = _as_optional_text(row.get('event_id'), 128)
    external_id = _as_optional_text(row.get('external_id'), 128) or event_id
    doc_date = _as_optional_doc_date(
        row.get('doc_date') or row.get('document_date') or row.get('balance_date') or row.get('expense_date')
    )

    stmt = insert(model).values(
        connector_type=connector_type,
        stream=stream,
        event_id=event_id,
        external_id=external_id,
        doc_date=doc_date,
        transform_status='loaded',
        error_message=None,
        source_payload_json=payload_json,
        ingested_at=datetime.utcnow(),
        processed_at=None,
    )
    result = await tenant_db.execute(stmt.returning(model.id))
    return result.scalar_one_or_none()


async def _mark_staging_row_processed(tenant_db: AsyncSession, *, stream: OperationalIngestStream, stage_id: int | None) -> None:
    if stage_id is None:
        return
    model = STAGING_MODEL_BY_STREAM.get(stream)
    if model is None:
        return
    await tenant_db.execute(
        model.__table__.update()
        .where(model.id == stage_id)
        .values(
            transform_status='processed',
            error_message=None,
            processed_at=datetime.utcnow(),
        )
    )


async def _mark_staging_row_failed(
    tenant_db: AsyncSession,
    *,
    stream: OperationalIngestStream,
    stage_id: int | None,
    error_message: str,
) -> None:
    if stage_id is None:
        return
    model = STAGING_MODEL_BY_STREAM.get(stream)
    if model is None:
        return
    await tenant_db.execute(
        model.__table__.update()
        .where(model.id == stage_id)
        .values(
            transform_status='failed',
            error_message=str(error_message)[:2000],
            processed_at=datetime.utcnow(),
        )
    )


def _upsert_dim_stmt(model, external_id: str, name: str):
    ins = insert(model).values(external_id=external_id, name=name)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={'updated_at': datetime.utcnow(), 'name': ins.excluded.name},
    )


def _upsert_branch_dim_stmt(external_id: str, name: str, company_id: str | None = None):
    ins = insert(DimBranch).values(
        external_id=external_id,
        branch_code=external_id,
        name=name,
        branch_name=name,
        company_id=company_id,
    )
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'updated_at': datetime.utcnow(),
            'name': ins.excluded.name,
            'branch_name': func.coalesce(ins.excluded.branch_name, DimBranch.branch_name, DimBranch.name),
            'branch_code': func.coalesce(DimBranch.branch_code, ins.excluded.branch_code),
            'company_id': func.coalesce(DimBranch.company_id, ins.excluded.company_id),
        },
    )


def _upsert_account_dim_stmt(
    external_id: str,
    name: str,
    account_type: str | None = None,
    currency: str | None = None,
):
    ins = insert(DimAccount).values(
        external_id=external_id,
        name=name,
        account_type=account_type,
        currency=currency,
    )
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'updated_at': datetime.utcnow(),
            'name': func.coalesce(ins.excluded.name, DimAccount.name),
            'account_type': func.coalesce(ins.excluded.account_type, DimAccount.account_type),
            'currency': func.coalesce(ins.excluded.currency, DimAccount.currency),
        },
    )


def _upsert_document_type_dim_stmt(external_id: str, name: str, stream: str | None = None):
    ins = insert(DimDocumentType).values(
        external_id=external_id,
        name=name,
        stream=stream,
    )
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'updated_at': datetime.utcnow(),
            'name': func.coalesce(ins.excluded.name, DimDocumentType.name),
            'stream': func.coalesce(ins.excluded.stream, DimDocumentType.stream),
        },
    )


def _seen_once(cache: dict[str, set[str]] | None, bucket: str, value: str | None) -> bool:
    if cache is None:
        return True
    key = str(value or '').strip()
    if not key:
        return False
    values = cache.setdefault(bucket, set())
    if key in values:
        return False
    values.add(key)
    return True


async def _upsert_dims_from_row(
    tenant_db: AsyncSession,
    entity: str,
    row: dict[str, Any],
    fact: dict,
    *,
    dim_seen_cache: dict[str, set[str]] | None = None,
    dim_id_cache: dict[tuple[str, str], Any] | None = None,
) -> None:
    get = _row_getter(row)

    branch = fact.get('branch_ext_id')
    if branch and _seen_once(dim_seen_cache, 'branch', str(branch)):
        branch_name = str(get('branch_name') or branch)[:255]
        company_id = _as_optional_text(get('company_id', 'company_code', 'legal_entity_id', 'organization_id'), 64)
        await tenant_db.execute(_upsert_branch_dim_stmt(str(branch)[:64], branch_name, company_id=company_id))
    warehouse = fact.get('warehouse_ext_id')
    if warehouse and _seen_once(dim_seen_cache, 'warehouse', str(warehouse)):
        await tenant_db.execute(
            _upsert_dim_stmt(DimWarehouse, str(warehouse)[:64], str(get('warehouse_name') or warehouse)[:255])
        )
    brand = fact.get('brand_ext_id')
    if brand and _seen_once(dim_seen_cache, 'brand', str(brand)):
        await tenant_db.execute(_upsert_dim_stmt(DimBrand, str(brand)[:64], str(get('brand_name') or brand)[:255]))
    category = fact.get('category_ext_id')
    if category and _seen_once(dim_seen_cache, 'category', str(category)):
        await tenant_db.execute(
            _upsert_dim_stmt(DimCategory, str(category)[:64], str(get('category_name') or category)[:255])
        )
    expense_category = _as_optional_text(
        get('expense_category_code', 'category_code', 'opex_category_code', 'expense_category_id'),
        128,
    ) or _as_optional_text(fact.get('expense_category_code'), 128)
    if expense_category and _seen_once(dim_seen_cache, 'expense_category', str(expense_category)):
        category_name = _as_optional_text(
            get('expense_category_name', 'category_name', 'opex_category_name'),
            255,
        ) or expense_category
        ins = insert(DimExpenseCategory).values(
            external_id=_as_optional_text(get('expense_category_external_id', 'expense_category_id'), 128),
            category_code=expense_category,
            category_name=category_name,
            level_no=_as_optional_int(get('expense_category_level', 'category_level', 'level_no')) or 1,
            classification=_as_optional_text(get('classification', 'category_classification'), 32) or 'opex',
            gl_account_code=_as_optional_text(get('gl_account_code', 'ledger_account_code', 'account_code'), 128),
            is_active=_as_optional_bool(get('is_active', 'category_active', 'active')) is not False,
        )
        await tenant_db.execute(
            ins.on_conflict_do_update(
                index_elements=['category_code'],
                set_={
                    'updated_at': datetime.utcnow(),
                    'category_name': func.coalesce(ins.excluded.category_name, DimExpenseCategory.category_name),
                    'classification': func.coalesce(ins.excluded.classification, DimExpenseCategory.classification),
                    'gl_account_code': func.coalesce(ins.excluded.gl_account_code, DimExpenseCategory.gl_account_code),
                    'is_active': func.coalesce(ins.excluded.is_active, DimExpenseCategory.is_active),
                },
            )
        )
    group = fact.get('group_ext_id')
    if group and _seen_once(dim_seen_cache, 'group', str(group)):
        await tenant_db.execute(_upsert_dim_stmt(DimGroup, str(group)[:64], str(get('group_name') or group)[:255]))
    item = fact.get('item_code')
    if item and _seen_once(dim_seen_cache, 'item', str(item)):
        item_external_id = str(item)[:128]
        item_barcode = _as_optional_softone_text(
            get('barcode', 'item_barcode', 'barcode_code', 'ean', 'ean13', 'code1', 'item_code1'),
            128,
        )
        item_sku = _as_optional_softone_text(get('sku', 'item_sku', 'stock_code', 'item_sku_code', 'code'), 128)
        item_name = _as_optional_text(
            get(
                'item_name',
                'name',
                'item_description',
                'description',
                'item_desc',
                'item_descr',
                'item_title',
                'product_name',
                'product_description',
                'mtrl_name',
                'mtrl_descr',
                'mtrl_description',
                'title',
                'descr',
            ),
            255,
        )

        item_values = {
            'external_id': item_external_id,
            'sku': item_sku or item_external_id,
            'barcode': item_barcode,
            'alternate_barcodes': _as_optional_softone_text_list(
                get('alternate_barcodes', 'alt_barcodes', 'secondary_barcodes', 'substitute_codes'),
                128,
            ),
            'softone_sotype': _as_optional_int(get('softone_sotype', 'item_sotype', 'sotype', 'mtrl_sotype')),
            'name': item_name,
            'main_unit': _as_optional_text(get('main_unit', 'unit', 'uom', 'unit_main', 'primary_unit'), 64),
            'vat_rate': _as_optional_float(get('vat_rate', 'vat_percent', 'vat', 'fpa', 'tax_rate')),
            'vat_label': _as_optional_text(get('vat_label', 'vat_name', 'vat_text', 'fpa_label'), 64),
            'use_batch': _as_optional_bool(get('use_batch', 'batch_tracking', 'lot_tracking', 'use_lot')),
            'commercial_category': _as_optional_softone_text(get('commercial_category', 'commercial_category_name'), 255),
            'category_1': _as_optional_softone_text(get('category_1', 'category1', 'category_l1', 'category_level1'), 255),
            'category_2': _as_optional_softone_text(get('category_2', 'category2', 'category_l2', 'category_level2'), 255),
            'category_3': _as_optional_softone_text(get('category_3', 'category3', 'category_l3', 'category_level3'), 255),
            'manufacturer_code': _as_optional_softone_text(
                get('manufacturer_code', 'manufacturer_ext_id', 'manufacturer_external_id', 'mtrmanfctr'),
                128,
            ),
            'manufacturer_name': _as_optional_softone_text(
                get('manufacturer_name', 'manufacturer', 'manufacturer_label', 'mtrmanfctr_name'),
                255,
            ),
            'preferred_supplier_ext_id': _as_optional_text(
                get('preferred_supplier_ext_id', 'preferred_supplier_code', 'mtrsup', 'default_supplier_ext_id'),
                128,
            ),
            'preferred_supplier_name': _as_optional_softone_text(
                get('preferred_supplier_name', 'preferred_supplier', 'mtrsup_supplier_name', 'default_supplier_name'),
                255,
            ),
            'model_name': _as_optional_text(get('model', 'model_name'), 255),
            'business_unit_name': _as_optional_text(get('business_unit', 'business_unit_name', 'businessunit'), 255),
            'unit2': _as_optional_text(get('unit2', 'second_unit', 'secondary_unit'), 64),
            'purchase_unit': _as_optional_text(get('purchase_unit', 'buy_unit'), 64),
            'sales_unit': _as_optional_text(get('sales_unit', 'sell_unit'), 64),
            'rel_2_to_1': _as_optional_float(get('rel_2_to_1', 'ratio_2_to_1', 'relation_2_to_1')),
            'rel_purchase_to_1': _as_optional_float(get('rel_purchase_to_1', 'purchase_ratio', 'relation_purchase_to_1')),
            'rel_sale_to_1': _as_optional_float(get('rel_sale_to_1', 'sales_ratio', 'relation_sale_to_1')),
            'strict_rel_2_to_1': _as_optional_bool(get('strict_rel_2_to_1', 'strict_ratio_2_to_1')),
            'strict_purchase_rel': _as_optional_bool(get('strict_purchase_rel', 'strict_purchase_ratio')),
            'strict_sale_rel': _as_optional_bool(get('strict_sale_rel', 'strict_sales_ratio')),
            'abc_category': _as_optional_text(
                get(
                    'abc_category',
                    'abc_class',
                    'abc',
                    'abc_movement',
                    'abc_velocity',
                    'market_abc_category',
                    'market_abc_class',
                    'market_abc',
                    'market_speed_abc',
                    'new_item_abc',
                ),
                32,
            ),
            'manual_order_category': _as_optional_softone_text(
                get(
                    'manual_order_category',
                    'manual_item_category',
                    'order_category',
                    'softone_utbl04',
                    'utbl04',
                    'iteextra_utbl04',
                    'item_utbl04',
                ),
                128,
            ),
            'commercial_status': _as_optional_softone_text(
                get(
                    'commercial_status',
                    'item_commercial_status',
                    'softone_utbl05',
                    'utbl05',
                    'iteextra_utbl05',
                    'item_utbl05',
                ),
                128,
            ),
            'replenishment_status_1': _as_optional_softone_text(
                get('replenishment_status_1', 'repl_status_1', 'fnr_status_1', 'status_1', 'item_status_1'),
                64,
            ),
            'replenishment_status_2': _as_optional_softone_text(
                get('replenishment_status_2', 'repl_status_2', 'fnr_status_2', 'status_2', 'item_status_2'),
                128,
            ),
            'min_stock': _as_optional_float(
                get('min_stock', 'minimum_stock', 'repl_min_stock', 'replenishment_min_stock')
            )
            or 1,
            'replenishment_moq': _as_optional_float(
                get('replenishment_moq', 'repl_moq', 'reorder_moq', 'order_moq', 'item_moq')
            )
            or 1,
            'vendor_moq': _as_optional_float(get('vendor_moq', 'supplier_moq', 'vendor_min_order_qty')) or 1,
            'current_purchase_price': _as_optional_float(
                get('current_purchase_price', 'purchase_price', 'last_purchase_price', 'final_purchase_price')
            ),
            'image_url': _as_optional_text(
                get(
                    'image_url',
                    'photo_url',
                    'item_image_url',
                    'product_image_url',
                    'thumbnail_url',
                    'image_link',
                    'photo_link',
                ),
                1024,
            ),
            'discount_pct': _as_optional_float(get('discount_pct', 'discount_percent', 'discount', 'item_discount_pct')),
            'is_active_source': _as_optional_bool(
                get(
                    'is_active_source',
                    'item_is_active_source',
                    'is_active',
                    'active',
                    'enabled',
                    'item_active',
                    'isactive',
                )
            ),
        }
        if group:
            group_external_id = str(group)[:64]
            group_cache_key = ('DimGroup', group_external_id)
            if dim_id_cache is not None and group_cache_key in dim_id_cache:
                item_values['group_id'] = dim_id_cache[group_cache_key]
            else:
                group_id = await tenant_db.scalar(
                    select(DimGroup.id).where(DimGroup.external_id == group_external_id).limit(1)
                )
                if dim_id_cache is not None:
                    dim_id_cache[group_cache_key] = group_id
                item_values['group_id'] = group_id

        ins = insert(DimItem).values(
            **item_values,
        )

        def _sf(excluded_col, current_col):
            # item_master (entity == 'items') is the authoritative full-catalog pull: take the
            # SoftOne value verbatim so a CHANGED *or CLEARED* status/category propagates to BI.
            # Other entities send partial rows -> keep the existing value when a field is absent.
            return excluded_col if entity == 'items' else func.coalesce(excluded_col, current_col)

        # Every ingest row used to rewrite its dim_items row even when nothing
        # changed: 355M updates against 193K items, of which only 0.004% could go
        # HOT (the table carries 13 indexes and updated_at always moved). The
        # heap bloated to 3 GB for 82 MB of live data. The comparison below is
        # applied as the ON CONFLICT ... WHERE predicate so unchanged rows are
        # skipped entirely.
        item_update_values = {
                    'sku': func.coalesce(ins.excluded.sku, DimItem.sku),
                    'barcode': func.coalesce(ins.excluded.barcode, DimItem.barcode),
                    'alternate_barcodes': func.coalesce(ins.excluded.alternate_barcodes, DimItem.alternate_barcodes),
                    'softone_sotype': func.coalesce(ins.excluded.softone_sotype, DimItem.softone_sotype),
                    'name': func.coalesce(
                        func.nullif(
                            func.nullif(func.btrim(ins.excluded.name), ''),
                            ins.excluded.external_id,
                        ),
                        DimItem.name,
                    ),
                    'main_unit': func.coalesce(ins.excluded.main_unit, DimItem.main_unit),
                    'vat_rate': func.coalesce(ins.excluded.vat_rate, DimItem.vat_rate),
                    'vat_label': func.coalesce(ins.excluded.vat_label, DimItem.vat_label),
                    'use_batch': func.coalesce(ins.excluded.use_batch, DimItem.use_batch),
                    'commercial_category': _sf(ins.excluded.commercial_category, DimItem.commercial_category),
                    'category_1': _sf(ins.excluded.category_1, DimItem.category_1),
                    'category_2': _sf(ins.excluded.category_2, DimItem.category_2),
                    'category_3': _sf(ins.excluded.category_3, DimItem.category_3),
                    'manufacturer_code': func.coalesce(ins.excluded.manufacturer_code, DimItem.manufacturer_code),
                    'manufacturer_name': func.coalesce(ins.excluded.manufacturer_name, DimItem.manufacturer_name),
                    'preferred_supplier_ext_id': func.coalesce(ins.excluded.preferred_supplier_ext_id, DimItem.preferred_supplier_ext_id),
                    'preferred_supplier_name': func.coalesce(ins.excluded.preferred_supplier_name, DimItem.preferred_supplier_name),
                    'model_name': func.coalesce(ins.excluded.model_name, DimItem.model_name),
                    'business_unit_name': func.coalesce(ins.excluded.business_unit_name, DimItem.business_unit_name),
                    'unit2': func.coalesce(ins.excluded.unit2, DimItem.unit2),
                    'purchase_unit': func.coalesce(ins.excluded.purchase_unit, DimItem.purchase_unit),
                    'sales_unit': func.coalesce(ins.excluded.sales_unit, DimItem.sales_unit),
                    'rel_2_to_1': func.coalesce(ins.excluded.rel_2_to_1, DimItem.rel_2_to_1),
                    'rel_purchase_to_1': func.coalesce(ins.excluded.rel_purchase_to_1, DimItem.rel_purchase_to_1),
                    'rel_sale_to_1': func.coalesce(ins.excluded.rel_sale_to_1, DimItem.rel_sale_to_1),
                    'strict_rel_2_to_1': func.coalesce(ins.excluded.strict_rel_2_to_1, DimItem.strict_rel_2_to_1),
                    'strict_purchase_rel': func.coalesce(ins.excluded.strict_purchase_rel, DimItem.strict_purchase_rel),
                    'strict_sale_rel': func.coalesce(ins.excluded.strict_sale_rel, DimItem.strict_sale_rel),
                    'abc_category': func.coalesce(ins.excluded.abc_category, DimItem.abc_category),
                    'manual_order_category': _sf(
                        ins.excluded.manual_order_category,
                        DimItem.manual_order_category,
                    ),
                    'commercial_status': _sf(
                        ins.excluded.commercial_status,
                        DimItem.commercial_status,
                    ),
                    'replenishment_status_1': func.coalesce(
                        ins.excluded.replenishment_status_1,
                        DimItem.replenishment_status_1,
                    ),
                    'replenishment_status_2': func.coalesce(
                        ins.excluded.replenishment_status_2,
                        DimItem.replenishment_status_2,
                    ),
                    'min_stock': func.coalesce(ins.excluded.min_stock, DimItem.min_stock),
                    'replenishment_moq': func.coalesce(ins.excluded.replenishment_moq, DimItem.replenishment_moq),
                    'vendor_moq': func.coalesce(ins.excluded.vendor_moq, DimItem.vendor_moq),
                    'current_purchase_price': func.coalesce(
                        ins.excluded.current_purchase_price,
                        DimItem.current_purchase_price,
                    ),
                    'image_url': func.coalesce(ins.excluded.image_url, DimItem.image_url),
                    'discount_pct': func.coalesce(ins.excluded.discount_pct, DimItem.discount_pct),
                    'is_active_source': func.coalesce(ins.excluded.is_active_source, DimItem.is_active_source),
                    'group_id': func.coalesce(ins.excluded.group_id, DimItem.group_id),
        }
        item_table = DimItem.__table__
        await tenant_db.execute(
            ins.on_conflict_do_update(
                index_elements=['external_id'],
                set_={'updated_at': datetime.utcnow(), **item_update_values},
                where=tuple_(*(item_table.c[col] for col in item_update_values)).is_distinct_from(
                    tuple_(*item_update_values.values())
                ),
            )
        )
    if entity in {'purchases', 'supplier_balances', 'expenses', 'supplier_orders'}:
        supplier = fact.get('supplier_ext_id')
        if supplier and _seen_once(dim_seen_cache, 'supplier', str(supplier)):
            await tenant_db.execute(
                _upsert_dim_stmt(DimSupplier, str(supplier)[:64], str(get('supplier_name') or supplier)[:255])
            )

    customer_external = _as_optional_text(
        get('customer_id', 'customer_code', 'customer_ext_id', 'customer_external_id'),
        128,
    ) or _as_optional_text(fact.get('customer_code'), 128)
    customer_name = _as_optional_text(get('customer_name', 'customer', 'customer_title'), 255)
    if customer_external or customer_name:
        customer_id = str(customer_external or customer_name)[:128]
        if _seen_once(dim_seen_cache, 'customer', customer_id):
            customer_label = str(customer_name or customer_external)[:255]
            ins = insert(DimCustomer).values(
                external_id=customer_id,
                customer_code=_as_optional_text(get('customer_code', 'customer_id'), 128) or customer_external,
                name=customer_label,
            )
            await tenant_db.execute(
                ins.on_conflict_do_update(
                    index_elements=['external_id'],
                    set_={
                        'updated_at': datetime.utcnow(),
                        'name': func.coalesce(ins.excluded.name, DimCustomer.name),
                        'customer_code': func.coalesce(ins.excluded.customer_code, DimCustomer.customer_code),
                    },
                )
            )

    document_type = _as_optional_text(
        get('document_type', 'doc_type', 'doctype', 'series', 'series_label'),
        128,
    ) or _as_optional_text(fact.get('document_type'), 128)
    if document_type and _seen_once(dim_seen_cache, 'document_type', document_type):
        document_stream = _as_optional_text(get('stream', 'module', 'entity'), 64) or entity
        await tenant_db.execute(
            _upsert_document_type_dim_stmt(
                external_id=document_type[:128],
                name=document_type[:255],
                stream=document_stream[:64] if document_stream else None,
            )
        )

    payment_method = _as_optional_text(
        get('payment_method', 'payment_method_name', 'payment_mode', 'pay_method'),
        128,
    ) or _as_optional_text(fact.get('payment_method'), 128)
    if payment_method and _seen_once(dim_seen_cache, 'payment_method', payment_method):
        await tenant_db.execute(
            _upsert_dim_stmt(DimPaymentMethod, payment_method[:128], payment_method[:255])
        )

    account_external = _as_optional_text(
        get('account_id', 'account_code', 'account_external_id', 'cash_account_id'),
        128,
    ) or _as_optional_text(fact.get('account_id'), 128)
    account_name = _as_optional_text(get('account_name', 'cash_account_name', 'account_title'), 255)
    if account_external and _seen_once(dim_seen_cache, 'account', account_external):
        await tenant_db.execute(
            _upsert_account_dim_stmt(
                external_id=account_external[:128],
                name=(account_name or account_external)[:255],
                account_type=_as_optional_text(get('account_type', 'ledger_type'), 64),
                currency=_as_optional_text(get('currency', 'account_currency'), 3),
            )
        )


async def _load_sync_state(
    tenant_db: AsyncSession,
    connector_key: str,
    legacy_connector_key: str | None = None,
) -> SyncState:
    state = (await tenant_db.execute(select(SyncState).where(SyncState.connector_type == connector_key))).scalar_one_or_none()
    if state:
        return state
    if legacy_connector_key:
        legacy_state = (
            await tenant_db.execute(select(SyncState).where(SyncState.connector_type == legacy_connector_key))
        ).scalar_one_or_none()
        if legacy_state:
            legacy_state.connector_type = connector_key
            await tenant_db.flush()
            return legacy_state
    state = SyncState(connector_type=connector_key)
    tenant_db.add(state)
    await tenant_db.flush()
    return state


async def _resolve_dim_id(tenant_db: AsyncSession, model, external_id: str | None):
    if not external_id:
        return None
    return (
        await tenant_db.execute(select(model.id).where(model.external_id == str(external_id)))
    ).scalar_one_or_none()


async def _resolve_expense_category_id(tenant_db: AsyncSession, category_code: str | None):
    if not category_code:
        return None
    key = str(category_code).strip()
    if not key:
        return None
    return (
        await tenant_db.execute(
            select(DimExpenseCategory.id).where(
                (DimExpenseCategory.category_code == key) | (DimExpenseCategory.external_id == key)
            )
        )
    ).scalar_one_or_none()


async def _build_context(
    control_db: AsyncSession,
    *,
    tenant_id: int,
    connector_type: str,
    connector,
    connection: TenantConnection | None,
) -> ConnectorContext:
    source_connection_string = None
    source_type = _resolve_source_type(connector_type, connection, connector)
    connection_parameters = (
        dict(connection.connection_parameters)
        if connection and isinstance(connection.connection_parameters, dict)
        else {}
    )
    if connection and connection.enc_payload:
        if source_type == 'api' or connector_type == 'external_api':
            connection_parameters['auth_secret'] = decrypt_json_secret(connection.enc_payload)
        else:
            secret = decrypt_sqlserver_secret(connection.enc_payload)
            source_connection_string = build_odbc_connection_string(secret)

    supported_streams = _resolve_supported_streams(connection, connector, connector_type)
    enabled_streams = _resolve_enabled_streams(connection, supported_streams)
    configured_query_mapping = (
        connection.stream_query_mapping if connection and isinstance(connection.stream_query_mapping, dict) else {}
    )
    configured_field_mapping = (
        connection.stream_field_mapping if connection and isinstance(connection.stream_field_mapping, dict) else {}
    )

    context = ConnectorContext(
        tenant_slug='',
        incremental_column=(connection.incremental_column if connection else 'updated_at'),
        id_column=(connection.id_column if connection else 'id'),
        date_column=(connection.date_column if connection else 'doc_date'),
        branch_column=(connection.branch_column if connection else 'branch_ext_id'),
        item_column=(connection.item_column if connection else 'item_code'),
        amount_column=(connection.amount_column if connection else 'net_value'),
        cost_column=(connection.cost_column if connection else 'cost_amount'),
        qty_column=(connection.qty_column if connection else 'qty'),
        source_type=source_type,
        supported_streams=supported_streams,
        enabled_streams=enabled_streams,
        source_connection_string=source_connection_string,
        connection_parameters=connection_parameters,
        stream_query_mapping={
            str(k): str(v)
            for k, v in configured_query_mapping.items()
            if isinstance(v, str) and str(v).strip()
        },
        stream_field_mapping={
            str(stream): {
                str(k): str(v)
                for k, v in mapping.items()
                if isinstance(k, str) and isinstance(v, str) and str(k).strip() and str(v).strip()
            }
            for stream, mapping in configured_field_mapping.items()
            if isinstance(stream, str) and isinstance(mapping, dict)
        },
        stream_file_mapping=(connection.stream_file_mapping if connection and isinstance(connection.stream_file_mapping, dict) else {}),
        stream_api_endpoint=(connection.stream_api_endpoint if connection and isinstance(connection.stream_api_endpoint, dict) else {}),
        sales_query=(connection.sales_query_template if connection else None),
        purchases_query=(connection.purchases_query_template if connection else None),
        inventory_query=(connection.inventory_query_template if connection else None),
        cashflow_query=(connection.cashflow_query_template if connection else None),
        supplier_balances_query=(connection.supplier_balances_query_template if connection else None),
        customer_balances_query=(connection.customer_balances_query_template if connection else None),
        expenses_query=(
            str(configured_query_mapping.get('operating_expenses') or '').strip()
            if isinstance(configured_query_mapping, dict)
            else None
        ) or None,
        supplier_orders_query=(
            str(configured_query_mapping.get('supplier_orders') or '').strip()
            if isinstance(configured_query_mapping, dict)
            else None
        ) or None,
        item_master_query=(
            str(configured_query_mapping.get('item_master') or '').strip()
            if isinstance(configured_query_mapping, dict)
            else None
        ) or None,
    )

    fallback_query_by_stream: dict[OperationalIngestStream, str] = {
        'sales_documents': context.stream_query_mapping.get('sales_documents') or context.sales_query or '',
        'purchase_documents': context.stream_query_mapping.get('purchase_documents') or context.purchases_query or '',
        'inventory_documents': context.stream_query_mapping.get('inventory_documents') or context.inventory_query or '',
        'item_master': context.stream_query_mapping.get('item_master') or context.item_master_query or '',
        'cash_transactions': context.stream_query_mapping.get('cash_transactions') or context.cashflow_query or '',
        'supplier_balances': context.stream_query_mapping.get('supplier_balances') or context.supplier_balances_query or '',
        'customer_balances': context.stream_query_mapping.get('customer_balances') or context.customer_balances_query or '',
        'operating_expenses': context.stream_query_mapping.get('operating_expenses') or context.expenses_query or '',
        'supplier_orders': context.stream_query_mapping.get('supplier_orders') or context.supplier_orders_query or '',
    }
    stream_enum_lookup: dict[OperationalIngestStream, OperationalStream] = {
        'sales_documents': OperationalStream.sales_documents,
        'purchase_documents': OperationalStream.purchase_documents,
        'inventory_documents': OperationalStream.inventory_documents,
        'cash_transactions': OperationalStream.cash_transactions,
        'supplier_balances': OperationalStream.supplier_balances,
        'customer_balances': OperationalStream.customer_balances,
    }
    for stream in ALL_OPERATIONAL_STREAMS:
        stream_enum = stream_enum_lookup.get(stream)
        if stream_enum is None:
            resolved = fallback_query_by_stream.get(stream, '') or ''
        else:
            resolved = await resolve_source_query_template(
                control_db,
                tenant_id=tenant_id,
                stream=stream_enum,
                fallback_query_template=fallback_query_by_stream.get(stream, ''),
            )
        if resolved.strip():
            context.stream_query_mapping[stream] = resolved

    context.sales_query = context.stream_query_mapping.get('sales_documents') or context.sales_query
    context.purchases_query = context.stream_query_mapping.get('purchase_documents') or context.purchases_query
    context.inventory_query = context.stream_query_mapping.get('inventory_documents') or context.inventory_query
    context.item_master_query = context.stream_query_mapping.get('item_master') or context.item_master_query
    context.cashflow_query = context.stream_query_mapping.get('cash_transactions') or context.cashflow_query
    context.supplier_balances_query = (
        context.stream_query_mapping.get('supplier_balances') or context.supplier_balances_query
    )
    context.customer_balances_query = (
        context.stream_query_mapping.get('customer_balances') or context.customer_balances_query
    )
    context.expenses_query = context.stream_query_mapping.get('operating_expenses') or context.expenses_query
    context.supplier_orders_query = context.stream_query_mapping.get('supplier_orders') or context.supplier_orders_query
    return context


def _build_fact(
    entity: str,
    row: dict[str, Any],
    context: ConnectorContext,
    default_prefix: str,
    stream: OperationalIngestStream,
) -> tuple[dict[str, Any], Any]:
    stream_field_mapping = {}
    if isinstance(context.stream_field_mapping, dict):
        mapped = context.stream_field_mapping.get(stream)
        if isinstance(mapped, dict):
            stream_field_mapping = mapped
    get = _row_getter(row, field_mapping=stream_field_mapping)

    incremental_val = get(context.incremental_column, 'updated_at', 'doc_date', 'document_date', 'event_id')
    updated_at = _as_datetime(incremental_val) or datetime.utcnow()

    branch_ext = _scope_branch_to_company(
        get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'),
        get('company_id', 'company_code', 'legal_entity_id', 'organization_id'),
    )
    warehouse_ext = get('warehouse_ext_id', 'warehouse_external_id', 'warehouse_code')
    brand_ext = get('brand_ext_id', 'brand_external_id', 'brand_code')
    category_ext = get('category_ext_id', 'category_external_id', 'category_code')
    group_ext = get('group_ext_id', 'group_external_id', 'group_code')
    item_code = get(context.item_column, 'item_code', 'item_external_id')
    supplier_ext = get('supplier_ext_id', 'supplier_external_id', 'supplier_code', 'entity_ext_id')

    raw_external = get('external_id', 'event_id') or f"{default_prefix}:{incremental_val}:{branch_ext}:{item_code}"
    external_id = str(raw_external)[:128]

    doc_date = _as_doc_date(get('doc_date', 'document_date', context.date_column, 'updated_at') or incremental_val)

    if entity == 'items':
        return (
            {
                'external_id': external_id,
                'event_id': str(get('event_id') or external_id)[:128],
                'doc_date': doc_date,
                'updated_at': updated_at,
                'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
                'warehouse_ext_id': str(warehouse_ext)[:64] if warehouse_ext is not None else None,
                'brand_ext_id': str(brand_ext)[:64] if brand_ext is not None else None,
                'category_ext_id': str(category_ext)[:64] if category_ext is not None else None,
                'group_ext_id': str(group_ext)[:64] if group_ext is not None else None,
                'item_code': str(item_code)[:128] if item_code is not None else None,
                'source_payload_json': _sanitize_payload_json(row),
            },
            incremental_val,
        )

    if entity in {'sales', 'purchases'}:
        qty = _as_float(get(context.qty_column, 'qty', 'quantity'))
        net = _as_float(get(context.amount_column, 'net_value', 'net_amount', 'amount'))
        cost = _as_float(get(context.cost_column, 'cost_amount', 'cost_value'))
        source_module_id = _as_optional_int(get('source_module_id', 'sosource', 'module_id', 'source_module'))
        redirect_module_id = _as_optional_int(get('redirect_module_id', 'soredir', 'redirect_module'))
        source_entity_id = _as_optional_int(get('source_entity_id', 'sodtype', 'entity_type_id'))
        object_id = _as_optional_int(get('object_id', 'refobjid', 'objectid'))
        if object_id is None and source_module_id is not None:
            object_id = int(source_module_id) + int(redirect_module_id or 0)

        base = {
            'external_id': external_id,
            'event_id': str(get('event_id') or external_id)[:128],
            'doc_date': doc_date,
            'updated_at': updated_at,
            'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
            'warehouse_ext_id': str(warehouse_ext)[:64] if warehouse_ext is not None else None,
            'brand_ext_id': str(brand_ext)[:64] if brand_ext is not None else None,
            'category_ext_id': str(category_ext)[:64] if category_ext is not None else None,
            'group_ext_id': str(group_ext)[:64] if group_ext is not None else None,
            'item_code': str(item_code)[:128] if item_code is not None else None,
            'qty': qty,
            'net_value': net,
            'cost_amount': cost,
        }
        if entity == 'sales':
            gross = _as_float(get('gross_value', 'gross_amount', context.amount_column, 'net_value', 'net_amount'))
            document_id_raw = _as_optional_text(
                get(
                    'document_id',
                    'doc_id',
                    'sale_document_id',
                    'invoice_id',
                    'voucher_id',
                    'header_id',
                    'document_external_id',
                    'document_no',
                    'document_number',
                ),
                128,
            )
            document_no = _as_optional_text(
                get('document_no', 'document_number', 'doc_no', 'voucher_no', 'invoice_no', 'reference_no'),
                128,
            )
            document_id = document_id_raw or document_no or external_id
            unit_price = _as_optional_float(get('unit_price', 'price', 'sale_price', 'item_price'))
            if unit_price is None and qty:
                unit_price = net / qty if qty else None
            discount_pct = _as_optional_float(get('discount_pct', 'discount_percent', 'disc_pct', 'line_discount_pct'))
            discount_amount = _as_optional_float(
                get('discount_amount', 'disc_amount', 'line_discount', 'discount_value')
            )
            qty_executed = _as_optional_float(get('qty_executed', 'executed_qty', 'qty_exec', 'qty_delivered'))
            vat_amount = _as_optional_float(get('vat_amount', 'tax_amount', 'fpa_amount'))

            base.update(
                {
                    'document_id': document_id,
                    'document_no': document_no or document_id,
                    'document_series': _as_optional_text(get('series', 'document_series', 'doc_series'), 128),
                    'document_type': _as_optional_text(
                        get('document_type', 'doc_type', 'type_name', 'voucher_type'),
                        128,
                    ),
                    'document_status': _as_optional_text(get('status', 'document_status', 'doc_status'), 128),
                    'eshop_code': _as_optional_text(
                        get('eshop_code', 'eshop_no', 'web_order_no', 'eshop_order_no', 'order_no'),
                        128,
                    ),
                    'customer_code': _as_optional_text(
                        get('customer_code', 'customer_id', 'customer_external_id', 'client_code', 'entity_ext_id'),
                        128,
                    ),
                    'customer_name': _as_optional_text(
                        get('customer_name', 'customer', 'client_name', 'entity_name'),
                        255,
                    ),
                    'payment_method': _as_optional_text(
                        get('payment_method', 'payment_type', 'payment_mode', 'payment_name'),
                        128,
                    ),
                    'shipping_method': _as_optional_text(
                        get('shipping_method', 'shipping_type', 'shipment_method', 'dispatch_method', 'shipment_name'),
                        128,
                    ),
                    'reason': _as_optional_text(get('reason', 'reason_text', 'cause', 'etiologia'), 255),
                    'origin_ref': _as_optional_text(
                        get('origin_ref', 'from_ref', 'from_document', 'source_doc', 'apo'),
                        128,
                    ),
                    'destination_ref': _as_optional_text(
                        get('destination_ref', 'to_ref', 'to_document', 'target_doc', 'se'),
                        128,
                    ),
                    'delivery_address': _as_optional_text(
                        get('delivery_address', 'ship_address', 'address_delivery', 'delivery_street'),
                        1024,
                    ),
                    'delivery_zip': _as_optional_text(
                        get('delivery_zip', 'delivery_postal_code', 'zip', 'postal_code', 'delivery_tk'),
                        32,
                    ),
                    'delivery_city': _as_optional_text(get('delivery_city', 'city', 'ship_city', 'delivery_city_name'), 128),
                    'delivery_area': _as_optional_text(get('delivery_area', 'area', 'region', 'delivery_region'), 128),
                    'movement_type': _as_optional_text(
                        get('movement_type', 'movement', 'dispatch_movement', 'delivery_movement', 'diakinisi'),
                        128,
                    ),
                    'carrier_name': _as_optional_text(
                        get('carrier', 'carrier_name', 'transport_company', 'metaforeas'),
                        255,
                    ),
                    'transport_medium': _as_optional_text(
                        get('transport_medium', 'transport_means', 'vehicle_type', 'metaf_meso'),
                        128,
                    ),
                    'transport_no': _as_optional_text(
                        get('transport_no', 'vehicle_no', 'transport_number', 'plate_no', 'metaf_meso_no'),
                        128,
                    ),
                    'route_name': _as_optional_text(get('route', 'route_name', 'itinerary', 'dromologio'), 255),
                    'loading_date': _as_optional_doc_date(get('loading_date', 'load_date', 'shipping_date', 'date_loading')),
                    'delivery_date': _as_optional_doc_date(
                        get('delivery_date', 'ship_date', 'eta_date', 'date_delivery')
                    ),
                    'notes': _as_optional_text(get('notes', 'remarks', 'comment', 'observation', 'paratiriseis'), 4000),
                    'notes_2': _as_optional_text(get('notes_2', 'notes2', 'comments2', 'remarks_2', 'aitiologia2'), 4000),
                    'source_created_at': _as_datetime(
                        get('created_at', 'source_created_at', 'inserted_at', 'created_datetime')
                    ),
                    'source_created_by': _as_optional_text(
                        get('created_by', 'insert_user', 'user_created', 'entry_user'),
                        128,
                    ),
                    'source_updated_at': _as_datetime(
                        get('source_updated_at', 'modified_at', 'last_update_at', 'updated_datetime')
                    )
                    or updated_at,
                    'source_updated_by': _as_optional_text(
                        get('updated_by', 'modified_by', 'user_updated', 'update_user'),
                        128,
                    ),
                    'line_no': _as_optional_int(get('line_no', 'line_number', 'line_id', 'aa')),
                    'qty_executed': qty_executed if qty_executed is not None else qty,
                    'unit_price': unit_price,
                    'discount_pct': discount_pct,
                    'discount_amount': discount_amount,
                    # Preserve VAT sign from source economics. For credits/returns, VAT can be negative.
                    'vat_amount': vat_amount if vat_amount is not None else (gross - net),
                    'channel_ext_id': _as_optional_text(get('channel_ext_id', 'channel_external_id', 'channel_code'), 64),
                    'channel_name': _as_optional_text(get('channel_name', 'channel'), 255),
                    'source_payload_json': _sanitize_payload_json(row),
                    # Denormalised out of the payload so KPI queries filter on a
                    # plain integer column instead of parsing JSON per row.
                    'behavior_code': _as_optional_int(get('source_transaction_type_id')),
                }
            )
            base['gross_value'] = gross
            # Gross profit = NET selling price - acquisition cost (both VAT-exclusive).
            # (Previously gross - cost, which over-stated margin by the VAT amount.)
            base['profit_amount'] = net - cost
        else:
            purchase_document_id = _as_optional_text(
                get(
                    'document_id',
                    'doc_id',
                    'purchase_document_id',
                    'invoice_id',
                    'voucher_id',
                    'header_id',
                    'document_external_id',
                    'document_no',
                    'document_number',
                ),
                128,
            )
            purchase_document_no = _as_optional_text(
                get('document_no', 'document_number', 'doc_no', 'voucher_no', 'invoice_no', 'reference_no'),
                128,
            )
            purchase_document_id = purchase_document_id or purchase_document_no or external_id
            discount_pct = _as_optional_float(
                get('discount_pct', 'discount_percent', 'disc_pct', 'line_discount_pct')
            )
            discount_amount = _as_optional_float(
                get('discount_amount', 'discount_value', 'disc_amount', 'line_discount', 'line_discount_amount')
            )
            no_discount_amount = _as_optional_float(
                get('no_discount_amount', 'nodscamnt', 'no_discount_value', 'gross_line_value_before_discount')
            )
            if discount_amount is None and no_discount_amount is not None:
                inferred_discount = no_discount_amount - net
                if abs(inferred_discount) > 0.004:
                    discount_amount = inferred_discount
            if discount_pct is None and discount_amount is not None and no_discount_amount:
                try:
                    discount_pct = (discount_amount / no_discount_amount) * 100
                except ZeroDivisionError:
                    discount_pct = None
            discount1_pct = _as_optional_float(get('discount1_pct', 'disc1prc', 'discount_1_pct', 'disc1_prc'))
            discount2_pct = _as_optional_float(get('discount2_pct', 'disc2prc', 'discount_2_pct', 'disc2_prc'))
            discount3_pct = _as_optional_float(get('discount3_pct', 'disc3prc', 'discount_3_pct', 'disc3_prc'))
            discount1_amount = _as_optional_float(get('discount1_amount', 'disc1val', 'discount_1_amount', 'disc1_val'))
            discount2_amount = _as_optional_float(get('discount2_amount', 'disc2val', 'discount_2_amount', 'disc2_val'))
            discount3_amount = _as_optional_float(get('discount3_amount', 'disc3val', 'discount_3_amount', 'disc3_val'))
            base.update(
                {
                    'supplier_ext_id': str(supplier_ext)[:64] if supplier_ext is not None else None,
                    'document_id': purchase_document_id,
                    'document_no': purchase_document_no or purchase_document_id,
                    'document_series': _as_optional_text(
                        get('series', 'document_series', 'doc_series', 'document_series_name'),
                        128,
                    ),
                    'document_type': _as_optional_text(
                        get('document_type', 'doc_type', 'type_name', 'voucher_type', 'document_series_name'),
                        128,
                    ),
                    'source_module_id': source_module_id,
                    'redirect_module_id': redirect_module_id,
                    'source_entity_id': source_entity_id,
                    'object_id': object_id,
                    'channel_ext_id': _as_optional_text(get('channel_ext_id', 'channel_external_id', 'channel_code'), 64),
                    'channel_name': _as_optional_text(get('channel_name', 'channel'), 255),
                    'discount1_pct': discount1_pct,
                    'discount2_pct': discount2_pct,
                    'discount3_pct': discount3_pct,
                    'discount1_amount': discount1_amount,
                    'discount2_amount': discount2_amount,
                    'discount3_amount': discount3_amount,
                    'discount_pct': discount_pct,
                    'discount_amount': discount_amount,
                    'source_payload_json': _sanitize_payload_json(row),
                }
            )
        return base, incremental_val

    if entity == 'inventory':
        source_module_id = _as_optional_int(get('source_module_id', 'sosource', 'module_id', 'source_module'))
        redirect_module_id = _as_optional_int(get('redirect_module_id', 'soredir', 'redirect_module'))
        source_entity_id = _as_optional_int(get('source_entity_id', 'sodtype', 'entity_type_id'))
        object_id = _as_optional_int(get('object_id', 'refobjid', 'objectid'))
        if object_id is None and source_module_id is not None:
            object_id = int(source_module_id) + int(redirect_module_id or 0)

        document_id = _as_optional_text(
            get(
                'document_id',
                'doc_id',
                'movement_id',
                'header_id',
                'document_external_id',
                'document_no',
                'document_number',
            ),
            128,
        )
        document_no = _as_optional_text(
            get('document_no', 'document_number', 'doc_no', 'voucher_no', 'invoice_no', 'reference_no'),
            128,
        )
        document_series = _as_optional_text(
            get('series', 'document_series', 'doc_series', 'document_series_name'),
            128,
        )
        document_type = _as_optional_text(
            get('document_type', 'doc_type', 'type_name', 'voucher_type', 'document_series_name'),
            128,
        )
        movement_type = _as_optional_text(get('movement_type', 'movement', 'movement_kind', 'transaction_type'), 64)
        event_id = _as_optional_text(get('event_id', 'movement_id', 'line_id'), 128) or external_id
        document_id = document_id or document_no or event_id

        is_financial_doc = _as_optional_bool(get('is_financial_doc', 'financial_doc', 'affects_financials'))
        if is_financial_doc is None:
            is_financial_doc = _inventory_financial_hint(document_type, document_series)

        qty_on_hand = _as_float(get('qty_on_hand', context.qty_column, 'qty', 'quantity'))
        qty_reserved = _as_float(get('qty_reserved', 'reserved_qty'))
        qty_expected = _as_float(get('qty_expected', 'expected_qty', 'on_order_qty', 'incoming_qty'))
        qty_available = _as_float(get('qty_available', 'available_qty'))
        if get('qty_available', 'available_qty') is None:
            qty_available = qty_on_hand - qty_reserved
        cost_amount = _as_float(get('cost_amount', context.cost_column, 'cost_value'))
        explicit_value_amount = get('value_amount', context.amount_column, 'net_value', 'net_amount')
        value_amount = _as_float(explicit_value_amount)
        financial_value_amount = _as_optional_float(
            get('financial_value_amount', 'value_financial', 'financial_impact_value', 'expense_value')
        )

        if financial_value_amount is not None and explicit_value_amount is None:
            value_amount = financial_value_amount
        # Zero the monetary fields only for non-financial *movement* documents (e.g. a
        # delivery note without an invoice), so they don't double-count in financials.
        # A stock *snapshot* is flagged non-financial too, but its value_amount IS the
        # stock valuation we need (stock value / dead stock / DIO) — keep it.
        if is_financial_doc is False and movement_type != 'snapshot':
            value_amount = 0.0
            cost_amount = 0.0

        return (
            {
                'external_id': external_id,
                'event_id': event_id,
                'doc_date': doc_date,
                'updated_at': updated_at,
                'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
                'warehouse_ext_id': str(warehouse_ext)[:64] if warehouse_ext is not None else None,
                'item_code': str(item_code)[:128] if item_code is not None else None,
                'branch_id': None,
                'warehouse_id': None,
                'item_id': None,
                'document_id': document_id,
                'document_no': document_no or document_id,
                'document_series': document_series,
                'document_type': document_type,
                'movement_type': movement_type,
                'source_module_id': source_module_id,
                'redirect_module_id': redirect_module_id,
                'source_entity_id': source_entity_id,
                'object_id': object_id,
                'source_payload_json': _sanitize_payload_json(row),
                'qty_on_hand': qty_on_hand,
                'qty_reserved': qty_reserved,
                'qty_expected': qty_expected,
                'qty_available': qty_available,
                'cost_amount': cost_amount,
                'value_amount': value_amount,
            },
            incremental_val,
        )

    if entity == 'supplier_balances':
        supplier_id = _as_optional_text(
            get('supplier_id', 'supplier_ext_id', 'supplier_external_id', 'supplier_code', 'entity_ext_id'),
            64,
        )
        balance_date = _as_doc_date(get('balance_date', 'doc_date', context.date_column, 'updated_at') or incremental_val)
        branch_ext = _as_optional_text(_scope_branch_to_company(get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'), get('company_id', 'company_code', 'legal_entity_id', 'organization_id')), 64)
        open_balance = _as_float(get('open_balance', 'balance', 'open_amount', 'amount'))
        overdue_balance = _as_float(get('overdue_balance', 'overdue_amount'))
        aging_0_30 = _as_float(get('aging_bucket_0_30', 'bucket_0_30', 'aging0_30'))
        aging_31_60 = _as_float(get('aging_bucket_31_60', 'bucket_31_60', 'aging31_60'))
        aging_61_90 = _as_float(get('aging_bucket_61_90', 'bucket_61_90', 'aging61_90'))
        aging_90_plus = _as_float(get('aging_bucket_90_plus', 'bucket_90_plus', 'aging90_plus', 'aging_90_plus'))
        last_payment_date = _as_optional_doc_date(get('last_payment_date', 'last_payment', 'payment_date'))
        trend_vs_previous = _as_optional_float(get('trend_vs_previous', 'trend_amount', 'delta_vs_previous'))
        currency = str(get('currency', 'currency_code') or 'EUR').upper()[:3]

        return (
            {
                'external_id': external_id,
                'supplier_id': None,
                'supplier_ext_id': supplier_id,
                'supplier_afm': _as_optional_text(
                    get('supplier_afm', 'supplier_vat_no', 'supplier_vat_number', 'supplier_tax_id', 'afm', 'vat_no', 'vat_number'),
                    64,
                ),
                'balance_date': balance_date,
                'branch_id': None,
                'branch_ext_id': branch_ext,
                'open_balance': open_balance,
                'overdue_balance': overdue_balance,
                'aging_bucket_0_30': aging_0_30,
                'aging_bucket_31_60': aging_31_60,
                'aging_bucket_61_90': aging_61_90,
                'aging_bucket_90_plus': aging_90_plus,
                'last_payment_date': last_payment_date,
                'trend_vs_previous': trend_vs_previous,
                'currency': currency,
                'updated_at': updated_at,
            },
            incremental_val,
        )

    if entity == 'supplier_orders':
        supplier_id = _as_optional_text(
            get('supplier_ext_id', 'supplier_external_id', 'supplier_code', 'supplier_id', 'entity_ext_id'),
            64,
        )
        branch_ext = _as_optional_text(_scope_branch_to_company(get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'), get('company_id', 'company_code', 'legal_entity_id', 'organization_id')), 64)
        item_code = _as_optional_text(get(context.item_column, 'item_code', 'item_external_id'), 128)
        document_id = _as_optional_text(
            get('document_id', 'doc_id', 'purchase_order_id', 'order_id', 'header_id', 'document_external_id'),
            128,
        )
        document_no = _as_optional_text(
            get('document_no', 'document_number', 'doc_no', 'order_no', 'reference_no'),
            128,
        )
        has_transformation = _as_optional_bool(get('has_transformation', 'fully_transformed', 'is_transformed'))
        order_status = _as_optional_text(get('order_status', 'status'), 32)
        if order_status is None:
            order_status = 'closed' if has_transformation else 'open'
        return (
            {
                'external_id': external_id,
                'event_id': _as_optional_text(get('event_id', 'line_id'), 128) or external_id,
                'doc_date': doc_date,
                'updated_at': updated_at,
                'branch_id': None,
                'branch_ext_id': branch_ext,
                'supplier_id': None,
                'supplier_ext_id': supplier_id,
                'supplier_name': _as_optional_text(get('supplier_name', 'supplier', 'entity_name'), 255),
                'supplier_afm': _as_optional_text(
                    get('supplier_afm', 'supplier_vat_no', 'supplier_vat_number', 'supplier_tax_id', 'afm', 'vat_no', 'vat_number'),
                    64,
                ),
                'item_id': None,
                'item_code': item_code,
                'item_name': _as_optional_text(get('item_name', 'name', 'item_description', 'description'), 512),
                'document_id': document_id or document_no or external_id,
                'document_no': document_no or document_id or external_id,
                'document_series': _as_optional_text(get('document_series', 'series', 'doc_series'), 128),
                'document_series_name': _as_optional_text(
                    get('document_series_name', 'series_name', 'document_type', 'doc_type'),
                    255,
                ),
                'document_behavior_code': _as_optional_int(
                    get('document_behavior_code', 'behavior_id', 'tfprms', 'source_transaction_type_id')
                ),
                'order_qty': _as_float(get('order_qty', 'qty', 'quantity', context.qty_column)),
                'covered_qty': _as_float(get('covered_qty', 'qty_covered', 'executed_qty')),
                'cancelled_qty': _as_float(get('cancelled_qty', 'qty_cancelled')),
                'line_value': _as_float(get('line_value', 'net_value', 'net_amount', 'amount', context.amount_column)),
                'has_transformation': bool(has_transformation),
                'order_status': str(order_status or 'open')[:32],
                'source_payload_json': _sanitize_payload_json(row),
            },
            incremental_val,
        )

    if entity == 'customer_balances':
        customer_id = _as_optional_text(
            get('customer_id', 'customer_ext_id', 'customer_external_id', 'customer_code', 'entity_ext_id'),
            128,
        )
        balance_date = _as_doc_date(get('balance_date', 'doc_date', context.date_column, 'updated_at') or incremental_val)
        branch_ext = _as_optional_text(_scope_branch_to_company(get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'), get('company_id', 'company_code', 'legal_entity_id', 'organization_id')), 64)
        open_balance = _as_float(get('open_balance', 'balance', 'open_amount', 'amount'))
        overdue_balance = _as_float(get('overdue_balance', 'overdue_amount'))
        aging_0_30 = _as_float(get('aging_bucket_0_30', 'bucket_0_30', 'aging0_30'))
        aging_31_60 = _as_float(get('aging_bucket_31_60', 'bucket_31_60', 'aging31_60'))
        aging_61_90 = _as_float(get('aging_bucket_61_90', 'bucket_61_90', 'aging61_90'))
        aging_90_plus = _as_float(get('aging_bucket_90_plus', 'bucket_90_plus', 'aging90_plus', 'aging_90_plus'))
        last_collection_date = _as_optional_doc_date(get('last_collection_date', 'last_collection', 'collection_date'))
        trend_vs_previous = _as_optional_float(get('trend_vs_previous', 'trend_amount', 'delta_vs_previous'))
        currency = str(get('currency', 'currency_code') or 'EUR').upper()[:3]

        return (
            {
                'external_id': external_id,
                'customer_ext_id': customer_id,
                'customer_name': _as_optional_text(get('customer_name', 'customer', 'client_name', 'entity_name'), 255),
                'customer_afm': _as_optional_text(
                    get('customer_afm', 'customer_vat_no', 'customer_vat_number', 'customer_tax_id', 'afm', 'vat_no', 'vat_number'),
                    64,
                ),
                'balance_date': balance_date,
                'branch_id': None,
                'branch_ext_id': branch_ext,
                'open_balance': open_balance,
                'overdue_balance': overdue_balance,
                'aging_bucket_0_30': aging_0_30,
                'aging_bucket_31_60': aging_31_60,
                'aging_bucket_61_90': aging_61_90,
                'aging_bucket_90_plus': aging_90_plus,
                'last_collection_date': last_collection_date,
                'trend_vs_previous': trend_vs_previous,
                'currency': currency,
                'updated_at': updated_at,
            },
            incremental_val,
        )

    if entity == 'expenses':
        expense_date = _as_doc_date(
            get('expense_date', 'doc_date', 'document_date', context.date_column, 'updated_at') or incremental_val
        )
        posting_date = _as_optional_doc_date(get('posting_date', 'posted_date', 'accounting_date'))
        due_date = _as_optional_doc_date(get('due_date', 'payment_due_date'))
        amount_net = _as_float(get(context.amount_column, 'amount_net', 'net_amount', 'amount'))
        amount_gross = _as_float(get('amount_gross', 'gross_amount'))
        amount_tax = _as_optional_float(get('amount_tax', 'tax_amount', 'vat_amount'))
        if amount_tax is None:
            amount_tax = max(0.0, amount_gross - amount_net)
        supplier_ext = _as_optional_text(
            get('supplier_ext_id', 'supplier_external_id', 'supplier_code', 'supplier_id'),
            64,
        )
        account_ext = _as_optional_text(
            get('account_id', 'account_external_id', 'account_code', 'ledger_account'),
            128,
        )
        expense_category_code = _as_optional_text(
            get('expense_category_code', 'category_code', 'opex_category_code', 'expense_category_id'),
            128,
        )
        return (
            {
                'external_id': external_id,
                'expense_date': expense_date,
                'posting_date': posting_date,
                'updated_at': updated_at,
                'branch_id': None,
                'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
                'location_id': None,
                'category_id': None,
                'expense_category_code': expense_category_code,
                'supplier_id': None,
                'supplier_ext_id': supplier_ext,
                'account_id': None,
                'account_ext_id': account_ext,
                'document_type': _as_optional_text(get('document_type', 'doc_type', 'type_name'), 128),
                'document_no': _as_optional_text(get('document_no', 'document_number', 'doc_no'), 128),
                'cost_center': _as_optional_text(get('cost_center', 'cost_center_code', 'department_code'), 128),
                'payment_status': _as_optional_text(get('payment_status', 'status'), 32),
                'due_date': due_date,
                'currency_code': str(get('currency', 'currency_code') or 'EUR').upper()[:3],
                'amount_net': amount_net,
                'amount_tax': amount_tax or 0.0,
                'amount_gross': amount_gross,
            },
            incremental_val,
        )

    amount = _as_float(get('amount', context.amount_column, 'net_amount', 'net_value'))
    entry_type = str(get('entry_type', 'cash_subcategory', 'transaction_type') or 'unknown').strip().lower()[:32]
    transaction_type = _as_optional_text(get('transaction_type', 'type', 'entry_type', 'cashflow_type'), 64)
    if str(transaction_type or '').strip() == '2':
        transaction_type = '102'
    subcategory_raw = _as_optional_text(get('subcategory', 'cash_subcategory', 'category'), 32)
    subcategory = _normalize_cashflow_subcategory(subcategory_raw or entry_type)
    reference_no = get('reference_no', 'reference', 'document_id')
    notes = get('notes', 'description')
    currency = str(get('currency', 'currency_code') or 'EUR').upper()[:3]
    account_id = _as_optional_text(get('account_id', 'account_code', 'account', 'bank_account'), 128)
    counterparty_id = _as_optional_text(
        get('counterparty_id', 'party_id', 'customer_id', 'customer_code', 'supplier_id', 'supplier_code', 'entity_ext_id'),
        128,
    )
    counterparty_type = _as_optional_text(get('counterparty_type', 'party_type'), 32)
    if not counterparty_type:
        if get('customer_id', 'customer_code', 'client_code') is not None:
            counterparty_type = 'customer'
        elif get('supplier_id', 'supplier_code') is not None:
            counterparty_type = 'supplier'
        elif account_id:
            counterparty_type = 'internal'
    transaction_id = _as_optional_text(get('transaction_id', 'txn_id', 'transaction_no', 'reference_no'), 128)
    if not transaction_id:
        transaction_id = external_id
    return (
        {
            'external_id': external_id,
            'transaction_id': transaction_id,
            'doc_date': doc_date,
            'transaction_date': doc_date,
            'updated_at': updated_at,
            'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
            'branch_id': None,
            'entry_type': entry_type,
            'transaction_type': transaction_type or entry_type,
            'subcategory': subcategory or None,
            'account_id': account_id,
            'counterparty_type': counterparty_type,
            'counterparty_id': counterparty_id,
            'amount': amount,
            'currency': currency,
            'reference_no': str(reference_no)[:64] if reference_no else None,
            'notes': str(notes) if notes is not None else None,
        },
        incremental_val,
    )


async def process_job(job: dict[str, Any]) -> dict[str, Any]:
    max_attempts = max(1, int(getattr(settings, 'ingest_db_transient_retries', 3) or 3))
    for attempt_no in range(1, max_attempts + 1):
        try:
            result = await _process_job_once(job)
            if attempt_no > 1:
                result['db_retry_attempts'] = attempt_no - 1
            return result
        except Exception as exc:
            if not _is_transient_postgres_error(exc) or attempt_no >= max_attempts:
                raise
            delay_seconds = min(10.0, 0.75 * attempt_no)
            logger.warning(
                'ingestion_transient_db_retry tenant=%s connector=%s stream=%s attempt=%s/%s delay=%.2fs error=%s',
                job.get('tenant_slug'),
                job.get('connector'),
                job.get('stream') or job.get('entity'),
                attempt_no,
                max_attempts,
                delay_seconds,
                str(exc)[:300],
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError('unreachable transient retry state')


async def _process_job_once(job: dict[str, Any]) -> dict[str, Any]:
    tenant_slug = job['tenant_slug']
    connector_type = job['connector']
    stream = normalize_stream_name(job.get('stream'))
    if stream is None:
        stream = ENTITY_TO_STREAM.get(str(job.get('entity') or '').strip())  # type: ignore[arg-type]
    if stream is None:
        raise RuntimeError(f"Unsupported ingest stream/entity: stream={job.get('stream')} entity={job.get('entity')}")
    entity = STREAM_TO_ENTITY[stream]
    payload = job.get('payload') or {}
    fast_backfill_mode = bool(payload.get('backfill'))
    bulk_upsert_enabled = bool(getattr(settings, 'sqlserver_bulk_upsert_enabled', True))
    bulk_fact_mode = (
        bulk_upsert_enabled
        and connector_type in SQL_CONNECTOR_ALIASES
        and (fast_backfill_mode or bool(payload.get('ensure_complete')) or bool(payload.get('bulk_upsert')))
        and entity in {
            'sales',
            'purchases',
            'inventory',
            'cashflows',
            'supplier_balances',
            'customer_balances',
            'expenses',
            'supplier_orders',
            'items',
        }
    )
    bulk_fact_batch_size = max(100, int(getattr(settings, 'sqlserver_bulk_upsert_batch_size', 1000) or 1000))

    connector = CONNECTORS.get(connector_type)
    if connector is None:
        raise RuntimeError(f'Unknown connector: {connector_type}')

    tenant_id: int | None = None
    tenant_key: str | None = None
    tenant_db_name: str | None = None
    tenant_db_user: str | None = None
    tenant_db_password: str | None = None
    connection_id: int | None = None
    context: ConnectorContext | None = None
    duplicate_protection = {'enabled': True, 'mode': 'natural_key'}

    # Load control metadata in a short-lived session so long-running fetch/ingest
    # does not keep the control DB connection open.
    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if not tenant:
            raise RuntimeError(f'Tenant not found: {tenant_slug}')

        connection = await _load_connection_for_connector(
            control_db,
            tenant_id=tenant.id,
            connector_type=connector_type,
        )

        source_type = _resolve_source_type(connector_type, connection, connector)
        if source_type == 'sql' and connector_type in SQL_CONNECTOR_ALIASES and not connection:
            raise RuntimeError(f'No SQL Server mapping found for tenant {tenant_slug}')

        context = await _build_context(
            control_db,
            tenant_id=tenant.id,
            connector_type=connector_type,
            connector=connector,
            connection=connection,
        )
        context.tenant_slug = tenant_slug

        tenant_id = int(tenant.id)
        tenant_key = str(tenant.id)
        tenant_db_name = str(tenant.db_name)
        tenant_db_user = str(tenant.db_user)
        tenant_db_password = str(tenant.db_password)
        duplicate_protection = _duplicate_protection_settings_from_flags(tenant.feature_flags)
        if connection is not None and getattr(connection, 'id', None) is not None:
            connection_id = int(connection.id)

    if context is None or tenant_key is None or tenant_db_name is None or tenant_db_user is None or tenant_db_password is None:
        raise RuntimeError(f'Invalid ingest context for tenant: {tenant_slug}')

    if not context.stream_enabled(stream):
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'connector': connector_type,
            'stream': stream,
            'entity': entity,
            'reason': 'stream_disabled',
        }

    sync_key = f'{connector_type}_{stream}'
    legacy_sync_key = f'{connector_type}_{entity}'
    ignore_sync_state = bool(payload.get('ignore_sync_state'))

    # Read sync cursor in a short-lived tenant DB session, then call the external
    # endpoint without holding an asyncpg connection open.
    initial_last_ts = None
    initial_last_id = None
    if not ignore_sync_state:
        async for tenant_db in get_tenant_db_session(
            tenant_key=tenant_key,
            db_name=tenant_db_name,
            db_user=tenant_db_user,
            db_password=tenant_db_password,
        ):
            seed_state = await _load_sync_state(
                tenant_db,
                connector_key=sync_key,
                legacy_connector_key=legacy_sync_key if legacy_sync_key != sync_key else None,
            )
            initial_last_ts = seed_state.last_sync_timestamp
            initial_last_id = seed_state.last_sync_id
            await tenant_db.commit()

    if (
        connector_type in SQL_CONNECTOR_ALIASES
        and not ignore_sync_state
        and initial_last_ts is None
        and not payload.get('from_date')
        and payload.get('lookback_days') is not None
    ):
        try:
            lookback_days = max(1, int(payload.get('lookback_days') or 1))
            payload = dict(payload)
            payload['from_date'] = (date_cls.today() - timedelta(days=lookback_days)).isoformat()
        except (TypeError, ValueError):
            pass

    fetch_last_ts = None if ignore_sync_state else initial_last_ts
    fetch_last_id = None if ignore_sync_state else initial_last_id
    cursor_overlap_minutes = 0
    if (
        connector_type in SQL_CONNECTOR_ALIASES
        and not ignore_sync_state
        and isinstance(initial_last_ts, datetime)
        and not bool(payload.get('disable_cursor_overlap'))
    ):
        try:
            cursor_overlap_minutes = max(
                0,
                int(
                    payload.get('cursor_overlap_minutes')
                    or getattr(settings, 'incremental_sync_overlap_minutes', 5)
                    or 0
                ),
            )
        except (TypeError, ValueError):
            cursor_overlap_minutes = 5
        if cursor_overlap_minutes > 0:
            fetch_last_ts = initial_last_ts - timedelta(minutes=cursor_overlap_minutes)
            fetch_last_id = None

    inc_state = IncrementalState(
        last_sync_timestamp=fetch_last_ts,
        last_sync_id=fetch_last_id,
    )
    # Connectors backed by pyodbc (SQL Server) execute synchronous blocking I/O.
    # Running that in the event-loop thread freezes all coroutines (lock heartbeat,
    # asyncio.wait_for timeout). Collect rows in a thread-pool executor so the
    # event loop remains responsive while the SQL query and paging loop run.
    _loop = asyncio.get_running_loop()
    rows: list[dict[str, Any]] = await _loop.run_in_executor(
        None,
        lambda: list(connector.fetch_rows(stream=stream, entity=entity, context=context, state=inc_state, payload=payload)),
    )

    processed = 0
    heartbeat_every_rows = 250
    commit_every_rows = 1000
    fact_batch_size = bulk_fact_batch_size if bulk_fact_mode else 1
    last_ts = None if ignore_sync_state else initial_last_ts
    last_id = None if ignore_sync_state else initial_last_id
    min_doc_date = None
    max_doc_date = None
    skipped_company_mismatch = 0
    dim_id_cache: dict[tuple[str, str], Any] = {}
    expense_category_cache: dict[str, Any] = {}
    fact_identity_cache: dict[tuple[Any, ...], str] = {}
    sales_charge_seen: set[str] = set()
    relink_counts: dict[str, int] = {}

    async def _resolve_dim_id_cached(model, external_id: str | None):
        if not external_id:
            return None
        key = (str(getattr(model, '__name__', 'dim')), str(external_id))
        if key in dim_id_cache:
            return dim_id_cache[key]
        value = await _resolve_dim_id(tenant_db, model, external_id)
        dim_id_cache[key] = value
        return value

    async def _resolve_expense_category_id_cached(category_code: str | None):
        if not category_code:
            return None
        key = str(category_code).strip()
        if not key:
            return None
        if key in expense_category_cache:
            return expense_category_cache[key]
        value = await _resolve_expense_category_id(tenant_db, key)
        expense_category_cache[key] = value
        return value

    async for tenant_db in get_tenant_db_session(
        tenant_key=tenant_key,
        db_name=tenant_db_name,
        db_user=tenant_db_user,
        db_password=tenant_db_password,
    ):
        sync_state = await _load_sync_state(
            tenant_db,
            connector_key=sync_key,
            legacy_connector_key=legacy_sync_key if legacy_sync_key != sync_key else None,
        )
        if not ignore_sync_state and last_ts is None and last_id is None:
            last_ts = sync_state.last_sync_timestamp
            last_id = sync_state.last_sync_id

        fact_batch: list[dict[str, Any]] = []
        dim_seen_cache: dict[str, set[str]] | None = {} if bulk_fact_mode else None
        rows_since_commit = 0

        async def _flush_fact_batch() -> None:
            nonlocal fact_batch
            if not fact_batch:
                return
            stmt = _build_fact_upsert_stmt(entity, fact_batch)
            if stmt is not None:
                await tenant_db.execute(stmt)
            fact_batch = []

        for row in rows:
            if connector_type == 'external_api':
                raw_event_id = str(row.get('event_id') or row.get('external_id') or '')[:128]
                if raw_event_id:
                    staging_stmt = (
                        insert(StagingIngestEvent)
                        .values(
                            entity=entity,
                            event_id=raw_event_id,
                            payload_json=json.dumps(row, default=str),
                        )
                        .on_conflict_do_nothing(index_elements=['event_id', 'entity'])
                    )
                    await tenant_db.execute(staging_stmt)

            stage_id = None
            if not bulk_fact_mode and entity != 'items':
                stage_id = await _stage_ingest_row(
                    tenant_db,
                    connector_type=connector_type,
                    stream=stream,
                    row=row,
                )
                if stage_id is None:
                    raise RuntimeError(f'Row rejected: stream={stream} has no persisted staging id')
            try:
                fact, incremental_val = _build_fact(
                    entity,
                    row,
                    context,
                    default_prefix=entity[:1].upper(),
                    stream=stream,
                )
                fact['source_connector_id'] = str(connector_type)[:64]
                if _company_mismatch(row, fact, context):
                    skipped_company_mismatch += 1
                    last_ts, last_id = _update_incremental_state(last_ts, last_id, incremental_val)
                    if not bulk_fact_mode:
                        await _mark_staging_row_processed(tenant_db, stream=stream, stage_id=stage_id)
                    continue

                if duplicate_protection.get('enabled', True) and entity != 'items':
                    await _canonicalize_fact_external_id(
                        tenant_db,
                        entity,
                        fact,
                        fact_identity_cache,
                        mode=str(duplicate_protection.get('mode') or 'natural_key'),
                    )
                await _upsert_dims_from_row(
                    tenant_db,
                    entity,
                    row,
                    fact,
                    dim_seen_cache=dim_seen_cache,
                    dim_id_cache=dim_id_cache,
                )
                if entity == 'items':
                    stmt = None
                elif entity == 'sales':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id_cached(DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['item_id'] = await _resolve_dim_id_cached(DimItem, fact.get('item_code'))
                        fact['customer_id'] = await _resolve_dim_id_cached(DimCustomer, fact.get('customer_code'))
                    stmt = _upsert_sales_stmt(fact)
                    charge_document_id = str(fact.get('document_id') or fact.get('document_no') or '')
                    if charge_document_id and charge_document_id not in sales_charge_seen:
                        charge_facts = _build_sales_document_charge_facts(fact)
                        charge_stmt = _upsert_sales_document_charges_stmt(charge_facts)
                        if charge_stmt is not None:
                            await tenant_db.execute(charge_stmt)
                        sales_charge_seen.add(charge_document_id)
                elif entity == 'purchases':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id_cached(DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['supplier_id'] = await _resolve_dim_id_cached(DimSupplier, fact.get('supplier_ext_id'))
                        fact['item_id'] = await _resolve_dim_id_cached(DimItem, fact.get('item_code'))
                    stmt = _upsert_purchases_stmt(fact)
                elif entity == 'inventory':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id_cached(DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['item_id'] = await _resolve_dim_id_cached(DimItem, fact.get('item_code'))
                    stmt = _upsert_inventory_stmt(fact)
                elif entity == 'cashflows':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                    stmt = _upsert_cashflow_stmt(fact)
                elif entity == 'supplier_balances':
                    if not fast_backfill_mode:
                        fact['supplier_id'] = await _resolve_dim_id_cached(DimSupplier, fact.get('supplier_ext_id'))
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                    stmt = _upsert_supplier_balance_stmt(fact)
                elif entity == 'supplier_orders':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['supplier_id'] = await _resolve_dim_id_cached(DimSupplier, fact.get('supplier_ext_id'))
                        fact['item_id'] = await _resolve_dim_id_cached(DimItem, fact.get('item_code'))
                    stmt = _upsert_supplier_order_stmt(fact)
                elif entity == 'expenses':
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['category_id'] = await _resolve_expense_category_id_cached(fact.get('expense_category_code'))
                        fact['supplier_id'] = await _resolve_dim_id_cached(DimSupplier, fact.get('supplier_ext_id'))
                        fact['account_id'] = await _resolve_dim_id_cached(DimAccount, fact.get('account_ext_id'))
                    stmt = _upsert_expense_stmt(fact)
                else:
                    if not fast_backfill_mode:
                        fact['branch_id'] = await _resolve_dim_id_cached(DimBranch, fact.get('branch_ext_id'))
                        fact['customer_id'] = await _resolve_dim_id_cached(DimCustomer, fact.get('customer_ext_id'))
                    stmt = _upsert_customer_balance_stmt(fact)

                if entity == 'items':
                    await _mark_staging_row_processed(tenant_db, stream=stream, stage_id=stage_id)
                elif bulk_fact_mode:
                    fact_batch.append(fact)
                    if len(fact_batch) >= fact_batch_size:
                        await _flush_fact_batch()
                else:
                    await tenant_db.execute(stmt)
                    await _mark_staging_row_processed(tenant_db, stream=stream, stage_id=stage_id)
            except Exception as row_exc:
                if not bulk_fact_mode:
                    # Clear the failed statement/transaction state first so the
                    # staging failure marker can be persisted reliably and the
                    # worker does not cascade into an "InFailedSQLTransaction"
                    # follow-up error that hides the original root cause.
                    await tenant_db.rollback()
                    await _mark_staging_row_failed(
                        tenant_db,
                        stream=stream,
                        stage_id=stage_id,
                        error_message=str(row_exc),
                    )
                raise

            last_ts, last_id = _update_incremental_state(last_ts, last_id, incremental_val)
            doc_date = fact.get('doc_date')
            if doc_date is not None:
                if min_doc_date is None or doc_date < min_doc_date:
                    min_doc_date = doc_date
                if max_doc_date is None or doc_date > max_doc_date:
                    max_doc_date = doc_date
            processed += 1
            rows_since_commit += 1

            if rows_since_commit >= commit_every_rows:
                await _flush_fact_batch()
                if not ignore_sync_state:
                    sync_state.last_sync_timestamp = last_ts
                    sync_state.last_sync_id = last_id
                    if hasattr(sync_state, 'stream_code'):
                        sync_state.stream_code = str(stream)
                    if hasattr(sync_state, 'source_connector_id'):
                        sync_state.source_connector_id = str(connector_type)[:64]
                await tenant_db.commit()
                update_ingest_progress(tenant_slug, status='running')
                rows_since_commit = 0
            elif processed % heartbeat_every_rows == 0:
                update_ingest_progress(tenant_slug, status='running')

        await _flush_fact_batch()
        # Relink surrogate FKs after the flush. bulk_fact_mode covers SQL connectors,
        # but non-SQL connectors (external_api) skip both the bulk path and — in
        # backfill mode — per-row resolution, leaving fact rows with NULL item_id and
        # breaking every item/group/category join downstream. The relink only touches
        # NULL item_id rows (idempotent, cheap), so it is safe to run here for them too.
        should_relink_facts = bulk_fact_mode or connector_type not in SQL_CONNECTOR_ALIASES
        relink_counts = await _relink_fact_dimensions_after_bulk_ingest(tenant_db, entity) if should_relink_facts else {}
        if entity == 'sales':
            # Fill missing COGS from the weighted-average purchase cost (net fallback -> real margin).
            relink_counts['sales_cost_from_purchases'] = await _backfill_sales_cost_from_purchases(tenant_db)
        if not ignore_sync_state:
            sync_state.last_sync_timestamp = last_ts
            sync_state.last_sync_id = last_id
            if hasattr(sync_state, 'stream_code'):
                sync_state.stream_code = str(stream)
            if hasattr(sync_state, 'source_connector_id'):
                sync_state.source_connector_id = str(connector_type)[:64]

        await tenant_db.commit()

    if connection_id is not None:
        async with ControlSessionLocal() as control_db:
            connection_row = (
                await control_db.execute(select(TenantConnection).where(TenantConnection.id == connection_id))
            ).scalar_one_or_none()
            if connection_row is not None:
                # UI should show when the latest successful sync finished, not the source-system watermark.
                connection_row.last_sync_at = datetime.utcnow()
                connection_row.sync_status = 'ok'
                try:
                    await control_db.commit()
                except Exception as control_commit_exc:
                    await control_db.rollback()
                    logger.warning(
                        'ingestion_control_commit_failed tenant=%s connector=%s stream=%s',
                        tenant_slug,
                        connector_type,
                        stream,
                        exc_info=control_commit_exc,
                    )

    return {
        'status': 'ok',
        'tenant': tenant_slug,
        'connector': connector_type,
        'stream': stream,
        'entity': entity,
        'processed': processed,
        'min_doc_date': str(min_doc_date) if min_doc_date else None,
        'max_doc_date': str(max_doc_date) if max_doc_date else None,
        'skipped_company_mismatch': skipped_company_mismatch,
        'last_sync_timestamp': last_ts.isoformat() if isinstance(last_ts, datetime) else None,
        'last_sync_id': last_id,
        'cursor_overlap_minutes': cursor_overlap_minutes,
        'dimension_relink': relink_counts if bulk_fact_mode else {},
    }


async def persist_dead_letter(
    *,
    tenant_slug: str,
    connector_type: str,
    entity: str,
    payload: dict[str, Any],
    error_message: str,
) -> None:
    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if not tenant:
            return
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            tenant_db.add(
                IngestDeadLetter(
                    connector_type=connector_type,
                    entity=entity,
                    event_id=str(payload.get('event_id') or payload.get('batch_id') or '')[:128] or None,
                    payload_json=json.dumps(payload, default=str),
                    error_message=error_message[:1024],
                )
            )
            await tenant_db.commit()
