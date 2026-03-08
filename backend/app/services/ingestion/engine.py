from __future__ import annotations

import json
from datetime import date as date_cls, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

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
    DimGroup,
    DimItem,
    DimPaymentMethod,
    DimSupplier,
    DimWarehouse,
    FactCashflow,
    FactCustomerBalance,
    FactInventory,
    FactPurchases,
    FactSales,
    FactSupplierBalance,
    IngestDeadLetter,
    StgCashTransaction,
    StgCustomerBalance,
    StgInventoryDocument,
    StgPurchaseDocument,
    StgSalesDocument,
    StgSupplierBalance,
    StagingIngestEvent,
    SyncState,
)
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret
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
from app.services.rule_config import resolve_source_query_template

CONNECTORS = {
    'sql_connector': GenericSqlConnector(),
    'pharmacyone_sql': PharmacyOneSqlConnector(),
    'external_api': ExternalApiIngestConnector(),
    'file_import': FileImportConnector(),
}

SQL_CONNECTOR_ALIASES = ('sql_connector', 'pharmacyone_sql')


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
        return ['sales_documents', 'purchase_documents']
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
    if isinstance(incremental_val, datetime):
        if last_ts is None or incremental_val > last_ts:
            return incremental_val, last_id
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


def _normalize_cashflow_subcategory(value: str | None) -> str:
    raw = str(value or '').strip().lower()
    if not raw:
        return ''
    raw = raw.replace('-', '_').replace(' ', '_')
    aliases = {
        'customer_collection': 'customer_collections',
        'collections': 'customer_collections',
        'collection': 'customer_collections',
        'in': 'customer_collections',
        'inflow': 'customer_collections',
        'credit': 'customer_collections',
        'income': 'customer_collections',
        'customer_transfer': 'customer_transfers',
        'customer_bank_transfer': 'customer_transfers',
        'customer_wire_transfer': 'customer_transfers',
        'customer_wire': 'customer_transfers',
        'supplier_payment': 'supplier_payments',
        'payments': 'supplier_payments',
        'payment': 'supplier_payments',
        'out': 'supplier_payments',
        'outflow': 'supplier_payments',
        'debit': 'supplier_payments',
        'expense': 'supplier_payments',
        'supplier_transfer': 'supplier_transfers',
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


def _upsert_sales_stmt(fact: dict):
    ins = insert(FactSales).values(**fact)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'updated_at': ins.excluded.updated_at,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'warehouse_ext_id': ins.excluded.warehouse_ext_id,
            'brand_ext_id': ins.excluded.brand_ext_id,
            'category_ext_id': ins.excluded.category_ext_id,
            'group_ext_id': ins.excluded.group_ext_id,
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


def _upsert_purchases_stmt(fact: dict):
    ins = insert(FactPurchases).values(**fact)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'updated_at': ins.excluded.updated_at,
            'branch_ext_id': ins.excluded.branch_ext_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'warehouse_ext_id': ins.excluded.warehouse_ext_id,
            'supplier_ext_id': ins.excluded.supplier_ext_id,
            'brand_ext_id': ins.excluded.brand_ext_id,
            'category_ext_id': ins.excluded.category_ext_id,
            'group_ext_id': ins.excluded.group_ext_id,
            'item_code': ins.excluded.item_code,
            'qty': ins.excluded.qty,
            'net_value': ins.excluded.net_value,
            'cost_amount': ins.excluded.cost_amount,
        },
    )


def _upsert_inventory_stmt(fact: dict):
    payload = {
        'external_id': fact.get('external_id'),
        'doc_date': fact.get('doc_date'),
        'branch_id': fact.get('branch_id'),
        'item_id': fact.get('item_id'),
        'warehouse_id': fact.get('warehouse_id'),
        'source_connector_id': fact.get('source_connector_id'),
        'qty_on_hand': fact.get('qty_on_hand') or 0,
        'qty_reserved': fact.get('qty_reserved') or 0,
        'cost_amount': fact.get('cost_amount') or 0,
        'value_amount': fact.get('value_amount') or 0,
        'updated_at': fact.get('updated_at'),
    }
    ins = insert(FactInventory).values(**payload)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={
            'doc_date': ins.excluded.doc_date,
            'branch_id': ins.excluded.branch_id,
            'item_id': ins.excluded.item_id,
            'warehouse_id': ins.excluded.warehouse_id,
            'source_connector_id': ins.excluded.source_connector_id,
            'qty_on_hand': ins.excluded.qty_on_hand,
            'qty_reserved': ins.excluded.qty_reserved,
            'cost_amount': ins.excluded.cost_amount,
            'value_amount': ins.excluded.value_amount,
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
    doc_date = _as_optional_doc_date(row.get('doc_date') or row.get('document_date') or row.get('balance_date'))

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


async def _upsert_dims_from_row(tenant_db: AsyncSession, entity: str, row: dict[str, Any], fact: dict) -> None:
    get = _row_getter(row)

    branch = fact.get('branch_ext_id')
    if branch:
        branch_name = str(get('branch_name') or branch)[:255]
        company_id = _as_optional_text(get('company_id', 'company_code', 'legal_entity_id', 'organization_id'), 64)
        await tenant_db.execute(_upsert_branch_dim_stmt(str(branch)[:64], branch_name, company_id=company_id))
    warehouse = fact.get('warehouse_ext_id')
    if warehouse:
        await tenant_db.execute(
            _upsert_dim_stmt(DimWarehouse, str(warehouse)[:64], str(get('warehouse_name') or warehouse)[:255])
        )
    brand = fact.get('brand_ext_id')
    if brand:
        await tenant_db.execute(_upsert_dim_stmt(DimBrand, str(brand)[:64], str(get('brand_name') or brand)[:255]))
    category = fact.get('category_ext_id')
    if category:
        await tenant_db.execute(
            _upsert_dim_stmt(DimCategory, str(category)[:64], str(get('category_name') or category)[:255])
        )
    group = fact.get('group_ext_id')
    if group:
        await tenant_db.execute(_upsert_dim_stmt(DimGroup, str(group)[:64], str(get('group_name') or group)[:255]))
    item = fact.get('item_code')
    if item:
        item_external_id = str(item)[:128]
        item_barcode = _as_optional_text(
            get('barcode', 'item_barcode', 'barcode_code', 'ean', 'ean13'),
            128,
        )
        item_sku = _as_optional_text(get('sku', 'item_sku', 'stock_code', 'item_sku_code'), 128)
        item_name = _as_optional_text(get('item_name', 'name', 'item_description', 'description'), 255) or str(item)[:255]

        item_values = {
            'external_id': item_external_id,
            'sku': item_sku or item_barcode or item_external_id,
            'barcode': item_barcode or item_sku,
            'name': item_name,
            'main_unit': _as_optional_text(get('main_unit', 'unit', 'uom', 'unit_main', 'primary_unit'), 64),
            'vat_rate': _as_optional_float(get('vat_rate', 'vat_percent', 'vat', 'fpa', 'tax_rate')),
            'vat_label': _as_optional_text(get('vat_label', 'vat_text', 'fpa_label'), 64),
            'use_batch': _as_optional_bool(get('use_batch', 'batch_tracking', 'lot_tracking', 'use_lot')),
            'commercial_category': _as_optional_text(get('commercial_category', 'commercial_category_name'), 255),
            'category_1': _as_optional_text(get('category_1', 'category1', 'category_l1', 'category_level1'), 255),
            'category_2': _as_optional_text(get('category_2', 'category2', 'category_l2', 'category_level2'), 255),
            'category_3': _as_optional_text(get('category_3', 'category3', 'category_l3', 'category_level3'), 255),
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
            'is_active_source': _as_optional_bool(get('is_active', 'active', 'enabled', 'item_active')),
        }

        ins = insert(DimItem).values(
            **item_values,
        )
        await tenant_db.execute(
            ins.on_conflict_do_update(
                index_elements=['external_id'],
                set_={
                    'updated_at': datetime.utcnow(),
                    'sku': func.coalesce(ins.excluded.sku, DimItem.sku),
                    'barcode': func.coalesce(ins.excluded.barcode, DimItem.barcode),
                    'name': func.coalesce(ins.excluded.name, DimItem.name),
                    'main_unit': func.coalesce(ins.excluded.main_unit, DimItem.main_unit),
                    'vat_rate': func.coalesce(ins.excluded.vat_rate, DimItem.vat_rate),
                    'vat_label': func.coalesce(ins.excluded.vat_label, DimItem.vat_label),
                    'use_batch': func.coalesce(ins.excluded.use_batch, DimItem.use_batch),
                    'commercial_category': func.coalesce(ins.excluded.commercial_category, DimItem.commercial_category),
                    'category_1': func.coalesce(ins.excluded.category_1, DimItem.category_1),
                    'category_2': func.coalesce(ins.excluded.category_2, DimItem.category_2),
                    'category_3': func.coalesce(ins.excluded.category_3, DimItem.category_3),
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
                    'image_url': func.coalesce(ins.excluded.image_url, DimItem.image_url),
                    'discount_pct': func.coalesce(ins.excluded.discount_pct, DimItem.discount_pct),
                    'is_active_source': func.coalesce(ins.excluded.is_active_source, DimItem.is_active_source),
                },
            )
        )
    if entity in {'purchases', 'supplier_balances'}:
        supplier = fact.get('supplier_ext_id')
        if supplier:
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
    if document_type:
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
    if payment_method:
        await tenant_db.execute(
            _upsert_dim_stmt(DimPaymentMethod, payment_method[:128], payment_method[:255])
        )

    account_external = _as_optional_text(
        get('account_id', 'account_code', 'account_external_id', 'cash_account_id'),
        128,
    ) or _as_optional_text(fact.get('account_id'), 128)
    account_name = _as_optional_text(get('account_name', 'cash_account_name', 'account_title'), 255)
    if account_external:
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


async def _build_context(
    control_db: AsyncSession,
    *,
    tenant_id: int,
    connector_type: str,
    connector,
    connection: TenantConnection | None,
) -> ConnectorContext:
    source_connection_string = None
    if connection and connection.enc_payload:
        secret = decrypt_sqlserver_secret(connection.enc_payload)
        source_connection_string = build_odbc_connection_string(secret)

    supported_streams = _resolve_supported_streams(connection, connector, connector_type)
    enabled_streams = _resolve_enabled_streams(connection, supported_streams)
    source_type = _resolve_source_type(connector_type, connection, connector)
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
        connection_parameters=(
            connection.connection_parameters if connection and isinstance(connection.connection_parameters, dict) else {}
        ),
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
    )

    fallback_query_by_stream: dict[OperationalIngestStream, str] = {
        'sales_documents': context.stream_query_mapping.get('sales_documents') or context.sales_query or '',
        'purchase_documents': context.stream_query_mapping.get('purchase_documents') or context.purchases_query or '',
        'inventory_documents': context.stream_query_mapping.get('inventory_documents') or context.inventory_query or '',
        'cash_transactions': context.stream_query_mapping.get('cash_transactions') or context.cashflow_query or '',
        'supplier_balances': context.stream_query_mapping.get('supplier_balances') or context.supplier_balances_query or '',
        'customer_balances': context.stream_query_mapping.get('customer_balances') or context.customer_balances_query or '',
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
        resolved = await resolve_source_query_template(
            control_db,
            tenant_id=tenant_id,
            stream=stream_enum_lookup[stream],
            fallback_query_template=fallback_query_by_stream.get(stream, ''),
        )
        if resolved.strip():
            context.stream_query_mapping[stream] = resolved

    context.sales_query = context.stream_query_mapping.get('sales_documents') or context.sales_query
    context.purchases_query = context.stream_query_mapping.get('purchase_documents') or context.purchases_query
    context.inventory_query = context.stream_query_mapping.get('inventory_documents') or context.inventory_query
    context.cashflow_query = context.stream_query_mapping.get('cash_transactions') or context.cashflow_query
    context.supplier_balances_query = (
        context.stream_query_mapping.get('supplier_balances') or context.supplier_balances_query
    )
    context.customer_balances_query = (
        context.stream_query_mapping.get('customer_balances') or context.customer_balances_query
    )
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

    branch_ext = get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code')
    warehouse_ext = get('warehouse_ext_id', 'warehouse_external_id', 'warehouse_code')
    brand_ext = get('brand_ext_id', 'brand_external_id', 'brand_code')
    category_ext = get('category_ext_id', 'category_external_id', 'category_code')
    group_ext = get('group_ext_id', 'group_external_id', 'group_code')
    item_code = get(context.item_column, 'item_code', 'item_external_id')
    supplier_ext = get('supplier_ext_id', 'supplier_external_id', 'supplier_code', 'entity_ext_id')

    raw_external = get('external_id', 'event_id') or f"{default_prefix}:{incremental_val}:{branch_ext}:{item_code}"
    external_id = str(raw_external)[:128]

    doc_date = _as_doc_date(get('doc_date', 'document_date', context.date_column, 'updated_at') or incremental_val)

    if entity in {'sales', 'purchases'}:
        qty = _as_float(get(context.qty_column, 'qty', 'quantity'))
        net = _as_float(get(context.amount_column, 'net_value', 'net_amount', 'amount'))
        cost = _as_float(get(context.cost_column, 'cost_amount', 'cost_value'))
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
                    'vat_amount': vat_amount if vat_amount is not None else max(0.0, gross - net),
                    'source_payload_json': _sanitize_payload_json(row),
                }
            )
            base['gross_value'] = gross
            base['profit_amount'] = gross - cost
        else:
            base['supplier_ext_id'] = str(supplier_ext)[:64] if supplier_ext is not None else None
        return base, incremental_val

    if entity == 'inventory':
        qty_on_hand = _as_float(get('qty_on_hand', context.qty_column, 'qty', 'quantity'))
        qty_reserved = _as_float(get('qty_reserved', 'reserved_qty'))
        cost_amount = _as_float(get('cost_amount', context.cost_column, 'cost_value'))
        value_amount = _as_float(get('value_amount', context.amount_column, 'net_value', 'net_amount'))
        return (
            {
                'external_id': external_id,
                'doc_date': doc_date,
                'updated_at': updated_at,
                'branch_ext_id': str(branch_ext)[:64] if branch_ext is not None else None,
                'warehouse_ext_id': str(warehouse_ext)[:64] if warehouse_ext is not None else None,
                'item_code': str(item_code)[:128] if item_code is not None else None,
                'branch_id': None,
                'warehouse_id': None,
                'item_id': None,
                'qty_on_hand': qty_on_hand,
                'qty_reserved': qty_reserved,
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
        branch_ext = _as_optional_text(get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'), 64)
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

    if entity == 'customer_balances':
        customer_id = _as_optional_text(
            get('customer_id', 'customer_ext_id', 'customer_external_id', 'customer_code', 'entity_ext_id'),
            128,
        )
        balance_date = _as_doc_date(get('balance_date', 'doc_date', context.date_column, 'updated_at') or incremental_val)
        branch_ext = _as_optional_text(get(context.branch_column, 'branch_ext_id', 'branch_external_id', 'branch_code'), 64)
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

    amount = _as_float(get('amount', context.amount_column, 'net_amount', 'net_value'))
    entry_type = str(get('entry_type', 'cash_subcategory', 'transaction_type') or 'unknown').strip().lower()[:32]
    transaction_type = _as_optional_text(get('transaction_type', 'type', 'entry_type', 'cashflow_type'), 64)
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
    tenant_slug = job['tenant_slug']
    connector_type = job['connector']
    stream = normalize_stream_name(job.get('stream'))
    if stream is None:
        stream = ENTITY_TO_STREAM.get(str(job.get('entity') or '').strip())  # type: ignore[arg-type]
    if stream is None:
        raise RuntimeError(f"Unsupported ingest stream/entity: stream={job.get('stream')} entity={job.get('entity')}")
    entity = STREAM_TO_ENTITY[stream]
    payload = job.get('payload') or {}

    connector = CONNECTORS.get(connector_type)
    if connector is None:
        raise RuntimeError(f'Unknown connector: {connector_type}')

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

        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            context = await _build_context(
                control_db,
                tenant_id=tenant.id,
                connector_type=connector_type,
                connector=connector,
                connection=connection,
            )
            context.tenant_slug = tenant_slug
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
            sync_state = await _load_sync_state(
                tenant_db,
                connector_key=sync_key,
                legacy_connector_key=legacy_sync_key if legacy_sync_key != sync_key else None,
            )
            ignore_sync_state = bool(payload.get('ignore_sync_state'))
            inc_state = IncrementalState(
                last_sync_timestamp=None if ignore_sync_state else sync_state.last_sync_timestamp,
                last_sync_id=None if ignore_sync_state else sync_state.last_sync_id,
            )

            rows = connector.fetch_rows(stream=stream, entity=entity, context=context, state=inc_state, payload=payload)

            processed = 0
            last_ts = sync_state.last_sync_timestamp
            last_id = sync_state.last_sync_id
            min_doc_date = None
            max_doc_date = None

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
                    await _upsert_dims_from_row(tenant_db, entity, row, fact)
                    if entity == 'sales':
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id(tenant_db, DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['item_id'] = await _resolve_dim_id(tenant_db, DimItem, fact.get('item_code'))
                        fact['customer_id'] = await _resolve_dim_id(tenant_db, DimCustomer, fact.get('customer_code'))
                        stmt = _upsert_sales_stmt(fact)
                    elif entity == 'purchases':
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id(tenant_db, DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['supplier_id'] = await _resolve_dim_id(tenant_db, DimSupplier, fact.get('supplier_ext_id'))
                        fact['item_id'] = await _resolve_dim_id(tenant_db, DimItem, fact.get('item_code'))
                        stmt = _upsert_purchases_stmt(fact)
                    elif entity == 'inventory':
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        fact['warehouse_id'] = await _resolve_dim_id(tenant_db, DimWarehouse, fact.get('warehouse_ext_id'))
                        fact['item_id'] = await _resolve_dim_id(tenant_db, DimItem, fact.get('item_code'))
                        stmt = _upsert_inventory_stmt(fact)
                    elif entity == 'cashflows':
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        stmt = _upsert_cashflow_stmt(fact)
                    elif entity == 'supplier_balances':
                        fact['supplier_id'] = await _resolve_dim_id(tenant_db, DimSupplier, fact.get('supplier_ext_id'))
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        stmt = _upsert_supplier_balance_stmt(fact)
                    else:
                        fact['branch_id'] = await _resolve_dim_id(tenant_db, DimBranch, fact.get('branch_ext_id'))
                        fact['customer_id'] = await _resolve_dim_id(tenant_db, DimCustomer, fact.get('customer_ext_id'))
                        stmt = _upsert_customer_balance_stmt(fact)
                    await tenant_db.execute(stmt)
                    await _mark_staging_row_processed(tenant_db, stream=stream, stage_id=stage_id)
                except Exception as row_exc:
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

            sync_state.last_sync_timestamp = last_ts
            sync_state.last_sync_id = last_id
            if hasattr(sync_state, 'stream_code'):
                sync_state.stream_code = str(stream)
            if hasattr(sync_state, 'source_connector_id'):
                sync_state.source_connector_id = str(connector_type)[:64]
            if connection is not None:
                connection.last_sync_at = last_ts or datetime.utcnow()
                connection.sync_status = 'ok'

            await tenant_db.commit()
        await control_db.commit()

    return {
        'status': 'ok',
        'tenant': tenant_slug,
        'connector': connector_type,
        'stream': stream,
        'entity': entity,
        'processed': processed,
        'min_doc_date': str(min_doc_date) if min_doc_date else None,
        'max_doc_date': str(max_doc_date) if max_doc_date else None,
        'last_sync_timestamp': last_ts.isoformat() if isinstance(last_ts, datetime) else None,
        'last_sync_id': last_id,
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
