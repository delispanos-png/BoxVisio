from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.control_session import ControlSessionLocal
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import Tenant, TenantConnection
from app.models.tenant import (
    DimBranch,
    DimBrand,
    DimCategory,
    DimGroup,
    DimItem,
    DimSupplier,
    DimWarehouse,
    FactPurchases,
    FactSales,
    IngestDeadLetter,
    StagingIngestEvent,
    SyncState,
)
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret
from app.services.ingestion.base import ConnectorContext, IncrementalState
from app.services.ingestion.external_api_connector import ExternalApiIngestConnector
from app.services.ingestion.pharmacyone_connector import PharmacyOneSqlConnector

CONNECTORS = {
    'pharmacyone_sql': PharmacyOneSqlConnector(),
    'external_api': ExternalApiIngestConnector(),
}


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


def _upsert_dim_stmt(model, external_id: str, name: str):
    ins = insert(model).values(external_id=external_id, name=name)
    return ins.on_conflict_do_update(
        index_elements=['external_id'],
        set_={'updated_at': datetime.utcnow(), 'name': ins.excluded.name},
    )


async def _upsert_dims_from_row(tenant_db: AsyncSession, entity: str, row: dict[str, Any], fact: dict) -> None:
    branch = fact.get('branch_ext_id')
    if branch:
        await tenant_db.execute(_upsert_dim_stmt(DimBranch, str(branch)[:64], str(row.get('branch_name') or branch)[:255]))
    warehouse = fact.get('warehouse_ext_id')
    if warehouse:
        await tenant_db.execute(
            _upsert_dim_stmt(DimWarehouse, str(warehouse)[:64], str(row.get('warehouse_name') or warehouse)[:255])
        )
    brand = fact.get('brand_ext_id')
    if brand:
        await tenant_db.execute(_upsert_dim_stmt(DimBrand, str(brand)[:64], str(row.get('brand_name') or brand)[:255]))
    category = fact.get('category_ext_id')
    if category:
        await tenant_db.execute(
            _upsert_dim_stmt(DimCategory, str(category)[:64], str(row.get('category_name') or category)[:255])
        )
    group = fact.get('group_ext_id')
    if group:
        await tenant_db.execute(_upsert_dim_stmt(DimGroup, str(group)[:64], str(row.get('group_name') or group)[:255]))
    item = fact.get('item_code')
    if item:
        item_external_id = str(item)[:128]
        ins = insert(DimItem).values(external_id=item_external_id, sku=item_external_id, name=str(row.get('item_name') or item)[:255])
        await tenant_db.execute(
            ins.on_conflict_do_update(
                index_elements=['external_id'],
                set_={
                    'updated_at': datetime.utcnow(),
                    'sku': ins.excluded.sku,
                    'name': ins.excluded.name,
                },
            )
        )
    if entity == 'purchases':
        supplier = fact.get('supplier_ext_id')
        if supplier:
            await tenant_db.execute(
                _upsert_dim_stmt(DimSupplier, str(supplier)[:64], str(row.get('supplier_name') or supplier)[:255])
            )


async def _load_sync_state(tenant_db: AsyncSession, connector_type: str) -> SyncState:
    state = (await tenant_db.execute(select(SyncState).where(SyncState.connector_type == connector_type))).scalar_one_or_none()
    if state:
        return state
    state = SyncState(connector_type=connector_type)
    tenant_db.add(state)
    await tenant_db.flush()
    return state


def _build_context(connection: TenantConnection | None) -> ConnectorContext:
    source_connection_string = None
    if connection and connection.enc_payload:
        secret = decrypt_sqlserver_secret(connection.enc_payload)
        source_connection_string = build_odbc_connection_string(secret)

    return ConnectorContext(
        tenant_slug='',
        incremental_column=(connection.incremental_column if connection else 'updated_at'),
        id_column=(connection.id_column if connection else 'id'),
        date_column=(connection.date_column if connection else 'doc_date'),
        branch_column=(connection.branch_column if connection else 'branch_ext_id'),
        item_column=(connection.item_column if connection else 'item_code'),
        amount_column=(connection.amount_column if connection else 'net_value'),
        cost_column=(connection.cost_column if connection else 'cost_amount'),
        qty_column=(connection.qty_column if connection else 'qty'),
        source_connection_string=source_connection_string,
        sales_query=(connection.sales_query_template if connection else None),
        purchases_query=(connection.purchases_query_template if connection else None),
    )


def _build_fact(entity: str, row: dict[str, Any], context: ConnectorContext, default_prefix: str) -> tuple[dict[str, Any], Any]:
    incremental_val = row.get(context.incremental_column) or row.get('updated_at') or row.get('doc_date') or row.get('event_id')
    updated_at = _as_datetime(incremental_val) or datetime.utcnow()

    raw_external = row.get('external_id') or row.get('event_id') or f"{default_prefix}:{incremental_val}:{row.get(context.branch_column)}:{row.get(context.item_column)}"
    external_id = str(raw_external)[:128]

    qty = float(row.get(context.qty_column) or row.get('qty') or 0)
    net = float(row.get(context.amount_column) or row.get('net_value') or 0)
    cost = float(row.get(context.cost_column) or row.get('cost_amount') or 0)

    base = {
        'external_id': external_id,
        'event_id': str(row.get('event_id') or external_id)[:128],
        'doc_date': _as_doc_date(row.get('doc_date') or incremental_val),
        'updated_at': updated_at,
        'branch_ext_id': (str(row.get(context.branch_column) or row.get('branch_ext_id'))[:64] if row.get(context.branch_column) is not None or row.get('branch_ext_id') is not None else None),
        'warehouse_ext_id': (str(row.get('warehouse_ext_id'))[:64] if row.get('warehouse_ext_id') is not None else None),
        'brand_ext_id': (str(row.get('brand_ext_id'))[:64] if row.get('brand_ext_id') is not None else None),
        'category_ext_id': (str(row.get('category_ext_id'))[:64] if row.get('category_ext_id') is not None else None),
        'group_ext_id': (str(row.get('group_ext_id'))[:64] if row.get('group_ext_id') is not None else None),
        'item_code': (str(row.get(context.item_column) or row.get('item_code'))[:128] if row.get(context.item_column) is not None or row.get('item_code') is not None else None),
        'qty': qty,
        'net_value': net,
        'cost_amount': cost,
    }

    if entity == 'sales':
        base['gross_value'] = float(row.get('gross_value') or net)
        base['profit_amount'] = float(base['gross_value']) - cost
    else:
        base['supplier_ext_id'] = (str(row.get('supplier_ext_id'))[:64] if row.get('supplier_ext_id') is not None else None)

    return base, incremental_val


async def process_job(job: dict[str, Any]) -> dict[str, Any]:
    tenant_slug = job['tenant_slug']
    connector_type = job['connector']
    entity = job['entity']
    payload = job.get('payload') or {}

    connector = CONNECTORS.get(connector_type)
    if connector is None:
        raise RuntimeError(f'Unknown connector: {connector_type}')

    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if not tenant:
            raise RuntimeError(f'Tenant not found: {tenant_slug}')

        connection = None
        if connector_type == 'pharmacyone_sql':
            connection = (
                await control_db.execute(
                    select(TenantConnection).where(
                        TenantConnection.tenant_id == tenant.id,
                        TenantConnection.connector_type == 'pharmacyone_sql',
                    )
                )
            ).scalar_one_or_none()
            if not connection:
                raise RuntimeError(f'No SQL Server mapping found for tenant {tenant_slug}')

        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            context = _build_context(connection)
            context.tenant_slug = tenant_slug
            sync_state = await _load_sync_state(tenant_db, f'{connector_type}_{entity}')
            ignore_sync_state = bool(payload.get('ignore_sync_state'))
            inc_state = IncrementalState(
                last_sync_timestamp=None if ignore_sync_state else sync_state.last_sync_timestamp,
                last_sync_id=None if ignore_sync_state else sync_state.last_sync_id,
            )

            rows = connector.fetch_rows(entity=entity, context=context, state=inc_state, payload=payload)

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

                fact, incremental_val = _build_fact(entity, row, context, default_prefix=entity[:1].upper())
                await _upsert_dims_from_row(tenant_db, entity, row, fact)
                stmt = _upsert_sales_stmt(fact) if entity == 'sales' else _upsert_purchases_stmt(fact)
                await tenant_db.execute(stmt)

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
            if connection is not None:
                connection.last_sync_at = last_ts or datetime.utcnow()
                connection.sync_status = 'ok'

            await tenant_db.commit()
        await control_db.commit()

    return {
        'status': 'ok',
        'tenant': tenant_slug,
        'connector': connector_type,
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
