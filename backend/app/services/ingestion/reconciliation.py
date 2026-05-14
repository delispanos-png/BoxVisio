from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import Tenant, TenantConnection
from app.services.connection_secrets import build_odbc_connection_string, decrypt_sqlserver_secret
from app.services.querypacks import load_querypack
from app.services.sqlserver_connector import _bind_template_params, _connect


@dataclass(frozen=True)
class ReconcileStreamConfig:
    stream: str
    target_table: str
    target_date: str
    target_amount: str
    source_query_key: str
    source_date: str
    source_amount: str
    target_amount2: str | None = None
    source_amount2: str | None = None
    source_doc_field: str = 'external_id'
    target_doc_field: str = 'external_id'
    kind: str = 'monthly'


STREAM_CONFIGS: dict[str, ReconcileStreamConfig] = {
    'sales_documents': ReconcileStreamConfig(
        stream='sales_documents',
        target_table='fact_sales',
        target_date='doc_date',
        target_amount='net_value',
        target_amount2='gross_value',
        source_query_key='sales_documents',
        source_date='doc_date',
        source_amount='net_amount',
        source_amount2='gross_value',
        source_doc_field='document_id',
        target_doc_field='document_id',
    ),
    'purchase_documents': ReconcileStreamConfig(
        stream='purchase_documents',
        target_table='fact_purchases',
        target_date='doc_date',
        target_amount='net_value',
        target_amount2='cost_amount',
        source_query_key='purchase_documents',
        source_date='doc_date',
        source_amount='net_amount',
        source_amount2='cost_amount',
        source_doc_field='document_id',
        target_doc_field='document_id',
    ),
    'inventory_documents': ReconcileStreamConfig(
        stream='inventory_documents',
        target_table='fact_inventory',
        target_date='doc_date',
        target_amount='value_amount',
        target_amount2='qty_on_hand',
        source_query_key='inventory_documents',
        source_date='doc_date',
        source_amount='value_amount',
        source_amount2='qty_on_hand',
        source_doc_field='document_id',
        target_doc_field='document_id',
    ),
    'cash_transactions': ReconcileStreamConfig(
        stream='cash_transactions',
        target_table='fact_cashflows',
        target_date='doc_date',
        target_amount='amount',
        source_query_key='cash_transactions',
        source_date='doc_date',
        source_amount='amount',
    ),
    'operating_expenses': ReconcileStreamConfig(
        stream='operating_expenses',
        target_table='fact_expenses',
        target_date='expense_date',
        target_amount='amount_gross',
        target_amount2='amount_net',
        source_query_key='operating_expenses',
        source_date='expense_date',
        source_amount='amount_gross',
        source_amount2='amount_net',
    ),
    'supplier_balances': ReconcileStreamConfig(
        stream='supplier_balances',
        target_table='fact_supplier_balances',
        target_date='balance_date',
        target_amount='open_balance',
        target_amount2='overdue_balance',
        source_query_key='supplier_balances',
        source_date='balance_date',
        source_amount='open_balance',
        source_amount2='overdue_balance',
        kind='as_of',
    ),
    'customer_balances': ReconcileStreamConfig(
        stream='customer_balances',
        target_table='fact_customer_balances',
        target_date='balance_date',
        target_amount='open_balance',
        target_amount2='overdue_balance',
        source_query_key='customer_balances',
        source_date='balance_date',
        source_amount='open_balance',
        source_amount2='overdue_balance',
        kind='as_of',
    ),
}


def _num(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _company_id(conn: TenantConnection) -> int:
    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    auth = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
    return int(params.get('company_id') or params.get('company') or auth.get('company') or auth.get('COMPANY') or 1001)


def _templates(conn: TenantConnection) -> dict[str, str]:
    pack = load_querypack('erp_sql', 'default')
    mapping = conn.stream_query_mapping if isinstance(conn.stream_query_mapping, dict) else {}
    return {
        'sales_documents': str(mapping.get('sales_documents') or conn.sales_query_template or pack.sales_sql),
        'purchase_documents': str(mapping.get('purchase_documents') or conn.purchases_query_template or pack.purchases_sql),
        'inventory_documents': str(mapping.get('inventory_documents') or conn.inventory_query_template or pack.inventory_sql),
        'cash_transactions': str(mapping.get('cash_transactions') or conn.cashflow_query_template or pack.cashflow_sql),
        'operating_expenses': str(mapping.get('operating_expenses') or pack.expenses_sql),
        'supplier_balances': str(mapping.get('supplier_balances') or conn.supplier_balances_query_template or pack.supplier_balances_sql),
        'customer_balances': str(mapping.get('customer_balances') or conn.customer_balances_query_template or pack.customer_balances_sql),
    }


def _source_wrapper_sql(template: str, cfg: ReconcileStreamConfig) -> str:
    bucket_expr = (
        f"CONVERT(varchar(7), CAST(src.{cfg.source_date} AS date), 23)"
        if cfg.kind == 'monthly'
        else f"CONVERT(varchar(10), CAST(src.{cfg.source_date} AS date), 23)"
    )
    amount2_expr = (
        f"CAST(SUM(COALESCE(TRY_CAST(src.{cfg.source_amount2} AS decimal(28,8)), 0)) AS decimal(28,2))"
        if cfg.source_amount2
        else "CAST(NULL AS decimal(28,2))"
    )
    base = template.strip().rstrip(';')
    return f"""
SELECT
  {bucket_expr} AS bucket,
  COUNT_BIG(*) AS rows_count,
  COUNT(DISTINCT src.{cfg.source_doc_field}) AS docs_count,
  CAST(SUM(COALESCE(TRY_CAST(src.{cfg.source_amount} AS decimal(28,8)), 0)) AS decimal(28,2)) AS amount,
  {amount2_expr} AS amount2,
  MAX(src.updated_at) AS max_updated_at
FROM (
{base}
) src
GROUP BY {bucket_expr}
ORDER BY bucket
"""


def _bind(sql: str, *, from_date: date, to_date: date, company_id: int) -> tuple[str, list[Any]]:
    return _bind_template_params(
        sql,
        from_date=datetime.combine(from_date, time.min),
        to_date=datetime.combine(to_date, time.min),
        last_sync_timestamp=None,
        last_sync_id=None,
        company_id=company_id,
    )


def _source_stream(conn: TenantConnection, cfg: ReconcileStreamConfig, from_date: date, to_date: date) -> dict[str, dict[str, Any]]:
    template = _templates(conn)[cfg.source_query_key]
    sql = _source_wrapper_sql(template, cfg)
    bound_sql, params = _bind(sql, from_date=from_date, to_date=to_date, company_id=_company_id(conn))
    secret = decrypt_sqlserver_secret(conn.enc_payload)
    out: dict[str, dict[str, Any]] = {}
    with _connect(build_odbc_connection_string(secret), query_timeout=180) as db:
        cur = db.cursor()
        cur.execute(bound_sql, *params)
        for row in cur.fetchall():
            out[str(row[0])] = {
                'rows': int(row[1] or 0),
                'docs': int(row[2] or 0),
                'amount': round(_num(row[3]), 2),
                'amount2': None if row[4] is None else round(_num(row[4]), 2),
                'max_updated_at': str(row[5]) if row[5] is not None else None,
            }
    return out


async def _target_stream(
    tenant_db: AsyncSession,
    cfg: ReconcileStreamConfig,
    from_date: date,
    to_date: date,
) -> dict[str, dict[str, Any]]:
    bucket_expr = (
        f"TO_CHAR({cfg.target_date}, 'YYYY-MM')"
        if cfg.kind == 'monthly'
        else f"TO_CHAR({cfg.target_date}, 'YYYY-MM-DD')"
    )
    amount2_expr = (
        f"ROUND(COALESCE(SUM({cfg.target_amount2}), 0)::numeric, 2)"
        if cfg.target_amount2
        else "NULL::numeric"
    )
    sql = f"""
SELECT
  {bucket_expr} AS bucket,
  COUNT(*)::bigint AS rows_count,
  COUNT(DISTINCT {cfg.target_doc_field})::bigint AS docs_count,
  ROUND(COALESCE(SUM({cfg.target_amount}), 0)::numeric, 2) AS amount,
  {amount2_expr} AS amount2,
  MAX(updated_at) AS max_updated_at
FROM {cfg.target_table}
WHERE {cfg.target_date} >= :from_date AND {cfg.target_date} <= :to_date
GROUP BY {bucket_expr}
ORDER BY bucket
"""
    rows = (await tenant_db.execute(text(sql), {'from_date': from_date, 'to_date': to_date})).all()
    return {
        str(row[0]): {
            'rows': int(row[1] or 0),
            'docs': int(row[2] or 0),
            'amount': round(_num(row[3]), 2),
            'amount2': None if row[4] is None else round(_num(row[4]), 2),
            'max_updated_at': str(row[5]) if row[5] is not None else None,
        }
        for row in rows
    }


async def reconcile_stream(
    tenant_db: AsyncSession,
    conn: TenantConnection,
    stream: str,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    cfg = STREAM_CONFIGS[stream]
    source_from = from_date
    source_to = to_date
    target_from = from_date if cfg.kind == 'monthly' else to_date
    source = _source_stream(conn, cfg, source_from, source_to)
    target = await _target_stream(tenant_db, cfg, target_from, to_date)
    buckets = []
    for key in sorted(set(source) | set(target)):
        s = source.get(key, {'rows': 0, 'docs': 0, 'amount': 0.0, 'amount2': None})
        t = target.get(key, {'rows': 0, 'docs': 0, 'amount': 0.0, 'amount2': None})
        row = {
            'bucket': key,
            'source_docs': s['docs'],
            'bi_docs': t['docs'],
            'docs_delta': int(s['docs']) - int(t['docs']),
            'source_rows': s['rows'],
            'bi_rows': t['rows'],
            'rows_delta': int(s['rows']) - int(t['rows']),
            'source_amount': s['amount'],
            'bi_amount': t['amount'],
            'amount_delta': round(float(s['amount']) - float(t['amount']), 2),
            'source_amount2': s['amount2'],
            'bi_amount2': t['amount2'],
            'amount2_delta': None
            if s['amount2'] is None and t['amount2'] is None
            else round(_num(s['amount2']) - _num(t['amount2']), 2),
            'source_max_updated_at': s.get('max_updated_at'),
            'bi_max_updated_at': t.get('max_updated_at'),
        }
        buckets.append(row)
    mismatches = [
        row
        for row in buckets
        if row['docs_delta']
        or row['rows_delta']
        or abs(float(row['amount_delta'])) >= 0.01
        or (row['amount2_delta'] is not None and abs(float(row['amount2_delta'])) >= 0.01)
    ]
    return {
        'stream': stream,
        'kind': cfg.kind,
        'from': from_date.isoformat(),
        'to': to_date.isoformat(),
        'bucket_count': len(buckets),
        'mismatch_count': len(mismatches),
        'mismatches': mismatches,
    }


async def reconcile_tenant_streams(
    tenant: Tenant,
    tenant_db: AsyncSession,
    conn: TenantConnection,
    *,
    from_date: date,
    to_date: date,
    streams: list[str],
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for stream in streams:
        if stream not in STREAM_CONFIGS:
            continue
        results[stream] = await reconcile_stream(tenant_db, conn, stream, from_date, to_date)
    mismatch_count = sum(int(row.get('mismatch_count') or 0) for row in results.values())
    return {
        'tenant': tenant.slug,
        'from': from_date.isoformat(),
        'to': to_date.isoformat(),
        'mismatch_count': mismatch_count,
        'streams': results,
    }
