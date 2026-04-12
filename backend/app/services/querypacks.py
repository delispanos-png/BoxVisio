from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.models.control import TenantConnection

_PROVIDER_ALIASES: dict[str, str] = {
    'sql': 'pharmacyone',
    'sql_connector': 'pharmacyone',
    'erp_sql': 'pharmacyone',
    'erp_generic': 'pharmacyone',
}


@dataclass
class QueryPack:
    name: str
    version: int
    mapping: dict[str, Any]
    sales_sql: str
    purchases_sql: str
    inventory_sql: str | None = None
    cashflow_sql: str | None = None
    supplier_balances_sql: str | None = None
    customer_balances_sql: str | None = None
    expenses_sql: str | None = None


def _querypack_dir(provider: str) -> Path:
    base = Path(__file__).resolve().parents[2] / 'querypacks'
    resolved = _PROVIDER_ALIASES.get(str(provider or '').strip().lower(), provider)
    return base / str(resolved)


def _read_sql(root: Path, relative_candidates: list[str], *, required: bool) -> str | None:
    for rel in relative_candidates:
        candidate = root / rel
        if candidate.exists():
            return candidate.read_text(encoding='utf-8').strip()
    if required:
        tried = ', '.join(relative_candidates)
        raise FileNotFoundError(f'QueryPack SQL not found. Tried: {tried}')
    return None


def load_querypack(provider: str = 'erp_sql', pack_name: str = 'default') -> QueryPack:
    _ = pack_name
    root = _querypack_dir(provider)
    mapping = json.loads((root / 'mapping.json').read_text(encoding='utf-8'))
    # Production ingestion must always use row-level facts queries.
    # Keep fallback paths for backward compatibility with existing deployments.
    sales_sql = _read_sql(root, ['facts/sales_facts.sql', 'sales_facts.sql'], required=True)
    purchases_sql = _read_sql(root, ['facts/purchases_facts.sql', 'purchases_facts.sql'], required=True)
    inventory_sql = _read_sql(root, ['facts/inventory_facts.sql', 'inventory_facts.sql'], required=False)
    cashflow_sql = _read_sql(root, ['facts/cashflow_facts.sql', 'cashflow_facts.sql'], required=False)
    supplier_balances_sql = _read_sql(
        root,
        ['facts/supplier_balances_facts.sql', 'supplier_balances_facts.sql'],
        required=False,
    )
    customer_balances_sql = _read_sql(
        root,
        ['facts/customer_balances_facts.sql', 'customer_balances_facts.sql'],
        required=False,
    )
    expenses_sql = _read_sql(
        root,
        ['facts/expenses_facts.sql', 'expenses_facts.sql'],
        required=False,
    )

    return QueryPack(
        name=str(mapping.get('name') or 'unknown'),
        version=int(mapping.get('version') or 1),
        mapping=mapping,
        sales_sql=sales_sql,
        purchases_sql=purchases_sql,
        inventory_sql=inventory_sql,
        cashflow_sql=cashflow_sql,
        supplier_balances_sql=supplier_balances_sql,
        customer_balances_sql=customer_balances_sql,
        expenses_sql=expenses_sql,
    )


def apply_querypack_to_connection(conn: TenantConnection, pack: QueryPack) -> None:
    colmap = (pack.mapping.get('column_mappings') or {}) if isinstance(pack.mapping, dict) else {}
    conn.sales_query_template = pack.sales_sql
    conn.purchases_query_template = pack.purchases_sql
    if pack.inventory_sql:
        conn.inventory_query_template = pack.inventory_sql
    if pack.cashflow_sql:
        conn.cashflow_query_template = pack.cashflow_sql
    if pack.supplier_balances_sql:
        conn.supplier_balances_query_template = pack.supplier_balances_sql
    if pack.customer_balances_sql:
        conn.customer_balances_query_template = pack.customer_balances_sql
    if pack.expenses_sql:
        conn.stream_query_mapping = conn.stream_query_mapping or {}
        conn.stream_query_mapping['operating_expenses'] = pack.expenses_sql
    conn.incremental_column = str(colmap.get('incremental_column') or 'updated_at')
    conn.id_column = str(colmap.get('id_column') or 'external_id')
    conn.date_column = str(colmap.get('date_column') or 'doc_date')
    conn.branch_column = str(colmap.get('branch_column') or 'branch_external_id')
    conn.item_column = str(colmap.get('item_column') or 'item_external_id')
    conn.amount_column = str(colmap.get('amount_column') or 'net_amount')
    conn.cost_column = str(colmap.get('cost_column') or 'cost_amount')
    conn.qty_column = str(colmap.get('qty_column') or 'qty')
    conn.source_type = 'sql'
    conn.supported_streams = [
        'sales_documents',
        'purchase_documents',
        'inventory_documents',
        'cash_transactions',
        'supplier_balances',
        'customer_balances',
        'operating_expenses',
    ]
    # Keep operating expenses supported but disabled by default to avoid
    # breaking ingest on SQL sources that do not expose this object/table.
    default_enabled_streams = [
        'sales_documents',
        'purchase_documents',
        'inventory_documents',
        'cash_transactions',
        'supplier_balances',
        'customer_balances',
    ]
    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    if bool(params.get('enable_operating_expenses')):
        default_enabled_streams.append('operating_expenses')
    conn.enabled_streams = list(default_enabled_streams)
    conn.stream_query_mapping = {
        'sales_documents': conn.sales_query_template,
        'purchase_documents': conn.purchases_query_template,
        'inventory_documents': conn.inventory_query_template or '',
        'cash_transactions': conn.cashflow_query_template or '',
        'supplier_balances': conn.supplier_balances_query_template or '',
        'customer_balances': conn.customer_balances_query_template or '',
        'operating_expenses': (pack.expenses_sql or ''),
    }
    conn.stream_field_mapping = conn.stream_field_mapping or {}
    conn.stream_file_mapping = conn.stream_file_mapping or {}
    conn.stream_api_endpoint = conn.stream_api_endpoint or {}
    conn.connection_parameters = conn.connection_parameters or {'connector_type': conn.connector_type, 'source_type': 'sql'}
