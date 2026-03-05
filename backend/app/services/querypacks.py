from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.models.control import TenantConnection


@dataclass
class QueryPack:
    name: str
    version: int
    mapping: dict[str, Any]
    sales_sql: str
    purchases_sql: str
    inventory_sql: str | None = None
    cashflow_sql: str | None = None


def _querypack_dir(provider: str) -> Path:
    base = Path(__file__).resolve().parents[2] / 'querypacks'
    return base / provider


def _read_sql(root: Path, relative_candidates: list[str], *, required: bool) -> str | None:
    for rel in relative_candidates:
        candidate = root / rel
        if candidate.exists():
            return candidate.read_text(encoding='utf-8').strip()
    if required:
        tried = ', '.join(relative_candidates)
        raise FileNotFoundError(f'QueryPack SQL not found. Tried: {tried}')
    return None


def load_querypack(provider: str = 'pharmacyone', pack_name: str = 'default') -> QueryPack:
    _ = pack_name
    root = _querypack_dir(provider)
    mapping = json.loads((root / 'mapping.json').read_text(encoding='utf-8'))
    # Production ingestion must always use row-level facts queries.
    # Keep fallback paths for backward compatibility with existing deployments.
    sales_sql = _read_sql(root, ['facts/sales_facts.sql', 'sales_facts.sql'], required=True)
    purchases_sql = _read_sql(root, ['facts/purchases_facts.sql', 'purchases_facts.sql'], required=True)
    inventory_sql = _read_sql(root, ['facts/inventory_facts.sql', 'inventory_facts.sql'], required=False)
    cashflow_sql = _read_sql(root, ['facts/cashflow_facts.sql', 'cashflow_facts.sql'], required=False)

    return QueryPack(
        name=str(mapping.get('name') or 'unknown'),
        version=int(mapping.get('version') or 1),
        mapping=mapping,
        sales_sql=sales_sql,
        purchases_sql=purchases_sql,
        inventory_sql=inventory_sql,
        cashflow_sql=cashflow_sql,
    )


def apply_querypack_to_connection(conn: TenantConnection, pack: QueryPack) -> None:
    colmap = (pack.mapping.get('column_mappings') or {}) if isinstance(pack.mapping, dict) else {}
    conn.sales_query_template = pack.sales_sql
    conn.purchases_query_template = pack.purchases_sql
    conn.incremental_column = str(colmap.get('incremental_column') or 'updated_at')
    conn.id_column = str(colmap.get('id_column') or 'external_id')
    conn.date_column = str(colmap.get('date_column') or 'doc_date')
    conn.branch_column = str(colmap.get('branch_column') or 'branch_external_id')
    conn.item_column = str(colmap.get('item_column') or 'item_external_id')
    conn.amount_column = str(colmap.get('amount_column') or 'net_amount')
    conn.cost_column = str(colmap.get('cost_column') or 'cost_amount')
    conn.qty_column = str(colmap.get('qty_column') or 'qty')
