from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

IngestEntity = Literal['sales', 'purchases', 'inventory', 'cashflows', 'supplier_balances', 'customer_balances']
OperationalIngestStream = Literal[
    'sales_documents',
    'purchase_documents',
    'inventory_documents',
    'cash_transactions',
    'supplier_balances',
    'customer_balances',
]
ConnectorSourceType = Literal['sql', 'api', 'file']

ALL_OPERATIONAL_STREAMS: tuple[OperationalIngestStream, ...] = (
    'sales_documents',
    'purchase_documents',
    'inventory_documents',
    'cash_transactions',
    'supplier_balances',
    'customer_balances',
)

STREAM_TO_ENTITY: dict[OperationalIngestStream, IngestEntity] = {
    'sales_documents': 'sales',
    'purchase_documents': 'purchases',
    'inventory_documents': 'inventory',
    'cash_transactions': 'cashflows',
    'supplier_balances': 'supplier_balances',
    'customer_balances': 'customer_balances',
}
ENTITY_TO_STREAM: dict[IngestEntity, OperationalIngestStream] = {
    entity: stream for stream, entity in STREAM_TO_ENTITY.items()
}


def normalize_stream_name(value: str | None) -> OperationalIngestStream | None:
    raw = str(value or '').strip().lower().replace('-', '_').replace(' ', '_')
    aliases = {
        'sales': 'sales_documents',
        'sales_docs': 'sales_documents',
        'sale_documents': 'sales_documents',
        'sale_docs': 'sales_documents',
        'purchases': 'purchase_documents',
        'purchase_docs': 'purchase_documents',
        'inventory': 'inventory_documents',
        'warehouse_documents': 'inventory_documents',
        'warehouse_docs': 'inventory_documents',
        'cashflows': 'cash_transactions',
        'cashflow': 'cash_transactions',
        'cash': 'cash_transactions',
        'supplier_balance': 'supplier_balances',
        'customer_balance': 'customer_balances',
    }
    candidate = aliases.get(raw, raw)
    if candidate in ALL_OPERATIONAL_STREAMS:
        return candidate  # type: ignore[return-value]
    return None


def normalize_stream_values(values: list[str] | tuple[str, ...] | set[str] | None) -> list[OperationalIngestStream]:
    if not values:
        return []
    out: list[OperationalIngestStream] = []
    seen: set[str] = set()
    for value in values:
        stream = normalize_stream_name(value)
        if stream and stream not in seen:
            out.append(stream)
            seen.add(stream)
    return out


@dataclass
class IncrementalState:
    last_sync_timestamp: datetime | None
    last_sync_id: str | None


@dataclass
class ConnectorContext:
    tenant_slug: str
    incremental_column: str
    id_column: str
    date_column: str
    branch_column: str
    item_column: str
    amount_column: str
    cost_column: str
    qty_column: str
    source_type: str = 'sql'
    supported_streams: list[OperationalIngestStream] = field(default_factory=list)
    enabled_streams: list[OperationalIngestStream] = field(default_factory=list)
    source_connection_string: str | None = None
    connection_parameters: dict[str, Any] = field(default_factory=dict)
    stream_query_mapping: dict[str, str] = field(default_factory=dict)
    stream_field_mapping: dict[str, dict[str, str]] = field(default_factory=dict)
    stream_file_mapping: dict[str, Any] = field(default_factory=dict)
    stream_api_endpoint: dict[str, str] = field(default_factory=dict)
    sales_query: str | None = None
    purchases_query: str | None = None
    inventory_query: str | None = None
    cashflow_query: str | None = None
    supplier_balances_query: str | None = None
    customer_balances_query: str | None = None

    def stream_enabled(self, stream: OperationalIngestStream) -> bool:
        if self.enabled_streams:
            return stream in self.enabled_streams
        if self.supported_streams:
            return stream in self.supported_streams
        return True

    def stream_query(self, stream: OperationalIngestStream) -> str | None:
        mapped = (self.stream_query_mapping or {}).get(stream)
        if isinstance(mapped, str) and mapped.strip():
            return mapped
        if stream == 'sales_documents':
            return self.sales_query
        if stream == 'purchase_documents':
            return self.purchases_query
        if stream == 'inventory_documents':
            return self.inventory_query
        if stream == 'cash_transactions':
            return self.cashflow_query
        if stream == 'supplier_balances':
            return self.supplier_balances_query
        return self.customer_balances_query


class Connector(ABC):
    connector_name: str
    source_type: ConnectorSourceType = 'sql'
    supported_streams: tuple[OperationalIngestStream, ...] = ALL_OPERATIONAL_STREAMS
    required_connection_parameters: tuple[str, ...] = ()

    def declaration(self) -> dict[str, Any]:
        return {
            'connector_type': self.connector_name,
            'source_type': self.source_type,
            'connection_parameters': list(self.required_connection_parameters),
            'supported_streams': list(self.supported_streams),
        }

    @abstractmethod
    def fetch_rows(
        self,
        *,
        stream: OperationalIngestStream,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError
