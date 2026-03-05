from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

IngestEntity = Literal['sales', 'purchases']


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
    source_connection_string: str | None = None
    sales_query: str | None = None
    purchases_query: str | None = None


class Connector(ABC):
    connector_name: str

    @abstractmethod
    def fetch_rows(
        self,
        *,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError
