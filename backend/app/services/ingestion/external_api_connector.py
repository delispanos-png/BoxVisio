from __future__ import annotations

from datetime import datetime

from app.services.ingestion.base import (
    Connector,
    ConnectorContext,
    IncrementalState,
    IngestEntity,
    OperationalIngestStream,
)


class ExternalApiIngestConnector(Connector):
    connector_name = 'external_api'
    source_type = 'api'
    supported_streams = ('sales_documents', 'purchase_documents')
    required_connection_parameters = ('base_url', 'auth_type', 'auth_config')

    def fetch_rows(
        self,
        *,
        stream: OperationalIngestStream,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict | None = None,
    ) -> list[dict]:
        del stream
        del entity
        del context
        del state

        if not payload:
            return []

        records = payload.get('records', [])
        out: list[dict] = []
        for record in records:
            row = dict(record)
            if row.get('updated_at') is None:
                row['updated_at'] = datetime.utcnow().isoformat()
            out.append(row)
        return out
