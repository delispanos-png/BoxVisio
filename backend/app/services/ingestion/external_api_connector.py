from __future__ import annotations

from datetime import datetime

from app.services.ingestion.base import Connector, ConnectorContext, IncrementalState, IngestEntity


class ExternalApiIngestConnector(Connector):
    connector_name = 'external_api'

    def fetch_rows(
        self,
        *,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict | None = None,
    ) -> list[dict]:
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
