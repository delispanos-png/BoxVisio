from __future__ import annotations

from datetime import datetime

from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, Connector, ConnectorContext, IncrementalState, IngestEntity, OperationalIngestStream


class FileImportConnector(Connector):
    connector_name = 'file_import'
    source_type = 'file'
    supported_streams = ALL_OPERATIONAL_STREAMS
    required_connection_parameters = ('file_type', 'path_pattern', 'sftp_profile')

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

        # File parsers load normalized records into payload['records'].
        records = payload.get('records', [])
        out: list[dict] = []
        for record in records:
            row = dict(record)
            if row.get('updated_at') is None:
                row['updated_at'] = datetime.utcnow().isoformat()
            out.append(row)
        return out
