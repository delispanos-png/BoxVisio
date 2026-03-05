from __future__ import annotations

from app.core.config import settings
from app.services.ingestion.base import Connector, ConnectorContext, IncrementalState, IngestEntity
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    fetch_incremental_rows,
)


class PharmacyOneSqlConnector(Connector):
    connector_name = 'pharmacyone_sql'

    def fetch_rows(
        self,
        *,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict | None = None,
    ) -> list[dict]:
        if not context.source_connection_string:
            return []

        query_template = context.sales_query if entity == 'sales' else context.purchases_query
        if not query_template:
            query_template = DEFAULT_GENERIC_SALES_QUERY if entity == 'sales' else DEFAULT_GENERIC_PURCHASES_QUERY

        rows = fetch_incremental_rows(
            connection_string=context.source_connection_string,
            query_template=query_template,
            incremental_column=context.incremental_column,
            id_column=context.id_column,
            date_column=context.date_column,
            last_sync_timestamp=state.last_sync_timestamp,
            last_sync_id=state.last_sync_id,
            from_date=(payload or {}).get('from_date'),
            to_date=(payload or {}).get('to_date'),
            retries=settings.ingest_job_max_retries,
            retry_sleep_sec=settings.sqlserver_retry_sleep_seconds,
        )
        return list(rows)
