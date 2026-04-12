from __future__ import annotations

from app.core.config import settings
from app.services.ingestion.base import (
    ALL_OPERATIONAL_STREAMS,
    Connector,
    ConnectorContext,
    IncrementalState,
    IngestEntity,
    OperationalIngestStream,
)
from app.services.sqlserver_connector import (
    DEFAULT_GENERIC_CASHFLOW_QUERY,
    DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY,
    DEFAULT_GENERIC_EXPENSES_QUERY,
    DEFAULT_GENERIC_INVENTORY_QUERY,
    DEFAULT_GENERIC_PURCHASES_QUERY,
    DEFAULT_GENERIC_SALES_QUERY,
    DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY,
    fetch_incremental_rows,
)


class PharmacyOneSqlConnector(Connector):
    connector_name = 'pharmacyone_sql'
    source_type = 'sql'
    supported_streams = ALL_OPERATIONAL_STREAMS
    required_connection_parameters = ('host', 'port', 'database', 'username', 'password', 'options')

    def fetch_rows(
        self,
        *,
        stream: OperationalIngestStream,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict | None = None,
    ) -> list[dict]:
        del entity
        if not context.source_connection_string:
            return []

        mapped_query = context.stream_query(stream)
        if stream == 'sales_documents':
            query_template = mapped_query or DEFAULT_GENERIC_SALES_QUERY
        elif stream == 'purchase_documents':
            query_template = mapped_query or DEFAULT_GENERIC_PURCHASES_QUERY
        elif stream == 'inventory_documents':
            query_template = mapped_query or DEFAULT_GENERIC_INVENTORY_QUERY
        elif stream == 'cash_transactions':
            query_template = mapped_query or DEFAULT_GENERIC_CASHFLOW_QUERY
        elif stream == 'supplier_balances':
            query_template = mapped_query or DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY
        elif stream == 'customer_balances':
            query_template = mapped_query or DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY
        else:
            query_template = mapped_query or DEFAULT_GENERIC_EXPENSES_QUERY

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
        return rows


class GenericSqlConnector(PharmacyOneSqlConnector):
    connector_name = 'sql_connector'
