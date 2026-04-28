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

        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        auth_config = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
        company_id = params.get('company_id') or params.get('company') or auth_config.get('company') or auth_config.get('COMPANY')
        payload_data = payload or {}
        explicit_limit = payload_data.get('limit')
        effective_limit = max(
            100,
            int(
                explicit_limit
                or settings.sqlserver_default_fetch_limit
                or settings.incremental_sync_limit
                or 4000
            ),
        )
        exhaustive_requested = payload_data.get('ensure_complete')
        if exhaustive_requested is None:
            exhaustive_requested = settings.sqlserver_incremental_exhaustive_fetch
        exhaustive_mode = bool(exhaustive_requested)
        if (
            settings.sqlserver_period_sync_exhaustive_fetch
            and payload_data.get('from_date')
            and payload_data.get('to_date')
        ):
            exhaustive_mode = True

        rows = fetch_incremental_rows(
            connection_string=context.source_connection_string,
            query_template=query_template,
            incremental_column=context.incremental_column,
            id_column=context.id_column,
            date_column=context.date_column,
            last_sync_timestamp=state.last_sync_timestamp,
            last_sync_id=state.last_sync_id,
            from_date=payload_data.get('from_date'),
            to_date=payload_data.get('to_date'),
            company_id=company_id,
            limit=effective_limit,
            exhaustive=exhaustive_mode,
            max_pages=settings.sqlserver_period_sync_max_pages,
            retries=settings.ingest_job_max_retries,
            retry_sleep_sec=settings.sqlserver_retry_sleep_seconds,
        )
        return rows


class GenericSqlConnector(PharmacyOneSqlConnector):
    connector_name = 'sql_connector'
