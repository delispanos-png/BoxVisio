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


LIVE_SALES_LIGHT_QUERY = """
SELECT
  CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CONVERT(varchar(10), F.TRNDATE, 23) AS doc_date,
  CONVERT(varchar(19), ISNULL(F.UPDDATE, F.TRNDATE), 126) AS updated_at,
  CAST(F.FINDOC AS varchar(128)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS varchar(128)) AS document_no,
  CAST(ISNULL(F.SERIES, 0) AS varchar(128)) AS document_series,
  CAST('sales_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS varchar(16)) AS varchar(128)) AS document_type,
  CAST(CASE
    WHEN ISNULL(F.ISCANCEL, 0) = 1 THEN 'Cancelled'
    WHEN ISNULL(F.FINSTATES, 0) IN (10) THEN 'Completed'
    WHEN ISNULL(F.FINSTATES, 0) IN (3, 4, 6) THEN 'Closed'
    WHEN ISNULL(F.FINSTATES, 0) IN (1, 2) THEN 'Open'
    ELSE 'Open'
  END AS varchar(128)) AS document_status,
  CAST(ISNULL(F.BRANCH, 0) AS varchar(64)) AS branch_ext_id,
  CAST(ISNULL(BR.NAME, CAST(ISNULL(F.BRANCH, 0) AS varchar(64))) AS varchar(255)) AS branch_name,
  CAST(ISNULL(F.COMPANY, 0) AS varchar(64)) AS company_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS varchar(64)) AS warehouse_ext_id,
  CAST(ISNULL(W.NAME, CAST(ISNULL(MD.WHOUSE, 0) AS varchar(64))) AS varchar(255)) AS warehouse_name,
  CAST(ISNULL(C.CODE, F.TRDR) AS varchar(128)) AS customer_code,
  CAST(ISNULL(C.NAME, '') AS varchar(255)) AS customer_name,
  CAST(ISNULL(I.CODE, L.MTRL) AS varchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS varchar(255)) AS item_name,
  CAST(COALESCE(NULLIF(CAST(ISNULL(F.CCC88ECHANNEL, 0) AS varchar(64)), '0'), CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(F.CCC88EORDERNO, ''))), '') IS NOT NULL THEN '1' ELSE NULL END) AS varchar(64)) AS channel_ext_id,
  CAST(CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(F.CCC88EORDERNO, ''))), '') IS NOT NULL THEN 'Site' ELSE '' END AS varchar(255)) AS channel_name,
  CAST(ISNULL(F.CCC88EORDERNO, '') AS varchar(128)) AS eshop_code,
  CAST(NULL AS varchar(128)) AS payment_method,
  CAST(NULL AS varchar(128)) AS shipping_method,
  CAST(ISNULL(F.COMMENTS, '') AS varchar(255)) AS reason,
  CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS int) AS line_no,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS float) AS qty,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS float) AS net_value,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * (ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) + ISNULL(L.VATAMNT, 0)) AS float) AS gross_value,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(L.VATAMNT, 0) AS float) AS vat_amount,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(L.SALESCVAL, ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS float) AS cost_amount,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.NETAMNT, 0) AS float) AS doc_net_total,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.VATAMNT, 0) AS float) AS doc_tax_total,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.SUMAMNT, 0) AS float) AS doc_gross_total,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.EXPN, 0) AS float) AS shipping_expense_value,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.EXPN, 0) AS float) AS charge_revenue_net_value,
  CAST(0 AS float) AS charge_revenue_vat_value,
  CAST((CASE
    WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
    WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
    ELSE 1
  END) * ISNULL(F.EXPN, 0) AS float) AS charge_revenue_gross_value,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.TFPRMS, 0) AS int) AS source_transaction_type_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id
FROM (
  SELECT TOP 5000
    F.FINDOC,
    F.COMPANY,
    F.TRNDATE,
    F.UPDDATE,
    ISNULL(F.UPDDATE, F.TRNDATE) AS UPDATED_AT_RAW,
    F.FINCODE,
    F.SERIES,
    F.SOSOURCE,
    F.SOREDIR,
    F.SODTYPE,
    F.TFPRMS,
    F.EXPN,
    F.NETAMNT,
    F.VATAMNT,
    F.SUMAMNT,
    F.ISCANCEL,
    F.FINSTATES,
    F.BRANCH,
    F.TRDR,
    F.CCC88ECHANNEL,
    F.CCC88EORDERNO,
    F.COMMENTS
  FROM FINDOC F WITH (NOLOCK)
  WHERE
    (@company_id IS NULL OR F.COMPANY = @company_id)
    AND ISNULL(F.SODTYPE, 0) = 13
    AND (
      (
        ISNULL(F.SOSOURCE, 0) = 1351
        AND ISNULL(F.SOREDIR, 0) IN (0, 10000)
        AND ISNULL(F.TFPRMS, 0) IN (102, 103, 131, 151, 152, 181)
      )
      OR (
        ISNULL(F.SOSOURCE, 0) <> 1351
        AND ISNULL(F.TFPRMS, 0) IN (101, 102, 131, 181)
      )
    )
    AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
    AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
    AND (@last_sync_ts IS NULL OR ISNULL(F.UPDDATE, F.TRNDATE) >= @last_sync_ts)
  ORDER BY ISNULL(F.UPDDATE, F.TRNDATE) ASC, F.FINDOC ASC
) F
INNER JOIN MTRLINES L WITH (NOLOCK) ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
LEFT JOIN MTRDOC MD WITH (NOLOCK) ON MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK) ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN WHOUSE W WITH (NOLOCK) ON W.WHOUSE = MD.WHOUSE AND W.COMPANY = F.COMPANY
LEFT JOIN TRDR C WITH (NOLOCK) ON C.TRDR = F.TRDR AND C.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK) ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
WHERE
  (
    @last_sync_ts IS NULL
    OR F.UPDATED_AT_RAW > @last_sync_ts
    OR (
      F.UPDATED_AT_RAW = @last_sync_ts
      AND CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
"""


def _use_light_live_sales_query(stream: OperationalIngestStream, payload: dict | None, params: dict) -> bool:
    if stream != 'sales_documents':
        return False
    payload_data = payload or {}
    if payload_data.get('backfill') or payload_data.get('ensure_complete') or payload_data.get('ignore_sync_state'):
        return False
    raw = params.get('use_light_live_sales_query')
    if raw is None:
        raw = params.get('live_sales_light_query')
    if raw is None:
        return True
    return str(raw).strip().lower() not in {'0', 'false', 'no', 'off'}


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

        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        mapped_query = context.stream_query(stream)
        if stream == 'sales_documents':
            query_template = LIVE_SALES_LIGHT_QUERY if _use_light_live_sales_query(stream, payload, params) else (mapped_query or DEFAULT_GENERIC_SALES_QUERY)
        elif stream == 'purchase_documents':
            query_template = mapped_query or DEFAULT_GENERIC_PURCHASES_QUERY
        elif stream == 'inventory_documents':
            query_template = mapped_query or DEFAULT_GENERIC_INVENTORY_QUERY
        elif stream == 'item_master':
            query_template = mapped_query
            if not query_template:
                return []
        elif stream == 'cash_transactions':
            query_template = mapped_query or DEFAULT_GENERIC_CASHFLOW_QUERY
        elif stream == 'supplier_balances':
            query_template = mapped_query or DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY
        elif stream == 'customer_balances':
            query_template = mapped_query or DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY
        elif stream == 'supplier_orders':
            query_template = mapped_query
            if not query_template:
                return []
        else:
            query_template = mapped_query or DEFAULT_GENERIC_EXPENSES_QUERY

        auth_config = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
        company_id = params.get('company_id') or params.get('company') or auth_config.get('company') or auth_config.get('COMPANY')
        payload_data = payload or {}
        explicit_limit = payload_data.get('limit')
        stream_limit_cap = 10000
        if stream in {'supplier_balances', 'customer_balances'} and not payload_data.get('backfill'):
            stream_limit_cap = 250
        effective_limit: int | None = max(
            100,
            min(
                stream_limit_cap,
                int(
                    explicit_limit
                    or settings.sqlserver_default_fetch_limit
                    or settings.incremental_sync_limit
                    or 4000
                ),
            ),
        )
        exhaustive_requested = payload_data.get('ensure_complete')
        if exhaustive_requested is None:
            exhaustive_requested = settings.sqlserver_incremental_exhaustive_fetch
        exhaustive_mode = bool(exhaustive_requested)
        if stream == 'inventory_documents' and (
            bool(payload_data.get('ensure_complete'))
            or bool(payload_data.get('ignore_sync_state'))
            or bool(payload_data.get('full_snapshot'))
        ):
            # Inventory balance snapshots already bind @last_sync_ts inside the
            # template. Adding TOP/paged cursoring turns a full stock snapshot
            # into a partial sample, so complete snapshot runs must execute as
            # one unbounded read.
            effective_limit = None
            exhaustive_mode = False
        if stream == 'item_master':
            # Full item master is dimension data. Do not page by date cursor,
            # otherwise unmoving items keep missing barcode/VAT/status.
            effective_limit = None
            exhaustive_mode = False
        if stream in {'supplier_balances', 'customer_balances'} and (
            bool(payload_data.get('ensure_complete'))
            or bool(payload_data.get('ignore_sync_state'))
            or bool(payload_data.get('full_snapshot'))
            or bool(payload_data.get('backfill'))
        ):
            # Balance streams are "as-of date" snapshots. The PharmacyOne/SoftOne
            # templates intentionally do not page by the incremental cursor, so
            # applying TOP + exhaustive paging can reread the same page forever.
            effective_limit = None
            exhaustive_mode = False
        if stream in {'supplier_balances', 'customer_balances'} and not payload_data.get('backfill'):
            exhaustive_mode = False
        if (
            settings.sqlserver_period_sync_exhaustive_fetch
            and payload_data.get('from_date')
            and payload_data.get('to_date')
            and (stream not in {'supplier_balances', 'customer_balances'} or payload_data.get('backfill'))
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
