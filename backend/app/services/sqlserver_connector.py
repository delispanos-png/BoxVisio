from collections.abc import Iterable
from datetime import datetime
import re
import time
from typing import Any

from app.core.config import settings

DEFAULT_GENERIC_SALES_QUERY = "SELECT TOP 5000 * FROM dbo.SalesLines"

DEFAULT_GENERIC_PURCHASES_QUERY = "SELECT TOP 5000 * FROM dbo.PurchaseLines"

DEFAULT_GENERIC_INVENTORY_QUERY = "SELECT TOP 5000 * FROM dbo.InventorySnapshots"

DEFAULT_GENERIC_CASHFLOW_QUERY = "SELECT TOP 5000 * FROM dbo.CashflowEntries"
DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY = "SELECT TOP 5000 * FROM dbo.SupplierBalances"
DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY = "SELECT TOP 5000 * FROM dbo.CustomerBalances"
DEFAULT_GENERIC_EXPENSES_QUERY = "SELECT TOP 5000 * FROM dbo.OperatingExpenses"

TABLE_KEYWORDS = ['sale', 'sales', 'invoice', 'inv', 'receipt', 'purchase', 'buy', 'mov', 'movement', 'doc', 'document']
COLUMN_HINTS = {
    'date': ['date', 'docdate', 'created', 'updated', 'datetime', 'timestamp'],
    'qty': ['qty', 'quantity', 'qnt', 'pieces'],
    'amount': ['amount', 'net', 'total', 'value', 'gross'],
    'cost': ['cost', 'buy', 'purchasecost', 'costvalue'],
    'branch': ['branch', 'store', 'shop', 'location', 'site'],
}


def _connect(connection_string: str, query_timeout: int = 0):
    try:
        import pyodbc
    except Exception as exc:
        raise RuntimeError('pyodbc runtime is unavailable (missing unixODBC libraries)') from exc
    conn = pyodbc.connect(connection_string.replace('{ODBC_DRIVER}', settings.odbc_driver), timeout=30)
    # conn.timeout sets the per-statement query timeout (distinct from the login timeout above).
    # pyodbc cursors do not expose a .timeout attribute, so hasattr(cur, 'timeout') is always False.
    if query_timeout > 0:
        conn.timeout = query_timeout
    _prepare_read_session(conn)
    return conn


def _prepare_read_session(conn: Any) -> None:
    statements: list[str] = []
    if bool(getattr(settings, 'sqlserver_read_uncommitted', True)):
        statements.append('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
    lock_timeout_ms = int(getattr(settings, 'sqlserver_lock_timeout_ms', 15000) or 0)
    if lock_timeout_ms > 0:
        statements.append(f'SET LOCK_TIMEOUT {max(1000, min(lock_timeout_ms, 120000))}')
    statements.append('SET DEADLOCK_PRIORITY LOW')
    if not statements:
        return
    cur = conn.cursor()
    try:
        for statement in statements:
            cur.execute(statement)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _quote_identifier(identifier: str) -> str:
    safe = ''.join(ch for ch in str(identifier) if ch.isalnum() or ch == '_')
    if not safe:
        raise ValueError('Invalid identifier')
    return f'[{safe}]'


def _build_final_query(
    query_template: str,
    *,
    incremental_column: str,
    id_column: str,
    date_column: str,
    limit: int | None = None,
) -> str:
    base = (query_template or '').strip().rstrip(';')
    if not base:
        raise ValueError('query_template is empty')

    inc_col = _quote_identifier(incremental_column)
    id_col = _quote_identifier(id_column)
    dt_col = _quote_identifier(date_column)

    top_clause = ''
    if limit is not None:
        top_n = max(1, min(int(limit), 10000))
        top_clause = f'TOP {top_n} '

    wrapped = (
        f"SELECT {top_clause}* FROM ("
        f"{base}"
        ") AS src "
        "WHERE (? IS NULL OR src.{date_col} >= ?) "
        "AND (? IS NULL OR src.{date_col} <= ?) "
        "AND ("
        "  ? IS NULL OR src.{inc_col} > ? "
        "  OR (src.{inc_col} = ? AND (? IS NULL OR src.{id_col} > ?))"
        ") "
        "ORDER BY src.{inc_col} ASC, src.{id_col} ASC"
    ).format(date_col=dt_col, inc_col=inc_col, id_col=id_col)
    return wrapped


def _template_has_bound_filters(query_template: str) -> bool:
    text = str(query_template or '').lower()
    return any(token in text for token in ('@last_sync_ts', '@from_date', '@to_date', '@company_id'))


def _add_top_to_select(query: str, limit: int | None) -> str:
    if limit is None:
        return query
    top_n = max(1, min(int(limit), 10000))
    text = str(query or '').lstrip()
    leading_ws_len = len(query) - len(text)
    if not text[:6].lower() == 'select':
        return query
    if text[:20].lower().startswith('select top '):
        return query
    return query[:leading_ws_len] + f'SELECT TOP {top_n} ' + text[6:].lstrip()


def _append_final_order_by(query: str, incremental_column: str, id_column: str) -> str:
    base = str(query or '').strip().rstrip(';')
    inc_col = _safe_identifier(incremental_column)
    id_col = _safe_identifier(id_column)
    return f'{base} ORDER BY {inc_col} ASC, {id_col} ASC'


def _bind_template_params(
    query_template: str,
    *,
    from_date: Any,
    to_date: Any,
    last_sync_timestamp: Any,
    last_sync_id: Any,
    company_id: Any = None,
) -> tuple[str, list[Any]]:
    tokens = ['@from_date', '@to_date', '@last_sync_ts', '@last_sync_id', '@company_id']
    params_map = {
        '@from_date': from_date,
        '@to_date': to_date,
        '@last_sync_ts': last_sync_timestamp,
        '@last_sync_id': last_sync_id,
        '@company_id': company_id,
    }
    params: list[Any] = []
    token_re = re.compile('|'.join(re.escape(token) for token in tokens))

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        params.append(params_map[token])
        return '?'

    normalized = token_re.sub(_replace, query_template)
    return normalized, params


def _normalize_param_datetime(value: Any) -> Any:
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        for candidate in (raw, f'{raw}T00:00:00'):
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                continue
    return value


def test_connection(connection_string: str) -> None:
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.fetchone()


def test_connection_with_version(connection_string: str) -> str:
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.fetchone()
        cur.execute('SELECT @@VERSION')
        row = cur.fetchone()
        if not row:
            return 'unknown'
        return str(row[0]).strip()


def discover_candidate_tables(connection_string: str) -> list[dict[str, Any]]:
    like_params = [f"%{k}%" for k in TABLE_KEYWORDS]
    where_like = ' OR '.join(['LOWER(TABLE_NAME) LIKE ?' for _ in like_params])

    query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE ({where_like})
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """

    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(query, *like_params)
        rows = cur.fetchall()

    return [
        {'schema': r[0], 'name': r[1], 'type': r[2]}
        for r in rows
    ]


def discover_columns(connection_string: str, schema: str, table: str) -> list[dict[str, Any]]:
    query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(query, schema, table)
        rows = cur.fetchall()

    out = []
    for r in rows:
        name = str(r[0])
        name_l = name.lower()
        tags = [tag for tag, words in COLUMN_HINTS.items() if any(w in name_l for w in words)]
        out.append({'name': name, 'data_type': r[1], 'tags': tags})
    return out


def _safe_identifier(identifier: str) -> str:
    ident = str(identifier).strip()
    if not ident:
        raise ValueError('Empty identifier')
    if not all(ch.isalnum() or ch == '_' for ch in ident):
        raise ValueError('Invalid identifier')
    return ident


def discover_sample_rows(connection_string: str, schema: str, table: str, top: int = 5) -> list[dict[str, Any]]:
    schema_id = _safe_identifier(schema)
    table_id = _safe_identifier(table)
    top_n = max(1, min(int(top), 50))
    query = f"SELECT TOP {top_n} * FROM [{schema_id}].[{table_id}]"
    with _connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(query)
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for col, value in zip(columns, row):
            if value is None or isinstance(value, (str, int, float, bool)):
                item[col] = value
            else:
                item[col] = str(value)
        out.append(item)
    return out


def discover_sqlserver(connection_string: str) -> list[dict[str, Any]]:
    tables = discover_candidate_tables(connection_string)
    discovered = []
    for t in tables:
        cols = discover_columns(connection_string, t['schema'], t['name'])
        discovered.append({**t, 'columns': cols})
    return discovered


def fetch_incremental_rows(
    connection_string: str,
    query_template: str,
    incremental_column: str,
    id_column: str,
    date_column: str,
    last_sync_timestamp: datetime | None = None,
    last_sync_id: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    company_id: str | int | None = None,
    limit: int | None = None,
    exhaustive: bool = False,
    max_pages: int = 10000,
    retries: int = 3,
    retry_sleep_sec: int = 2,
) -> Iterable[dict[str, Any]]:
    from_date = _normalize_param_datetime(from_date)
    to_date = _normalize_param_datetime(to_date)
    last_sync_timestamp = _normalize_param_datetime(last_sync_timestamp)

    fetch_batch_size = max(100, int(settings.sqlserver_fetch_batch_size or 1000))
    query_timeout = max(30, int(settings.sqlserver_query_timeout_seconds or 120))
    retries = max(1, int(getattr(settings, 'sqlserver_query_retries', retries) or retries))
    page_limit = max(1, min(int(limit), 10000)) if limit is not None else None
    cursor_ts = last_sync_timestamp
    cursor_id: Any = last_sync_id
    if cursor_id is not None and str(cursor_id).isdigit():
        cursor_id = int(cursor_id)
    total_pages = 0

    def _row_value(row_dict: dict[str, Any], key: str) -> Any:
        if key in row_dict:
            return row_dict[key]
        key_l = str(key).lower()
        for k, v in row_dict.items():
            if str(k).lower() == key_l:
                return v
        return None

    conn = _connect(connection_string, query_timeout=query_timeout)
    try:
        while True:
            total_pages += 1
            if exhaustive and total_pages > max(1, int(max_pages)):
                raise RuntimeError(f'max_pages exceeded while paging SQL rows (max_pages={max_pages})')

            templated_query, template_params = _bind_template_params(
                query_template,
                from_date=from_date,
                to_date=to_date,
                last_sync_timestamp=cursor_ts,
                last_sync_id=cursor_id,
                company_id=company_id,
            )
            if _template_has_bound_filters(query_template):
                effective_query = _append_final_order_by(
                    _add_top_to_select(templated_query, page_limit),
                    incremental_column=incremental_column,
                    id_column=id_column,
                )
                params = template_params
            else:
                effective_query = _build_final_query(
                    templated_query,
                    incremental_column=incremental_column,
                    id_column=id_column,
                    date_column=date_column,
                    limit=page_limit,
                )
                filter_params = [
                    from_date,
                    from_date,
                    to_date,
                    to_date,
                    cursor_ts,
                    cursor_ts,
                    cursor_ts,
                    cursor_id,
                    cursor_id,
                ]
                params = template_params + filter_params
            page_rows = 0
            page_last_ts = cursor_ts
            page_last_id = cursor_id

            for attempt in range(1, retries + 1):
                try:
                    cur = conn.cursor()
                    cur.execute(effective_query, *params)
                    columns = [col[0] for col in cur.description]
                    while True:
                        rows = cur.fetchmany(fetch_batch_size)
                        if not rows:
                            break
                        for row in rows:
                            row_dict = dict(zip(columns, row))
                            page_rows += 1
                            page_last_ts = _row_value(row_dict, incremental_column)
                            page_last_id = _row_value(row_dict, id_column)
                            yield row_dict
                    break
                except Exception:
                    if attempt >= retries:
                        raise
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(retry_sleep_sec)
                    conn = _connect(connection_string, query_timeout=query_timeout)

            if not exhaustive or page_limit is None:
                break
            if page_rows < page_limit:
                break
            if page_last_ts == cursor_ts and page_last_id == cursor_id:
                raise RuntimeError(
                    'cursor did not advance while paging SQL rows '
                    f'(inc={incremental_column}, id={id_column})'
                )
            cursor_ts = page_last_ts
            cursor_id = page_last_id
    finally:
        try:
            conn.close()
        except Exception:
            pass
