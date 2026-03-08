from collections.abc import Iterable
from datetime import datetime
import time
from typing import Any

from app.core.config import settings

DEFAULT_GENERIC_SALES_QUERY = "SELECT TOP 5000 * FROM dbo.SalesLines"

DEFAULT_GENERIC_PURCHASES_QUERY = "SELECT TOP 5000 * FROM dbo.PurchaseLines"

DEFAULT_GENERIC_INVENTORY_QUERY = "SELECT TOP 5000 * FROM dbo.InventorySnapshots"

DEFAULT_GENERIC_CASHFLOW_QUERY = "SELECT TOP 5000 * FROM dbo.CashflowEntries"
DEFAULT_GENERIC_SUPPLIER_BALANCES_QUERY = "SELECT TOP 5000 * FROM dbo.SupplierBalances"
DEFAULT_GENERIC_CUSTOMER_BALANCES_QUERY = "SELECT TOP 5000 * FROM dbo.CustomerBalances"

TABLE_KEYWORDS = ['sale', 'sales', 'invoice', 'inv', 'receipt', 'purchase', 'buy', 'mov', 'movement', 'doc', 'document']
COLUMN_HINTS = {
    'date': ['date', 'docdate', 'created', 'updated', 'datetime', 'timestamp'],
    'qty': ['qty', 'quantity', 'qnt', 'pieces'],
    'amount': ['amount', 'net', 'total', 'value', 'gross'],
    'cost': ['cost', 'buy', 'purchasecost', 'costvalue'],
    'branch': ['branch', 'store', 'shop', 'location', 'site'],
}


def _connect(connection_string: str):
    try:
        import pyodbc
    except Exception as exc:
        raise RuntimeError('pyodbc runtime is unavailable (missing unixODBC libraries)') from exc
    return pyodbc.connect(connection_string.replace('{ODBC_DRIVER}', settings.odbc_driver), timeout=30)


def _quote_identifier(identifier: str) -> str:
    safe = ''.join(ch for ch in str(identifier) if ch.isalnum() or ch == '_')
    if not safe:
        raise ValueError('Invalid identifier')
    return f'[{safe}]'


def _build_final_query(query_template: str, *, incremental_column: str, id_column: str, date_column: str) -> str:
    base = (query_template or '').strip().rstrip(';')
    if not base:
        raise ValueError('query_template is empty')

    inc_col = _quote_identifier(incremental_column)
    id_col = _quote_identifier(id_column)
    dt_col = _quote_identifier(date_column)

    wrapped = (
        "SELECT * FROM ("
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


def _bind_template_params(
    query_template: str,
    *,
    from_date: Any,
    to_date: Any,
    last_sync_timestamp: Any,
    last_sync_id: Any,
) -> tuple[str, list[Any]]:
    tokens = ['@from_date', '@to_date', '@last_sync_ts', '@last_sync_id']
    params_map = {
        '@from_date': from_date,
        '@to_date': to_date,
        '@last_sync_ts': last_sync_timestamp,
        '@last_sync_id': last_sync_id,
    }
    params: list[Any] = []
    normalized = query_template
    for token in tokens:
        count = normalized.count(token)
        if count <= 0:
            continue
        normalized = normalized.replace(token, '?')
        params.extend([params_map[token]] * count)
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
    retries: int = 3,
    retry_sleep_sec: int = 2,
) -> Iterable[dict[str, Any]]:
    from_date = _normalize_param_datetime(from_date)
    to_date = _normalize_param_datetime(to_date)

    templated_query, template_params = _bind_template_params(
        query_template,
        from_date=from_date,
        to_date=to_date,
        last_sync_timestamp=last_sync_timestamp,
        last_sync_id=last_sync_id,
    )
    effective_query = _build_final_query(
        templated_query,
        incremental_column=incremental_column,
        id_column=id_column,
        date_column=date_column,
    )
    last_id: Any = last_sync_id
    if last_id is not None and str(last_id).isdigit():
        last_id = int(last_id)
    filter_params = [
        from_date,
        from_date,
        to_date,
        to_date,
        last_sync_timestamp,
        last_sync_timestamp,
        last_sync_timestamp,
        last_id,
        last_id,
    ]
    params = template_params + filter_params

    for attempt in range(1, retries + 1):
        try:
            with _connect(connection_string) as conn:
                cur = conn.cursor()
                cur.execute(effective_query, *params)
                columns = [col[0] for col in cur.description]
                for row in cur.fetchall():
                    yield dict(zip(columns, row))
            break
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(retry_sleep_sec)
