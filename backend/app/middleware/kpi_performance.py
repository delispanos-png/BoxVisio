import json
import logging
import time

from fastapi import Request

logger = logging.getLogger('app.kpi_perf')

_PROFILED_KPI_PATHS = {
    '/v1/dashboard/executive-summary',
    '/api/dashboard/executive-summary',
    '/dashboard/executive-summary',
    '/v1/dashboard/finance-summary',
    '/api/dashboard/finance-summary',
    '/dashboard/finance-summary',
    '/v1/streams/sales/summary',
    '/api/streams/sales/summary',
    '/streams/sales/summary',
    '/v1/streams/purchases/summary',
    '/api/streams/purchases/summary',
    '/streams/purchases/summary',
    '/v1/streams/inventory/summary',
    '/api/streams/inventory/summary',
    '/streams/inventory/summary',
    '/v1/streams/cash/summary',
    '/api/streams/cash/summary',
    '/streams/cash/summary',
    '/v1/streams/balances/summary',
    '/api/streams/balances/summary',
    '/streams/balances/summary',
    '/v1/kpi/sales/summary',
    '/kpi/sales/summary',
    '/v1/kpi/sales/by-branch',
    '/kpi/sales/by-branch',
    '/v1/kpi/sales/trend-monthly',
    '/kpi/sales/trend-monthly',
    '/v1/kpi/purchases/summary',
    '/kpi/purchases/summary',
}


def _is_kpi_path(path: str) -> bool:
    return (
        path.startswith('/v1/kpi/')
        or path.startswith('/kpi/')
        or path.startswith('/v1/dashboard/')
        or path.startswith('/api/dashboard/')
        or path.startswith('/dashboard/')
        or path.startswith('/v1/streams/')
        or path.startswith('/api/streams/')
        or path.startswith('/streams/')
    )


def _safe_response_size(response) -> int:
    length = response.headers.get('content-length')
    if length is not None:
        try:
            return int(length)
        except (TypeError, ValueError):
            return 0
    body = getattr(response, 'body', None)
    if isinstance(body, (bytes, bytearray)):
        return len(body)
    return 0


async def kpi_performance_middleware(request: Request, call_next):
    if not _is_kpi_path(request.url.path):
        return await call_next(request)

    started = time.perf_counter()
    profile_db = request.url.path in _PROFILED_KPI_PATHS
    if profile_db:
        request.state.kpi_perf = {'query_count': 0, 'db_time_ms': 0.0, 'slow_queries': []}
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        api_time_ms = round((time.perf_counter() - started) * 1000, 2)
        perf = (getattr(request.state, 'kpi_perf', {}) or {}) if profile_db else {}
        query_count = int(perf.get('query_count', 0) or 0)
        db_time_ms = round(float(perf.get('db_time_ms', 0.0) or 0.0), 2)
        slow_queries = perf.get('slow_queries', []) or []

        if response is not None:
            response.headers['X-KPI-API-Time-Ms'] = f'{api_time_ms:.2f}'
            response.headers['X-KPI-DB-Time-Ms'] = f'{db_time_ms:.2f}'
            response.headers['X-KPI-DB-Query-Count'] = str(query_count)
            response.headers['X-KPI-Response-Bytes'] = str(_safe_response_size(response))
            if slow_queries:
                response.headers['X-KPI-Slow-Query-Count'] = str(len(slow_queries))

        logger.info(
            'kpi_request_perf',
            extra={
                'request_id': getattr(request.state, 'request_id', None),
                'method': request.method,
                'path': request.url.path,
                'query_count': query_count,
                'db_time_ms': db_time_ms,
                'api_time_ms': api_time_ms,
                'slow_queries_json': json.dumps(slow_queries, ensure_ascii=False),
            },
        )
