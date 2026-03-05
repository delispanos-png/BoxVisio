import logging
import time

from fastapi import Request

from app.observability.metrics import http_request_duration_seconds, http_requests_total

logger = logging.getLogger('app.request')


async def request_logging_middleware(request: Request, call_next):
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed_sec = time.perf_counter() - started
        elapsed_ms = round(elapsed_sec * 1000, 2)
        http_requests_total.labels(method=request.method, path=request.url.path, status_code=str(status_code)).inc()
        http_request_duration_seconds.labels(method=request.method, path=request.url.path).observe(elapsed_sec)
        logger.info(
            'http_request',
            extra={
                'request_id': getattr(request.state, 'request_id', None),
                'method': request.method,
                'path': request.url.path,
                'status_code': status_code,
                'elapsed_ms': elapsed_ms,
                'client_ip': request.client.host if request.client else None,
            },
        )
