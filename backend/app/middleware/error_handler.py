import logging
import uuid

from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse

from app.observability.metrics import app_errors_total

logger = logging.getLogger(__name__)


def _is_api_path(path: str) -> bool:
    return (
        path.startswith('/v1/')
        or path.startswith('/api/')
        or path in {'/health', '/ready', '/metrics'}
    )


async def error_handler_middleware(request: Request, call_next):
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    request.state.request_id = request_id
    try:
        response = await call_next(request)
        if (
            response.status_code == 401
            and request.method.upper() == 'GET'
            and not _is_api_path(request.url.path)
            and not request.cookies.get('access_token')
        ):
            redirect = RedirectResponse(url='/login', status_code=302)
            redirect.headers['X-Request-ID'] = request_id
            return redirect
        response.headers['X-Request-ID'] = request_id
        return response
    except HTTPException as exc:
        app_errors_total.labels(error_type='http_exception', path=request.url.path).inc()
        # For UI pages, redirect unauthenticated browser requests to login.
        if (
            exc.status_code == 401
            and request.method.upper() == 'GET'
            and not _is_api_path(request.url.path)
        ):
            response = RedirectResponse(url='/login', status_code=302)
            response.headers['X-Request-ID'] = request_id
            return response
        return JSONResponse(
            status_code=exc.status_code,
            content={'detail': exc.detail, 'request_id': request_id},
            headers={'X-Request-ID': request_id},
        )
    except Exception:
        app_errors_total.labels(error_type='unhandled_exception', path=request.url.path).inc()
        logger.exception(
            'unhandled_exception',
            extra={
                'request_id': request_id,
                'method': request.method,
                'path': request.url.path,
            },
        )
        return JSONResponse(
            status_code=500,
            content={
                'detail': 'Internal Server Error',
                'request_id': request_id,
            },
            headers={'X-Request-ID': request_id},
        )
