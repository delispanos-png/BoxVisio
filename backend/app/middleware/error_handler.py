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


def _is_ui_path(path: str) -> bool:
    return path.startswith('/admin/') or path.startswith('/tenant/') or path in {'/', '/login', '/logout'}


def _safe_ui_redirect_url(request: Request, *, request_id: str, reason: str) -> str:
    path = request.url.path
    referer = request.headers.get('referer') or ''
    if referer and path not in referer:
        separator = '&' if '?' in referer else '?'
        return f'{referer}{separator}error={reason}&request_id={request_id}'
    if path.startswith('/admin/settings/mail-server'):
        return f'/admin/settings/mail-server?error={reason}&request_id={request_id}'
    if path.startswith('/admin/tenants'):
        return f'/admin/tenants?error={reason}&request_id={request_id}'
    if path.startswith('/admin/connections') or path.startswith('/admin/data-sources'):
        return f'/admin/connections?error={reason}&request_id={request_id}'
    if path.startswith('/admin/'):
        return f'/admin/dashboard?error={reason}&request_id={request_id}'
    if path.startswith('/tenant/'):
        return f'/tenant?error={reason}&request_id={request_id}'
    return f'/login?error={reason}&request_id={request_id}'


def _login_redirect(request_id: str) -> RedirectResponse:
    response = RedirectResponse(url='/login', status_code=302)
    response.headers['X-Request-ID'] = request_id
    for cookie_name in ('access_token', 'refresh_token', 'csrf_token'):
        response.delete_cookie(cookie_name, path='/')
    return response


async def error_handler_middleware(request: Request, call_next):
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    request.state.request_id = request_id
    try:
        response = await call_next(request)
        if (
            response.status_code == 405
            and _is_ui_path(request.url.path)
            and not _is_api_path(request.url.path)
        ):
            redirect = RedirectResponse(
                url=_safe_ui_redirect_url(request, request_id=request_id, reason='method_not_allowed'),
                status_code=303,
            )
            redirect.headers['X-Request-ID'] = request_id
            return redirect
        if (
            response.status_code == 401
            and request.method.upper() == 'GET'
            and not _is_api_path(request.url.path)
        ):
            return _login_redirect(request_id)
        response.headers['X-Request-ID'] = request_id
        return response
    except HTTPException as exc:
        app_errors_total.labels(error_type='http_exception', path=request.url.path).inc()
        if (
            exc.status_code == 405
            and _is_ui_path(request.url.path)
            and not _is_api_path(request.url.path)
        ):
            redirect = RedirectResponse(
                url=_safe_ui_redirect_url(request, request_id=request_id, reason='method_not_allowed'),
                status_code=303,
            )
            redirect.headers['X-Request-ID'] = request_id
            return redirect
        # For UI pages, redirect unauthenticated browser requests to login.
        if (
            exc.status_code == 401
            and request.method.upper() == 'GET'
            and not _is_api_path(request.url.path)
        ):
            return _login_redirect(request_id)
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
        if _is_ui_path(request.url.path) and not _is_api_path(request.url.path):
            redirect = RedirectResponse(
                url=_safe_ui_redirect_url(request, request_id=request_id, reason='internal_server_error'),
                status_code=303,
            )
            redirect.headers['X-Request-ID'] = request_id
            return redirect
        return JSONResponse(
            status_code=500,
            content={
                'detail': 'Internal Server Error',
                'request_id': request_id,
            },
            headers={'X-Request-ID': request_id},
        )
