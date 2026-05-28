import secrets
from urllib.parse import parse_qs
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse


UNSAFE_METHODS = {'POST', 'PUT', 'PATCH', 'DELETE'}


def _secure_cookie(request: Request) -> bool:
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    return request.url.scheme == 'https' or forwarded_proto == 'https'


def _should_seed_csrf(path: str, method: str) -> bool:
    if method != 'GET':
        return False
    return path == '/login' or path.startswith('/admin') or path.startswith('/tenant')


def should_protect(path: str, method: str) -> bool:
    if method not in UNSAFE_METHODS:
        return False
    if path == '/login':
        return False
    return path.startswith('/admin') or path.startswith('/tenant') or path.startswith('/logout')


def _same_origin(request: Request) -> bool:
    expected_host = request.headers.get('host') or request.url.netloc
    for header_name in ('origin', 'referer'):
        raw_value = request.headers.get(header_name)
        if not raw_value:
            continue
        parsed = urlparse(raw_value)
        if parsed.netloc and parsed.netloc == expected_host:
            return True
    return False


def _csrf_redirect_url(request: Request) -> str:
    current_path = request.url.path
    if current_path.startswith('/admin/settings/mail-server'):
        fallback = '/admin/settings/mail-server'
    elif current_path.startswith('/admin/tenants') and current_path.endswith('/delete'):
        fallback = '/admin/tenants'
    elif current_path.startswith('/admin'):
        fallback = '/admin/tenants'
    else:
        fallback = '/tenant'
    referer = request.headers.get('referer') or ''
    if referer:
        parsed = urlparse(referer)
        expected_host = request.headers.get('host') or request.url.netloc
        if parsed.netloc == expected_host and (parsed.path.startswith('/admin') or parsed.path.startswith('/tenant')):
            if parsed.path == current_path and request.method.upper() in UNSAFE_METHODS:
                return f'{fallback}?csrf_error=1'
            sep = '&' if parsed.query else '?'
            return f'{parsed.path}?{parsed.query}&csrf_error=1' if parsed.query else f'{parsed.path}{sep}csrf_error=1'
    return f'{fallback}?csrf_error=1'


async def csrf_middleware(request: Request, call_next):
    if not should_protect(request.url.path, request.method.upper()):
        response = await call_next(request)
        if _should_seed_csrf(request.url.path, request.method.upper()) and not request.cookies.get('csrf_token'):
            response.set_cookie('csrf_token', secrets.token_urlsafe(24), httponly=False, samesite='lax', secure=_secure_cookie(request))
        return response

    cookie_token = request.cookies.get('csrf_token')
    header_token = request.headers.get('X-CSRF-Token')
    query_token = request.query_params.get('csrf_token')
    supplied = header_token or query_token

    content_type = (request.headers.get('content-type') or '').lower()
    body: bytes | None = None
    if not supplied and 'application/x-www-form-urlencoded' in content_type:
        body = await request.body()
        form_token = None
        try:
            parsed = parse_qs(body.decode('utf-8', errors='ignore'), keep_blank_values=True)
            form_token = (parsed.get('csrf_token') or [None])[0]
        except Exception:
            form_token = None
        supplied = form_token

    if not cookie_token or not supplied or supplied != cookie_token:
        if _same_origin(request):
            if body is not None:
                async def receive() -> dict:
                    return {'type': 'http.request', 'body': body, 'more_body': False}
                request = Request(request.scope, receive)
            response = await call_next(request)
            if not cookie_token:
                response.set_cookie('csrf_token', secrets.token_urlsafe(24), httponly=False, samesite='lax', secure=_secure_cookie(request))
            return response
        if request.url.path.startswith('/admin') or request.url.path.startswith('/tenant') or request.url.path.startswith('/logout'):
            response = RedirectResponse(url=_csrf_redirect_url(request), status_code=303)
            return response
        return JSONResponse(status_code=403, content={'detail': 'CSRF validation failed'})

    if body is None:
        return await call_next(request)

    async def receive() -> dict:
        return {'type': 'http.request', 'body': body, 'more_body': False}
    request = Request(request.scope, receive)
    return await call_next(request)
