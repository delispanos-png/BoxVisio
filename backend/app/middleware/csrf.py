import secrets
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse


UNSAFE_METHODS = {'POST', 'PUT', 'PATCH', 'DELETE'}


def should_protect(path: str, method: str) -> bool:
    if method not in UNSAFE_METHODS:
        return False
    if path == '/login':
        return False
    return path.startswith('/admin') or path.startswith('/tenant') or path.startswith('/logout')


async def csrf_middleware(request: Request, call_next):
    if not should_protect(request.url.path, request.method.upper()):
        response = await call_next(request)
        if request.url.path == '/login' and request.method.upper() == 'GET' and not request.cookies.get('csrf_token'):
            response.set_cookie('csrf_token', secrets.token_urlsafe(24), httponly=False, samesite='lax', secure=False)
        return response

    cookie_token = request.cookies.get('csrf_token')
    header_token = request.headers.get('X-CSRF-Token')

    body = await request.body()

    form_token = None
    content_type = (request.headers.get('content-type') or '').lower()
    if 'application/x-www-form-urlencoded' in content_type and body:
        try:
            parsed = parse_qs(body.decode('utf-8', errors='ignore'), keep_blank_values=True)
            form_token = (parsed.get('csrf_token') or [None])[0]
        except Exception:
            form_token = None

    supplied = header_token or form_token
    if not cookie_token or not supplied or supplied != cookie_token:
        if request.url.path.startswith('/admin') or request.url.path.startswith('/tenant') or request.url.path.startswith('/logout'):
            response = RedirectResponse(url='/login', status_code=303)
            response.delete_cookie('access_token', path='/')
            response.delete_cookie('refresh_token', path='/')
            response.delete_cookie('csrf_token', path='/')
            return response
        return JSONResponse(status_code=403, content={'detail': 'CSRF validation failed'})

    async def receive() -> dict:
        return {'type': 'http.request', 'body': body, 'more_body': False}

    request = Request(request.scope, receive)
    return await call_next(request)
