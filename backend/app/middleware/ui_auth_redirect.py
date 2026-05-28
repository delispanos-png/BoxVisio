from fastapi import Request
from fastapi.responses import RedirectResponse

from app.core.config import settings
from app.core.security import expected_audience_for_host, safe_decode


def _protected_ui_path(path: str) -> bool:
    return path.startswith('/tenant/') or path == '/admin' or path.startswith('/admin/')


async def ui_auth_redirect_middleware(request: Request, call_next):
    path = request.url.path
    host = (request.headers.get('host') or '').split(':')[0].lower()
    if request.method.upper() in {'GET', 'HEAD'} and host == settings.admin_portal_host.lower() and path.startswith('/tenant/'):
        target = f'https://{settings.tenant_portal_host}{path}'
        if request.url.query:
            target = f'{target}?{request.url.query}'
        return RedirectResponse(url=target, status_code=307)

    if request.method.upper() == 'GET' and _protected_ui_path(path):
        token = request.cookies.get('access_token')
        if not token:
            return RedirectResponse(url='/login', status_code=302)
        expected_aud = expected_audience_for_host(request.headers.get('host'))
        payload = safe_decode(token, audience=expected_aud, token_type='access')
        if not payload:
            response = RedirectResponse(url='/login', status_code=302)
            response.delete_cookie('access_token', path='/')
            response.delete_cookie('refresh_token', path='/')
            response.delete_cookie('csrf_token', path='/')
            return response

    return await call_next(request)
