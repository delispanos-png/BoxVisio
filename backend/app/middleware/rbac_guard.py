from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.api.deps import has_permission
from app.core.config import settings
from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import RoleName


def _required_permission(request: Request) -> str | None:
    path = request.url.path
    method = request.method.upper()

    if path.startswith('/v1/admin'):
        return 'admin:read' if method == 'GET' else 'admin:write'
    if path.startswith('/v1/kpi') or path.startswith('/kpi'):
        return 'kpi:read'
    if path.startswith('/v1/ingest'):
        return 'ingest:write'
    if path.startswith('/admin'):
        return 'admin:ui'
    if path.startswith('/tenant'):
        return 'tenant:ui'
    return None


async def rbac_guard_middleware(request: Request, call_next):
    permission = _required_permission(request)
    if permission is None:
        return await call_next(request)

    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    else:
        token = request.cookies.get('access_token')

    if not token:
        return await call_next(request)

    expected_aud = expected_audience_for_host(request.headers.get('host'))
    payload = safe_decode(token, audience=expected_aud, token_type='access')
    if not payload:
        host = (request.headers.get('host') or '').split(':')[0].lower()
        if request.method.upper() in {'GET', 'HEAD'} and request.url.path.startswith('/admin') and host == settings.admin_portal_host.lower():
            response = RedirectResponse(url='/login', status_code=303)
            for domain in (None, settings.admin_portal_host.lower(), settings.tenant_portal_host.lower(), '.boxvisio.com'):
                response.delete_cookie('access_token', path='/', domain=domain)
                response.delete_cookie('refresh_token', path='/', domain=domain)
                response.delete_cookie('csrf_token', path='/', domain=domain)
            return response
        return JSONResponse(status_code=401, content={'detail': 'Invalid token'})

    role_raw = payload.get('role')
    if role_raw is None:
        return JSONResponse(status_code=403, content={'detail': f'Missing permission: {permission}'})

    try:
        role = RoleName(role_raw)
    except ValueError:
        return JSONResponse(status_code=403, content={'detail': f'Missing permission: {permission}'})

    if permission.startswith('admin:'):
        if role != RoleName.cloudon_admin:
            if request.method.upper() in {'GET', 'HEAD'} and request.url.path.startswith('/admin'):
                host = (request.headers.get('host') or '').split(':')[0].lower()
                if host == settings.admin_portal_host.lower():
                    response = RedirectResponse(url='/login', status_code=303)
                    response.delete_cookie('access_token', path='/')
                    response.delete_cookie('refresh_token', path='/')
                    response.delete_cookie('csrf_token', path='/')
                    return response
                return RedirectResponse(url='/tenant/dashboard', status_code=303)
            return JSONResponse(status_code=403, content={'detail': f'Missing permission: {permission}'})
        return await call_next(request)

    if has_permission(role, permission):
        return await call_next(request)

    return JSONResponse(status_code=403, content={'detail': f'Missing permission: {permission}'})
