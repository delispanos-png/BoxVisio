from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.core.config import settings
from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import RoleName


def _host_without_port(request: Request) -> str:
    return (request.headers.get('host') or '').split(':')[0].lower()


def _extract_role(request: Request) -> RoleName | None:
    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    else:
        token = request.cookies.get('access_token')
    if not token:
        return None
    expected_aud = expected_audience_for_host(request.headers.get('host'))
    payload = safe_decode(token, audience=expected_aud, token_type='access')
    if not payload:
        return None
    role_raw = payload.get('role')
    if not role_raw:
        return None
    try:
        return RoleName(role_raw)
    except ValueError:
        return None


def _is_tenant_endpoint(path: str) -> bool:
    return (
        path.startswith('/tenant')
        or path.startswith('/v1/kpi')
        or path.startswith('/kpi')
        or path.startswith('/v1/ingest')
    )


def _is_cloudon_admin_endpoint(path: str) -> bool:
    return path.startswith('/admin') or path.startswith('/v1/admin')


def _is_tenant_host(host: str) -> bool:
    tenant_root = str(settings.tenant_domain_root or '').strip('.').lower()
    return host == settings.tenant_portal_host.lower() or bool(tenant_root and host.endswith(f'.{tenant_root}'))


def _redirect_to_tenant_host(request: Request, path: str | None = None) -> RedirectResponse:
    target_path = path or request.url.path
    target = f'https://{settings.tenant_portal_host}{target_path}'
    if request.url.query:
        target = f'{target}?{request.url.query}'
    return RedirectResponse(url=target, status_code=307)


def _redirect_to_admin_login_clearing_cookies() -> RedirectResponse:
    response = RedirectResponse(url='/login', status_code=303)
    for domain in (None, settings.admin_portal_host.lower(), settings.tenant_portal_host.lower(), '.boxvisio.com'):
        response.delete_cookie('access_token', path='/', domain=domain)
        response.delete_cookie('refresh_token', path='/', domain=domain)
        response.delete_cookie('csrf_token', path='/', domain=domain)
    return response


async def host_access_guard_middleware(request: Request, call_next):
    host = _host_without_port(request)
    role = _extract_role(request)
    path = request.url.path

    if host == settings.admin_portal_host.lower() and path in {'/login', '/logout', '/favicon.ico'}:
        return await call_next(request)

    # Tenant UI accidentally opened on adminpanel.boxvisio.com should navigate to the tenant portal,
    # not return a raw JSON authorization error to the browser.
    if host == settings.admin_portal_host.lower() and _is_tenant_endpoint(path):
        if request.method.upper() in {'GET', 'HEAD'}:
            return _redirect_to_tenant_host(request)
        return JSONResponse(status_code=403, content={'detail': 'Tenant endpoints are not available on admin host'})

    # adminpanel.boxvisio.com is reserved for cloudon_admin only.
    if host == settings.admin_portal_host.lower() and role is not None and role != RoleName.cloudon_admin:
        if request.method.upper() in {'GET', 'HEAD'}:
            return _redirect_to_admin_login_clearing_cookies()
        return JSONResponse(status_code=403, content={'detail': 'Admin host allows cloudon_admin only'})

    # bi.boxvisio.com must not serve cloudon_admin-only endpoints.
    if _is_tenant_host(host) and _is_cloudon_admin_endpoint(path):
        if path.startswith('/admin') and request.method.upper() in {'GET', 'HEAD'}:
            target = f'https://{settings.admin_portal_host}{path}'
            if request.url.query:
                target = f'{target}?{request.url.query}'
            return RedirectResponse(url=target, status_code=307)
        return JSONResponse(status_code=403, content={'detail': 'Admin endpoints are not available on tenant host'})

    # cloudon_admin cannot directly use tenant endpoints on bi host.
    if _is_tenant_host(host) and role == RoleName.cloudon_admin and _is_tenant_endpoint(path):
        if request.method.upper() in {'GET', 'HEAD'}:
            return RedirectResponse(url=f'https://{settings.admin_portal_host}/admin/dashboard', status_code=307)
        return JSONResponse(status_code=403, content={'detail': 'cloudon_admin is denied on tenant endpoints'})

    return await call_next(request)
