from datetime import datetime, timedelta

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import func, select, update

from app.core.config import settings
from app.core.security import safe_decode
from app.db.control_session import ControlSessionLocal
from app.models.control import RefreshToken


IDLE_TIMEOUT_MINUTES = 30
TOUCH_THROTTLE_MINUTES = 5


def _client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get('x-forwarded-for') or '').split(',')[0].strip()
    if forwarded_for:
        return forwarded_for[:64]
    return (request.client.host if request.client else '')[:64]


def _clear_auth_response(path: str = '/login') -> RedirectResponse:
    response = RedirectResponse(url=path, status_code=303)
    for domain in (None, settings.admin_portal_host.lower(), settings.tenant_portal_host.lower(), '.boxvisio.com'):
        response.delete_cookie('access_token', path='/', domain=domain)
        response.delete_cookie('refresh_token', path='/', domain=domain)
        response.delete_cookie('csrf_token', path='/', domain=domain)
    return response


def _should_skip(path: str) -> bool:
    return (
        path in {'/login', '/logout', '/ready', '/health', '/metrics', '/favicon.ico'}
        or path.startswith('/static/')
    )


def _is_ui_request(request: Request) -> bool:
    path = request.url.path
    if path == '/' or path.startswith('/admin') or path.startswith('/tenant'):
        return True
    if request.method.upper() not in {'GET', 'HEAD'}:
        return False
    accept = (request.headers.get('accept') or '').lower()
    return 'text/html' in accept


async def session_idle_middleware(request: Request, call_next):
    path = request.url.path
    if _should_skip(path):
        return await call_next(request)

    refresh_cookie = request.cookies.get('refresh_token')
    if not refresh_cookie:
        return await call_next(request)

    payload = safe_decode(refresh_cookie, token_type='refresh')
    jti = str((payload or {}).get('jti') or '').strip()
    if not jti:
        return await call_next(request)

    now = datetime.utcnow()
    idle_cutoff = now - timedelta(minutes=IDLE_TIMEOUT_MINUTES)
    touch_cutoff = now - timedelta(minutes=TOUCH_THROTTLE_MINUTES)

    async with ControlSessionLocal() as db:
        redis_client = getattr(request.app.state, 'redis', None)
        should_cleanup = True
        if redis_client is not None:
            try:
                should_cleanup = bool(redis_client.set('session_idle_cleanup_lock', '1', nx=True, ex=60))
            except Exception:
                should_cleanup = True
        if should_cleanup:
            await db.execute(
                update(RefreshToken)
                .where(
                    RefreshToken.revoked_at.is_(None),
                    RefreshToken.expires_at > now,
                    func.coalesce(RefreshToken.last_seen_at, RefreshToken.created_at) < idle_cutoff,
                )
                .values(revoked_at=now)
            )

        token_row = (
            await db.execute(select(RefreshToken).where(RefreshToken.token_jti == jti).limit(1))
        ).scalar_one_or_none()
        if not token_row:
            await db.commit()
            return await call_next(request)

        last_seen = (token_row.last_seen_at or token_row.created_at).replace(tzinfo=None)
        is_idle = last_seen < idle_cutoff
        is_invalid = token_row.revoked_at is not None or token_row.expires_at.replace(tzinfo=None) <= now or is_idle
        if is_idle and token_row.revoked_at is None:
            token_row.revoked_at = now
        if is_invalid:
            await db.commit()
            if _is_ui_request(request):
                return _clear_auth_response('/login')
            return JSONResponse(status_code=401, content={'detail': 'Session expired due to inactivity'})

        if token_row.last_seen_at is None or token_row.last_seen_at.replace(tzinfo=None) <= touch_cutoff:
            token_row.last_seen_at = now
            token_row.last_seen_path = path[:255]
            token_row.last_seen_ip = _client_ip(request)
            token_row.last_seen_user_agent = (request.headers.get('user-agent') or '')[:255]
        await db.commit()

    return await call_next(request)
