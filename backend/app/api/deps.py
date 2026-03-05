from collections.abc import AsyncGenerator

from fastapi import Cookie, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.db.control_session import get_control_db
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import RoleName, Tenant, TenantStatus, User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/v1/auth/login', auto_error=False)

ROLE_PERMISSIONS: dict[RoleName, set[str]] = {
    RoleName.cloudon_admin: {'*', 'admin:read', 'admin:write', 'admin:ui'},
    RoleName.tenant_admin: {'kpi:read', 'ingest:write', 'tenant:ui'},
    RoleName.tenant_user: {'kpi:read', 'tenant:ui'},
}


async def get_token_payload(
    request: Request,
    token: str | None = Depends(oauth2_scheme),
    access_token: str | None = Cookie(default=None),
) -> dict:
    effective_token = token or access_token
    if not effective_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Missing token')
    expected_aud = expected_audience_for_host(request.headers.get('host'))
    payload = safe_decode(effective_token, audience=expected_aud, token_type='access')
    if not payload or 'sub' not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid token')
    return payload


async def get_current_user(
    payload: dict = Depends(get_token_payload),
    db: AsyncSession = Depends(get_control_db),
) -> User:
    result = await db.execute(select(User).where(User.id == int(payload['sub']), User.is_active.is_(True)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='User not found')
    from datetime import datetime

    now = datetime.utcnow()
    if user.access_starts_at is not None and user.access_starts_at > now:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Access not started')
    if user.access_expires_at is not None and user.access_expires_at < now:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Access expired')
    return user


def require_roles(*roles: RoleName):
    async def checker(user: User = Depends(get_current_user)) -> User:
        if user.role not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Insufficient role')
        return user

    return checker


def has_permission(role: RoleName, permission: str) -> bool:
    allowed = ROLE_PERMISSIONS.get(role, set())
    return '*' in allowed or permission in allowed


def require_permission(permission: str):
    async def checker(user: User = Depends(get_current_user)) -> User:
        if not has_permission(user.role, permission):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f'Missing permission: {permission}')
        return user

    return checker


async def get_request_tenant(
    request: Request,
    payload: dict = Depends(get_token_payload),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_control_db),
) -> Tenant:
    tenant_id = payload.get('tenant_id')
    if tenant_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='tenant_id missing in JWT')
    if user.tenant_id != tenant_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='JWT tenant mismatch')

    result = await db.execute(select(Tenant).where(Tenant.id == tenant_id))
    tenant = result.scalar_one_or_none()

    if not tenant:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Tenant not resolved')
    if tenant.status != TenantStatus.active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Tenant inactive')

    request.state.tenant = tenant
    return tenant


async def get_tenant_db(
    tenant: Tenant = Depends(get_request_tenant),
) -> AsyncGenerator[AsyncSession, None]:
    async for session in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        yield session
