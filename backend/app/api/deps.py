from collections.abc import AsyncGenerator
import time

from fastapi import Cookie, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.db.control_session import get_control_db
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import ProfessionalProfile, RoleName, Tenant, TenantStatus, User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/v1/auth/login', auto_error=False)

ROLE_PERMISSIONS: dict[RoleName, set[str]] = {
    RoleName.cloudon_admin: {'*', 'admin:read', 'admin:write', 'admin:ui'},
    RoleName.tenant_admin: {'kpi:read', 'ingest:write', 'tenant:ui'},
    RoleName.tenant_user: {'kpi:read', 'tenant:ui'},
}


def resolve_default_professional_profile_code(user: User) -> str:
    if user.role == RoleName.cloudon_admin:
        return 'OWNER'
    if user.role == RoleName.tenant_user:
        return 'FINANCE'
    return 'MANAGER'


def resolve_ui_persona(user: User, professional_profile_code: str | None = None) -> str:
    code = (professional_profile_code or resolve_default_professional_profile_code(user)).strip().upper()
    if code == 'FINANCE':
        return 'finance'
    return 'manager'


def resolve_menu_visibility(user: User, professional_profile_code: str | None = None) -> dict[str, bool]:
    code = (professional_profile_code or resolve_default_professional_profile_code(user)).strip().upper()

    full_access = {
        'dashboard_executive': True,
        'dashboard_finance': True,
        'insights': True,
        'stream_sales_documents': True,
        'stream_purchase_documents': True,
        'stream_inventory_documents': True,
        'stream_cash_transactions': True,
        'stream_supplier_balances': True,
        'stream_customer_balances': True,
        'analytics_sales': True,
        'analytics_purchases': True,
        'analytics_inventory': True,
        'analytics_cashflows': True,
        'analytics_receivables_payables': True,
        'analytics_supplier_targets': True,
        'comparisons': True,
        'exports': True,
    }

    if code in {'OWNER', 'MANAGER'}:
        return full_access

    if code == 'FINANCE':
        return {
            'dashboard_executive': False,
            'dashboard_finance': True,
            'insights': True,
            'stream_sales_documents': False,
            'stream_purchase_documents': False,
            'stream_inventory_documents': False,
            'stream_cash_transactions': True,
            'stream_supplier_balances': True,
            'stream_customer_balances': True,
            'analytics_sales': False,
            'analytics_purchases': False,
            'analytics_inventory': False,
            'analytics_cashflows': True,
            'analytics_receivables_payables': True,
            'analytics_supplier_targets': False,
            'comparisons': False,
            'exports': True,
        }

    if code == 'INVENTORY':
        return {
            'dashboard_executive': True,
            'dashboard_finance': False,
            'insights': True,
            'stream_sales_documents': False,
            'stream_purchase_documents': False,
            'stream_inventory_documents': True,
            'stream_cash_transactions': False,
            'stream_supplier_balances': False,
            'stream_customer_balances': False,
            'analytics_sales': False,
            'analytics_purchases': False,
            'analytics_inventory': True,
            'analytics_cashflows': False,
            'analytics_receivables_payables': False,
            'analytics_supplier_targets': False,
            'comparisons': False,
            'exports': True,
        }

    if code == 'SALES':
        return {
            'dashboard_executive': True,
            'dashboard_finance': False,
            'insights': True,
            'stream_sales_documents': True,
            'stream_purchase_documents': False,
            'stream_inventory_documents': False,
            'stream_cash_transactions': False,
            'stream_supplier_balances': False,
            'stream_customer_balances': False,
            'analytics_sales': True,
            'analytics_purchases': False,
            'analytics_inventory': False,
            'analytics_cashflows': False,
            'analytics_receivables_payables': False,
            'analytics_supplier_targets': True,
            'comparisons': True,
            'exports': True,
        }

    return full_access


class _InstrumentedAsyncSession:
    _SLOW_QUERY_THRESHOLD_MS = 20.0
    _MAX_SLOW_QUERIES = 5

    def __init__(self, inner: AsyncSession, request: Request) -> None:
        self._inner = inner
        self._request = request

    async def execute(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return await self._inner.execute(*args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            perf = getattr(self._request.state, 'kpi_perf', None)
            if isinstance(perf, dict):
                perf['query_count'] = int(perf.get('query_count', 0) or 0) + 1
                perf['db_time_ms'] = float(perf.get('db_time_ms', 0.0) or 0.0) + elapsed_ms
                if elapsed_ms >= self._SLOW_QUERY_THRESHOLD_MS:
                    statement = args[0] if args else ''
                    sql = ' '.join(str(statement).split())[:260]
                    slow_queries = list(perf.get('slow_queries', []) or [])
                    slow_queries.append({'duration_ms': round(elapsed_ms, 2), 'sql': sql})
                    slow_queries.sort(key=lambda x: float(x.get('duration_ms', 0.0)), reverse=True)
                    perf['slow_queries'] = slow_queries[: self._MAX_SLOW_QUERIES]

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


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
    request: Request,
    payload: dict = Depends(get_token_payload),
    db: AsyncSession = Depends(get_control_db),
) -> User:
    result = await db.execute(
        select(User, ProfessionalProfile.profile_code, ProfessionalProfile.profile_name).outerjoin(
            ProfessionalProfile, User.professional_profile_id == ProfessionalProfile.id
        ).where(
            User.id == int(payload['sub']),
            User.is_active.is_(True),
        )
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='User not found')
    user, profile_code, profile_name = row
    from datetime import datetime

    now = datetime.utcnow()
    if user.access_starts_at is not None and user.access_starts_at > now:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Access not started')
    if user.access_expires_at is not None and user.access_expires_at < now:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Access expired')
    resolved_profile_code = (str(profile_code or '').strip().upper()) or resolve_default_professional_profile_code(user)
    resolved_profile_name = (str(profile_name or '').strip()) or resolved_profile_code.title()
    request.state.current_user = user
    request.state.user_role = user.role.value
    request.state.professional_profile_code = resolved_profile_code
    request.state.professional_profile_name = resolved_profile_name
    request.state.ui_persona = resolve_ui_persona(user, resolved_profile_code)
    request.state.menu_visibility = resolve_menu_visibility(user, resolved_profile_code)
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
    request: Request,
    tenant: Tenant = Depends(get_request_tenant),
) -> AsyncGenerator[AsyncSession, None]:
    async for session in get_tenant_db_session(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    ):
        if isinstance(getattr(request.state, 'kpi_perf', None), dict):
            yield _InstrumentedAsyncSession(session, request)
        else:
            yield session
