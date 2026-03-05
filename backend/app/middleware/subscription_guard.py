from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import SubscriptionStatus, Tenant
from app.services.subscriptions import apply_subscription_time_transitions, get_or_create_subscription, sync_tenant_from_subscription


ENFORCED_PREFIXES = (
    '/v1/kpi/',
    '/kpi/',
    '/v1/ingest/',
    '/tenant/',
)


def _is_enforced_path(path: str) -> bool:
    return path.startswith(ENFORCED_PREFIXES)


def _ingest_path(path: str) -> bool:
    return path.startswith('/v1/ingest/')


async def subscription_guard_middleware(request: Request, call_next):
    path = request.url.path
    if not _is_enforced_path(path):
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
        return await call_next(request)

    tenant_id = payload.get('tenant_id')
    if tenant_id is None:
        return await call_next(request)

    session_maker = request.app.state.control_sessionmaker
    async with session_maker() as db:  # type: AsyncSession
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if not tenant:
            return JSONResponse(status_code=400, content={'detail': 'Tenant not found'})

        subscription = await get_or_create_subscription(db, tenant)
        changed = await apply_subscription_time_transitions(db, tenant, subscription)
        if changed:
            await db.commit()
        else:
            await sync_tenant_from_subscription(db, tenant, subscription)
            await db.commit()

        status = subscription.status
        if status in {SubscriptionStatus.suspended, SubscriptionStatus.canceled}:
            return JSONResponse(status_code=403, content={'detail': 'Subscription blocked'})

        if status == SubscriptionStatus.past_due and _ingest_path(path):
            return JSONResponse(status_code=402, content={'detail': 'Subscription past_due: ingestion disabled'})

    return await call_next(request)
