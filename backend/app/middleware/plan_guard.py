from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import Tenant
from app.services.subscriptions import get_or_create_subscription, is_feature_enabled
from app.services.subscription_features import feature_key_for_path


def required_feature_for_path(path: str) -> str | None:
    return feature_key_for_path(path)


async def plan_guard_middleware(request: Request, call_next):
    feature = required_feature_for_path(request.url.path)
    if not feature:
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
        if not await is_feature_enabled(db, tenant, subscription, feature):
            return JSONResponse(status_code=403, content={'detail': f'Feature {feature} disabled for current plan'})
        await db.commit()

    return await call_next(request)
