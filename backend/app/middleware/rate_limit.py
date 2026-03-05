from datetime import datetime, timezone

from fastapi import Request
from fastapi.responses import JSONResponse
from redis import Redis

from app.core.config import settings
from app.core.security import expected_audience_for_host, safe_decode


API_PREFIXES = ('/v1/',)


def _is_limited_path(path: str) -> bool:
    return path.startswith(API_PREFIXES)


def _minute_bucket() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d%H%M')


def _resolve_limit_subject(request: Request) -> str:
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        expected_aud = expected_audience_for_host(request.headers.get('host'))
        payload = safe_decode(auth_header.split(' ', 1)[1], audience=expected_aud, token_type='access')
        if payload:
            tenant_id = payload.get('tenant_id')
            sub = payload.get('sub')
            role = payload.get('role')
            if tenant_id is not None:
                return f'tenant:{tenant_id}'
            if sub:
                return f'user:{sub}:{role}'

    x_tenant = request.headers.get('X-Tenant')
    if x_tenant:
        return f'tenant_slug:{x_tenant}'

    return f"ip:{request.client.host if request.client else 'unknown'}"


async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if not _is_limited_path(path):
        return await call_next(request)

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    subject = _resolve_limit_subject(request)
    bucket = _minute_bucket()
    key = f'rate:{subject}:{bucket}'

    current = int(redis.incr(key))
    if current == 1:
        redis.expire(key, 65)

    limit = settings.rate_limit_per_minute
    if current > limit:
        retry_after = int(redis.ttl(key))
        return JSONResponse(
            status_code=429,
            content={'detail': 'Rate limit exceeded'},
            headers={
                'Retry-After': str(max(retry_after, 1)),
                'X-RateLimit-Limit': str(limit),
                'X-RateLimit-Remaining': '0',
            },
        )

    response = await call_next(request)
    response.headers['X-RateLimit-Limit'] = str(limit)
    response.headers['X-RateLimit-Remaining'] = str(max(limit - current, 0))
    return response
