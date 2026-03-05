import json
from datetime import datetime

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import AuditLog


AUDITED_PREFIXES = ('/v1/admin', '/admin')
AUDITED_METHODS = {'POST', 'PUT', 'PATCH', 'DELETE'}


def _should_audit(path: str, method: str) -> bool:
    return method.upper() in AUDITED_METHODS and path.startswith(AUDITED_PREFIXES)


def _safe_body(body_bytes: bytes) -> dict:
    if not body_bytes:
        return {'body_present': False}
    try:
        text = body_bytes.decode('utf-8', errors='ignore')
        if not text:
            return {'body_present': False}
        if len(text) > 4096:
            text = text[:4096] + '...'
        try:
            as_json = json.loads(text)
            redact_keys = {
                'password',
                'db_password',
                'connection_string',
                'key_secret',
                'secret',
                'enc_payload',
                'host',
                'port',
                'database',
                'username',
                'options',
            }

            def _redact(value):
                if isinstance(value, dict):
                    out = {}
                    for k, v in value.items():
                        if str(k).lower() in redact_keys:
                            out[k] = '***'
                        else:
                            out[k] = _redact(v)
                    return out
                if isinstance(value, list):
                    return [_redact(v) for v in value]
                return value

            return {'body': _redact(as_json)}
        except Exception:
            return {'body': text}
    except Exception:
        return {'body_present': True}


async def admin_audit_middleware(request: Request, call_next):
    path = request.url.path
    method = request.method.upper()

    if not _should_audit(path, method):
        return await call_next(request)

    body = await request.body()

    async def receive() -> dict:
        return {'type': 'http.request', 'body': body, 'more_body': False}

    request = Request(request.scope, receive)
    response = await call_next(request)

    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    else:
        token = request.cookies.get('access_token')

    expected_aud = expected_audience_for_host(request.headers.get('host'))
    payload = safe_decode(token, audience=expected_aud, token_type='access') if token else None
    actor_user_id = int(payload['sub']) if payload and payload.get('sub') and str(payload.get('sub')).isdigit() else None
    tenant_id = payload.get('tenant_id') if payload else None

    session_maker = request.app.state.control_sessionmaker
    async with session_maker() as db:  # type: AsyncSession
        db.add(
            AuditLog(
                tenant_id=tenant_id,
                actor_user_id=actor_user_id,
                action=f'admin_action:{method}:{path}',
                entity_type='http_request',
                entity_id=None,
                payload={
                    'path': path,
                    'method': method,
                    'query': dict(request.query_params),
                    'status_code': response.status_code,
                    'request': _safe_body(body),
                    'created_at': datetime.utcnow().isoformat(),
                },
            )
        )
        await db.commit()

    return response
