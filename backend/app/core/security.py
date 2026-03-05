import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
ALGORITHM = 'HS256'
AUDIENCE_ADMIN = 'admin'
AUDIENCE_TENANT = 'tenant'


def audience_for_role(role: str) -> str:
    return AUDIENCE_ADMIN if role == 'cloudon_admin' else AUDIENCE_TENANT


def expected_audience_for_host(host: str | None) -> str | None:
    if not host:
        return None
    host_only = host.split(':')[0].lower()
    if host_only == settings.admin_portal_host.lower():
        return AUDIENCE_ADMIN
    if host_only == settings.tenant_portal_host.lower():
        return AUDIENCE_TENANT
    return None


def create_access_token(
    subject: str,
    tenant_id: int | None,
    role: str,
    expires_delta: timedelta | None = None,
    audience: str | None = None,
) -> str:
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    payload: dict[str, Any] = {
        'sub': subject,
        'exp': expire,
        'tenant_id': tenant_id,
        'role': role,
        'typ': 'access',
        'aud': audience or audience_for_role(role),
    }
    return jwt.encode(payload, settings.secret_key, algorithm=ALGORITHM)


def create_refresh_token(subject: str) -> tuple[str, str, datetime]:
    expires_at = datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days)
    jti = secrets.token_urlsafe(24)
    payload: dict[str, Any] = {
        'sub': subject,
        'exp': expires_at,
        'jti': jti,
        'typ': 'refresh',
    }
    token = jwt.encode(payload, settings.secret_key, algorithm=ALGORITHM)
    return token, jti, expires_at


def decode_token(token: str, audience: str | None = None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if audience:
        kwargs['audience'] = audience
    return jwt.decode(token, settings.secret_key, algorithms=[ALGORITHM], **kwargs)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def safe_decode(token: str, audience: str | None = None, token_type: str | None = None) -> dict[str, Any] | None:
    try:
        payload = decode_token(token, audience=audience)
        if token_type and payload.get('typ') != token_type:
            return None
        return payload
    except JWTError:
        return None
