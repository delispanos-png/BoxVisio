from __future__ import annotations

import asyncio
from functools import lru_cache
import hashlib
import json
import time
from typing import Any, Callable

from app.core.config import settings

try:
    from redis.asyncio import Redis  # type: ignore
except Exception:  # pragma: no cover - optional runtime dependency path
    Redis = None  # type: ignore


_cache_lock = asyncio.Lock()
_cache_store: dict[str, tuple[float, Any]] = {}
_key_locks: dict[str, asyncio.Lock] = {}
_MAX_KEYS = 4096
_CACHE_PREFIX = 'kpi_cache'


def _prune_expired(now: float) -> None:
    expired = [k for k, (exp, _) in _cache_store.items() if exp <= now]
    for key in expired:
        _cache_store.pop(key, None)
        _key_locks.pop(key, None)


def _make_cache_key(namespace: str, tenant_key: str, params: dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)
    digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    return f'{_CACHE_PREFIX}:{namespace}:{tenant_key}:{digest}'


@lru_cache
def _redis_client():
    if Redis is None:
        return None
    try:
        return Redis.from_url(settings.redis_url, decode_responses=False)
    except Exception:
        return None


async def _redis_get_json(key: str) -> Any | None:
    redis = _redis_client()
    if redis is None:
        return None
    try:
        raw = await redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')
        return json.loads(raw)
    except Exception:
        return None


async def _redis_set_json(key: str, value: Any, ttl_seconds: int) -> None:
    redis = _redis_client()
    if redis is None:
        return
    try:
        payload = json.dumps(value, ensure_ascii=False, default=str)
        await redis.set(key, payload.encode('utf-8'), ex=max(1, int(ttl_seconds)))
    except Exception:
        return


async def invalidate_tenant_cache(tenant_key: str | None = None, namespace_prefix: str | None = None) -> int:
    redis_match = f'{_CACHE_PREFIX}:{namespace_prefix or "*"}:{tenant_key or "*"}:*'
    deleted = 0

    # Redis-backed invalidation (cross-process)
    redis = _redis_client()
    if redis is not None:
        try:
            keys: list[bytes | str] = []
            async for key in redis.scan_iter(match=redis_match, count=500):
                keys.append(key)
            if keys:
                deleted += int(await redis.delete(*keys))
        except Exception:
            pass

    # In-process fallback invalidation
    async with _cache_lock:
        if tenant_key is None and namespace_prefix is None:
            local_keys = list(_cache_store.keys())
        else:
            local_keys = []
            for key in _cache_store.keys():
                if namespace_prefix is not None and not key.startswith(f'{_CACHE_PREFIX}:{namespace_prefix}'):
                    continue
                if tenant_key is not None and f':{tenant_key}:' not in key:
                    continue
                local_keys.append(key)
        for key in local_keys:
            _cache_store.pop(key, None)
            _key_locks.pop(key, None)
        deleted += len(local_keys)
    return deleted


async def get_or_set_cache(
    *,
    namespace: str,
    tenant_key: str,
    params: dict[str, Any],
    ttl_seconds: int,
    producer: Callable[[], Any],
) -> tuple[Any, bool]:
    now = time.monotonic()
    key = _make_cache_key(namespace=namespace, tenant_key=tenant_key, params=params)
    redis_available = _redis_client() is not None

    if not redis_available:
        cached = _cache_store.get(key)
        if cached and cached[0] > now:
            return cached[1], True

    # Cross-process cache lookup first (Redis)
    redis_cached = await _redis_get_json(key)
    if redis_cached is not None:
        if not redis_available:
            expires_at = now + max(1, int(ttl_seconds))
            _cache_store[key] = (expires_at, redis_cached)
        return redis_cached, True

    async with _cache_lock:
        if not redis_available:
            cached = _cache_store.get(key)
            if cached and cached[0] > now:
                return cached[1], True
        lock = _key_locks.setdefault(key, asyncio.Lock())

    async with lock:
        now = time.monotonic()
        if not redis_available:
            cached = _cache_store.get(key)
            if cached and cached[0] > now:
                return cached[1], True

        redis_cached = await _redis_get_json(key)
        if redis_cached is not None:
            if not redis_available:
                expires_at = now + max(1, int(ttl_seconds))
                _cache_store[key] = (expires_at, redis_cached)
            return redis_cached, True

        value = await producer()
        if not redis_available:
            expires_at = now + max(1, int(ttl_seconds))
            _cache_store[key] = (expires_at, value)
        await _redis_set_json(key, value, ttl_seconds=ttl_seconds)

        if not redis_available:
            async with _cache_lock:
                _prune_expired(now)
                if len(_cache_store) > _MAX_KEYS:
                    oldest = sorted(_cache_store.items(), key=lambda item: item[1][0])[: len(_cache_store) - _MAX_KEYS]
                    for old_key, _ in oldest:
                        _cache_store.pop(old_key, None)
                        _key_locks.pop(old_key, None)

        return value, False
