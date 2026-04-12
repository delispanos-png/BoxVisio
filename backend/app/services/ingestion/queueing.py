from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from typing import Any

from redis import Redis

from app.core.config import settings


def tenant_queue_name(tenant_slug: str) -> str:
    return f'ingest:{tenant_slug}'


def tenant_dlq_name(tenant_slug: str) -> str:
    return f'dlq:{tenant_slug}'


def tenant_lock_name(tenant_slug: str) -> str:
    return f'lock:ingest:{tenant_slug}'


def tenant_throttle_key(tenant_slug: str) -> str:
    return f'throttle:ingest:{tenant_slug}'


def tenant_stop_key(tenant_slug: str) -> str:
    return f'stop:ingest:{tenant_slug}'


@lru_cache
def _redis() -> Redis:
    return Redis.from_url(settings.redis_url, decode_responses=True)


def enqueue_tenant_job(tenant_slug: str, job: dict[str, Any]) -> int:
    payload = dict(job)
    payload.setdefault('queued_at', datetime.utcnow().isoformat())
    return int(_redis().rpush(tenant_queue_name(tenant_slug), json.dumps(payload)))


def pop_tenant_job(tenant_slug: str) -> dict[str, Any] | None:
    raw = _redis().lpop(tenant_queue_name(tenant_slug))
    if not raw:
        return None
    return json.loads(raw)


def push_dead_letter(tenant_slug: str, dead_letter: dict[str, Any]) -> int:
    payload = dict(dead_letter)
    payload.setdefault('failed_at', datetime.utcnow().isoformat())
    return int(_redis().rpush(tenant_dlq_name(tenant_slug), json.dumps(payload)))


def acquire_tenant_lock(tenant_slug: str, ttl_seconds: int | None = None) -> str | None:
    token = f"{datetime.utcnow().timestamp()}:{tenant_slug}"
    ttl = ttl_seconds or settings.ingest_tenant_lock_ttl_seconds
    acquired = _redis().set(tenant_lock_name(tenant_slug), token, nx=True, ex=ttl)
    return token if acquired else None


def release_tenant_lock(tenant_slug: str, token: str) -> bool:
    key = tenant_lock_name(tenant_slug)
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """
    return bool(_redis().eval(script, 1, key, token))


def extend_tenant_lock(tenant_slug: str, token: str, ttl_seconds: int | None = None) -> bool:
    key = tenant_lock_name(tenant_slug)
    ttl = int(ttl_seconds or settings.ingest_tenant_lock_ttl_seconds)
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], ARGV[2])
    else
        return 0
    end
    """
    return bool(_redis().eval(script, 1, key, token, ttl))


def allow_tenant_ingestion(tenant_slug: str, jobs_per_window: int | None = None, window_seconds: int | None = None) -> bool:
    limit = jobs_per_window or settings.ingest_throttle_jobs_per_window
    window = window_seconds or settings.ingest_throttle_window_seconds
    key = tenant_throttle_key(tenant_slug)
    current = int(_redis().incr(key))
    if current == 1:
        _redis().expire(key, window)
    return current <= limit
