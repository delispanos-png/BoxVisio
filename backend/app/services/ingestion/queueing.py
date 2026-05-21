from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from redis import Redis

from app.core.config import settings


def tenant_queue_name(tenant_slug: str) -> str:
    return f'ingest:{tenant_slug}'


def tenant_live_queue_name(tenant_slug: str) -> str:
    return f'ingest:live:{tenant_slug}'


def tenant_dlq_name(tenant_slug: str) -> str:
    return f'dlq:{tenant_slug}'


def tenant_lock_name(tenant_slug: str) -> str:
    return f'lock:ingest:{tenant_slug}'


def tenant_throttle_key(tenant_slug: str) -> str:
    return f'throttle:ingest:{tenant_slug}'


def tenant_stop_key(tenant_slug: str) -> str:
    return f'stop:ingest:{tenant_slug}'


def tenant_delete_active_key(tenant_slug: str) -> str:
    return f'delete:active:{tenant_slug}'


def tenant_priority_pool_name() -> str:
    return 'ingest:tenant:priority_pool'


def tenant_priority_registry_name() -> str:
    return 'ingest:tenant:priority'


@lru_cache
def _redis() -> Redis:
    return Redis.from_url(settings.redis_url, decode_responses=True)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _tenant_registered_priority(tenant_slug: str) -> Any:
    slug = str(tenant_slug or '').strip()
    if not slug:
        return None
    return _redis().hget(tenant_priority_registry_name(), slug)


def set_tenant_ingest_priority(tenant_slug: str, priority: int | str) -> None:
    slug = str(tenant_slug or '').strip()
    if not slug:
        return
    _redis().hset(tenant_priority_registry_name(), slug, str(priority))
    mark_tenant_queue_available(slug, {'tenant_priority': priority}, force=True)


def clear_tenant_ingest_priority(tenant_slug: str) -> int:
    return int(_redis().hdel(tenant_priority_registry_name(), str(tenant_slug or '').strip()))


def _job_priority(job: dict[str, Any], *, tenant_slug: str = '') -> int:
    raw = job.get('tenant_priority', job.get('priority'))
    if raw is None:
        raw = _tenant_registered_priority(tenant_slug) or 100
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        aliases = {'critical': 0, 'high': 10, 'production': 20, 'normal': 100, 'low': 200, 'demo': 250}
        if lowered in aliases:
            return aliases[lowered]
    try:
        priority = int(raw)
    except Exception:
        priority = 100
    payload = job.get('payload') if isinstance(job.get('payload'), dict) else {}
    if bool(payload.get('backfill')) and 'tenant_priority' not in job and 'priority' not in job:
        priority += 20
    return max(0, min(999, priority))


def _tenant_pool_score(job: dict[str, Any]) -> float:
    return float((_job_priority(job, tenant_slug=str(job.get('tenant_slug') or '')) * 10_000_000_000_000) + _now_ms())


def mark_tenant_queue_available(tenant_slug: str, job: dict[str, Any] | None = None, *, force: bool = False) -> None:
    slug = str(tenant_slug or '').strip()
    if not slug:
        return
    redis = _redis()
    if redis.llen(tenant_queue_name(slug)) <= 0 and redis.llen(tenant_live_queue_name(slug)) <= 0:
        redis.zrem(tenant_priority_pool_name(), slug)
        return
    score_job = dict(job or {})
    score_job.setdefault('tenant_slug', slug)
    score = _tenant_pool_score(score_job)
    existing = redis.zscore(tenant_priority_pool_name(), slug)
    if force or existing is None or score < float(existing):
        redis.zadd(tenant_priority_pool_name(), {slug: score})


def remove_tenant_from_priority_pool(tenant_slug: str) -> int:
    return int(_redis().zrem(tenant_priority_pool_name(), str(tenant_slug or '').strip()))


def priority_pool_depth() -> int:
    return int(_redis().zcard(tenant_priority_pool_name()))


def priority_pool_snapshot(limit: int = 50) -> list[dict[str, Any]]:
    redis = _redis()
    rows: list[dict[str, Any]] = []
    pool = tenant_priority_pool_name()
    for rank, item in enumerate(redis.zrange(pool, 0, max(0, int(limit) - 1), withscores=True), start=1):
        slug, score = item
        tenant_slug = str(slug or '').strip()
        queue_depth = int(redis.llen(tenant_queue_name(tenant_slug)) + redis.llen(tenant_live_queue_name(tenant_slug)))
        if queue_depth <= 0:
            redis.zrem(pool, tenant_slug)
            continue
        lock_value = redis.get(tenant_lock_name(tenant_slug))
        registered_priority = _tenant_registered_priority(tenant_slug)
        priority = int(float(score) // 10_000_000_000_000)
        rows.append(
            {
                'rank': rank,
                'tenant_slug': tenant_slug,
                'priority': priority,
                'registered_priority': registered_priority,
                'queue_depth': queue_depth,
                'locked': bool(lock_value),
                'score': float(score),
            }
        )
    return rows


def select_next_tenant_from_priority_pool(
    *,
    preferred_tenant_slug: str | None = None,
    max_candidates: int = 50,
) -> str | None:
    redis = _redis()
    pool = tenant_priority_pool_name()
    candidates = redis.zrange(pool, 0, max(0, int(max_candidates) - 1), withscores=False)
    preferred = str(preferred_tenant_slug or '').strip()
    preferred_available = bool(
        preferred
        and (redis.llen(tenant_queue_name(preferred)) > 0 or redis.llen(tenant_live_queue_name(preferred)) > 0)
    )
    preferred_score = redis.zscore(pool, preferred) if preferred_available else None
    for raw_slug in candidates:
        slug = str(raw_slug or '').strip()
        if not slug:
            continue
        if redis.llen(tenant_queue_name(slug)) <= 0 and redis.llen(tenant_live_queue_name(slug)) <= 0:
            redis.zrem(pool, slug)
            continue
        if redis.get(tenant_lock_name(slug)):
            continue
        if preferred_available and preferred_score is not None:
            candidate_score = redis.zscore(pool, slug)
            if candidate_score is not None and float(candidate_score) > float(preferred_score):
                return preferred
        return slug
    if preferred_available and not redis.get(tenant_lock_name(preferred)):
        return preferred
    return None


def enqueue_tenant_job(tenant_slug: str, job: dict[str, Any]) -> int:
    payload = dict(job)
    payload.setdefault('queued_at', datetime.utcnow().isoformat())
    redis = _redis()
    job_payload = payload.get('payload') if isinstance(payload.get('payload'), dict) else {}
    front_of_queue = bool(
        payload.get('front_of_queue')
        or payload.get('live_priority')
        or job_payload.get('front_of_queue')
        or job_payload.get('live_priority')
    )
    if front_of_queue:
        queue_name = tenant_live_queue_name(tenant_slug)
        stream = str(payload.get('stream') or '').strip()
        entity = str(payload.get('entity') or '').strip()
        connector = str(payload.get('connector') or '').strip()
        if stream or entity:
            retained: list[str] = []
            for raw in redis.lrange(queue_name, 0, -1):
                try:
                    queued = json.loads(raw)
                except Exception:
                    retained.append(raw)
                    continue
                if not isinstance(queued, dict):
                    retained.append(raw)
                    continue
                same_job = (
                    str(queued.get('stream') or '').strip() == stream
                    and str(queued.get('entity') or '').strip() == entity
                    and str(queued.get('connector') or '').strip() == connector
                )
                if not same_job:
                    retained.append(raw)
            pipe = redis.pipeline()
            pipe.delete(queue_name)
            if retained:
                pipe.rpush(queue_name, *retained)
            pipe.rpush(queue_name, json.dumps(payload))
            results = pipe.execute()
            depth = int(results[-1])
        else:
            depth = int(redis.rpush(queue_name, json.dumps(payload)))
    else:
        depth = int(redis.rpush(tenant_queue_name(tenant_slug), json.dumps(payload)))
    mark_tenant_queue_available(tenant_slug, payload)
    return depth


def pop_tenant_job(tenant_slug: str) -> dict[str, Any] | None:
    redis = _redis()
    raw = redis.lpop(tenant_live_queue_name(tenant_slug))
    if not raw:
        raw = redis.lpop(tenant_queue_name(tenant_slug))
    if not raw:
        redis.zrem(tenant_priority_pool_name(), tenant_slug)
        return None
    payload = json.loads(raw)
    next_raw = redis.lindex(tenant_live_queue_name(tenant_slug), 0) or redis.lindex(tenant_queue_name(tenant_slug), 0)
    if next_raw:
        try:
            next_payload = json.loads(next_raw)
        except Exception:
            next_payload = {}
        mark_tenant_queue_available(
            tenant_slug,
            next_payload if isinstance(next_payload, dict) else {},
            force=True,
        )
    else:
        redis.zrem(tenant_priority_pool_name(), tenant_slug)
    return payload


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


# ── Circuit breaker ────────────────────────────────────────────────────────
# Opened automatically when a permanent configuration error is detected
# (missing connection, bad credentials).  Stays open until the admin fixes
# the connection and either saves/tests it successfully or calls
# close_ingest_circuit() explicitly.  Prevents endless drain storms.

_CIRCUIT_KEY_FMT = 'ingest:circuit:{}'
_CIRCUIT_TTL_SECONDS = 86400  # 24 h — re-check after fix


def open_ingest_circuit(tenant_slug: str, reason: str) -> None:
    _redis().set(_CIRCUIT_KEY_FMT.format(tenant_slug), reason[:500], ex=_CIRCUIT_TTL_SECONDS)


def get_ingest_circuit_reason(tenant_slug: str) -> str | None:
    val = _redis().get(_CIRCUIT_KEY_FMT.format(tenant_slug))
    if not val:
        return None
    return (val if isinstance(val, str) else val.decode('utf-8', errors='replace')) or None


def close_ingest_circuit(tenant_slug: str) -> None:
    _redis().delete(_CIRCUIT_KEY_FMT.format(tenant_slug))
