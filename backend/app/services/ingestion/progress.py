from __future__ import annotations

from datetime import datetime

from redis import Redis

from app.core.config import settings
from app.services.ingestion.queueing import tenant_lock_name, tenant_queue_name

PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60


def _redis() -> Redis:
    return Redis.from_url(settings.redis_url, decode_responses=True)


def progress_key(tenant_slug: str) -> str:
    return f'ingest:progress:{tenant_slug}'


def queue_depth(tenant_slug: str) -> int:
    return int(_redis().llen(tenant_queue_name(tenant_slug)))


def begin_ingest_progress(
    *,
    tenant_slug: str,
    operation: str,
    status: str = 'queued',
    total_jobs: int | None = None,
    start_queue_depth: int | None = None,
    target_queue_depth: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    chunk_records: int | None = None,
    chunk_days: int | None = None,
) -> dict[str, object]:
    redis = _redis()
    key = progress_key(tenant_slug)
    # Reset stale fields from previous runs (e.g. completed_at/last_error).
    redis.delete(key)
    now = datetime.utcnow().isoformat()
    current_depth = int(start_queue_depth) if start_queue_depth is not None else int(redis.llen(tenant_queue_name(tenant_slug)))
    total_value = max(0, int(total_jobs or 0))
    if target_queue_depth is None:
        target_queue_depth = max(0, current_depth - total_value)
    target_depth = max(0, int(target_queue_depth))
    window = max(1, current_depth - target_depth)
    done = max(0, current_depth - max(current_depth, target_depth))
    pct = round(min(100.0, max(0.0, (done / window) * 100.0)), 1)
    payload: dict[str, str] = {
        'tenant_slug': tenant_slug,
        'operation': str(operation or 'backfill'),
        'status': str(status or 'queued'),
        'total_jobs': str(total_value),
        'start_queue_depth': str(current_depth),
        'target_queue_depth': str(target_depth),
        'current_queue_depth': str(current_depth),
        'processed_jobs': str(done),
        'progress_pct': str(pct),
        'started_at': now,
        'updated_at': now,
    }
    if from_date:
        payload['from_date'] = from_date
    if to_date:
        payload['to_date'] = to_date
    if chunk_records is not None:
        payload['chunk_records'] = str(max(1, int(chunk_records)))
    if chunk_days is not None:
        payload['chunk_days'] = str(max(1, int(chunk_days)))
    redis.hset(key, mapping=payload)
    redis.expire(key, PROGRESS_TTL_SECONDS)
    return get_ingest_progress(tenant_slug)


def update_ingest_progress(tenant_slug: str, *, status: str | None = None, error: str | None = None) -> dict[str, object]:
    redis = _redis()
    key = progress_key(tenant_slug)
    existing = redis.hgetall(key)
    if not existing:
        return get_ingest_progress(tenant_slug)

    operation = str(existing.get('operation') or 'backfill')
    queue_depth_now = int(redis.llen(tenant_queue_name(tenant_slug)))
    start_depth = int(existing.get('start_queue_depth') or queue_depth_now)
    target_depth = int(existing.get('target_queue_depth') or 0)
    total_jobs = int(existing.get('total_jobs') or max(0, start_depth - target_depth))
    window = max(1, start_depth - target_depth)
    done = max(0, start_depth - max(queue_depth_now, target_depth))
    pct = round(min(100.0, max(0.0, (done / window) * 100.0)), 1)
    now = datetime.utcnow().isoformat()
    next_status = status or str(existing.get('status') or 'running')
    current_depth_value = queue_depth_now

    if operation == 'delete':
        total_jobs = max(1, int(existing.get('total_jobs') or 1))
        current_depth_value = int(existing.get('current_queue_depth') or 1)
        done = int(existing.get('processed_jobs') or 0)
        pct = round(min(100.0, max(0.0, (done / max(1, total_jobs)) * 100.0)), 1)
        if next_status == 'completed':
            current_depth_value = 0
            done = total_jobs
            pct = 100.0
        elif next_status in {'queued', 'running'}:
            current_depth_value = 1
    elif queue_depth_now <= target_depth:
        next_status = 'completed'

    update_map: dict[str, str] = {
        'status': next_status,
        'total_jobs': str(max(0, total_jobs)),
        'current_queue_depth': str(max(0, current_depth_value)),
        'processed_jobs': str(done),
        'progress_pct': str(pct),
        'updated_at': now,
    }
    if next_status in {'completed', 'stopped'}:
        update_map.setdefault('completed_at', now)
    if error:
        update_map['last_error'] = error
    elif next_status == 'completed':
        # Prevent stale historical errors from looking like an active failure
        # after the queue has fully completed.
        update_map['last_error'] = ''
    redis.hset(key, mapping=update_map)
    redis.expire(key, PROGRESS_TTL_SECONDS)
    return get_ingest_progress(tenant_slug)


def get_ingest_progress(tenant_slug: str) -> dict[str, object]:
    redis = _redis()
    key = progress_key(tenant_slug)
    raw = redis.hgetall(key)
    current_depth = int(redis.llen(tenant_queue_name(tenant_slug)))
    lock_value = redis.get(tenant_lock_name(tenant_slug))
    if not raw:
        is_running = current_depth > 0
        return {
            'tenant_slug': tenant_slug,
            'operation': 'backfill',
            'status': 'running' if is_running else 'idle',
            'total_jobs': current_depth if is_running else 0,
            'start_queue_depth': current_depth,
            'target_queue_depth': 0 if is_running else current_depth,
            'current_queue_depth': current_depth,
            'processed_jobs': 0 if is_running else 0,
            'progress_pct': 0.0 if is_running else 100.0,
            'lock_active': bool(lock_value),
            'updated_at': datetime.utcnow().isoformat(),
        }

    operation = str(raw.get('operation') or 'backfill')
    status = str(raw.get('status') or 'running')
    if operation == 'delete':
        total_jobs = max(1, int(raw.get('total_jobs') or 1))
        current_depth = int(raw.get('current_queue_depth') or (0 if status == 'completed' else 1))
        done = int(raw.get('processed_jobs') or (total_jobs if status == 'completed' else 0))
        if status == 'completed':
            current_depth = 0
            done = total_jobs
        pct = round(min(100.0, max(0.0, (done / max(1, total_jobs)) * 100.0)), 1)
        start_depth = int(raw.get('start_queue_depth') or 1)
        target_depth = int(raw.get('target_queue_depth') or 0)
    else:
        start_depth = int(raw.get('start_queue_depth') or current_depth)
        target_depth = int(raw.get('target_queue_depth') or 0)
        total_jobs = int(raw.get('total_jobs') or max(0, start_depth - target_depth))
        window = max(1, start_depth - target_depth)
        done = max(0, start_depth - max(current_depth, target_depth))
        pct = round(min(100.0, max(0.0, (done / window) * 100.0)), 1)
        if current_depth <= target_depth and status not in {'failed', 'stopped'}:
            status = 'completed'
    return {
        'tenant_slug': tenant_slug,
        'operation': operation,
        'status': status,
        'total_jobs': max(0, total_jobs),
        'start_queue_depth': start_depth,
        'target_queue_depth': target_depth,
        'current_queue_depth': current_depth,
        'processed_jobs': done,
        'progress_pct': pct,
        'from_date': raw.get('from_date') or None,
        'to_date': raw.get('to_date') or None,
        'chunk_records': int(raw.get('chunk_records') or 0),
        'chunk_days': int(raw.get('chunk_days') or 0),
        'started_at': raw.get('started_at') or None,
        'updated_at': raw.get('updated_at') or None,
        'completed_at': raw.get('completed_at') or None,
        'last_error': raw.get('last_error') or None,
        'lock_active': bool(lock_value),
    }
