import ast
import asyncio
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote_plus

from celery import current_app, shared_task
from redis import Redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.control_session import ControlSessionLocal
from app.db.tenant_manager import get_tenant_db_session, get_tenant_session_factory
from app.models.control import (
    GlobalRuleEntry,
    GlobalRuleSet,
    OperationalStream,
    OverrideMode,
    RuleDomain,
    SubscriptionStatus,
    Tenant,
    TenantConnection,
    TenantRuleOverride,
    TenantStatus,
)
from app.observability.metrics import (
    ingest_dead_letters_total,
    ingest_job_duration_seconds,
    ingest_jobs_total,
    ingest_jobs_tenant_total,
    ingest_retries_total,
    sync_field_fill_pct,
    sync_duration_seconds,
)
from app.services.ingestion import (
    acquire_tenant_lock,
    allow_tenant_ingestion,
    enqueue_tenant_job,
    extend_tenant_lock,
    pop_tenant_job,
    process_job,
    push_dead_letter,
    release_tenant_lock,
    tenant_delete_active_key,
    tenant_lock_name,
    tenant_stop_key,
    tenant_queue_name,
)
from app.services.ingestion.queueing import (
    close_ingest_circuit,
    get_ingest_circuit_reason,
    open_ingest_circuit,
)
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, ENTITY_TO_STREAM, STREAM_TO_ENTITY, normalize_stream_name
from app.services.ingestion.completeness_audit import (
    check_recent_batch_health,
    collect_tenant_sync_completeness,
    verify_tenant_sync_window,
)
from app.services.ingestion.engine import persist_dead_letter
from app.services.ingestion.progress import begin_ingest_progress, queue_depth, update_ingest_progress
from app.services.ingestion.sync_planner import SQL_CONNECTOR_ALIASES, plan_tenant_sync_jobs
from app.services.intelligence_service import generate_daily_insights
from app.services.kpi_cache import invalidate_tenant_cache

logger = logging.getLogger(__name__)
_TASK_LOOP: asyncio.AbstractEventLoop | None = None


def _run_coro(coro):
    global _TASK_LOOP
    if _TASK_LOOP is None or _TASK_LOOP.is_closed():
        _TASK_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_TASK_LOOP)
    return _TASK_LOOP.run_until_complete(coro)


def _default_job(connector: str, stream: str, tenant_slug: str, payload: dict | None = None) -> dict:
    normalized_stream = normalize_stream_name(stream)
    if normalized_stream is None:
        raise ValueError(f'unsupported operational stream: {stream}')
    entity = STREAM_TO_ENTITY[normalized_stream]
    return {
        'connector': connector,
        'stream': normalized_stream,
        'entity': entity,
        'tenant_slug': tenant_slug,
        'payload': payload or {},
        'attempt': 0,
        'max_retries': settings.ingest_job_max_retries,
    }


def _iter_date_chunks(from_date: date, to_date: date, chunk_days: int):
    current = from_date
    step = max(1, int(chunk_days))
    while current <= to_date:
        chunk_end = min(current + timedelta(days=step - 1), to_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _parse_iso_date(value: Any) -> date | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _recovery_streams() -> list[str]:
    raw = str(getattr(settings, 'ingest_recovery_streams_csv', '') or '').strip()
    if not raw:
        raw = 'sales_documents,purchase_documents,inventory_documents,cash_transactions,operating_expenses'
    streams: list[str] = []
    seen: set[str] = set()
    for token in raw.split(','):
        normalized = normalize_stream_name(token)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        streams.append(normalized)
    return streams


def _resolve_recovery_window_from_progress(progress: dict[str, str]) -> tuple[date, date] | None:
    if not bool(getattr(settings, 'ingest_recovery_enabled', True)):
        return None

    operation = str(progress.get('operation') or '').strip().lower()
    is_backfill = 'backfill' in operation
    is_incremental = operation.startswith('incremental_sync')

    if is_backfill and not bool(getattr(settings, 'ingest_recovery_on_backfill', True)):
        return None
    if is_incremental and not bool(getattr(settings, 'ingest_recovery_on_incremental', True)):
        return None
    if not is_backfill and not is_incremental:
        return None

    if is_backfill:
        from_date = _parse_iso_date(progress.get('from_date'))
        to_date = _parse_iso_date(progress.get('to_date'))
        if to_date is None:
            return None
        if from_date is None:
            from_date = to_date
        if from_date > to_date:
            from_date, to_date = to_date, from_date
        return from_date, to_date

    days = max(1, int(getattr(settings, 'ingest_recovery_incremental_days', 2) or 2))
    to_date = date.today()
    from_date = to_date - timedelta(days=days - 1)
    return from_date, to_date


def _resolve_tenant_primary_connector(tenant_id: int) -> str | None:
    """
    Return the connector type to use when enqueuing recovery jobs.
    Mirrors sync_planner priority: SQL connectors beat API/file.
    Returns None when no active connection exists — callers must guard against this
    to avoid enqueuing jobs that will immediately fail with 'No connection' errors.
    Falls back to 'sql_connector' only when the control DB is unreachable (transient).
    """
    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with Session(control_engine) as ctrl_session:
            rows = ctrl_session.execute(
                select(TenantConnection)
                .where(
                    TenantConnection.tenant_id == tenant_id,
                    TenantConnection.is_active.is_(True),
                )
                .order_by(TenantConnection.id.asc())
            ).scalars().all()
    except Exception:
        return 'sql_connector'  # control DB unreachable — optimistic fallback for transient errors
    finally:
        control_engine.dispose()

    if not rows:
        return None  # no active connection — do not enqueue recovery jobs
    for row in rows:
        ct = str(row.connector_type or '').strip().lower()
        if ct in SQL_CONNECTOR_ALIASES:
            return ct
    return str(rows[0].connector_type or '').strip().lower() or None


# ── Drain scheduling deduplication ────────────────────────────────────────
# A Redis key with a short TTL ensures at most ONE drain task is pending per
# tenant at any time.  Without this, concurrent drain tasks (throttle loop,
# retry loop, recovery sweep) each schedule another drain independently,
# causing exponential growth in queued drain tasks.

_DRAIN_SCHED_KEY_FMT = 'ingest:drain:sched:{}'


def _try_schedule_drain(
    redis_client: Any,
    tenant_slug: str,
    countdown: int = 1,
    max_jobs: int = 50,
    queue: str = 'ingest',
) -> bool:
    """Schedule a drain only if no drain is already pending for this tenant.

    Returns True if a new drain was scheduled, False if one was already queued.
    The caller should set next_drain_scheduled = True when this returns True.
    """
    sched_key = _DRAIN_SCHED_KEY_FMT.format(tenant_slug)
    ttl = max(countdown + 30, 60)
    if not redis_client.set(sched_key, '1', nx=True, ex=ttl):
        return False  # drain already pending — skip duplicate
    drain_tenant_ingest_queue.apply_async(
        kwargs={'tenant_slug': tenant_slug, 'max_jobs': max_jobs},
        countdown=countdown,
        queue=queue,
    )
    return True


def _enqueue_recovery_sweep_jobs(
    tenant_slug: str,
    *,
    from_date: date,
    to_date: date,
    fallback_limit: int,
    connector: str = 'sql_connector',
) -> dict[str, Any]:
    if from_date > to_date:
        return {'jobs': 0, 'streams': [], 'from_date': from_date.isoformat(), 'to_date': to_date.isoformat()}

    streams = _recovery_streams()
    if not streams:
        return {'jobs': 0, 'streams': [], 'from_date': from_date.isoformat(), 'to_date': to_date.isoformat()}

    chunk_days = max(1, int(getattr(settings, 'ingest_recovery_chunk_days', 1) or 1))
    effective_limit = max(100, int(getattr(settings, 'ingest_recovery_limit', fallback_limit) or fallback_limit))
    max_jobs = max(1, int(getattr(settings, 'ingest_recovery_max_jobs', 5000) or 5000))

    jobs = 0
    stopped_early = False
    for chunk_from, chunk_to in _iter_date_chunks(from_date, to_date, chunk_days):
        for stream in streams:
            if jobs >= max_jobs:
                stopped_early = True
                break
            payload = {
                'from_date': chunk_from.isoformat(),
                'to_date': chunk_to.isoformat(),
                'ignore_sync_state': True,
                'backfill': True,
                'recovery_check': True,
                'limit': effective_limit,
            }
            enqueue_tenant_job(
                tenant_slug,
                _default_job(connector, stream, tenant_slug, payload=payload),
            )
            jobs += 1
        if stopped_early:
            break

    return {
        'jobs': jobs,
        'streams': streams,
        'connector': connector,
        'from_date': from_date.isoformat(),
        'to_date': to_date.isoformat(),
        'chunk_days': chunk_days,
        'limit': effective_limit,
        'max_jobs': max_jobs,
        'stopped_early': stopped_early,
    }


def _clear_tenant_runtime_queue_state(
    tenant_slug: str,
    *,
    clear_lock: bool = True,
    clear_stop: bool = True,
    clear_delete_active: bool = False,
) -> int:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    keys = [
        tenant_queue_name(tenant_slug),
        f'dlq:{tenant_slug}',
        f'throttle:ingest:{tenant_slug}',
    ]
    if clear_stop:
        keys.append(tenant_stop_key(tenant_slug))
    if clear_delete_active:
        keys.append(tenant_delete_active_key(tenant_slug))
    if clear_lock:
        keys.append(f'lock:ingest:{tenant_slug}')
    return int(redis.delete(*keys))


async def _tenant_has_active_connector(tenant_slug: str, connector_type: str) -> bool:
    async with ControlSessionLocal() as db:
        tenant_id = (await db.execute(select(Tenant.id).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if tenant_id is None:
            return False
        conn_id = (
            await db.execute(
                select(TenantConnection.id).where(
                    TenantConnection.tenant_id == int(tenant_id),
                    TenantConnection.connector_type == str(connector_type).strip().lower(),
                    TenantConnection.is_active.is_(True),
                )
            )
        ).scalar_one_or_none()
        return conn_id is not None


def _enqueue_stream_job(tenant_slug: str, stream: str, *, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if bool(redis.get(tenant_delete_active_key(tenant_slug))):
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'connector': str(connector or '').strip().lower() or 'sql_connector',
            'stream': normalize_stream_name(stream),
            'reason': 'delete_active',
        }
    connector_norm = str(connector or '').strip().lower() or 'sql_connector'
    if connector_norm == 'external_api':
        has_active = _run_coro(_tenant_has_active_connector(tenant_slug, connector_norm))
        if not has_active:
            return {
                'status': 'skipped',
                'tenant': tenant_slug,
                'connector': connector_norm,
                'stream': normalize_stream_name(stream),
                'reason': 'inactive_connector',
            }
    enqueue_tenant_job(tenant_slug, _default_job(connector, stream, tenant_slug, payload=payload))
    drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
    normalized_stream = normalize_stream_name(stream)
    entity = STREAM_TO_ENTITY[normalized_stream] if normalized_stream else None
    return {
        'status': 'queued',
        'tenant': tenant_slug,
        'connector': connector,
        'stream': normalized_stream,
        'entity': entity,
    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _age_seconds(now: datetime, dt: datetime | None, default: int = 0) -> int:
    if dt is None:
        return default
    return max(0, int((now - dt).total_seconds()))


def _tenant_slug_from_task_kwargs(task_kwargs: Any) -> str | None:
    if isinstance(task_kwargs, dict):
        tenant = str(task_kwargs.get('tenant_slug') or '').strip()
        return tenant or None
    if isinstance(task_kwargs, str):
        text = task_kwargs.strip()
        if not text:
            return None
        parsed: Any = None
        try:
            parsed = json.loads(text)
        except Exception:
            try:
                parsed = ast.literal_eval(text)
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            tenant = str(parsed.get('tenant_slug') or '').strip()
            return tenant or None
    return None


def _collect_active_drain_tasks() -> dict[str, list[dict[str, Any]]]:
    now_ts = time.time()
    try:
        inspector = current_app.control.inspect(timeout=1.0)
        active_by_worker = inspector.active() or {}
    except Exception:
        logger.exception('auto_recover_inspect_active_failed')
        return {}

    by_tenant: dict[str, list[dict[str, Any]]] = {}
    for worker_tasks in active_by_worker.values():
        for task_meta in worker_tasks or []:
            if str(task_meta.get('name') or '') != 'worker.tasks.drain_tenant_ingest_queue':
                continue
            tenant_slug = _tenant_slug_from_task_kwargs(task_meta.get('kwargs'))
            if not tenant_slug:
                continue
            task_id = str(task_meta.get('id') or '').strip()
            started_raw = task_meta.get('time_start')
            age_seconds: int | None = None
            try:
                if started_raw is not None:
                    age_seconds = max(0, int(now_ts - float(started_raw)))
            except Exception:
                age_seconds = None
            by_tenant.setdefault(tenant_slug, []).append(
                {
                    'id': task_id,
                    'age_seconds': age_seconds,
                }
            )
    return by_tenant


@shared_task(
    name='worker.tasks.auto_recover_stuck_ingest',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3},
)
def auto_recover_stuck_ingest() -> dict:
    if not bool(getattr(settings, 'ingest_auto_recover_enabled', True)):
        return {'status': 'disabled'}

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    stale_after_seconds = max(60, int(getattr(settings, 'ingest_stuck_heartbeat_seconds', 180) or 180))
    force_after_seconds = max(
        stale_after_seconds + 60,
        int(getattr(settings, 'ingest_auto_recover_force_seconds', 600) or 600),
    )
    max_recoveries = max(1, int(getattr(settings, 'ingest_auto_recover_max_tenants_per_run', 10) or 10))
    now = datetime.utcnow()

    active_drain_tasks = _collect_active_drain_tasks()
    progress_keys = [str(k) for k in redis.scan_iter(match='ingest:progress:*', count=500)]

    recovered: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    checked = 0
    for key in progress_keys:
        raw = redis.hgetall(key)
        tenant_slug = str(raw.get('tenant_slug') or key.rsplit(':', 1)[-1]).strip()
        if not tenant_slug:
            continue

        status = str(raw.get('status') or '').strip().lower()
        operation = str(raw.get('operation') or '').strip().lower() or 'backfill'
        if status not in {'running', 'queued'}:
            continue
        if operation == 'delete':
            continue

        checked += 1
        queue_left = int(redis.llen(tenant_queue_name(tenant_slug)))
        if queue_left <= 0:
            update_ingest_progress(tenant_slug, status='completed')
            continue

        updated_at = _parse_iso_datetime(raw.get('updated_at'))
        if updated_at is None:
            skipped.append({'tenant': tenant_slug, 'reason': 'missing_updated_at'})
            continue
        heartbeat_age_seconds = _age_seconds(now, updated_at)

        active = active_drain_tasks.get(tenant_slug, [])
        active_ages = [int(t.get('age_seconds')) for t in active if isinstance(t.get('age_seconds'), int)]
        max_active_age_seconds = max(active_ages) if active_ages else None
        processed_jobs = int(raw.get('processed_jobs') or 0)
        has_lock = bool(redis.get(tenant_lock_name(tenant_slug)))
        # last_job_completed_at / last_job_started_at are set only on actual job events,
        # never by the heartbeat — so they reveal stalls the heartbeat would otherwise mask.
        last_job_completed_dt = _parse_iso_datetime(raw.get('last_job_completed_at'))
        last_job_started_dt = _parse_iso_datetime(raw.get('last_job_started_at'))
        job_progress_age_seconds = _age_seconds(now, last_job_completed_dt, default=heartbeat_age_seconds)
        # A job is "in flight" when it was started more recently than the last completion.
        job_in_flight = (
            last_job_started_dt is not None
            and (last_job_completed_dt is None or last_job_started_dt > last_job_completed_dt)
        )
        current_job_age_seconds = _age_seconds(now, last_job_started_dt) if job_in_flight else 0

        # Ultra-early recover:
        # If the queue is non-empty, nothing has been processed yet, there is no active
        # drain task, but a tenant lock still exists, we are almost certainly stuck
        # behind a stale lock left behind by a crashed/aborted drain attempt.
        orphan_lock_recover_after = max(stale_after_seconds, 180)
        allow_orphan_lock_recover = (
            processed_jobs <= 0
            and not active
            and has_lock
            and heartbeat_age_seconds >= orphan_lock_recover_after
        )

        # A fresh heartbeat means the asyncio event loop is running — the SQL query is
        # executing in run_in_executor (a thread) and will eventually be cancelled by
        # asyncio.wait_for or by pyodbc conn.timeout. Treat it as alive up to force_after_seconds.
        heartbeat_fresh = heartbeat_age_seconds < stale_after_seconds

        # Fast-recover: job was picked up but the event loop appears stuck (heartbeat stale).
        # If the heartbeat is fresh, the loop is alive — the SQL runs in a thread and may
        # legitimately take longer than stale_after_seconds (e.g. inventory snapshot scans).
        # Only trigger early recovery when heartbeat has gone stale (loop blocked or dead).
        fast_recover_after = max(stale_after_seconds + 30, 210)
        allow_early_recover = (
            processed_jobs <= 0
            and (
                (job_in_flight and current_job_age_seconds >= stale_after_seconds and not heartbeat_fresh)
                or heartbeat_age_seconds >= fast_recover_after
            )
        )
        active_is_making_progress = (
            active
            and processed_jobs > 0
            and job_progress_age_seconds < stale_after_seconds
            and (
                not job_in_flight
                or current_job_age_seconds < stale_after_seconds
                or (heartbeat_fresh and current_job_age_seconds < force_after_seconds)
            )
        )
        # Force-recover only when the *current job* has been running past the absolute ceiling.
        # Using drain-task age (max_active_age_seconds) was wrong: a drain that processes many
        # jobs over 17+ minutes is healthy — its Celery task age is not a stuck-job signal.
        force_by_age = job_in_flight and current_job_age_seconds >= force_after_seconds

        if active_is_making_progress and not force_by_age:
            skipped.append(
                {
                    'tenant': tenant_slug,
                    'reason': 'active_drain_progressing',
                    'heartbeat_age_seconds': heartbeat_age_seconds,
                    'job_progress_age_seconds': job_progress_age_seconds,
                    'current_job_age_seconds': current_job_age_seconds,
                    'max_active_age_seconds': max_active_age_seconds,
                    'processed_jobs': processed_jobs,
                    'force_after_seconds': force_after_seconds,
                }
            )
            continue

        # A job is "stuck" only when the current job exceeds stale_after_seconds AND
        # either the heartbeat has gone stale (worker dead) or the job exceeded force_after_seconds.
        # A fresh heartbeat with a slow-but-alive SQL query is not a stuck job.
        job_stuck = (
            job_in_flight
            and current_job_age_seconds >= stale_after_seconds
            and (not heartbeat_fresh or current_job_age_seconds >= force_after_seconds)
        )
        # within_grace: the drain has not yet exceeded its tolerance window.
        # Use job_progress_age (time since last job completed) rather than drain-task age
        # (max_active_age_seconds) so a drain that has processed many jobs over a long
        # period is not force-killed simply because its Celery task is "old".
        within_grace = (
            job_progress_age_seconds < force_after_seconds
        ) and not allow_early_recover and not job_stuck

        if active and within_grace:
            skipped.append(
                {
                    'tenant': tenant_slug,
                    'reason': 'active_drain_running',
                    'heartbeat_age_seconds': heartbeat_age_seconds,
                    'current_job_age_seconds': current_job_age_seconds,
                    'max_active_age_seconds': max_active_age_seconds,
                    'processed_jobs': processed_jobs,
                    'fast_recover_after': fast_recover_after,
                }
            )
            continue

        if not active and not has_lock and heartbeat_age_seconds < stale_after_seconds:
            continue

        if not active and has_lock and not allow_orphan_lock_recover and heartbeat_age_seconds < stale_after_seconds:
            skipped.append(
                {
                    'tenant': tenant_slug,
                    'reason': 'waiting_orphan_lock_grace',
                    'heartbeat_age_seconds': heartbeat_age_seconds,
                    'processed_jobs': processed_jobs,
                    'orphan_lock_recover_after': orphan_lock_recover_after,
                }
            )
            continue

        # Skip tenants whose circuit breaker is open — their connection is broken
        # and re-launching a drain will only re-trigger the same permanent failures.
        _circuit = get_ingest_circuit_reason(tenant_slug)
        if _circuit:
            skipped.append({'tenant': tenant_slug, 'reason': 'circuit_open', 'circuit': _circuit[:100]})
            continue

        terminated_tasks = 0
        if active:
            for task_meta in active:
                task_id = str(task_meta.get('id') or '').strip()
                if not task_id:
                    continue
                try:
                    current_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
                    terminated_tasks += 1
                except Exception:
                    logger.exception('auto_recover_revoke_failed tenant=%s task_id=%s', tenant_slug, task_id)

        cleared_lock = int(redis.delete(f'lock:ingest:{tenant_slug}'))
        cleared_throttle = int(redis.delete(f'throttle:ingest:{tenant_slug}'))
        _try_schedule_drain(redis, tenant_slug, 0, 50, queue='ingest')
        task_id_logged = 'dedup_scheduled'
        update_ingest_progress(
            tenant_slug,
            status='running',
            error=f'auto_recovery_triggered_job_age_{current_job_age_seconds}s_heartbeat_{heartbeat_age_seconds}s',
        )
        recovered.append(
            {
                'tenant': tenant_slug,
                'queue_left': queue_left,
                'heartbeat_age_seconds': heartbeat_age_seconds,
                'job_progress_age_seconds': job_progress_age_seconds,
                'current_job_age_seconds': current_job_age_seconds,
                'terminated_tasks': terminated_tasks,
                'cleared_lock': cleared_lock,
                'cleared_throttle': cleared_throttle,
                'drain_task_id': task_id_logged,
            }
        )
        logger.warning(
            'auto_recover_stuck_ingest tenant=%s queue_left=%s heartbeat_age=%ss terminated_tasks=%s',
            tenant_slug,
            queue_left,
            heartbeat_age_seconds,
            terminated_tasks,
        )
        if len(recovered) >= max_recoveries:
            break

    # A completed/stopped progress hash can still leave a Redis queue item and a
    # tenant lock behind after a worker timeout. That state is invisible to the
    # progress-only recovery loop above, but it blocks the 5-minute incremental
    # sync forever. Recover those orphan locks directly from the lock keys.
    if len(recovered) < max_recoveries:
        for lock_key in [str(k) for k in redis.scan_iter(match='lock:ingest:*', count=500)]:
            tenant_slug = lock_key.rsplit(':', 1)[-1].strip()
            if not tenant_slug or bool(redis.get(tenant_delete_active_key(tenant_slug))):
                continue
            if tenant_slug in active_drain_tasks:
                continue
            queue_left = int(redis.llen(tenant_queue_name(tenant_slug)))
            if queue_left <= 0:
                cleared_lock = int(redis.delete(lock_key))
                if cleared_lock:
                    recovered.append(
                        {
                            'tenant': tenant_slug,
                            'queue_left': 0,
                            'reason': 'orphan_lock_without_queue',
                            'cleared_lock': cleared_lock,
                        }
                    )
                continue

            if get_ingest_circuit_reason(tenant_slug):
                continue  # broken connection — don't revive
            cleared_lock = int(redis.delete(lock_key))
            cleared_throttle = int(redis.delete(f'throttle:ingest:{tenant_slug}'))
            _try_schedule_drain(redis, tenant_slug, 0, 50, queue='ingest')
            begin_ingest_progress(
                tenant_slug=tenant_slug,
                operation='incremental_sync_recovery',
                status='running',
                total_jobs=queue_left,
                start_queue_depth=queue_left,
                target_queue_depth=0,
            )
            recovered.append(
                {
                    'tenant': tenant_slug,
                    'queue_left': queue_left,
                    'reason': 'orphan_lock_with_queue',
                    'cleared_lock': cleared_lock,
                    'cleared_throttle': cleared_throttle,
                    'drain_task_id': 'dedup_scheduled',
                }
            )
            logger.warning(
                'auto_recover_orphan_ingest_lock tenant=%s queue_left=%s cleared_lock=%s',
                tenant_slug,
                queue_left,
                cleared_lock,
            )
            if len(recovered) >= max_recoveries:
                break

    return {
        'status': 'ok',
        'stale_after_seconds': stale_after_seconds,
        'force_after_seconds': force_after_seconds,
        'checked_running_tenants': checked,
        'recovered_count': len(recovered),
        'recovered': recovered,
        'skipped': skipped[:max(1, max_recoveries)],
    }


@shared_task(
    name='worker.tasks.audit_sync_completeness',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3},
)
def audit_sync_completeness() -> dict:
    checked = 0
    warnings: list[dict[str, object]] = []
    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with Session(control_engine) as control_session:
            tenants = control_session.execute(
                select(Tenant).where(Tenant.status != TenantStatus.terminated)
            ).scalars().all()
    finally:
        control_engine.dispose()

    for tenant in tenants:
        tenant_engine = create_engine(
            settings.tenant_database_url_template_sync.format(
                user=tenant.db_user,
                password=tenant.db_password,
                db_name=tenant.db_name,
            ),
            future=True,
        )
        try:
            audit = collect_tenant_sync_completeness(tenant_engine)
        finally:
            tenant_engine.dispose()

        checked += 1
        metric_map = {
            ('items', 'softone_sotype'): audit['items']['sotype_fill_pct'],
            ('items', 'group_id'): audit['items']['group_id_fill_pct'],
            ('sales', 'document_type'): audit['sales']['document_type_fill_pct'],
            ('sales', 'document_status'): audit['sales']['document_status_fill_pct'],
            ('sales', 'payment_method'): audit['sales']['payment_method_fill_pct'],
            ('sales', 'source_created_at'): audit['sales']['source_created_at_fill_pct'],
            ('sales', 'channel_ext_id'): audit['sales']['channel_ext_id_fill_pct'],
            ('sales', 'group_ext_id'): audit['sales']['group_ext_id_fill_pct'],
        }
        for (dataset, field), pct in metric_map.items():
            sync_field_fill_pct.labels(tenant=tenant.slug, dataset=dataset, field=field).set(float(pct))

        if audit['sales']['total'] > 0 and float(audit['sales']['document_status_fill_pct']) < 95.0:
            warnings.append(
                {
                    'tenant': tenant.slug,
                    'issue': 'sales_document_status_low_fill',
                    'fill_pct': audit['sales']['document_status_fill_pct'],
                    'max_doc_date': audit['sales']['max_doc_date'],
                }
            )
        if audit['items']['total'] > 0 and float(audit['items']['group_id_fill_pct']) < 80.0:
            warnings.append(
                {
                    'tenant': tenant.slug,
                    'issue': 'item_group_id_low_fill',
                    'fill_pct': audit['items']['group_id_fill_pct'],
                }
            )

    if warnings:
        logger.warning('sync_completeness_warnings=%s', json.dumps(warnings, ensure_ascii=False))
    return {'status': 'ok', 'checked': checked, 'warnings': warnings}


def _build_tenant_sync_engine(tenant: Tenant):
    return create_engine(
        settings.tenant_database_url_template_sync.format(
            user=tenant.db_user,
            password=tenant.db_password,
            db_name=tenant.db_name,
        ),
        future=True,
    )


def _load_tenant_sync(tenant_slug: str) -> Tenant | None:
    control_engine = create_engine(settings.control_database_url_sync, future=True)
    try:
        with Session(control_engine) as ctrl_session:
            return ctrl_session.execute(
                select(Tenant).where(Tenant.slug == tenant_slug)
            ).scalar_one_or_none()
    finally:
        control_engine.dispose()


@shared_task(
    name='worker.tasks.verify_and_recover_tenant_sync',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 2},
)
def verify_and_recover_tenant_sync(
    tenant_slug: str,
    operation: str = '',
    from_date_str: str = '',
    to_date_str: str = '',
    connector: str = '',
) -> dict:
    """
    Post-sync verification with two distinct modes selected by operation type.

    INCREMENTAL mode (5-minute automatic syncs):
      - Lightweight batch-health check only: queries ingest_batches + ingest_dead_letter
        for the current drain window (~30 min look-back).
      - Does NOT scan historical date ranges or fact tables.
      - Does NOT enqueue historical recovery jobs.
      - On failure: re-enqueues ONE incremental sync job (picks up from last_sync_timestamp
        in the sync_state table). Max incremental retries: ingest_verify_incr_max_retries.

    BACKFILL mode (manual backfill / initial sync / admin-triggered verification):
      - Full date-coverage check via verify_tenant_sync_window().
      - Gap detection and fill-rate analysis.
      - On gap: enqueues recovery sweep for the missing sub-range ONLY (not a full re-sync).

    Both modes use separate Redis guard keys so an incremental guard never blocks a
    backfill verification and vice-versa.

    Logged events: sync_completed, verification_started, verification_passed,
    verification_failed, missing_chunks_detected, retry_enqueued, retry_exhausted.
    Result stored in Redis at ingest:verify:{slug}:result (TTL 48 h).
    """
    is_backfill = any(k in operation.lower() for k in ('backfill', 'initial', 'admin_verify'))
    mode = 'backfill' if is_backfill else 'incremental'

    result_ttl = 172800  # 48 h
    redis = Redis.from_url(settings.redis_url, decode_responses=True)

    # Separate guard keys per mode so they never interfere.
    guard_key = f'ingest:verify:{tenant_slug}:{mode}:guard'
    result_key = f'ingest:verify:{tenant_slug}:result'

    if mode == 'incremental':
        # ── INCREMENTAL path ───────────────────────────────────────────────────
        # Guard: at most one incremental check per guard window (default 10 min).
        incr_guard_ttl = max(60, int(getattr(settings, 'ingest_verify_incr_guard_ttl_seconds', 600) or 600))
        if not redis.set(guard_key, '1', nx=True, ex=incr_guard_ttl):
            return {'status': 'skipped', 'tenant': tenant_slug, 'mode': mode, 'reason': 'verify_guard_active'}

        # Look back across the recent drain window only.
        lookback_minutes = max(5, int(getattr(settings, 'ingest_verify_incr_lookback_minutes', 30) or 30))
        since = datetime.utcnow() - timedelta(minutes=lookback_minutes)

        logger.info('verification_started tenant=%s mode=incremental since=%s', tenant_slug, since.isoformat())

        tenant = _load_tenant_sync(tenant_slug)
        if tenant is None:
            logger.error('verification_started tenant=%s mode=incremental — tenant not found', tenant_slug)
            return {'status': 'error', 'tenant': tenant_slug, 'mode': mode, 'reason': 'tenant_not_found'}

        tenant_engine = _build_tenant_sync_engine(tenant)
        try:
            health = check_recent_batch_health(tenant_engine, since=since)
        finally:
            tenant_engine.dispose()

        passed = not health['has_failures']

        if not passed:
            # Count and cap incremental retries to avoid infinite loops.
            retry_count_key = f'ingest:verify:{tenant_slug}:incr_retry_count'
            max_incr_retries = max(1, int(getattr(settings, 'ingest_verify_incr_max_retries', 3) or 3))
            current_retries = int(redis.get(retry_count_key) or 0)

            if current_retries >= max_incr_retries:
                logger.warning(
                    'retry_exhausted tenant=%s mode=incremental retries=%s/%s failed_batches=%s dead_letters=%s',
                    tenant_slug, current_retries, max_incr_retries,
                    len(health['failed_batches']), health['pending_dead_letters'],
                )
            else:
                redis.incr(retry_count_key)
                redis.expire(retry_count_key, 3600)  # Reset counter after 1 h of no failures
                # Re-trigger one incremental sync. It uses sync_state.last_sync_timestamp
                # to resume from the correct point — no historical data is touched.
                enqueue_incremental_sync.apply_async(
                    kwargs={'tenant_slug': tenant_slug},
                    countdown=max(10, int(getattr(settings, 'ingest_verify_incr_retry_backoff_seconds', 30) or 30) * (current_retries + 1)),
                    queue='ingest',
                )
                logger.warning(
                    'retry_enqueued tenant=%s mode=incremental attempt=%s/%s failed_batches=%s dead_letters=%s',
                    tenant_slug, current_retries + 1, max_incr_retries,
                    len(health['failed_batches']), health['pending_dead_letters'],
                )
            logger.warning(
                'verification_failed tenant=%s mode=incremental since=%s failed_batches=%s dead_letters=%s',
                tenant_slug, since.isoformat(),
                len(health['failed_batches']), health['pending_dead_letters'],
            )
        else:
            # Reset retry counter on success.
            redis.delete(f'ingest:verify:{tenant_slug}:incr_retry_count')
            logger.info(
                'verification_passed tenant=%s mode=incremental since=%s',
                tenant_slug, since.isoformat(),
            )

        result_payload: dict = {
            'tenant': tenant_slug,
            'mode': mode,
            'operation': operation,
            'verified_at': datetime.utcnow().isoformat(),
            'status': 'passed' if passed else 'failed',
            'incremental_since': since.isoformat(),
            'failed_batches': health['failed_batches'],
            'pending_dead_letters': health['pending_dead_letters'],
        }
        redis.set(result_key, json.dumps(result_payload, ensure_ascii=False, default=str), ex=result_ttl)

        return {
            'status': 'ok',
            'tenant': tenant_slug,
            'mode': mode,
            'verification_status': 'passed' if passed else 'failed',
            'failed_batches': len(health['failed_batches']),
            'pending_dead_letters': health['pending_dead_letters'],
        }

    # ── BACKFILL / ADMIN VERIFY path ──────────────────────────────────────────
    # Guard: at most one backfill verification per guard window (default 20 min).
    backfill_guard_ttl = max(60, int(getattr(settings, 'ingest_verify_guard_ttl_seconds', 1200) or 1200))
    if not redis.set(guard_key, '1', nx=True, ex=backfill_guard_ttl):
        return {'status': 'skipped', 'tenant': tenant_slug, 'mode': mode, 'reason': 'verify_guard_active'}

    gap_tolerance_days = max(0, int(getattr(settings, 'ingest_verify_gap_tolerance_days', 3) or 3))
    fill_pct_threshold = max(0.0, float(getattr(settings, 'ingest_verify_fill_pct_threshold', 80.0) or 80.0))

    verify_to = _parse_iso_date(to_date_str) or date.today()
    verify_from = _parse_iso_date(from_date_str) or verify_to

    logger.info(
        'verification_started tenant=%s mode=backfill operation=%s from=%s to=%s',
        tenant_slug, operation, verify_from.isoformat(), verify_to.isoformat(),
    )

    tenant = _load_tenant_sync(tenant_slug)
    if tenant is None:
        logger.error('verification_started tenant=%s mode=backfill — tenant not found', tenant_slug)
        return {'status': 'error', 'tenant': tenant_slug, 'mode': mode, 'reason': 'tenant_not_found'}

    # Use the connector passed by the drain when available; otherwise resolve from control DB.
    recovery_connector: str | None = connector.strip() if connector.strip() else _resolve_tenant_primary_connector(tenant.id)
    if not recovery_connector:
        logger.warning(
            'verify_and_recover_skipped tenant=%s reason=no_active_connection',
            tenant_slug,
        )
        return {'status': 'skipped', 'tenant': tenant_slug, 'mode': mode, 'reason': 'no_active_connection'}

    tenant_engine = _build_tenant_sync_engine(tenant)
    try:
        window_data = verify_tenant_sync_window(
            tenant_engine,
            from_date=verify_from,
            to_date=verify_to,
            gap_tolerance_days=gap_tolerance_days,
        )
        global_audit = collect_tenant_sync_completeness(tenant_engine)
    finally:
        tenant_engine.dispose()

    gaps: list[dict] = []
    fill_warnings: list[dict] = []
    recovery_from: date | None = None
    recovery_to: date | None = None

    for circuit, cdata in window_data['circuits'].items():
        if not cdata.get('ok', True):
            logger.warning(
                'verification_circuit_error tenant=%s circuit=%s error=%s',
                tenant_slug, circuit, cdata.get('error', ''),
            )
            continue

        if cdata.get('gap_at_end') and cdata.get('suggested_gap_from') and cdata.get('suggested_gap_to'):
            gf = _parse_iso_date(cdata['suggested_gap_from'])
            gt = _parse_iso_date(cdata['suggested_gap_to'])
            if gf and gt:
                gaps.append({
                    'circuit': circuit,
                    'stream': cdata.get('stream', ''),
                    'type': 'end_gap',
                    'lag_days': (verify_to - date.fromisoformat(cdata['max_date'][:10])).days if cdata.get('max_date') else None,
                    'gap_from': gf.isoformat(),
                    'gap_to': gt.isoformat(),
                })
                recovery_from = gf if recovery_from is None else min(recovery_from, gf)
                recovery_to = gt if recovery_to is None else max(recovery_to, gt)

        if cdata.get('gap_at_start'):
            logger.info(
                'missing_chunks_detected tenant=%s circuit=%s type=start_gap min_date=%s expected_from=%s',
                tenant_slug, circuit, cdata.get('min_date'), verify_from.isoformat(),
            )

        if int(cdata.get('null_ext_id_count') or 0) > 0:
            logger.warning(
                'verification_failed tenant=%s circuit=%s issue=null_external_id count=%s',
                tenant_slug, circuit, cdata['null_ext_id_count'],
            )

    sales_total = global_audit['sales']['total']
    if sales_total > 0:
        for field, pct_key in [
            ('document_status', 'document_status_fill_pct'),
            ('payment_method', 'payment_method_fill_pct'),
        ]:
            pct = float(global_audit['sales'].get(pct_key) or 100.0)
            if pct < fill_pct_threshold:
                fill_warnings.append({'circuit': 'sales', 'field': field, 'fill_pct': pct})
                logger.warning(
                    'verification_failed tenant=%s mode=backfill circuit=sales field=%s fill_pct=%.1f threshold=%.1f',
                    tenant_slug, field, pct, fill_pct_threshold,
                )

    if window_data['dead_letters']:
        logger.warning(
            'verification_failed tenant=%s mode=backfill dead_letters=%s',
            tenant_slug, json.dumps(window_data['dead_letters'], ensure_ascii=False),
        )
    if window_data['failed_batches']:
        logger.warning(
            'verification_failed tenant=%s mode=backfill failed_batches=%s',
            tenant_slug, json.dumps(window_data['failed_batches'], ensure_ascii=False),
        )

    recovery_jobs_queued = 0
    if gaps and recovery_from is not None and recovery_to is not None:
        logger.warning(
            'missing_chunks_detected tenant=%s mode=backfill gaps=%s recovery_from=%s recovery_to=%s',
            tenant_slug, json.dumps(gaps, ensure_ascii=False),
            recovery_from.isoformat(), recovery_to.isoformat(),
        )
        if bool(getattr(settings, 'ingest_recovery_enabled', True)):
            recovery_meta = _enqueue_recovery_sweep_jobs(
                tenant_slug,
                from_date=recovery_from,
                to_date=recovery_to,
                fallback_limit=max(100, int(getattr(settings, 'incremental_sync_limit', 500) or 500)),
                connector=recovery_connector,
            )
            recovery_jobs_queued = int(recovery_meta.get('jobs') or 0)
            if recovery_jobs_queued > 0:
                # Reset progress so the UI shows accurate counts for the recovery phase
                # and the next drain's verify runs in incremental (lightweight) mode.
                _current_depth = int(redis.llen(tenant_queue_name(tenant_slug)))
                begin_ingest_progress(
                    tenant_slug=tenant_slug,
                    operation='incremental_sync_recovery',
                    status='queued',
                    total_jobs=recovery_jobs_queued,
                    start_queue_depth=_current_depth,
                    target_queue_depth=max(0, _current_depth - recovery_jobs_queued),
                    from_date=recovery_from.isoformat(),
                    to_date=recovery_to.isoformat(),
                )
                _try_schedule_drain(redis, tenant_slug, 5, 50, queue='ingest')
                logger.warning(
                    'retry_enqueued tenant=%s mode=backfill jobs=%s recovery_from=%s recovery_to=%s',
                    tenant_slug, recovery_jobs_queued,
                    recovery_from.isoformat(), recovery_to.isoformat(),
                )
            else:
                logger.warning('retry_exhausted tenant=%s mode=backfill reason=no_jobs_from_recovery_sweep', tenant_slug)

    passed = not gaps and not fill_warnings and not window_data['dead_letters'] and not window_data['failed_batches']
    if passed:
        logger.info(
            'verification_passed tenant=%s mode=backfill operation=%s from=%s to=%s circuits=%s',
            tenant_slug, operation, verify_from.isoformat(), verify_to.isoformat(),
            list(window_data['circuits'].keys()),
        )
    else:
        logger.warning(
            'verification_failed tenant=%s mode=backfill operation=%s from=%s to=%s gaps=%s fill_warnings=%s',
            tenant_slug, operation, verify_from.isoformat(), verify_to.isoformat(),
            len(gaps), len(fill_warnings),
        )

    result_payload = {
        'tenant': tenant_slug,
        'mode': mode,
        'operation': operation,
        'connector': recovery_connector,
        'from_date': verify_from.isoformat(),
        'to_date': verify_to.isoformat(),
        'verified_at': datetime.utcnow().isoformat(),
        'status': 'passed' if passed else 'failed',
        'circuits': window_data['circuits'],
        'items_total': window_data['items_total'],
        'gaps': gaps,
        'fill_warnings': fill_warnings,
        'dead_letters': window_data['dead_letters'],
        'failed_batches': window_data['failed_batches'],
        'recovery_jobs_enqueued': recovery_jobs_queued,
    }
    redis.set(result_key, json.dumps(result_payload, ensure_ascii=False, default=str), ex=result_ttl)

    return {
        'status': 'ok',
        'tenant': tenant_slug,
        'mode': mode,
        'verification_status': 'passed' if passed else 'failed',
        'gaps': len(gaps),
        'fill_warnings': len(fill_warnings),
        'recovery_jobs_queued': recovery_jobs_queued,
    }


@shared_task(name='worker.tasks.ingest_sales_documents', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_sales_documents(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'sales_documents', connector=connector, payload=payload)


@shared_task(name='worker.tasks.ingest_purchase_documents', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_purchase_documents(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'purchase_documents', connector=connector, payload=payload)


@shared_task(name='worker.tasks.ingest_inventory_documents', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_inventory_documents(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'inventory_documents', connector=connector, payload=payload)


@shared_task(name='worker.tasks.ingest_cash_transactions', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_cash_transactions(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'cash_transactions', connector=connector, payload=payload)


@shared_task(name='worker.tasks.ingest_supplier_balances', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_supplier_balances(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'supplier_balances', connector=connector, payload=payload)


@shared_task(name='worker.tasks.ingest_customer_balances', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def ingest_customer_balances(tenant_slug: str, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
    return _enqueue_stream_job(tenant_slug, 'customer_balances', connector=connector, payload=payload)


@shared_task(name='worker.tasks.sync_pharmacyone_sales', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_sales(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'sales_documents', connector='sql_connector')


@shared_task(name='worker.tasks.sync_pharmacyone_purchases', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_purchases(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'purchase_documents', connector='sql_connector')


@shared_task(name='worker.tasks.sync_pharmacyone_inventory', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_inventory(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'inventory_documents', connector='sql_connector')


@shared_task(name='worker.tasks.sync_pharmacyone_cashflows', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_cashflows(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'cash_transactions', connector='sql_connector')


@shared_task(name='worker.tasks.sync_pharmacyone_supplier_balances', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_supplier_balances(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'supplier_balances', connector='sql_connector')


@shared_task(name='worker.tasks.sync_pharmacyone_customer_balances', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def sync_pharmacyone_customer_balances(tenant_slug: str) -> dict:
    return _enqueue_stream_job(tenant_slug, 'customer_balances', connector='sql_connector')


@shared_task(name='worker.tasks.enqueue_external_ingest', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def enqueue_external_ingest(tenant_slug: str, entity: str, payload: dict) -> dict:
    stream = normalize_stream_name(entity) or ENTITY_TO_STREAM.get(entity)
    if stream is None:
        raise ValueError(f'Unsupported external ingestion stream/entity: {entity}')
    return _enqueue_stream_job(tenant_slug, stream, connector='external_api', payload=payload)


@shared_task(name='worker.tasks.enqueue_incremental_sync', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def enqueue_incremental_sync(tenant_slug: str, limit: int = 500) -> dict:
    return _run_coro(_enqueue_incremental_sync(tenant_slug=tenant_slug, limit=limit))


async def _enqueue_incremental_sync(tenant_slug: str, limit: int = 500) -> dict:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if bool(redis.get(tenant_delete_active_key(tenant_slug))):
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'reason': 'delete_active',
        }
    if bool(redis.get(tenant_stop_key(tenant_slug))):
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'reason': 'stop_requested',
        }
    progress = redis.hgetall(f'ingest:progress:{tenant_slug}')
    if str(progress.get('operation') or '') == 'delete' and str(progress.get('status') or '') in {'queued', 'running'}:
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'reason': 'delete_in_progress',
        }
    queue_depth = int(redis.llen(tenant_queue_name(tenant_slug)))
    if queue_depth > 0:
        lock_active = bool(redis.get(f'lock:ingest:{tenant_slug}'))
        progress_status = str(progress.get('status') or '').strip().lower()
        if lock_active and progress_status in {'completed', 'failed', 'stopped', 'idle'}:
            redis.delete(f'lock:ingest:{tenant_slug}', f'throttle:ingest:{tenant_slug}')
            drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
            begin_ingest_progress(
                tenant_slug=tenant_slug,
                operation='incremental_sync_recovery',
                status='running',
                total_jobs=queue_depth,
                start_queue_depth=queue_depth,
                target_queue_depth=0,
            )
            return {
                'status': 'resumed',
                'tenant': tenant_slug,
                'reason': 'queue_not_empty_completed_progress',
                'queue_depth': queue_depth,
            }
        if not lock_active:
            if progress_status in {'completed', 'failed', 'stopped', 'idle', ''}:
                begin_ingest_progress(
                    tenant_slug=tenant_slug,
                    operation='incremental_sync_recovery',
                    status='running',
                    total_jobs=queue_depth,
                    start_queue_depth=queue_depth,
                    target_queue_depth=0,
                )
            drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
            return {
                'status': 'resumed',
                'tenant': tenant_slug,
                'reason': 'queue_not_empty_no_lock',
                'queue_depth': queue_depth,
            }
        return {
            'status': 'skipped',
            'tenant': tenant_slug,
            'reason': 'queue_not_empty',
            'queue_depth': queue_depth,
        }

    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if not tenant:
            return {'status': 'not_found', 'tenant': tenant_slug}
        jobs = await plan_tenant_sync_jobs(control_db, tenant_id=tenant.id, tenant_slug=tenant.slug)

    if not jobs:
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'no_jobs'}

    queued = 0
    effective_limit = max(100, int(limit))
    for base_job in jobs:
        payload = dict(base_job.get('payload') or {})
        planned_limit = int(payload.get('limit') or effective_limit)
        payload['limit'] = max(100, min(planned_limit, effective_limit))
        job = dict(base_job)
        job['payload'] = payload
        job.setdefault('attempt', 0)
        job.setdefault('max_retries', settings.ingest_job_max_retries)
        enqueue_tenant_job(tenant_slug, job)
        queued += 1

    begin_ingest_progress(
        tenant_slug=tenant_slug,
        operation='incremental_sync',
        status='queued',
        total_jobs=queued,
        start_queue_depth=int(redis.llen(tenant_queue_name(tenant_slug))),
        target_queue_depth=0,
    )
    drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
    return {'status': 'queued', 'tenant': tenant_slug, 'jobs': queued}


@shared_task(
    name='worker.tasks.enqueue_incremental_sync_all_tenants',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3},
)
def enqueue_incremental_sync_all_tenants(
    limit: int | None = None,
    max_tenants: int | None = None,
) -> dict:
    return _run_coro(_enqueue_incremental_sync_all_tenants(limit=limit, max_tenants=max_tenants))


def _tenant_auto_sync_settings(tenant: Tenant) -> dict:
    flags = tenant.feature_flags if isinstance(getattr(tenant, 'feature_flags', None), dict) else {}
    cfg = flags.get('auto_sync') if isinstance(flags.get('auto_sync'), dict) else {}
    enabled_raw = cfg.get('enabled', True)
    if isinstance(enabled_raw, str):
        enabled = enabled_raw.strip().lower() not in {'0', 'false', 'no', 'off', 'disabled'}
    else:
        enabled = bool(enabled_raw)
    try:
        interval_minutes = int(cfg.get('interval_minutes') or 0)
    except Exception:
        interval_minutes = 0
    return {
        'enabled': enabled,
        'interval_minutes': max(0, interval_minutes),
    }


def _param_bool_enabled(params: dict, key: str, default: bool = True) -> bool:
    raw = params.get(key, default)
    if isinstance(raw, str):
        return raw.strip().lower() not in {'0', 'false', 'no', 'off', 'disabled'}
    return bool(raw)


async def _enqueue_incremental_sync_all_tenants(
    limit: int | None = None,
    max_tenants: int | None = None,
) -> dict:
    if not bool(getattr(settings, 'incremental_sync_all_tenants_enabled', True)):
        return {'status': 'disabled'}

    effective_limit = max(100, int(limit or settings.incremental_sync_limit or 500))
    cap = max(1, int(max_tenants or settings.incremental_sync_max_tenants_per_run or 100))
    global_interval_minutes = max(1, int(getattr(settings, 'incremental_sync_interval_minutes', 5) or 5))
    queued = 0
    skipped = 0
    tenants_checked = 0
    now = datetime.utcnow()

    async with ControlSessionLocal() as control_db:
        tenants = (
            await control_db.execute(
                select(Tenant).where(
                    Tenant.status == TenantStatus.active,
                    Tenant.subscription_status.in_([SubscriptionStatus.active, SubscriptionStatus.trial]),
                )
            )
        ).scalars().all()

        for tenant in tenants:
            if tenants_checked >= cap:
                break
            tenants_checked += 1

            tenant_auto_sync = _tenant_auto_sync_settings(tenant)
            if not bool(tenant_auto_sync.get('enabled', True)):
                skipped += 1
                continue

            # Queue only for tenants with at least one active connector.
            active_conn = (
                await control_db.execute(
                    select(TenantConnection).where(
                        TenantConnection.tenant_id == tenant.id,
                        TenantConnection.is_active.is_(True),
                    ).limit(1)
                )
            ).scalar_one_or_none()
            if not active_conn:
                skipped += 1
                continue

            # Per-tenant sync interval: read from connection_parameters, fall back to global.
            params = active_conn.connection_parameters if isinstance(active_conn.connection_parameters, dict) else {}
            if not _param_bool_enabled(params, 'auto_sync_enabled', True):
                skipped += 1
                continue
            tenant_interval_minutes = int(
                tenant_auto_sync.get('interval_minutes')
                or params.get('sync_interval_minutes')
                or global_interval_minutes
            )
            tenant_interval_minutes = max(1, tenant_interval_minutes)

            # Skip if last sync completed more recently than the tenant's interval.
            last_sync = active_conn.last_sync_at
            if last_sync is not None:
                elapsed_seconds = (now - last_sync).total_seconds()
                if elapsed_seconds < tenant_interval_minutes * 60:
                    skipped += 1
                    continue

            result = await _enqueue_incremental_sync(tenant_slug=tenant.slug, limit=effective_limit)
            if str(result.get('status')) in {'queued', 'resumed'}:
                queued += 1
            else:
                skipped += 1

    return {
        'status': 'ok',
        'limit': effective_limit,
        'max_tenants': cap,
        'tenants_checked': tenants_checked,
        'queued': queued,
        'skipped': skipped,
    }


@shared_task(name='worker.tasks.enqueue_pharmacyone_backfill', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def enqueue_pharmacyone_backfill(
    tenant_slug: str,
    from_date_str: str,
    to_date_str: str,
    chunk_days: int = 7,
    limit: int | None = None,
    include_sales: bool = True,
    include_purchases: bool = True,
    include_inventory: bool = True,
    include_cashflows: bool = True,
    include_supplier_balances: bool = True,
    include_customer_balances: bool = True,
    include_operating_expenses: bool = True,
    operation: str = 'backfill',
) -> dict:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if bool(redis.get(tenant_delete_active_key(tenant_slug))):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'delete_active'}
    if bool(redis.get(tenant_stop_key(tenant_slug))):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'stop_requested'}
    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    if from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    batches = 0
    jobs = 0
    queue_before = queue_depth(tenant_slug)
    stream_flags = {
        'sales_documents': bool(include_sales),
        'purchase_documents': bool(include_purchases),
        'inventory_documents': bool(include_inventory),
        'cash_transactions': bool(include_cashflows),
        'supplier_balances': bool(include_supplier_balances),
        'customer_balances': bool(include_customer_balances),
        'operating_expenses': bool(include_operating_expenses),
    }
    balance_streams = {'supplier_balances', 'customer_balances'}
    base_chunk_days = max(1, int(chunk_days))
    effective_limit = max(100, int(limit or settings.incremental_sync_limit or 500))
    sales_chunk_cap = max(1, int(getattr(settings, 'ingest_backfill_sales_chunk_days', 1) or 1))
    purchases_chunk_cap = max(1, int(getattr(settings, 'ingest_backfill_purchases_chunk_days', 1) or 1))
    inventory_chunk_cap = max(1, int(getattr(settings, 'ingest_backfill_inventory_chunk_days', 1) or 1))
    stream_chunk_days = {
        # Document streams are materially more stable with daily windows during SQL
        # backfills. Keep the caps configurable, but default them to 1 day.
        'sales_documents': min(sales_chunk_cap, base_chunk_days),
        'purchase_documents': min(purchases_chunk_cap, base_chunk_days),
        'inventory_documents': min(inventory_chunk_cap, base_chunk_days),
        # Other streams follow user-selected chunk.
        'cash_transactions': base_chunk_days,
        'operating_expenses': base_chunk_days,
    }

    # Interleave jobs across enabled streams (round-robin), so dashboards get
    # visible data from all circuits earlier instead of draining all sales first.
    ordered_streams = [
        stream
        for stream in ALL_OPERATIONAL_STREAMS
        if stream_flags.get(stream) and stream not in balance_streams
    ]
    stream_iters = {
        stream: iter(_iter_date_chunks(from_date, to_date, stream_chunk_days.get(stream, base_chunk_days)))
        for stream in ordered_streams
    }
    next_chunks = {stream: next(stream_iters[stream], None) for stream in ordered_streams}
    while any(chunk is not None for chunk in next_chunks.values()):
        for stream in ordered_streams:
            chunk = next_chunks.get(stream)
            if chunk is None:
                continue
            chunk_from, chunk_to = chunk
            payload = {
                'from_date': chunk_from.isoformat(),
                'to_date': chunk_to.isoformat(),
                'ignore_sync_state': True,
                'backfill': True,
                'limit': effective_limit,
                'ensure_complete': True,
            }
            enqueue_tenant_job(
                tenant_slug,
                _default_job('sql_connector', stream, tenant_slug, payload=payload),
            )
            jobs += 1
            batches += 1
            next_chunks[stream] = next(stream_iters[stream], None)

    snapshot_payload = {
        'from_date': to_date.isoformat(),
        'to_date': to_date.isoformat(),
        'ignore_sync_state': True,
        'backfill': True,
        'ensure_complete': True,
        'limit': effective_limit,
    }
    for stream in ('supplier_balances', 'customer_balances'):
        if stream_flags.get(stream):
            enqueue_tenant_job(
                tenant_slug,
                _default_job('sql_connector', stream, tenant_slug, payload=snapshot_payload),
            )
            jobs += 1
    queue_after = queue_depth(tenant_slug)
    begin_ingest_progress(
        tenant_slug=tenant_slug,
        operation=operation,
        status='running',
        total_jobs=jobs,
        start_queue_depth=queue_after,
        target_queue_depth=queue_before,
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        chunk_days=max(1, int(chunk_days)),
    )
    drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
    return {
        'status': 'queued',
        'tenant': tenant_slug,
        'from_date': from_date.isoformat(),
        'to_date': to_date.isoformat(),
        'chunk_days': max(1, int(chunk_days)),
        'batches': batches,
        'jobs': jobs,
        'include_purchases': bool(include_purchases),
        'include_inventory': bool(include_inventory),
        'include_cashflows': bool(include_cashflows),
        'include_supplier_balances': bool(include_supplier_balances),
        'include_customer_balances': bool(include_customer_balances),
        'include_operating_expenses': bool(include_operating_expenses),
        'operation': operation,
    }


@shared_task(name='worker.tasks.enqueue_sql_backfill', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def enqueue_sql_backfill(
    tenant_slug: str,
    from_date_str: str,
    to_date_str: str,
    chunk_days: int = 7,
    limit: int | None = None,
    include_sales: bool = True,
    include_purchases: bool = True,
    include_inventory: bool = True,
    include_cashflows: bool = True,
    include_supplier_balances: bool = True,
    include_customer_balances: bool = True,
    include_operating_expenses: bool = True,
    operation: str = 'backfill',
) -> dict:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if bool(redis.get(tenant_delete_active_key(tenant_slug))):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'delete_active'}
    if bool(redis.get(tenant_stop_key(tenant_slug))):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'stop_requested'}
    return enqueue_pharmacyone_backfill(
        tenant_slug=tenant_slug,
        from_date_str=from_date_str,
        to_date_str=to_date_str,
        chunk_days=chunk_days,
        limit=limit,
        include_sales=include_sales,
        include_purchases=include_purchases,
        include_inventory=include_inventory,
        include_cashflows=include_cashflows,
        include_supplier_balances=include_supplier_balances,
        include_customer_balances=include_customer_balances,
        include_operating_expenses=include_operating_expenses,
        operation=operation,
    )


async def _truncate_tenant_operational_tables(
    tenant: Tenant,
    *,
    include_notifications: bool = True,
    progress_tenant_slug: str | None = None,
) -> list[str]:
    factory = get_tenant_session_factory(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    )
    async with factory() as tenant_db:
        await tenant_db.execute(text("SET lock_timeout = '10s'"))
        rows = (
            await tenant_db.execute(
                text(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname='public'
                      AND (
                        tablename LIKE 'fact_%'
                        OR tablename LIKE 'agg_%'
                        OR tablename LIKE 'stg_%'
                        OR tablename IN (
                          'staging_ingest_events',
                          'ingest_batches',
                          'ingest_dead_letter',
                          'sync_state',
                          'insights',
                          'insight_runs'
                        )
                      )
                    ORDER BY tablename
                    """
                )
            )
        ).scalars().all()
        tables = [str(name) for name in rows]
        if not include_notifications:
            tables = [name for name in tables if name not in {'insights', 'insight_runs'}]
        total_steps = max(1, len(tables))
        if progress_tenant_slug:
            update_ingest_progress(
                progress_tenant_slug,
                status='running',
                total_jobs=total_steps,
                processed_jobs=0,
                current_queue_depth=1,
            )
        for idx, table in enumerate(tables, start=1):
            last_exc: Exception | None = None
            for attempt in range(5):
                try:
                    await tenant_db.execute(text(f'TRUNCATE TABLE {_quote_ident(table)} RESTART IDENTITY CASCADE'))
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    msg = str(exc).lower()
                    if 'lock timeout' not in msg and 'locknotavailable' not in msg and 'cannot switch to state' not in msg:
                        raise
                    await tenant_db.rollback()
                    logger.warning(
                        "Retrying tenant table truncate after lock contention",
                        extra={"tenant_slug": tenant.slug, "table": table, "attempt": attempt + 1},
                    )
                    await _terminate_tenant_db_backends(tenant)
                    await asyncio.sleep(min(2 + attempt, 5))
                    await tenant_db.execute(text("SET lock_timeout = '10s'"))
            if last_exc is not None:
                raise last_exc
            if progress_tenant_slug:
                update_ingest_progress(
                    progress_tenant_slug,
                    status='running',
                    total_jobs=total_steps,
                    processed_jobs=idx,
                    current_queue_depth=1 if idx < total_steps else 0,
                )
        await tenant_db.commit()
        return tables


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


async def _terminate_tenant_db_backends(tenant: Tenant) -> int:
    admin_url = (
        "postgresql+asyncpg://"
        f"{quote_plus(settings.tenant_db_superuser)}:{quote_plus(settings.tenant_db_superpass)}@"
        f"{settings.tenant_db_host}:{int(settings.tenant_db_port)}/{tenant.db_name}"
    )
    engine = create_async_engine(admin_url, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT count(*)::int
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                    """
                )
            )
            existing = int(result.scalar_one() or 0)
            if existing <= 0:
                return 0
            await conn.execute(
                text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                    """
                )
            )
            return existing
    finally:
        await engine.dispose()


async def _delete_tenant_operational_tables_by_date_range(
    tenant: Tenant,
    *,
    from_date: date,
    to_date: date,
    include_notifications: bool = True,
    progress_tenant_slug: str | None = None,
) -> dict[str, object]:
    if from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    supported_tables_filter = """
      (
        t.tablename LIKE 'fact_%'
        OR t.tablename LIKE 'agg_%'
        OR t.tablename LIKE 'stg_%'
      )
    """
    candidates_sql = text(
        f"""
        WITH candidates AS (
          SELECT
            t.tablename,
            c.column_name,
            CASE c.column_name
              WHEN 'doc_date' THEN 1
              WHEN 'balance_date' THEN 2
              WHEN 'expense_date' THEN 3
              WHEN 'snapshot_date' THEN 4
              WHEN 'transaction_date' THEN 5
              WHEN 'month_start' THEN 6
              ELSE 99
            END AS prio
          FROM pg_tables t
          JOIN information_schema.columns c
            ON c.table_schema = 'public'
           AND c.table_name = t.tablename
          WHERE t.schemaname = 'public'
            AND {supported_tables_filter}
            AND c.column_name IN (
              'doc_date',
              'balance_date',
              'expense_date',
              'snapshot_date',
              'transaction_date',
              'month_start'
            )
        ),
        ranked AS (
          SELECT
            tablename,
            column_name,
            ROW_NUMBER() OVER (PARTITION BY tablename ORDER BY prio ASC) AS rn
          FROM candidates
        )
        SELECT tablename, column_name
        FROM ranked
        WHERE rn = 1
        ORDER BY tablename
        """
    )

    factory = get_tenant_session_factory(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    )
    async with factory() as tenant_db:
        await tenant_db.execute(text("SET lock_timeout = '10s'"))
        rows = (await tenant_db.execute(candidates_sql)).all()

        month_from = from_date.replace(day=1)
        month_to = to_date.replace(day=1)
        deleted_rows_total = 0
        deleted_table_count = 0
        deleted_by_table: dict[str, int] = {}
        total_steps = max(1, len(rows) + (2 if include_notifications else 0))
        completed_steps = 0
        if progress_tenant_slug:
            update_ingest_progress(
                progress_tenant_slug,
                status='running',
                total_jobs=total_steps,
                processed_jobs=completed_steps,
                current_queue_depth=1,
            )

        for table_name, column_name in rows:
            table = str(table_name or '').strip()
            col = str(column_name or '').strip()
            if not table or not col:
                continue

            sql = text(
                f"DELETE FROM {_quote_ident(table)} "
                f"WHERE {_quote_ident(col)} BETWEEN :from_date AND :to_date"
            )
            params = {
                'from_date': month_from if col == 'month_start' else from_date,
                'to_date': month_to if col == 'month_start' else to_date,
            }
            result = await tenant_db.execute(sql, params)
            rowcount = int(result.rowcount or 0)
            deleted_by_table[table] = rowcount
            if rowcount > 0:
                deleted_rows_total += rowcount
                deleted_table_count += 1
            completed_steps += 1
            if progress_tenant_slug:
                update_ingest_progress(
                    progress_tenant_slug,
                    status='running',
                    total_jobs=total_steps,
                    processed_jobs=completed_steps,
                    current_queue_depth=1,
                )

        notifications_deleted: dict[str, int] = {}
        if include_notifications:
            insight_result = await tenant_db.execute(
                text('DELETE FROM insights WHERE created_at::date BETWEEN :from_date AND :to_date'),
                {'from_date': from_date, 'to_date': to_date},
            )
            insight_rows = int(insight_result.rowcount or 0)
            notifications_deleted['insights'] = insight_rows
            if insight_rows > 0:
                deleted_rows_total += insight_rows
                deleted_table_count += 1
            completed_steps += 1
            if progress_tenant_slug:
                update_ingest_progress(
                    progress_tenant_slug,
                    status='running',
                    total_jobs=total_steps,
                    processed_jobs=completed_steps,
                    current_queue_depth=1,
                )

            run_result = await tenant_db.execute(
                text('DELETE FROM insight_runs WHERE started_at::date BETWEEN :from_date AND :to_date'),
                {'from_date': from_date, 'to_date': to_date},
            )
            run_rows = int(run_result.rowcount or 0)
            notifications_deleted['insight_runs'] = run_rows
            if run_rows > 0:
                deleted_rows_total += run_rows
                deleted_table_count += 1
            completed_steps += 1
            if progress_tenant_slug:
                update_ingest_progress(
                    progress_tenant_slug,
                    status='running',
                    total_jobs=total_steps,
                    processed_jobs=completed_steps,
                    current_queue_depth=0,
                )

        await tenant_db.commit()
        return {
            'from_date': from_date.isoformat(),
            'to_date': to_date.isoformat(),
            'touched_tables': [str(r[0]) for r in rows],
            'deleted_table_count': int(deleted_table_count),
            'deleted_rows_total': int(deleted_rows_total),
            'deleted_by_table': deleted_by_table,
            'notifications_deleted': notifications_deleted,
        }


async def _reset_tenant_data_and_backfill(
    tenant_slug: str,
    from_date_str: str,
    to_date_str: str,
    chunk_days: int = 31,
    include_purchases: bool = True,
) -> dict:
    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    if from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    lock_token = acquire_tenant_lock(tenant_slug, ttl_seconds=max(settings.ingest_tenant_lock_ttl_seconds, 900))
    if not lock_token:
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'lock_contended'}

    try:
        async with ControlSessionLocal() as control_db:
            tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
            if not tenant:
                return {'status': 'not_found', 'tenant': tenant_slug}

            planned_jobs = await plan_tenant_sync_jobs(
                control_db,
                tenant_id=tenant.id,
                tenant_slug=tenant.slug,
            )
            all_external = bool(planned_jobs) and all(
                str(job.get('connector') or '').strip().lower() == 'external_api'
                for job in planned_jobs
            )

            extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=max(settings.ingest_tenant_lock_ttl_seconds, 900))
            cleared_keys = _clear_tenant_runtime_queue_state(tenant.slug, clear_lock=False)
            extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=max(settings.ingest_tenant_lock_ttl_seconds, 900))
            truncated_tables = await _truncate_tenant_operational_tables(tenant)
            extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=max(settings.ingest_tenant_lock_ttl_seconds, 900))

            sync_conns = (
                await control_db.execute(
                    select(TenantConnection).where(TenantConnection.tenant_id == tenant.id)
                )
            ).scalars().all()
            for conn in sync_conns:
                conn.sync_status = 'never'
                conn.last_sync_at = None
            await control_db.commit()

        queued_jobs = 0
        queued_batches = 0
        task_id = None
        task_name = 'worker.tasks.enqueue_sql_backfill'
        if all_external:
            default_chunk_days = max(1, int(chunk_days))
            for base_job in planned_jobs:
                stream = normalize_stream_name(base_job.get('stream'))
                if stream == 'purchase_documents' and not include_purchases:
                    continue

                # SoftOne document endpoints frequently timeout on larger windows.
                # Keep document streams in daily chunks while leaving non-document
                # streams configurable from the requested chunk size.
                stream_chunk_days = (
                    1 if stream in {'sales_documents', 'purchase_documents', 'inventory_documents'} else default_chunk_days
                )

                for chunk_from, chunk_to in _iter_date_chunks(from_date, to_date, stream_chunk_days):
                    payload = dict(base_job.get('payload') or {})
                    default_limit = 500 if stream in {'sales_documents', 'purchase_documents', 'inventory_documents'} else 2000
                    payload.update(
                        {
                            'from_date': chunk_from.isoformat(),
                            'to_date': chunk_to.isoformat(),
                            'ignore_sync_state': True,
                            'backfill': True,
                            'limit': max(100, int(payload.get('limit') or default_limit)),
                        }
                    )
                    queued_job = dict(base_job)
                    queued_job['payload'] = payload
                    queued_job.setdefault('attempt', 0)
                    queued_job.setdefault('max_retries', settings.ingest_job_max_retries)
                    enqueue_tenant_job(tenant_slug, queued_job)
                    queued_jobs += 1
                    queued_batches += 1
            if queued_jobs > 0:
                task_name = 'worker.tasks.drain_tenant_ingest_queue'
                task = drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
                task_id = task.id
            begin_ingest_progress(
                tenant_slug=tenant_slug,
                operation='reset_sync',
                status='running',
                total_jobs=queued_jobs,
                start_queue_depth=queue_depth(tenant_slug),
                target_queue_depth=0,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                chunk_days=max(1, int(chunk_days)),
            )
        else:
            task = enqueue_sql_backfill.delay(
                tenant_slug=tenant_slug,
                from_date_str=from_date.isoformat(),
                to_date_str=to_date.isoformat(),
                chunk_days=max(1, int(chunk_days)),
                include_purchases=bool(include_purchases),
                operation='reset_sync',
            )
            task_id = task.id

        return {
            'status': 'queued',
            'tenant': tenant_slug,
            'from_date': from_date.isoformat(),
            'to_date': to_date.isoformat(),
            'chunk_days': max(1, int(chunk_days)),
            'include_purchases': bool(include_purchases),
            'connector_mode': 'external_api' if all_external else 'sql_connector',
            'cleared_redis_keys': int(cleared_keys),
            'truncated_table_count': len(truncated_tables),
            'queued_jobs': int(queued_jobs),
            'queued_batches': int(queued_batches),
            'task_name': task_name,
            'task_id': task_id,
        }
    finally:
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        redis.delete(tenant_delete_active_key(tenant_slug), f'delete:retry:{tenant_slug}')
        release_tenant_lock(tenant_slug, lock_token)


@shared_task(name='worker.tasks.reset_tenant_data_and_backfill', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def reset_tenant_data_and_backfill(
    tenant_slug: str,
    from_date_str: str,
    to_date_str: str,
    chunk_days: int = 31,
    include_purchases: bool = True,
) -> dict:
    return _run_coro(
        _reset_tenant_data_and_backfill(
            tenant_slug=tenant_slug,
            from_date_str=from_date_str,
            to_date_str=to_date_str,
            chunk_days=chunk_days,
            include_purchases=include_purchases,
        )
    )


async def _delete_tenant_data_only(
    tenant_slug: str,
    *,
    from_date_str: str | None = None,
    to_date_str: str | None = None,
    include_notifications: bool = True,
) -> dict:
    scoped_delete = bool((from_date_str or '').strip() and (to_date_str or '').strip())
    from_date = date.fromisoformat(from_date_str) if scoped_delete and from_date_str else None
    to_date = date.fromisoformat(to_date_str) if scoped_delete and to_date_str else None
    if scoped_delete and from_date and to_date and from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    lock_token = acquire_tenant_lock(tenant_slug, ttl_seconds=max(settings.ingest_tenant_lock_ttl_seconds, 900))
    if not lock_token:
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        retry_key = f'delete:retry:{tenant_slug}'
        retry_count = int(redis.incr(retry_key))
        redis.expire(retry_key, 3600)
        if retry_count <= 18:
            update_ingest_progress(tenant_slug, status='queued', error=f'delete_waiting_for_lock_retry_{retry_count}')
            delete_tenant_data_only.apply_async(
                kwargs={
                    'tenant_slug': tenant_slug,
                    'from_date_str': from_date_str,
                    'to_date_str': to_date_str,
                    'include_notifications': bool(include_notifications),
                },
                queue='delete',
                countdown=10,
            )
            return {
                'status': 'requeued',
                'tenant': tenant_slug,
                'reason': 'lock_contended',
                'retry_count': retry_count,
            }
        redis.delete(tenant_delete_active_key(tenant_slug), retry_key)
        update_ingest_progress(tenant_slug, status='failed', error='delete_lock_contended')
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'lock_contended', 'retry_count': retry_count}

    try:
        update_ingest_progress(tenant_slug, status='running', total_jobs=1, processed_jobs=0, current_queue_depth=1)
        Redis.from_url(settings.redis_url, decode_responses=True).delete(f'delete:retry:{tenant_slug}')
        tenant = None
        tenant_id: int | None = None
        cleared_keys = 0
        async with ControlSessionLocal() as control_db:
            tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
            if not tenant:
                update_ingest_progress(tenant_slug, status='failed', error='tenant_not_found')
                return {'status': 'not_found', 'tenant': tenant_slug}
            tenant_id = int(tenant.id)

            cleared_keys = _clear_tenant_runtime_queue_state(
                tenant.slug,
                clear_stop=False,
                clear_delete_active=False,
            )

        if scoped_delete and from_date and to_date:
            await _terminate_tenant_db_backends(tenant)
            scoped_stats = await _delete_tenant_operational_tables_by_date_range(
                tenant,
                from_date=from_date,
                to_date=to_date,
                include_notifications=bool(include_notifications),
                progress_tenant_slug=tenant_slug,
            )
        else:
            await _terminate_tenant_db_backends(tenant)
            truncated_tables = await _truncate_tenant_operational_tables(
                tenant,
                include_notifications=bool(include_notifications),
                progress_tenant_slug=tenant_slug,
            )
            async with ControlSessionLocal() as control_db:
                sync_conns = (
                    await control_db.execute(
                        select(TenantConnection).where(TenantConnection.tenant_id == tenant_id)
                    )
                ).scalars().all()
                for conn in sync_conns:
                    conn.sync_status = 'never'
                    conn.last_sync_at = None
                await control_db.commit()

        update_ingest_progress(tenant_slug, status='completed')
        if scoped_delete and from_date and to_date:
            return {
                'status': 'completed',
                'tenant': tenant_slug,
                'mode': 'date_range',
                'cleared_redis_keys': int(cleared_keys),
                'include_notifications': bool(include_notifications),
                **scoped_stats,
            }
        return {
            'status': 'completed',
            'tenant': tenant_slug,
            'mode': 'full',
            'cleared_redis_keys': int(cleared_keys),
            'truncated_table_count': len(truncated_tables),
            'include_notifications': bool(include_notifications),
        }
    except Exception as exc:
        Redis.from_url(settings.redis_url, decode_responses=True).delete(f'delete:retry:{tenant_slug}')
        update_ingest_progress(tenant_slug, status='failed', error=str(exc)[:300])
        raise
    finally:
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        redis.delete(tenant_delete_active_key(tenant_slug), f'delete:retry:{tenant_slug}')
        release_tenant_lock(tenant_slug, lock_token)


@shared_task(name='worker.tasks.delete_tenant_data_only')
def delete_tenant_data_only(
    tenant_slug: str,
    from_date_str: str | None = None,
    to_date_str: str | None = None,
    include_notifications: bool = True,
) -> dict:
    return _run_coro(
        _delete_tenant_data_only(
            tenant_slug=tenant_slug,
            from_date_str=from_date_str,
            to_date_str=to_date_str,
            include_notifications=include_notifications,
        )
    )


@shared_task(name='worker.tasks.drain_tenant_ingest_queue', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def drain_tenant_ingest_queue(tenant_slug: str, max_jobs: int | None = None) -> dict:
    effective_max_jobs = settings.ingest_drain_max_jobs if max_jobs is None else max_jobs
    return _run_coro(_drain_tenant_ingest_queue(tenant_slug=tenant_slug, max_jobs=effective_max_jobs))


async def _drain_tenant_ingest_queue(tenant_slug: str, max_jobs: int) -> dict:
    drain_start = time.perf_counter()
    lock_ttl = max(int(settings.ingest_tenant_lock_ttl_seconds), 900)
    lock_token = acquire_tenant_lock(tenant_slug, ttl_seconds=lock_ttl)
    if not lock_token:
        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='lock_contended').observe(0)
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'lock_contended'}

    # We're now the active drain — clear the "pending" marker so future scheduling
    # can set it again if this drain triggers a retry or throttle.
    redis_sync_pre = Redis.from_url(settings.redis_url, decode_responses=True)
    redis_sync_pre.delete(_DRAIN_SCHED_KEY_FMT.format(tenant_slug))

    # Circuit breaker: permanent connection errors block all processing until
    # the admin fixes the connection (saving/testing clears the circuit).
    circuit_reason = get_ingest_circuit_reason(tenant_slug)
    if circuit_reason:
        release_tenant_lock(tenant_slug, lock_token)
        update_ingest_progress(tenant_slug, status='error', error=f'circuit_open: {circuit_reason[:200]}')
        logger.warning('ingest_circuit_open_skip tenant=%s reason=%s', tenant_slug, circuit_reason[:200])
        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='circuit_open').observe(0)
        return {'status': 'circuit_open', 'tenant': tenant_slug, 'reason': circuit_reason}

    processed = 0
    failures = 0
    permanent_failures = 0  # config errors that must not be retried or recovery-swept
    aggregates_refreshed = 0
    throttled = 0
    recovery_jobs_queued = 0
    next_drain_scheduled = False
    stopped_by_user = False
    processed_recovery_jobs = False
    backfill_entity_ranges: dict[str, dict[str, str]] = {}
    drain_connector = 'sql_connector'  # updated from actual jobs; SQL beats API per sync_planner policy
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    stop_key = tenant_stop_key(tenant_slug)
    lock_refresh_interval = max(10, min(60, lock_ttl // 6))

    async def _run_job_with_lock_heartbeat(job_payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
        heartbeat_task = None

        async def _heartbeat() -> None:
            while True:
                await asyncio.sleep(lock_refresh_interval)
                if not extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=lock_ttl):
                    logger.warning('ingest_lock_refresh_lost tenant=%s', tenant_slug)
                    break
                # Refresh progress heartbeat too, so the admin UI does not mark a
                # long-running but healthy chunk as stuck while it is still working.
                update_ingest_progress(tenant_slug, status='running')

        try:
            heartbeat_task = asyncio.create_task(_heartbeat())
            return await asyncio.wait_for(process_job(job_payload), timeout=timeout_seconds)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            # Keep lock alive for the next loop iteration as well.
            extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=lock_ttl)

    try:
        per_job_timeout_seconds = max(30, int(getattr(settings, 'ingest_job_timeout_seconds', 900) or 900))
        for _ in range(max_jobs):
            if bool(redis.get(stop_key)):
                stopped_by_user = True
                break
            extend_tenant_lock(tenant_slug, lock_token, ttl_seconds=lock_ttl)
            job = pop_tenant_job(tenant_slug)
            if not job:
                break
            job_payload = job.get('payload') or {}
            if bool(job_payload.get('recovery_check')):
                processed_recovery_jobs = True

            connector = str(job.get('connector') or 'unknown')
            # Track primary connector: SQL beats API, mirrors sync_planner priority.
            if connector not in {'unknown', ''}:
                if connector in SQL_CONNECTOR_ALIASES or drain_connector not in SQL_CONNECTOR_ALIASES:
                    drain_connector = connector
            stream = normalize_stream_name(job.get('stream'))
            if stream is None:
                stream = ENTITY_TO_STREAM.get(str(job.get('entity') or '').strip())  # type: ignore[arg-type]
            entity = STREAM_TO_ENTITY[stream] if stream else str(job.get('entity') or 'unknown')
            metric_entity = stream or entity
            if not allow_tenant_ingestion(tenant_slug):
                throttled += 1
                ingest_jobs_total.labels(connector=connector, entity=metric_entity, status='throttled').inc()
                ingest_jobs_tenant_total.labels(tenant=tenant_slug, connector=connector, entity=metric_entity, status='throttled').inc()
                enqueue_tenant_job(tenant_slug, job)
                if _try_schedule_drain(redis, tenant_slug, settings.ingest_throttle_window_seconds, max_jobs):
                    next_drain_scheduled = True
                break

            # Heartbeat progress as soon as a job is picked, so UI reflects active work
            # even when an external request takes long before completion.
            update_ingest_progress(
                tenant_slug,
                status='running',
                job_started=True,
                current_stream=stream,
                current_entity=entity,
                current_from_date=str(job_payload.get('from_date') or ''),
                current_to_date=str(job_payload.get('to_date') or ''),
            )

            try:
                started = time.perf_counter()
                is_backfill_job = bool(job_payload.get('backfill'))
                # SQL backfills can legitimately run much longer per chunk.
                # Keep a higher ceiling to avoid repeated timeout/retry loops.
                job_timeout_seconds = per_job_timeout_seconds
                if is_backfill_job:
                    # Backfill windows (especially sales line-level) can exceed 20 minutes
                    # on large tenants. Keep a much higher timeout to avoid partial loads
                    # caused by mid-batch cancellation.
                    backfill_timeout = max(
                        1200,
                        int(getattr(settings, 'ingest_backfill_job_timeout_seconds', 7200) or 7200),
                    )
                    job_timeout_seconds = max(job_timeout_seconds, backfill_timeout)
                elif stream in {'sales_documents', 'purchase_documents', 'inventory_documents'}:
                    # Incremental line-level streams can still be heavy; avoid premature timeouts.
                    job_timeout_seconds = max(job_timeout_seconds, 900)
                result = await _run_job_with_lock_heartbeat(job, timeout_seconds=job_timeout_seconds)
                processed += 1
                connector = str(result.get('connector') or connector)
                entity = str(result.get('entity') or entity)
                stream = normalize_stream_name(result.get('stream')) or stream
                metric_entity = stream or entity
                elapsed = time.perf_counter() - started
                ingest_jobs_total.labels(connector=connector, entity=metric_entity, status='ok').inc()
                ingest_jobs_tenant_total.labels(tenant=tenant_slug, connector=connector, entity=metric_entity, status='ok').inc()
                ingest_job_duration_seconds.labels(connector=connector, entity=metric_entity, status='ok').observe(elapsed)
                min_doc_date = result.get('min_doc_date')
                max_doc_date = result.get('max_doc_date')
                is_backfill_job = bool(job_payload.get('backfill'))
                if (
                    min_doc_date
                    and max_doc_date
                    and entity in {
                    'sales',
                    'purchases',
                    'inventory',
                    'cashflows',
                    'supplier_balances',
                    'customer_balances',
                    'expenses',
                }
                ):
                    if is_backfill_job:
                        bucket = backfill_entity_ranges.setdefault(entity, {'from': str(min_doc_date), 'to': str(max_doc_date)})
                        if str(min_doc_date) < bucket['from']:
                            bucket['from'] = str(min_doc_date)
                        if str(max_doc_date) > bucket['to']:
                            bucket['to'] = str(max_doc_date)
                    else:
                        # Incremental jobs stay visible quickly. Backfills refresh once at
                        # the end of the whole requested range to avoid hundreds of slow,
                        # duplicate aggregate/insight tasks blocking ingestion.
                        refresh_aggregates_for_entity.delay(
                            tenant_slug=tenant_slug,
                            entity=entity,
                            from_date_str=min_doc_date,
                            to_date_str=max_doc_date,
                        )
                        aggregates_refreshed += 1
                update_ingest_progress(tenant_slug, status='running', job_completed=True)
            except Exception as exc:
                if bool(redis.get(stop_key)):
                    stopped_by_user = True
                    update_ingest_progress(tenant_slug, status='stopped', error='stopped_by_user')
                    logger.warning('ingest_job_stopped tenant=%s connector=%s entity=%s', tenant_slug, connector, entity)
                    break
                ingest_jobs_total.labels(connector=connector, entity=metric_entity, status='failed').inc()
                ingest_jobs_tenant_total.labels(tenant=tenant_slug, connector=connector, entity=metric_entity, status='failed').inc()
                failures += 1
                attempt = int(job.get('attempt') or 0) + 1
                max_retries = int(job.get('max_retries') or settings.ingest_job_max_retries)

                dead_letter_payload = {
                    'tenant_slug': tenant_slug,
                    'connector': job.get('connector'),
                    'entity': job.get('entity'),
                    'stream': stream,
                    'payload': job.get('payload'),
                    'attempt': attempt,
                    'error': str(exc),
                }

                # Permanent configuration errors must not be retried — retrying would
                # fill the DLQ with thousands of identical failures and cause drain storms.
                _exc_str = str(exc)
                _permanent_error = isinstance(exc, RuntimeError) and any(
                    m in _exc_str for m in (
                        'No SQL Server mapping found',
                        'Tenant not found',
                        'No connection configured',
                        'missing clientID',      # SoftOne login/authenticate failed — bad credentials config
                        'Missing clientID',      # SoftOne API-level auth rejection
                    )
                )
                if _permanent_error:
                    permanent_failures += 1
                    open_ingest_circuit(tenant_slug, str(exc)[:500])
                    logger.error(
                        'ingest_circuit_opened tenant=%s reason=%s',
                        tenant_slug, str(exc)[:200],
                    )

                if attempt <= max_retries and not _permanent_error:
                    job['attempt'] = attempt
                    enqueue_tenant_job(tenant_slug, job)
                    ingest_retries_total.labels(connector=connector, entity=metric_entity).inc()
                    retry_delay = max(settings.ingest_retry_backoff_seconds * attempt, 1)
                    if _try_schedule_drain(redis, tenant_slug, retry_delay, max_jobs):
                        next_drain_scheduled = True
                else:
                    push_dead_letter(tenant_slug, dead_letter_payload)
                    ingest_dead_letters_total.labels(connector=connector, entity=metric_entity).inc()
                    await persist_dead_letter(
                        tenant_slug=tenant_slug,
                        connector_type=str(job.get('connector') or 'unknown'),
                        entity=str(job.get('entity') or (STREAM_TO_ENTITY[stream] if stream else 'unknown')),
                        payload=job.get('payload') or {},
                        error_message=str(exc),
                    )

                logger.exception('ingest_job_failed tenant=%s connector=%s entity=%s attempt=%s', tenant_slug, job.get('connector'), job.get('entity'), attempt)
                update_ingest_progress(tenant_slug, status='running', error=str(exc)[:300])

        if stopped_by_user:
            redis.delete(tenant_queue_name(tenant_slug), f'throttle:ingest:{tenant_slug}')
        remaining_jobs = int(redis.llen(tenant_queue_name(tenant_slug)))
        if stopped_by_user:
            update_ingest_progress(tenant_slug, status='stopped', error='stopped_by_user')
            sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='stopped').observe(time.perf_counter() - drain_start)
            return {
                'status': 'stopped',
                'tenant': tenant_slug,
                'processed': processed,
                'failures': failures,
                'throttled': throttled,
                'aggregates_refreshed': aggregates_refreshed,
                'recovery_jobs_queued': recovery_jobs_queued,
                'remaining_jobs': remaining_jobs,
                'next_drain_scheduled': False,
            }
        if remaining_jobs > 0 and not next_drain_scheduled:
            if _try_schedule_drain(redis, tenant_slug, 1, max_jobs):
                next_drain_scheduled = True
        elif remaining_jobs == 0 and not processed_recovery_jobs and permanent_failures == 0:
            progress_snapshot = redis.hgetall(f'ingest:progress:{tenant_slug}')
            recovery_window = _resolve_recovery_window_from_progress(progress_snapshot)
            if recovery_window is not None:
                recovery_meta = _enqueue_recovery_sweep_jobs(
                    tenant_slug,
                    from_date=recovery_window[0],
                    to_date=recovery_window[1],
                    fallback_limit=max(100, int(getattr(settings, 'incremental_sync_limit', 500) or 500)),
                    connector=drain_connector,
                )
                recovery_jobs_queued = int(recovery_meta.get('jobs') or 0)
                if recovery_jobs_queued > 0:
                    remaining_jobs = int(redis.llen(tenant_queue_name(tenant_slug)))
                    if remaining_jobs > 0:
                        logger.info(
                            'ingest_recovery_sweep_enqueued tenant=%s jobs=%s from=%s to=%s streams=%s chunk_days=%s',
                            tenant_slug,
                            recovery_jobs_queued,
                            recovery_meta.get('from_date'),
                            recovery_meta.get('to_date'),
                            ','.join(recovery_meta.get('streams') or []),
                            recovery_meta.get('chunk_days'),
                        )
                        # Reset progress so the UI reflects the recovery phase correctly,
                        # and block the upcoming verify from double-adding the same jobs.
                        _backfill_guard_ttl = max(60, int(getattr(settings, 'ingest_verify_guard_ttl_seconds', 1200) or 1200))
                        redis.set(f'ingest:verify:{tenant_slug}:backfill:guard', '1', ex=_backfill_guard_ttl)
                        begin_ingest_progress(
                            tenant_slug=tenant_slug,
                            operation='incremental_sync_recovery',
                            status='queued',
                            total_jobs=recovery_jobs_queued,
                            start_queue_depth=remaining_jobs,
                            target_queue_depth=max(0, remaining_jobs - recovery_jobs_queued),
                            from_date=recovery_meta.get('from_date'),
                            to_date=recovery_meta.get('to_date'),
                        )
                        if _try_schedule_drain(redis, tenant_slug, 1, max_jobs):
                            next_drain_scheduled = True
        if remaining_jobs == 0 and backfill_entity_ranges:
            for agg_entity, agg_range in backfill_entity_ranges.items():
                refresh_aggregates_for_entity.delay(
                    tenant_slug=tenant_slug,
                    entity=agg_entity,
                    from_date_str=agg_range['from'],
                    to_date_str=agg_range['to'],
                )
                aggregates_refreshed += 1
        update_ingest_progress(tenant_slug, status='running' if remaining_jobs > 0 else 'completed')

        if remaining_jobs == 0 and permanent_failures == 0:
            # Schedule post-sync verification for every completed sync.
            # Skip when all failures were permanent config errors — the verify task
            # would re-queue the same jobs that will immediately fail again.
            # The verify task has its own guard key (default TTL 20 min) so it
            # self-limits during high-frequency incremental runs.
            _prog = redis.hgetall(f'ingest:progress:{tenant_slug}')
            _op = str(_prog.get('operation') or '').strip().lower()
            logger.info('sync_completed tenant=%s operation=%s processed=%s failures=%s', tenant_slug, _op, processed, failures)
            verify_and_recover_tenant_sync.apply_async(
                kwargs={
                    'tenant_slug': tenant_slug,
                    'operation': _op,
                    'from_date_str': str(_prog.get('from_date') or ''),
                    'to_date_str': str(_prog.get('to_date') or ''),
                    'connector': drain_connector,
                },
                countdown=30,
                queue='default',
            )

        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='ok').observe(time.perf_counter() - drain_start)
        return {
            'status': 'ok',
            'tenant': tenant_slug,
            'processed': processed,
            'failures': failures,
            'throttled': throttled,
            'aggregates_refreshed': aggregates_refreshed,
            'recovery_jobs_queued': recovery_jobs_queued,
            'remaining_jobs': remaining_jobs,
            'next_drain_scheduled': next_drain_scheduled,
        }
    finally:
        release_tenant_lock(tenant_slug, lock_token)


@shared_task(name='worker.tasks.refresh_aggregates_for_entity', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def refresh_aggregates_for_entity(
    tenant_slug: str,
    entity: str,
    from_date_str: str | None = None,
    to_date_str: str | None = None,
    run_insights: bool = False,
) -> dict:
    return _run_coro(_refresh_aggregates_task(tenant_slug, entity, from_date_str, to_date_str, run_insights=run_insights))


@shared_task(name='worker.tasks.refresh_sales_aggregates', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def refresh_sales_aggregates(tenant_slug: str, from_date_str: str | None = None, to_date_str: str | None = None) -> dict:
    return _run_coro(_refresh_aggregates_task(tenant_slug, 'sales', from_date_str, to_date_str))


def _normalize_document_type_key(value: Any) -> str:
    txt = str(value or '').strip().lower()
    if not txt:
        return ''
    return ' '.join(txt.split())


def _normalize_behavior_code(value: Any) -> str:
    txt = str(value or '').strip()
    if not txt:
        return ''
    return ''.join(ch for ch in txt if ch.isalnum() or ch in {'_', '-'}).strip()[:32]


def _as_rule_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value) != 0
    txt = str(value or '').strip().lower()
    if not txt:
        return default
    return txt in {'1', 'true', 'yes', 'on', 'ναι'}


def _as_rule_sign(value: Any, default: int = 1) -> int:
    if isinstance(value, (int, float)):
        num = float(value)
        if num > 0:
            return 1
        if num < 0:
            return -1
        return 0
    txt = str(value or '').strip().lower()
    if txt in {'positive', 'pos', '+', '+1', '1'}:
        return 1
    if txt in {'negative', 'neg', '-', '-1'}:
        return -1
    if txt in {'none', 'neutral', '0', ''}:
        return 0
    return default


def _default_document_rule_for_stream(stream: OperationalStream) -> dict[str, Any]:
    if stream == OperationalStream.sales_documents:
        return {
            'include_revenue': True,
            'include_quantity': True,
            'include_cost': True,
            'amount_sign': 1,
            'quantity_sign': 1,
        }
    if stream == OperationalStream.purchase_documents:
        return {
            'include_revenue': False,
            'include_quantity': True,
            'include_cost': True,
            'amount_sign': 1,
            'quantity_sign': 1,
        }
    return {
        'include_revenue': True,
        'include_quantity': True,
        'include_cost': True,
        'amount_sign': 1,
        'quantity_sign': 1,
    }


def _parse_document_rule_payload(
    payload: dict[str, Any] | None,
    *,
    default_stream: OperationalStream,
) -> dict[str, Any]:
    source = dict(payload or {})
    defaults = _default_document_rule_for_stream(default_stream)
    behavior_code = _normalize_behavior_code(
        source.get('behavior_code') or source.get('softone_behavior') or source.get('source_transaction_type_id')
    )
    document_type = str(source.get('document_type') or '').strip()
    return {
        'behavior_code': behavior_code,
        'document_type': document_type,
        'include_revenue': _as_rule_bool(source.get('include_revenue'), defaults['include_revenue']),
        'include_quantity': _as_rule_bool(source.get('include_quantity'), defaults['include_quantity']),
        'include_cost': _as_rule_bool(source.get('include_cost'), defaults['include_cost']),
        'amount_sign': _as_rule_sign(source.get('amount_sign_label') or source.get('amount_sign'), defaults['amount_sign']),
        'quantity_sign': _as_rule_sign(source.get('quantity_sign_label') or source.get('quantity_sign'), defaults['quantity_sign']),
    }


async def _load_effective_document_rules(
    control_db,
    *,
    tenant_id: int,
    stream: OperationalStream,
) -> list[dict[str, Any]]:
    tenant_feature_flags = (
        await control_db.execute(select(Tenant.feature_flags).where(Tenant.id == tenant_id))
    ).scalar_one_or_none()
    selected_ruleset_code = ''
    if isinstance(tenant_feature_flags, dict):
        selected_ruleset_code = str(tenant_feature_flags.get('document_type_ruleset_code') or '').strip()

    global_stmt = (
        select(GlobalRuleEntry, GlobalRuleSet)
        .join(GlobalRuleSet, GlobalRuleSet.id == GlobalRuleEntry.ruleset_id)
        .where(
            GlobalRuleEntry.is_active.is_(True),
            GlobalRuleEntry.domain == RuleDomain.document_type_rules,
            GlobalRuleEntry.stream == stream,
        )
    )
    if selected_ruleset_code:
        global_stmt = global_stmt.where(GlobalRuleSet.code == selected_ruleset_code)
    else:
        global_stmt = global_stmt.where(GlobalRuleSet.is_active.is_(True))

    global_rows = (
        await control_db.execute(
            global_stmt.order_by(
                GlobalRuleSet.priority.desc(),
                GlobalRuleSet.id.desc(),
                GlobalRuleEntry.id.desc(),
            )
        )
    ).all()
    by_rule_key: dict[str, dict[str, Any]] = {}
    for entry, _ruleset in global_rows:
        if entry.rule_key in by_rule_key:
            continue
        by_rule_key[entry.rule_key] = {
            'rule_key': entry.rule_key,
            'payload': dict(entry.payload_json or {}),
        }

    overrides = (
        await control_db.execute(
            select(TenantRuleOverride)
            .where(
                TenantRuleOverride.tenant_id == tenant_id,
                TenantRuleOverride.is_active.is_(True),
                TenantRuleOverride.domain == RuleDomain.document_type_rules,
                TenantRuleOverride.stream == stream,
            )
            .order_by(TenantRuleOverride.id.desc())
        )
    ).scalars().all()
    for row in overrides:
        key = row.rule_key
        if row.override_mode == OverrideMode.disable:
            by_rule_key.pop(key, None)
            continue
        override_payload = dict(row.payload_json or {})
        if row.override_mode == OverrideMode.merge and key in by_rule_key:
            merged = dict(by_rule_key[key].get('payload') or {})
            merged.update(override_payload)
            by_rule_key[key] = {'rule_key': key, 'payload': merged}
        else:
            by_rule_key[key] = {'rule_key': key, 'payload': override_payload}

    parsed_rows: list[dict[str, Any]] = []
    for item in by_rule_key.values():
        parsed = _parse_document_rule_payload(item.get('payload'), default_stream=stream)
        if not parsed.get('behavior_code') and not _normalize_document_type_key(parsed.get('document_type')):
            continue
        parsed_rows.append(parsed)
    return parsed_rows


def _document_rules_sql_payload(rule_rows: list[dict[str, Any]]) -> str:
    return json.dumps(
        [
            {
                'behavior_code': _normalize_behavior_code(row.get('behavior_code')),
                'document_type': str(row.get('document_type') or '').strip(),
                'include_revenue': bool(row.get('include_revenue')),
                'include_quantity': bool(row.get('include_quantity')),
                'include_cost': bool(row.get('include_cost')),
                'amount_sign': int(row.get('amount_sign') or 0),
                'quantity_sign': int(row.get('quantity_sign') or 0),
            }
            for row in (rule_rows or [])
        ]
    )


async def _refresh_sales_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
    document_rules_json: str,
) -> None:
    sales_classified_cte = """
        WITH rules AS (
            SELECT
                NULLIF(BTRIM(COALESCE(x->>'behavior_code', '')), '') AS behavior_code,
                LOWER(NULLIF(BTRIM(COALESCE(x->>'document_type', '')), '')) AS document_type_norm,
                COALESCE((x->>'include_revenue')::boolean, true) AS include_revenue,
                COALESCE((x->>'include_quantity')::boolean, true) AS include_quantity,
                COALESCE((x->>'include_cost')::boolean, true) AS include_cost,
                COALESCE((x->>'amount_sign')::integer, 1) AS amount_sign,
                COALESCE((x->>'quantity_sign')::integer, 1) AS quantity_sign
            FROM jsonb_array_elements(CAST(:rules_json AS jsonb)) AS x
        ),
        base AS (
            SELECT
                f.*,
                NULLIF(
                    BTRIM(
                        COALESCE(
                            f.source_payload_json->>'behavior_code',
                            f.source_payload_json->>'behavior',
                            f.source_payload_json->>'tfprms',
                            f.source_payload_json->>'TFPRMS',
                            f.source_payload_json->>'source_transaction_type_id',
                            f.source_payload_json->>'source_entity_id',
                            f.source_payload_json->>'SODTYPE',
                            ''
                        )
                    ),
                    ''
                ) AS behavior_code_norm,
                LOWER(
                    BTRIM(
                        COALESCE(
                            f.document_type,
                            f.source_payload_json->>'document_type',
                            f.source_payload_json->>'doc_type',
                            f.source_payload_json->>'document_series_name',
                            ''
                        )
                    )
                ) AS document_type_norm
            FROM fact_sales f
            WHERE f.doc_date BETWEEN :from_date AND :to_date
        ),
        classified AS (
            SELECT
                b.*,
                COALESCE(rb.include_revenue, rd.include_revenue, true) AS include_revenue,
                COALESCE(rb.include_quantity, rd.include_quantity, true) AS include_quantity,
                COALESCE(rb.include_cost, rd.include_cost, true) AS include_cost,
                COALESCE(rb.amount_sign, rd.amount_sign, 1) AS amount_sign,
                COALESCE(rb.quantity_sign, rd.quantity_sign, 1) AS quantity_sign
            FROM base b
            LEFT JOIN LATERAL (
                SELECT
                    r.include_revenue,
                    r.include_quantity,
                    r.include_cost,
                    r.amount_sign,
                    r.quantity_sign
                FROM rules r
                WHERE r.behavior_code IS NOT NULL
                  AND r.behavior_code = b.behavior_code_norm
                LIMIT 1
            ) rb ON true
            LEFT JOIN LATERAL (
                SELECT
                    r.include_revenue,
                    r.include_quantity,
                    r.include_cost,
                    r.amount_sign,
                    r.quantity_sign
                FROM rules r
                WHERE r.document_type_norm IS NOT NULL
                  AND r.document_type_norm = b.document_type_norm
                LIMIT 1
            ) rd ON true
        )
    """
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_sales_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {sales_classified_cte}
            INSERT INTO agg_sales_daily (
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, updated_at, created_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(gross_value, 0) * amount_sign ELSE 0 END), 0),
                NOW(),
                NOW()
            FROM classified
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_sales_daily_company
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {sales_classified_cte}
            INSERT INTO agg_sales_daily_company (
                doc_date, qty, net_value, gross_value, cost_amount, branches, margin_pct, updated_at, created_at
            )
            SELECT
                doc_date,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0) AS qty,
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0) AS net_value,
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(gross_value, 0) * amount_sign ELSE 0 END), 0) AS gross_value,
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0) AS cost_amount,
                COALESCE(COUNT(DISTINCT CASE WHEN include_revenue THEN branch_ext_id END), 0) AS branches,
                CASE
                    WHEN COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0) <> 0
                        THEN (
                            (
                                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0)
                                - COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0)
                            )
                            / NULLIF(COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0), 0)
                        ) * 100
                    ELSE 0
                END AS margin_pct,
                NOW(),
                NOW()
            FROM classified
            GROUP BY doc_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_sales_daily_branch
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {sales_classified_cte}
            , branch_base AS (
                SELECT
                    doc_date,
                    branch_ext_id,
                    COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0) AS qty,
                    COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0) AS net_value,
                    COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(gross_value, 0) * amount_sign ELSE 0 END), 0) AS gross_value,
                    COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0) AS cost_amount
                FROM classified
                GROUP BY doc_date, branch_ext_id
            ),
            day_totals AS (
                SELECT
                    doc_date,
                    COALESCE(SUM(net_value), 0) AS total_net
                FROM branch_base
                GROUP BY doc_date
            )
            INSERT INTO agg_sales_daily_branch (
                doc_date, branch_ext_id, qty, net_value, gross_value, cost_amount, contribution_pct, margin_pct, updated_at, created_at
            )
            SELECT
                b.doc_date,
                b.branch_ext_id,
                b.qty,
                b.net_value,
                b.gross_value,
                b.cost_amount,
                CASE
                    WHEN COALESCE(t.total_net, 0) > 0 THEN (b.net_value / t.total_net) * 100
                    ELSE 0
                END AS contribution_pct,
                CASE
                    WHEN b.net_value <> 0 THEN ((b.net_value - b.cost_amount) / b.net_value) * 100
                    ELSE 0
                END AS margin_pct,
                NOW(),
                NOW()
            FROM branch_base b
            LEFT JOIN day_totals t ON t.doc_date = b.doc_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_sales_item_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {sales_classified_cte}
            INSERT INTO agg_sales_item_daily (
                doc_date, item_external_id, qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                doc_date,
                item_code AS item_external_id,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0),
                NOW(),
                NOW()
            FROM classified
            WHERE item_code IS NOT NULL
            GROUP BY doc_date, item_code
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )

    from_month = from_date.replace(day=1)
    to_month = to_date.replace(day=1)
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_sales_monthly
            WHERE month_start BETWEEN :from_month AND :to_month
            """
        ),
        {'from_month': from_month, 'to_month': to_month},
    )
    await tenant_db.execute(
        text(
            f"""
            {sales_classified_cte}
            INSERT INTO agg_sales_monthly (
                month_start, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, updated_at, created_at
            )
            SELECT
                DATE_TRUNC('month', doc_date)::date AS month_start,
                branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_revenue THEN COALESCE(gross_value, 0) * amount_sign ELSE 0 END), 0),
                NOW(),
                NOW()
            FROM classified
            GROUP BY DATE_TRUNC('month', doc_date)::date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )


async def _refresh_purchases_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
    document_rules_json: str,
) -> None:
    purchases_classified_cte = """
        WITH rules AS (
            SELECT
                NULLIF(BTRIM(COALESCE(x->>'behavior_code', '')), '') AS behavior_code,
                LOWER(NULLIF(BTRIM(COALESCE(x->>'document_type', '')), '')) AS document_type_norm,
                COALESCE((x->>'include_revenue')::boolean, false) AS include_revenue,
                COALESCE((x->>'include_quantity')::boolean, true) AS include_quantity,
                COALESCE((x->>'include_cost')::boolean, true) AS include_cost,
                COALESCE((x->>'amount_sign')::integer, 1) AS amount_sign,
                COALESCE((x->>'quantity_sign')::integer, 1) AS quantity_sign
            FROM jsonb_array_elements(CAST(:rules_json AS jsonb)) AS x
        ),
        base AS (
            SELECT
                f.*,
                NULLIF(
                    BTRIM(
                        COALESCE(
                            f.source_payload_json->>'behavior_code',
                            f.source_payload_json->>'behavior',
                            f.source_payload_json->>'tfprms',
                            f.source_payload_json->>'TFPRMS',
                            f.source_payload_json->>'source_transaction_type_id',
                            f.source_payload_json->>'source_entity_id',
                            f.source_entity_id::text,
                            f.object_id::text,
                            f.source_module_id::text,
                            ''
                        )
                    ),
                    ''
                ) AS behavior_code_norm,
                LOWER(
                    BTRIM(
                        COALESCE(
                            f.document_type,
                            f.source_payload_json->>'document_type',
                            f.source_payload_json->>'doc_type',
                            f.source_payload_json->>'document_series_name',
                            ''
                        )
                    )
                ) AS document_type_norm
            FROM fact_purchases f
            WHERE f.doc_date BETWEEN :from_date AND :to_date
        ),
        classified AS (
            SELECT
                b.*,
                COALESCE(rb.include_revenue, rd.include_revenue, false) AS include_revenue,
                COALESCE(rb.include_quantity, rd.include_quantity, true) AS include_quantity,
                COALESCE(rb.include_cost, rd.include_cost, true) AS include_cost,
                COALESCE(rb.amount_sign, rd.amount_sign, 1) AS amount_sign,
                COALESCE(rb.quantity_sign, rd.quantity_sign, 1) AS quantity_sign
            FROM base b
            LEFT JOIN LATERAL (
                SELECT
                    r.include_revenue,
                    r.include_quantity,
                    r.include_cost,
                    r.amount_sign,
                    r.quantity_sign
                FROM rules r
                WHERE r.behavior_code IS NOT NULL
                  AND r.behavior_code = b.behavior_code_norm
                LIMIT 1
            ) rb ON true
            LEFT JOIN LATERAL (
                SELECT
                    r.include_revenue,
                    r.include_quantity,
                    r.include_cost,
                    r.amount_sign,
                    r.quantity_sign
                FROM rules r
                WHERE r.document_type_norm IS NOT NULL
                  AND r.document_type_norm = b.document_type_norm
                LIMIT 1
            ) rd ON true
        )
    """
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_purchases_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {purchases_classified_cte}
            INSERT INTO agg_purchases_daily (
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0),
                NOW(),
                NOW()
            FROM classified
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_purchases_daily_company
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {purchases_classified_cte}
            INSERT INTO agg_purchases_daily_company (
                doc_date, qty, net_value, cost_amount, branches, suppliers, updated_at, created_at
            )
            SELECT
                doc_date,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0) AS qty,
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0) AS net_value,
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0) AS cost_amount,
                COALESCE(COUNT(DISTINCT CASE WHEN include_cost THEN branch_ext_id END), 0) AS branches,
                COALESCE(COUNT(DISTINCT CASE WHEN include_cost THEN supplier_ext_id END), 0) AS suppliers,
                NOW(),
                NOW()
            FROM classified
            GROUP BY doc_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_purchases_daily_branch
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            f"""
            {purchases_classified_cte}
            , branch_base AS (
                SELECT
                    doc_date,
                    branch_ext_id,
                    COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0) AS qty,
                    COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0) AS net_value,
                    COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0) AS cost_amount,
                    COALESCE(COUNT(DISTINCT CASE WHEN include_cost THEN supplier_ext_id END), 0) AS suppliers
                FROM classified
                GROUP BY doc_date, branch_ext_id
            ),
            day_totals AS (
                SELECT
                    doc_date,
                    COALESCE(SUM(net_value), 0) AS total_net
                FROM branch_base
                GROUP BY doc_date
            )
            INSERT INTO agg_purchases_daily_branch (
                doc_date, branch_ext_id, qty, net_value, cost_amount, contribution_pct, suppliers, updated_at, created_at
            )
            SELECT
                b.doc_date,
                b.branch_ext_id,
                b.qty,
                b.net_value,
                b.cost_amount,
                CASE
                    WHEN COALESCE(t.total_net, 0) > 0 THEN (b.net_value / t.total_net) * 100
                    ELSE 0
                END AS contribution_pct,
                b.suppliers,
                NOW(),
                NOW()
            FROM branch_base b
            LEFT JOIN day_totals t ON t.doc_date = b.doc_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )

    from_month = from_date.replace(day=1)
    to_month = to_date.replace(day=1)
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_purchases_monthly
            WHERE month_start BETWEEN :from_month AND :to_month
            """
        ),
        {'from_month': from_month, 'to_month': to_month},
    )
    await tenant_db.execute(
        text(
            f"""
            {purchases_classified_cte}
            INSERT INTO agg_purchases_monthly (
                month_start, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                DATE_TRUNC('month', doc_date)::date AS month_start,
                branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(CASE WHEN include_quantity THEN COALESCE(qty, 0) * quantity_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(net_value, 0) * amount_sign ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN include_cost THEN COALESCE(cost_amount, 0) * amount_sign ELSE 0 END), 0),
                NOW(),
                NOW()
            FROM classified
            GROUP BY DATE_TRUNC('month', doc_date)::date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'rules_json': document_rules_json},
    )


async def _refresh_inventory_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_inventory_snapshot_daily
            WHERE snapshot_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_stock_aging
            WHERE snapshot_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            WITH snapshot_days AS (
                SELECT DISTINCT fi.doc_date AS snapshot_date
                FROM fact_inventory fi
                WHERE fi.doc_date BETWEEN :from_date AND :to_date
            ),
            latest_inventory_rows AS (
                SELECT
                    sd.snapshot_date,
                    fi.branch_ext_id,
                    fi.warehouse_ext_id,
                    fi.item_id,
                    fi.item_code,
                    fi.qty_on_hand,
                    fi.value_amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            sd.snapshot_date,
                            fi.branch_id,
                            fi.warehouse_id,
                            COALESCE(fi.item_id::text, fi.item_code, '')
                        ORDER BY fi.doc_date DESC, fi.updated_at DESC, fi.id DESC
                    ) AS rn
                FROM fact_inventory fi
                JOIN snapshot_days sd ON fi.doc_date <= sd.snapshot_date
            )
            INSERT INTO agg_inventory_snapshot_daily (
                snapshot_date, item_external_id, qty_on_hand, value_amount, updated_at, created_at
            )
            SELECT
                lir.snapshot_date,
                COALESCE(di.external_id, lir.item_code, lir.item_id::text) AS item_external_id,
                COALESCE(SUM(lir.qty_on_hand), 0),
                COALESCE(SUM(lir.value_amount), 0),
                NOW(),
                NOW()
            FROM latest_inventory_rows lir
            LEFT JOIN dim_items di ON di.id = lir.item_id
            WHERE lir.rn = 1
              AND COALESCE(di.external_id, lir.item_code, lir.item_id::text) IS NOT NULL
              AND COALESCE(di.external_id, lir.item_code, lir.item_id::text) <> ''
            GROUP BY
                lir.snapshot_date,
                COALESCE(di.external_id, lir.item_code, lir.item_id::text)
            ON CONFLICT (snapshot_date, item_external_id) DO UPDATE
            SET
                qty_on_hand = EXCLUDED.qty_on_hand,
                value_amount = EXCLUDED.value_amount,
                updated_at = NOW()
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            WITH snapshot_days AS (
                SELECT DISTINCT fi.doc_date AS snapshot_date
                FROM fact_inventory fi
                WHERE fi.doc_date BETWEEN :from_date AND :to_date
            ),
            latest_inventory_rows AS (
                SELECT
                    sd.snapshot_date,
                    fi.branch_ext_id,
                    fi.warehouse_ext_id,
                    fi.item_id,
                    fi.item_code,
                    fi.qty_on_hand,
                    fi.value_amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            sd.snapshot_date,
                            fi.branch_id,
                            fi.warehouse_id,
                            COALESCE(fi.item_id::text, fi.item_code, '')
                        ORDER BY fi.doc_date DESC, fi.updated_at DESC, fi.id DESC
                    ) AS rn
                FROM fact_inventory fi
                JOIN snapshot_days sd ON fi.doc_date <= sd.snapshot_date
            ),
            inventory_base AS (
                SELECT
                    lir.snapshot_date,
                    COALESCE(di.external_id, lir.item_code, lir.item_id::text) AS item_external_id,
                    lir.branch_ext_id,
                    lir.item_id,
                    lir.item_code,
                    COALESCE(SUM(lir.qty_on_hand), 0) AS qty_on_hand,
                    COALESCE(SUM(lir.value_amount), 0) AS stock_value
                FROM latest_inventory_rows lir
                LEFT JOIN dim_items di ON di.id = lir.item_id
                WHERE lir.rn = 1
                  AND COALESCE(di.external_id, lir.item_code, lir.item_id::text) IS NOT NULL
                  AND COALESCE(di.external_id, lir.item_code, lir.item_id::text) <> ''
                GROUP BY
                    lir.snapshot_date,
                    COALESCE(di.external_id, lir.item_code, lir.item_id::text),
                    lir.branch_ext_id,
                    lir.item_id,
                    lir.item_code
            ),
            sales_last AS (
                SELECT
                    ib.snapshot_date,
                    ib.item_external_id,
                    ib.branch_ext_id,
                    MAX(fs.doc_date) AS last_sale_date
                FROM inventory_base ib
                LEFT JOIN fact_sales fs
                    ON fs.doc_date <= ib.snapshot_date
                   AND (
                        (ib.item_id IS NOT NULL AND fs.item_id = ib.item_id)
                        OR (
                            ib.item_id IS NULL
                            AND ib.item_code IS NOT NULL
                            AND fs.item_code = ib.item_code
                        )
                   )
                   AND COALESCE(fs.branch_ext_id, '') = COALESCE(ib.branch_ext_id, '')
                GROUP BY
                    ib.snapshot_date,
                    ib.item_external_id,
                    ib.branch_ext_id
            ),
            aging_base AS (
                SELECT
                    ib.snapshot_date,
                    ib.item_external_id,
                    ib.branch_ext_id,
                    COALESCE(SUM(ib.qty_on_hand), 0) AS qty_on_hand,
                    COALESCE(SUM(ib.stock_value), 0) AS stock_value,
                    MAX(sl.last_sale_date) AS last_sale_date
                FROM inventory_base ib
                LEFT JOIN sales_last sl
                    ON sl.snapshot_date = ib.snapshot_date
                   AND sl.item_external_id = ib.item_external_id
                   AND COALESCE(sl.branch_ext_id, '') = COALESCE(ib.branch_ext_id, '')
                GROUP BY
                    ib.snapshot_date,
                    ib.item_external_id,
                    ib.branch_ext_id
            )
            INSERT INTO agg_stock_aging (
                snapshot_date,
                item_external_id,
                branch_ext_id,
                qty_on_hand,
                stock_value,
                last_sale_date,
                days_since_last_sale,
                aging_bucket,
                updated_at,
                created_at
            )
            SELECT
                ab.snapshot_date,
                ab.item_external_id,
                ab.branch_ext_id,
                ab.qty_on_hand,
                ab.stock_value,
                ab.last_sale_date,
                CASE
                    WHEN ab.last_sale_date IS NULL THEN NULL
                    ELSE (ab.snapshot_date - ab.last_sale_date)
                END AS days_since_last_sale,
                CASE
                    WHEN ab.last_sale_date IS NULL THEN '90_plus'
                    WHEN (ab.snapshot_date - ab.last_sale_date) <= 30 THEN '0_30'
                    WHEN (ab.snapshot_date - ab.last_sale_date) <= 60 THEN '31_60'
                    WHEN (ab.snapshot_date - ab.last_sale_date) <= 90 THEN '61_90'
                    ELSE '90_plus'
                END AS aging_bucket,
                NOW(),
                NOW()
            FROM aging_base ab
            ON CONFLICT (snapshot_date, item_external_id, branch_ext_id) DO UPDATE
            SET
                qty_on_hand = EXCLUDED.qty_on_hand,
                stock_value = EXCLUDED.stock_value,
                last_sale_date = EXCLUDED.last_sale_date,
                days_since_last_sale = EXCLUDED.days_since_last_sale,
                aging_bucket = EXCLUDED.aging_bucket,
                updated_at = NOW()
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )


async def _refresh_cash_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_cash_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_cash_daily (
                doc_date, branch_ext_id, subcategory, transaction_type, account_id,
                entries, inflows, outflows, net_amount, updated_at, created_at
            )
            SELECT
                fc.doc_date,
                db.external_id AS branch_ext_id,
                COALESCE(NULLIF(LOWER(fc.subcategory), ''), CASE
                    WHEN LOWER(fc.entry_type) IN ('customer_collections','customer_collection','collections','collection','in','inflow','credit','income') THEN 'customer_collections'
                    WHEN LOWER(fc.entry_type) IN ('customer_transfers','customer_transfer','customer_bank_transfer','customer_wire_transfer','customer_wire') THEN 'customer_transfers'
                    WHEN LOWER(fc.entry_type) IN ('supplier_payments','supplier_payment','payments','payment','out','outflow','debit','expense') THEN 'supplier_payments'
                    WHEN LOWER(fc.entry_type) IN ('supplier_transfers','supplier_transfer','supplier_bank_transfer','supplier_wire_transfer','supplier_wire') THEN 'supplier_transfers'
                    WHEN LOWER(fc.entry_type) IN ('financial_accounts','financial_account','account_transfer','internal_transfer','transfer') THEN 'financial_accounts'
                    ELSE 'unknown'
                END) AS subcategory,
                COALESCE(fc.transaction_type, fc.entry_type) AS transaction_type,
                COALESCE(fc.account_id, fc.reference_no, fc.external_id) AS account_id,
                COUNT(fc.id) AS entries,
                SUM(CASE
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') IN ('customer_collections','customer_transfers') THEN ABS(fc.amount)
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') = 'financial_accounts' AND fc.amount >= 0 THEN ABS(fc.amount)
                    ELSE 0
                END) AS inflows,
                SUM(CASE
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') IN ('supplier_payments','supplier_transfers') THEN ABS(fc.amount)
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') = 'financial_accounts' AND fc.amount < 0 THEN ABS(fc.amount)
                    ELSE 0
                END) AS outflows,
                SUM(CASE
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') IN ('customer_collections','customer_transfers') THEN ABS(fc.amount)
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') = 'financial_accounts' THEN fc.amount
                    WHEN COALESCE(NULLIF(LOWER(fc.subcategory), ''), '') IN ('supplier_payments','supplier_transfers') THEN -ABS(fc.amount)
                    ELSE 0
                END) AS net_amount,
                NOW(),
                NOW()
            FROM fact_cashflows fc
            LEFT JOIN dim_branches db ON db.id = fc.branch_id
            WHERE fc.doc_date BETWEEN :from_date AND :to_date
            GROUP BY
                fc.doc_date,
                db.external_id,
                COALESCE(NULLIF(LOWER(fc.subcategory), ''), CASE
                    WHEN LOWER(fc.entry_type) IN ('customer_collections','customer_collection','collections','collection','in','inflow','credit','income') THEN 'customer_collections'
                    WHEN LOWER(fc.entry_type) IN ('customer_transfers','customer_transfer','customer_bank_transfer','customer_wire_transfer','customer_wire') THEN 'customer_transfers'
                    WHEN LOWER(fc.entry_type) IN ('supplier_payments','supplier_payment','payments','payment','out','outflow','debit','expense') THEN 'supplier_payments'
                    WHEN LOWER(fc.entry_type) IN ('supplier_transfers','supplier_transfer','supplier_bank_transfer','supplier_wire_transfer','supplier_wire') THEN 'supplier_transfers'
                    WHEN LOWER(fc.entry_type) IN ('financial_accounts','financial_account','account_transfer','internal_transfer','transfer') THEN 'financial_accounts'
                    ELSE 'unknown'
                END),
                COALESCE(fc.transaction_type, fc.entry_type),
                COALESCE(fc.account_id, fc.reference_no, fc.external_id)
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )

    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_cash_by_type
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_cash_by_type (
                doc_date, subcategory, entries, inflows, outflows, net_amount, updated_at, created_at
            )
            SELECT
                doc_date,
                subcategory,
                COALESCE(SUM(entries), 0),
                COALESCE(SUM(inflows), 0),
                COALESCE(SUM(outflows), 0),
                COALESCE(SUM(net_amount), 0),
                NOW(),
                NOW()
            FROM agg_cash_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY doc_date, subcategory
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )

    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_cash_accounts
            WHERE doc_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_cash_accounts (
                doc_date, account_id, entries, inflows, outflows, net_amount, updated_at, created_at
            )
            SELECT
                doc_date,
                account_id,
                COALESCE(SUM(entries), 0),
                COALESCE(SUM(inflows), 0),
                COALESCE(SUM(outflows), 0),
                COALESCE(SUM(net_amount), 0),
                NOW(),
                NOW()
            FROM agg_cash_daily
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY doc_date, account_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
async def _refresh_expenses_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_expenses_daily
            WHERE expense_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_expenses_daily (
                expense_date, branch_ext_id, expense_category_code, supplier_ext_id, account_ext_id,
                amount_net, amount_tax, amount_gross, entries, updated_at, created_at
            )
            SELECT
                expense_date,
                branch_ext_id,
                expense_category_code,
                supplier_ext_id,
                account_ext_id,
                COALESCE(SUM(amount_net), 0) AS amount_net,
                COALESCE(SUM(amount_tax), 0) AS amount_tax,
                COALESCE(SUM(amount_gross), 0) AS amount_gross,
                COUNT(*) AS entries,
                NOW(),
                NOW()
            FROM fact_expenses
            WHERE expense_date BETWEEN :from_date AND :to_date
            GROUP BY expense_date, branch_ext_id, expense_category_code, supplier_ext_id, account_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )

    from_month = from_date.replace(day=1)
    to_month = to_date.replace(day=1)
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_expenses_monthly
            WHERE month_start BETWEEN :from_month AND :to_month
            """
        ),
        {'from_month': from_month, 'to_month': to_month},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_expenses_monthly (
                month_start, branch_ext_id, expense_category_code, supplier_ext_id, account_ext_id,
                amount_net, amount_tax, amount_gross, entries, updated_at, created_at
            )
            SELECT
                DATE_TRUNC('month', expense_date)::date AS month_start,
                branch_ext_id,
                expense_category_code,
                supplier_ext_id,
                account_ext_id,
                COALESCE(SUM(amount_net), 0) AS amount_net,
                COALESCE(SUM(amount_tax), 0) AS amount_tax,
                COALESCE(SUM(amount_gross), 0) AS amount_gross,
                COUNT(*) AS entries,
                NOW(),
                NOW()
            FROM fact_expenses
            WHERE expense_date BETWEEN :from_date AND :to_date
            GROUP BY DATE_TRUNC('month', expense_date)::date, branch_ext_id, expense_category_code, supplier_ext_id, account_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )

    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_expenses_by_category_daily
            WHERE expense_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_expenses_by_category_daily (
                expense_date, expense_category_code, amount_net, amount_tax, amount_gross, entries, updated_at, created_at
            )
            SELECT
                expense_date,
                expense_category_code,
                COALESCE(SUM(amount_net), 0) AS amount_net,
                COALESCE(SUM(amount_tax), 0) AS amount_tax,
                COALESCE(SUM(amount_gross), 0) AS amount_gross,
                COUNT(*) AS entries,
                NOW(),
                NOW()
            FROM fact_expenses
            WHERE expense_date BETWEEN :from_date AND :to_date
            GROUP BY expense_date, expense_category_code
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )

    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_expenses_by_branch_daily
            WHERE expense_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_expenses_by_branch_daily (
                expense_date, branch_ext_id, amount_net, amount_tax, amount_gross, entries, updated_at, created_at
            )
            SELECT
                expense_date,
                branch_ext_id,
                COALESCE(SUM(amount_net), 0) AS amount_net,
                COALESCE(SUM(amount_tax), 0) AS amount_tax,
                COALESCE(SUM(amount_gross), 0) AS amount_gross,
                COUNT(*) AS entries,
                NOW(),
                NOW()
            FROM fact_expenses
            WHERE expense_date BETWEEN :from_date AND :to_date
            GROUP BY expense_date, branch_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )


async def _refresh_supplier_balances_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_supplier_balances_daily
            WHERE balance_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_supplier_balances_daily (
                balance_date,
                supplier_ext_id,
                branch_ext_id,
                open_balance,
                overdue_balance,
                aging_bucket_0_30,
                aging_bucket_31_60,
                aging_bucket_61_90,
                aging_bucket_90_plus,
                trend_vs_previous,
                suppliers,
                updated_at,
                created_at
            )
            SELECT
                fsb.balance_date,
                fsb.supplier_ext_id,
                fsb.branch_ext_id,
                COALESCE(SUM(fsb.open_balance), 0) AS open_balance,
                COALESCE(SUM(fsb.overdue_balance), 0) AS overdue_balance,
                COALESCE(SUM(fsb.aging_bucket_0_30), 0) AS aging_bucket_0_30,
                COALESCE(SUM(fsb.aging_bucket_31_60), 0) AS aging_bucket_31_60,
                COALESCE(SUM(fsb.aging_bucket_61_90), 0) AS aging_bucket_61_90,
                COALESCE(SUM(fsb.aging_bucket_90_plus), 0) AS aging_bucket_90_plus,
                COALESCE(SUM(fsb.trend_vs_previous), 0) AS trend_vs_previous,
                COUNT(DISTINCT fsb.supplier_ext_id) AS suppliers,
                NOW(),
                NOW()
            FROM fact_supplier_balances fsb
            WHERE fsb.balance_date BETWEEN :from_date AND :to_date
            GROUP BY fsb.balance_date, fsb.supplier_ext_id, fsb.branch_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )


async def _refresh_customer_balances_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
    await tenant_db.execute(
        text(
            """
            DELETE FROM agg_customer_balances_daily
            WHERE balance_date BETWEEN :from_date AND :to_date
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_customer_balances_daily (
                balance_date,
                customer_ext_id,
                branch_ext_id,
                open_balance,
                overdue_balance,
                aging_bucket_0_30,
                aging_bucket_31_60,
                aging_bucket_61_90,
                aging_bucket_90_plus,
                trend_vs_previous,
                customers,
                updated_at,
                created_at
            )
            SELECT
                fcb.balance_date,
                fcb.customer_ext_id,
                fcb.branch_ext_id,
                COALESCE(SUM(fcb.open_balance), 0) AS open_balance,
                COALESCE(SUM(fcb.overdue_balance), 0) AS overdue_balance,
                COALESCE(SUM(fcb.aging_bucket_0_30), 0) AS aging_bucket_0_30,
                COALESCE(SUM(fcb.aging_bucket_31_60), 0) AS aging_bucket_31_60,
                COALESCE(SUM(fcb.aging_bucket_61_90), 0) AS aging_bucket_61_90,
                COALESCE(SUM(fcb.aging_bucket_90_plus), 0) AS aging_bucket_90_plus,
                COALESCE(SUM(fcb.trend_vs_previous), 0) AS trend_vs_previous,
                COUNT(DISTINCT fcb.customer_ext_id) AS customers,
                NOW(),
                NOW()
            FROM fact_customer_balances fcb
            WHERE fcb.balance_date BETWEEN :from_date AND :to_date
            GROUP BY fcb.balance_date, fcb.customer_ext_id, fcb.branch_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )


async def _refresh_aggregates_task(
    tenant_slug: str,
    entity: str,
    from_date_str: str | None = None,
    to_date_str: str | None = None,
    *,
    run_insights: bool = False,
) -> dict:
    started = time.perf_counter()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if redis.get(tenant_delete_active_key(tenant_slug)):
        return {'status': 'skipped', 'tenant': tenant_slug, 'entity': entity, 'reason': 'tenant_delete_active'}
    if redis.get(tenant_stop_key(tenant_slug)):
        return {'status': 'skipped', 'tenant': tenant_slug, 'entity': entity, 'reason': 'tenant_stop_requested'}
    lock_key = f'{tenant_slug}:{entity}:agg'
    lock_token = acquire_tenant_lock(lock_key, ttl_seconds=settings.ingest_tenant_lock_ttl_seconds)
    if not lock_token:
        sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='lock_contended').observe(0)
        return {'status': 'skipped', 'reason': 'lock_contended', 'entity': entity}
    async with ControlSessionLocal() as control_db:
        try:
            tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
            if not tenant:
                return {'status': 'not_found'}
            sales_rules_json = '[]'
            purchases_rules_json = '[]'
            if entity == 'sales':
                sales_rules_json = _document_rules_sql_payload(
                    await _load_effective_document_rules(
                        control_db,
                        tenant_id=int(tenant.id),
                        stream=OperationalStream.sales_documents,
                    )
                )
            elif entity == 'purchases':
                purchases_rules_json = _document_rules_sql_payload(
                    await _load_effective_document_rules(
                        control_db,
                        tenant_id=int(tenant.id),
                        stream=OperationalStream.purchase_documents,
                    )
                )

            async for tenant_db in get_tenant_db_session(
                tenant_key=str(tenant.id),
                db_name=tenant.db_name,
                db_user=tenant.db_user,
                db_password=tenant.db_password,
            ):
                if from_date_str and to_date_str:
                    from_date = datetime.fromisoformat(from_date_str).date()
                    to_date = datetime.fromisoformat(to_date_str).date()
                else:
                    date_column = 'doc_date'
                    if entity == 'sales':
                        base_table = 'fact_sales'
                    elif entity == 'purchases':
                        base_table = 'fact_purchases'
                    elif entity == 'inventory':
                        base_table = 'fact_inventory'
                    elif entity == 'cashflows':
                        base_table = 'fact_cashflows'
                    elif entity == 'supplier_balances':
                        base_table = 'fact_supplier_balances'
                        date_column = 'balance_date'
                    elif entity == 'customer_balances':
                        base_table = 'fact_customer_balances'
                        date_column = 'balance_date'
                    elif entity == 'expenses':
                        base_table = 'fact_expenses'
                        date_column = 'expense_date'
                    else:
                        sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='error').observe(time.perf_counter() - started)
                        return {'status': 'error', 'detail': f'unsupported entity: {entity}'}
                    min_max = (await tenant_db.execute(text(f'SELECT MIN({date_column}), MAX({date_column}) FROM {base_table}'))).first()
                    if not min_max or not min_max[0] or not min_max[1]:
                        return {'status': 'ok', 'refreshed': 0}
                    from_date, to_date = min_max[0], min_max[1]

                if entity == 'sales':
                    await _refresh_sales_aggregates(
                        tenant_db,
                        from_date=from_date,
                        to_date=to_date,
                        document_rules_json=sales_rules_json,
                    )
                elif entity == 'purchases':
                    await _refresh_purchases_aggregates(
                        tenant_db,
                        from_date=from_date,
                        to_date=to_date,
                        document_rules_json=purchases_rules_json,
                    )
                elif entity == 'inventory':
                    await _refresh_inventory_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'cashflows':
                    await _refresh_cash_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'supplier_balances':
                    await _refresh_supplier_balances_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'customer_balances':
                    await _refresh_customer_balances_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'expenses':
                    await _refresh_expenses_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                else:
                    sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='error').observe(time.perf_counter() - started)
                    return {'status': 'error', 'detail': f'unsupported entity: {entity}'}
                await tenant_db.commit()
                await invalidate_tenant_cache(str(tenant.id))
                if (
                    run_insights
                    and entity in {'sales', 'purchases', 'inventory', 'cashflows', 'supplier_balances', 'customer_balances', 'expenses'}
                    and not Redis.from_url(settings.redis_url, decode_responses=True).get(tenant_delete_active_key(tenant_slug))
                ):
                    generate_insights_for_tenant.delay(tenant_slug=tenant_slug)
                sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='ok').observe(time.perf_counter() - started)
                return {'status': 'ok', 'entity': entity, 'from_date': str(from_date), 'to_date': str(to_date)}

            sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='ok').observe(time.perf_counter() - started)
            return {'status': 'ok'}
        finally:
            release_tenant_lock(lock_key, lock_token)


@shared_task(name='worker.tasks.generate_insights_for_tenant', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def generate_insights_for_tenant(tenant_slug: str, as_of_date_str: str | None = None) -> dict:
    return _run_coro(_generate_insights_for_tenant(tenant_slug, as_of_date_str))


async def _generate_insights_for_tenant(tenant_slug: str, as_of_date_str: str | None = None) -> dict:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    if redis.get(tenant_delete_active_key(tenant_slug)):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'tenant_delete_active'}
    if redis.get(tenant_stop_key(tenant_slug)):
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'tenant_stop_requested'}
    as_of_date = date.fromisoformat(as_of_date_str) if as_of_date_str else (date.today() - timedelta(days=1))
    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
        if not tenant:
            return {'status': 'not_found', 'tenant': tenant_slug}
        async for tenant_db in get_tenant_db_session(
            tenant_key=str(tenant.id),
            db_name=tenant.db_name,
            db_user=tenant.db_user,
            db_password=tenant.db_password,
        ):
            if str(tenant.plan.value) == 'enterprise':
                latest_snapshot = (await tenant_db.execute(text('SELECT MAX(doc_date) FROM fact_inventory'))).scalar_one_or_none()
                if latest_snapshot:
                    await _refresh_inventory_aggregates(tenant_db, from_date=latest_snapshot, to_date=latest_snapshot)
                    await tenant_db.commit()
            result = await generate_daily_insights(
                tenant_db,
                tenant_id=tenant.id,
                tenant_slug=tenant.slug,
                tenant_plan=tenant.plan.value,
                tenant_source=tenant.source,
                as_of=as_of_date,
            )
            return {'status': 'ok', 'tenant': tenant_slug, 'as_of': str(as_of_date), **result}
    return {'status': 'ok', 'tenant': tenant_slug, 'as_of': str(as_of_date), 'generated': 0, 'rules_enabled': 0}


@shared_task(name='worker.tasks.generate_daily_insights_all_tenants', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def generate_daily_insights_all_tenants(as_of_date_str: str | None = None) -> dict:
    return _run_coro(_generate_daily_insights_all_tenants(as_of_date_str))


async def _generate_daily_insights_all_tenants(as_of_date_str: str | None = None) -> dict:
    as_of_date = date.fromisoformat(as_of_date_str) if as_of_date_str else (date.today() - timedelta(days=1))
    queued = 0
    async with ControlSessionLocal() as control_db:
        tenants = (
            await control_db.execute(
                select(Tenant).where(
                    Tenant.status == TenantStatus.active,
                    Tenant.subscription_status.in_([SubscriptionStatus.active, SubscriptionStatus.trial]),
                )
            )
        ).scalars().all()
        for tenant in tenants:
            if Redis.from_url(settings.redis_url, decode_responses=True).get(tenant_delete_active_key(tenant.slug)):
                continue
            generate_insights_for_tenant.delay(tenant_slug=tenant.slug, as_of_date_str=as_of_date.isoformat())
            queued += 1
    return {'status': 'ok', 'as_of': str(as_of_date), 'queued': queued}
