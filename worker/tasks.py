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
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

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
    tenant_stop_key,
    tenant_queue_name,
)
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, ENTITY_TO_STREAM, STREAM_TO_ENTITY, normalize_stream_name
from app.services.ingestion.engine import persist_dead_letter
from app.services.ingestion.progress import begin_ingest_progress, queue_depth, update_ingest_progress
from app.services.ingestion.sync_planner import plan_tenant_sync_jobs
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


def _clear_tenant_runtime_queue_state(tenant_slug: str, *, clear_lock: bool = True) -> int:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    keys = [
        tenant_queue_name(tenant_slug),
        f'dlq:{tenant_slug}',
        f'throttle:ingest:{tenant_slug}',
        tenant_stop_key(tenant_slug),
    ]
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
        if operation != 'backfill':
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
        heartbeat_age_seconds = max(0, int((now - updated_at).total_seconds()))
        if heartbeat_age_seconds < stale_after_seconds:
            continue

        active = active_drain_tasks.get(tenant_slug, [])
        active_ages = [int(t.get('age_seconds')) for t in active if isinstance(t.get('age_seconds'), int)]
        max_active_age_seconds = max(active_ages) if active_ages else None
        processed_jobs = int(raw.get('processed_jobs') or 0)
        # Fast-recover mode:
        # If drain appears active but progress heartbeat is stale and no job has been
        # processed yet, recover earlier instead of waiting full force_after_seconds.
        # This avoids long "0/N jobs" stalls caused by stale locks or hung first job.
        fast_recover_after = max(stale_after_seconds + 30, 210)
        allow_early_recover = processed_jobs <= 0 and heartbeat_age_seconds >= fast_recover_after

        if active and (max_active_age_seconds is None or (max_active_age_seconds < force_after_seconds and not allow_early_recover)):
            skipped.append(
                {
                    'tenant': tenant_slug,
                    'reason': 'active_drain_running',
                    'heartbeat_age_seconds': heartbeat_age_seconds,
                    'max_active_age_seconds': max_active_age_seconds,
                    'processed_jobs': processed_jobs,
                    'fast_recover_after': fast_recover_after,
                }
            )
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
        task = drain_tenant_ingest_queue.apply_async(kwargs={'tenant_slug': tenant_slug}, queue='ingest')
        update_ingest_progress(
            tenant_slug,
            status='running',
            error=f'auto_recovery_triggered_heartbeat_{heartbeat_age_seconds}s',
        )
        recovered.append(
            {
                'tenant': tenant_slug,
                'queue_left': queue_left,
                'heartbeat_age_seconds': heartbeat_age_seconds,
                'terminated_tasks': terminated_tasks,
                'cleared_lock': cleared_lock,
                'cleared_throttle': cleared_throttle,
                'drain_task_id': task.id,
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

    return {
        'status': 'ok',
        'stale_after_seconds': stale_after_seconds,
        'force_after_seconds': force_after_seconds,
        'checked_running_tenants': checked,
        'recovered_count': len(recovered),
        'recovered': recovered,
        'skipped': skipped[:max(1, max_recoveries)],
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
        if not lock_active:
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
    for base_job in jobs:
        payload = dict(base_job.get('payload') or {})
        payload.setdefault('limit', max(100, int(limit)))
        job = dict(base_job)
        job['payload'] = payload
        job.setdefault('attempt', 0)
        job.setdefault('max_retries', settings.ingest_job_max_retries)
        enqueue_tenant_job(tenant_slug, job)
        queued += 1

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


async def _enqueue_incremental_sync_all_tenants(
    limit: int | None = None,
    max_tenants: int | None = None,
) -> dict:
    if not bool(getattr(settings, 'incremental_sync_all_tenants_enabled', True)):
        return {'status': 'disabled'}

    effective_limit = max(100, int(limit or settings.incremental_sync_limit or 500))
    cap = max(1, int(max_tenants or settings.incremental_sync_max_tenants_per_run or 100))
    queued = 0
    skipped = 0
    tenants_checked = 0

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
            # Queue only for tenants with at least one active connector.
            active_conn = (
                await control_db.execute(
                    select(TenantConnection.id).where(
                        TenantConnection.tenant_id == tenant.id,
                        TenantConnection.is_active.is_(True),
                    ).limit(1)
                )
            ).scalar_one_or_none()
            if not active_conn:
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
    include_purchases: bool = True,
    include_inventory: bool = True,
    include_cashflows: bool = True,
    include_supplier_balances: bool = True,
    include_customer_balances: bool = True,
    include_operating_expenses: bool = True,
    operation: str = 'backfill',
) -> dict:
    Redis.from_url(settings.redis_url, decode_responses=True).delete(tenant_stop_key(tenant_slug))
    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    if from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    batches = 0
    jobs = 0
    queue_before = queue_depth(tenant_slug)
    stream_flags = {
        'sales_documents': True,
        'purchase_documents': bool(include_purchases),
        'inventory_documents': bool(include_inventory),
        'cash_transactions': bool(include_cashflows),
        'supplier_balances': bool(include_supplier_balances),
        'customer_balances': bool(include_customer_balances),
        'operating_expenses': bool(include_operating_expenses),
    }
    balance_streams = {'supplier_balances', 'customer_balances'}
    base_chunk_days = max(1, int(chunk_days))
    sales_chunk_cap = max(1, int(getattr(settings, 'ingest_backfill_sales_chunk_days', 7) or 7))
    purchases_chunk_cap = max(1, int(getattr(settings, 'ingest_backfill_purchases_chunk_days', 7) or 7))
    stream_chunk_days = {
        # Sales is the heaviest stream. Keep chunking configurable so large tenants
        # don't create excessive queues during manual backfills.
        'sales_documents': min(sales_chunk_cap, base_chunk_days),
        # Purchases are lower volume but still configurable for consistency.
        'purchase_documents': min(purchases_chunk_cap, base_chunk_days),
        # Other streams follow user-selected chunk.
        'inventory_documents': base_chunk_days,
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
    include_purchases: bool = True,
    include_inventory: bool = True,
    include_cashflows: bool = True,
    include_supplier_balances: bool = True,
    include_customer_balances: bool = True,
    include_operating_expenses: bool = True,
    operation: str = 'backfill',
) -> dict:
    # Ensure a previous manual stop does not block a new explicitly requested backfill.
    Redis.from_url(settings.redis_url, decode_responses=True).delete(tenant_stop_key(tenant_slug))
    return enqueue_pharmacyone_backfill(
        tenant_slug=tenant_slug,
        from_date_str=from_date_str,
        to_date_str=to_date_str,
        chunk_days=chunk_days,
        include_purchases=include_purchases,
        include_inventory=include_inventory,
        include_cashflows=include_cashflows,
        include_supplier_balances=include_supplier_balances,
        include_customer_balances=include_customer_balances,
        include_operating_expenses=include_operating_expenses,
        operation=operation,
    )


async def _truncate_tenant_operational_tables(tenant: Tenant, *, include_notifications: bool = True) -> list[str]:
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
        for table in tables:
            await tenant_db.execute(text(f'TRUNCATE TABLE {_quote_ident(table)} RESTART IDENTITY CASCADE'))
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

            run_result = await tenant_db.execute(
                text('DELETE FROM insight_runs WHERE started_at::date BETWEEN :from_date AND :to_date'),
                {'from_date': from_date, 'to_date': to_date},
            )
            run_rows = int(run_result.rowcount or 0)
            notifications_deleted['insight_runs'] = run_rows
            if run_rows > 0:
                deleted_rows_total += run_rows
                deleted_table_count += 1

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
                queue='ingest',
                countdown=10,
            )
            return {
                'status': 'requeued',
                'tenant': tenant_slug,
                'reason': 'lock_contended',
                'retry_count': retry_count,
            }
        update_ingest_progress(tenant_slug, status='failed', error='delete_lock_contended')
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'lock_contended', 'retry_count': retry_count}

    try:
        Redis.from_url(settings.redis_url, decode_responses=True).delete(f'delete:retry:{tenant_slug}')
        async with ControlSessionLocal() as control_db:
            tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalar_one_or_none()
            if not tenant:
                update_ingest_progress(tenant_slug, status='failed', error='tenant_not_found')
                return {'status': 'not_found', 'tenant': tenant_slug}

            cleared_keys = _clear_tenant_runtime_queue_state(tenant.slug)
            if scoped_delete and from_date and to_date:
                await _terminate_tenant_db_backends(tenant)
                scoped_stats = await _delete_tenant_operational_tables_by_date_range(
                    tenant,
                    from_date=from_date,
                    to_date=to_date,
                    include_notifications=bool(include_notifications),
                )
                await control_db.commit()
            else:
                await _terminate_tenant_db_backends(tenant)
                truncated_tables = await _truncate_tenant_operational_tables(
                    tenant,
                    include_notifications=bool(include_notifications),
                )
                sync_conns = (
                    await control_db.execute(
                        select(TenantConnection).where(TenantConnection.tenant_id == tenant.id)
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
        release_tenant_lock(tenant_slug, lock_token)


@shared_task(name='worker.tasks.delete_tenant_data_only', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
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

    processed = 0
    failures = 0
    aggregates_refreshed = 0
    throttled = 0
    next_drain_scheduled = False
    stopped_by_user = False
    backfill_entity_ranges: dict[str, dict[str, str]] = {}
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

            connector = str(job.get('connector') or 'unknown')
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
                drain_tenant_ingest_queue.apply_async(
                    kwargs={'tenant_slug': tenant_slug, 'max_jobs': max_jobs},
                    countdown=settings.ingest_throttle_window_seconds,
                )
                next_drain_scheduled = True
                break

            # Heartbeat progress as soon as a job is picked, so UI reflects active work
            # even when an external request takes long before completion.
            update_ingest_progress(tenant_slug, status='running')

            try:
                started = time.perf_counter()
                job_payload = job.get('payload') or {}
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
                    # Refresh aggregates for every successful ingest chunk, including backfill jobs.
                    # This keeps KPI views live even when long backfills are still in progress.
                    refresh_aggregates_for_entity.delay(
                        tenant_slug=tenant_slug,
                        entity=entity,
                        from_date_str=min_doc_date,
                        to_date_str=max_doc_date,
                    )
                    aggregates_refreshed += 1
                update_ingest_progress(tenant_slug, status='running')
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

                if attempt <= max_retries:
                    job['attempt'] = attempt
                    enqueue_tenant_job(tenant_slug, job)
                    ingest_retries_total.labels(connector=connector, entity=metric_entity).inc()
                    retry_delay = max(settings.ingest_retry_backoff_seconds * attempt, 1)
                    drain_tenant_ingest_queue.apply_async(
                        kwargs={'tenant_slug': tenant_slug, 'max_jobs': max_jobs},
                        countdown=retry_delay,
                    )
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
                'remaining_jobs': remaining_jobs,
                'next_drain_scheduled': False,
            }
        if remaining_jobs > 0 and not next_drain_scheduled:
            drain_tenant_ingest_queue.apply_async(
                kwargs={'tenant_slug': tenant_slug, 'max_jobs': max_jobs},
                countdown=1,
            )
            next_drain_scheduled = True
        elif remaining_jobs == 0 and backfill_entity_ranges:
            for agg_entity, agg_range in backfill_entity_ranges.items():
                refresh_aggregates_for_entity.delay(
                    tenant_slug=tenant_slug,
                    entity=agg_entity,
                    from_date_str=agg_range['from'],
                    to_date_str=agg_range['to'],
                )
                aggregates_refreshed += 1
        update_ingest_progress(tenant_slug, status='running' if remaining_jobs > 0 else 'completed')

        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='ok').observe(time.perf_counter() - drain_start)
        return {
            'status': 'ok',
            'tenant': tenant_slug,
            'processed': processed,
            'failures': failures,
            'throttled': throttled,
            'aggregates_refreshed': aggregates_refreshed,
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
) -> dict:
    return _run_coro(_refresh_aggregates_task(tenant_slug, entity, from_date_str, to_date_str))


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
    await tenant_db.execute(
        text(
            """
            INSERT INTO agg_inventory_snapshot_daily (
                snapshot_date, item_external_id, qty_on_hand, value_amount, updated_at, created_at
            )
            SELECT
                fi.doc_date AS snapshot_date,
                COALESCE(di.external_id, fi.item_id::text) AS item_external_id,
                COALESCE(SUM(fi.qty_on_hand), 0),
                COALESCE(SUM(fi.value_amount), 0),
                NOW(),
                NOW()
            FROM fact_inventory fi
            LEFT JOIN dim_items di ON di.id = fi.item_id
            WHERE fi.doc_date BETWEEN :from_date AND :to_date
              AND (di.external_id IS NOT NULL OR fi.item_id IS NOT NULL)
            GROUP BY fi.doc_date, COALESCE(di.external_id, fi.item_id::text)
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
) -> dict:
    started = time.perf_counter()
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
                if entity in {'sales', 'purchases', 'inventory', 'cashflows', 'supplier_balances', 'customer_balances', 'expenses'}:
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
            generate_insights_for_tenant.delay(tenant_slug=tenant.slug, as_of_date_str=as_of_date.isoformat())
            queued += 1
    return {'status': 'ok', 'as_of': str(as_of_date), 'queued': queued}
