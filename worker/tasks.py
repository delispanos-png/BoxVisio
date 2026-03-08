import asyncio
import logging
import time
from datetime import date, datetime, timedelta

from celery import shared_task
from sqlalchemy import select, text

from app.core.config import settings
from app.db.control_session import ControlSessionLocal
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import SubscriptionStatus, Tenant, TenantStatus
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
    pop_tenant_job,
    process_job,
    push_dead_letter,
    release_tenant_lock,
)
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS, ENTITY_TO_STREAM, STREAM_TO_ENTITY, normalize_stream_name
from app.services.ingestion.engine import persist_dead_letter
from app.services.intelligence_service import generate_daily_insights
from app.services.kpi_cache import invalidate_tenant_cache

logger = logging.getLogger(__name__)


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


def _enqueue_stream_job(tenant_slug: str, stream: str, *, connector: str = 'sql_connector', payload: dict | None = None) -> dict:
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
    job = _default_job('external_api', stream, tenant_slug, payload=payload)
    enqueue_tenant_job(tenant_slug, job)
    drain_tenant_ingest_queue.delay(tenant_slug=tenant_slug)
    return {
        'status': 'queued',
        'tenant': tenant_slug,
        'connector': 'external_api',
        'stream': stream,
        'entity': STREAM_TO_ENTITY[stream],
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
) -> dict:
    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    if from_date > to_date:
        raise ValueError('from_date must be <= to_date')

    batches = 0
    jobs = 0
    stream_flags = {
        'sales_documents': True,
        'purchase_documents': bool(include_purchases),
        'inventory_documents': bool(include_inventory),
        'cash_transactions': bool(include_cashflows),
        'supplier_balances': bool(include_supplier_balances),
        'customer_balances': bool(include_customer_balances),
    }
    for chunk_from, chunk_to in _iter_date_chunks(from_date, to_date, chunk_days):
        payload = {
            'from_date': chunk_from.isoformat(),
            'to_date': chunk_to.isoformat(),
            'ignore_sync_state': True,
            'backfill': True,
        }
        for stream in ALL_OPERATIONAL_STREAMS:
            if not stream_flags.get(stream):
                continue
            enqueue_tenant_job(
                tenant_slug,
                _default_job('sql_connector', stream, tenant_slug, payload=payload),
            )
            jobs += 1
        batches += 1
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
) -> dict:
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
    )


@shared_task(name='worker.tasks.drain_tenant_ingest_queue', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def drain_tenant_ingest_queue(tenant_slug: str, max_jobs: int | None = None) -> dict:
    effective_max_jobs = settings.ingest_drain_max_jobs if max_jobs is None else max_jobs
    return asyncio.run(_drain_tenant_ingest_queue(tenant_slug=tenant_slug, max_jobs=effective_max_jobs))


async def _drain_tenant_ingest_queue(tenant_slug: str, max_jobs: int) -> dict:
    drain_start = time.perf_counter()
    lock_token = acquire_tenant_lock(tenant_slug, ttl_seconds=settings.ingest_tenant_lock_ttl_seconds)
    if not lock_token:
        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='lock_contended').observe(0)
        return {'status': 'skipped', 'tenant': tenant_slug, 'reason': 'lock_contended'}

    processed = 0
    failures = 0
    aggregates_refreshed = 0
    throttled = 0

    try:
        for _ in range(max_jobs):
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
                break

            try:
                started = time.perf_counter()
                result = await process_job(job)
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
                if min_doc_date and max_doc_date and entity in {'sales', 'purchases', 'inventory', 'cashflows', 'supplier_balances', 'customer_balances'}:
                    refresh_aggregates_for_entity.delay(
                        tenant_slug=tenant_slug,
                        entity=entity,
                        from_date_str=min_doc_date,
                        to_date_str=max_doc_date,
                    )
                    aggregates_refreshed += 1
            except Exception as exc:
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

        sync_duration_seconds.labels(task='drain_tenant_ingest_queue', entity='all', status='ok').observe(time.perf_counter() - drain_start)
        return {
            'status': 'ok',
            'tenant': tenant_slug,
            'processed': processed,
            'failures': failures,
            'throttled': throttled,
            'aggregates_refreshed': aggregates_refreshed,
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
    return asyncio.run(_refresh_aggregates_task(tenant_slug, entity, from_date_str, to_date_str))


@shared_task(name='worker.tasks.refresh_sales_aggregates', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def refresh_sales_aggregates(tenant_slug: str, from_date_str: str | None = None, to_date_str: str | None = None) -> dict:
    return asyncio.run(_refresh_aggregates_task(tenant_slug, 'sales', from_date_str, to_date_str))


async def _refresh_sales_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
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
            """
            INSERT INTO agg_sales_daily (
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, updated_at, created_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty), 0), COALESCE(SUM(net_value), 0), COALESCE(SUM(gross_value), 0), NOW(), NOW()
            FROM fact_sales
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
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
            """
            INSERT INTO agg_sales_item_daily (
                doc_date, item_external_id, qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                doc_date,
                item_code AS item_external_id,
                COALESCE(SUM(qty), 0),
                COALESCE(SUM(net_value), 0),
                COALESCE(SUM(cost_amount), 0),
                NOW(),
                NOW()
            FROM fact_sales
            WHERE doc_date BETWEEN :from_date AND :to_date
              AND item_code IS NOT NULL
            GROUP BY doc_date, item_code
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
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
            """
            INSERT INTO agg_sales_monthly (
                month_start, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, gross_value, updated_at, created_at
            )
            SELECT
                DATE_TRUNC('month', doc_date)::date AS month_start,
                branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty), 0), COALESCE(SUM(net_value), 0), COALESCE(SUM(gross_value), 0), NOW(), NOW()
            FROM fact_sales
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY DATE_TRUNC('month', doc_date)::date, branch_ext_id, warehouse_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
    )


async def _refresh_purchases_aggregates(
    tenant_db,
    from_date: date,
    to_date: date,
) -> None:
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
            """
            INSERT INTO agg_purchases_daily (
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty), 0), COALESCE(SUM(net_value), 0), COALESCE(SUM(cost_amount), 0), NOW(), NOW()
            FROM fact_purchases
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY doc_date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
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
            """
            INSERT INTO agg_purchases_monthly (
                month_start, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                qty, net_value, cost_amount, updated_at, created_at
            )
            SELECT
                DATE_TRUNC('month', doc_date)::date AS month_start,
                branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id,
                COALESCE(SUM(qty), 0), COALESCE(SUM(net_value), 0), COALESCE(SUM(cost_amount), 0), NOW(), NOW()
            FROM fact_purchases
            WHERE doc_date BETWEEN :from_date AND :to_date
            GROUP BY DATE_TRUNC('month', doc_date)::date, branch_ext_id, warehouse_ext_id, supplier_ext_id, brand_ext_id, category_ext_id, group_ext_id
            """
        ),
        {'from_date': from_date, 'to_date': to_date},
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
            GROUP BY fc.doc_date, db.external_id, subcategory, COALESCE(fc.transaction_type, fc.entry_type), COALESCE(fc.account_id, fc.reference_no, fc.external_id)
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
                    else:
                        sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='error').observe(time.perf_counter() - started)
                        return {'status': 'error', 'detail': f'unsupported entity: {entity}'}
                    min_max = (await tenant_db.execute(text(f'SELECT MIN({date_column}), MAX({date_column}) FROM {base_table}'))).first()
                    if not min_max or not min_max[0] or not min_max[1]:
                        return {'status': 'ok', 'refreshed': 0}
                    from_date, to_date = min_max[0], min_max[1]

                if entity == 'sales':
                    await _refresh_sales_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'purchases':
                    await _refresh_purchases_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'inventory':
                    await _refresh_inventory_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'cashflows':
                    await _refresh_cash_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'supplier_balances':
                    await _refresh_supplier_balances_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                elif entity == 'customer_balances':
                    await _refresh_customer_balances_aggregates(tenant_db, from_date=from_date, to_date=to_date)
                else:
                    sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='error').observe(time.perf_counter() - started)
                    return {'status': 'error', 'detail': f'unsupported entity: {entity}'}
                await tenant_db.commit()
                await invalidate_tenant_cache(str(tenant.id))
                if entity in {'sales', 'purchases', 'inventory', 'cashflows', 'supplier_balances', 'customer_balances'}:
                    generate_insights_for_tenant.delay(tenant_slug=tenant_slug)
                sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='ok').observe(time.perf_counter() - started)
                return {'status': 'ok', 'entity': entity, 'from_date': str(from_date), 'to_date': str(to_date)}

            sync_duration_seconds.labels(task='refresh_aggregates', entity=entity, status='ok').observe(time.perf_counter() - started)
            return {'status': 'ok'}
        finally:
            release_tenant_lock(lock_key, lock_token)


@shared_task(name='worker.tasks.generate_insights_for_tenant', autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def generate_insights_for_tenant(tenant_slug: str, as_of_date_str: str | None = None) -> dict:
    return asyncio.run(_generate_insights_for_tenant(tenant_slug, as_of_date_str))


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
    return asyncio.run(_generate_daily_insights_all_tenants(as_of_date_str))


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
