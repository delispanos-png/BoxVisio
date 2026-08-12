from celery import Celery
from celery.schedules import crontab
from datetime import timedelta

from app.core.config import settings

celery = Celery('cloudon_bi')
celery.conf.broker_url = settings.celery_broker_url
celery.conf.result_backend = settings.celery_result_backend
celery.conf.worker_prefetch_multiplier = settings.celery_worker_prefetch_multiplier
celery.conf.worker_concurrency = settings.celery_worker_concurrency
celery.conf.worker_max_tasks_per_child = settings.celery_worker_max_tasks_per_child
celery.conf.task_acks_late = True
celery.conf.task_reject_on_worker_lost = True
celery.conf.task_default_queue = 'default'
celery.conf.task_default_exchange = 'default'
celery.conf.task_default_routing_key = 'default'
celery.conf.task_create_missing_queues = True
celery.conf.broker_transport_options = {'visibility_timeout': 3600}
celery.conf.task_routes = {
    'worker.tasks.refresh_item_status_for_tenant': {'queue': 'ingest'},
    'worker.tasks.ingest_sales_documents': {'queue': 'ingest'},
    'worker.tasks.ingest_purchase_documents': {'queue': 'ingest'},
    'worker.tasks.ingest_inventory_documents': {'queue': 'ingest'},
    'worker.tasks.ingest_cash_transactions': {'queue': 'ingest'},
    'worker.tasks.ingest_supplier_balances': {'queue': 'ingest'},
    'worker.tasks.ingest_customer_balances': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_sales': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_purchases': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_inventory': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_cashflows': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_supplier_balances': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_customer_balances': {'queue': 'ingest'},
    'worker.tasks.enqueue_external_ingest': {'queue': 'ingest'},
    'worker.tasks.enqueue_incremental_sync': {'queue': 'ingest'},
    'worker.tasks.enqueue_incremental_sync_all_tenants': {'queue': 'ingest'},
    'worker.tasks.sync_3cx_call_center_due_tenants': {'queue': 'ingest'},
    'worker.tasks.refresh_inventory_snapshots_all_tenants': {'queue': 'ingest'},
    'worker.tasks.enqueue_daily_recovery_sync_all_tenants': {'queue': 'ingest'},
    'worker.tasks.enqueue_daily_reconciliation_checks': {'queue': 'default'},
    'worker.tasks.run_daily_reconciliation_for_tenant': {'queue': 'default'},
    # Backfill fan-out should not wait behind tenant ingest jobs on the same queue.
    # Run the planner on the default queue, then let it enqueue stream jobs to ingest.
    'worker.tasks.enqueue_sql_backfill': {'queue': 'default'},
    'worker.tasks.enqueue_pharmacyone_backfill': {'queue': 'default'},
    'worker.tasks.auto_recover_stuck_ingest': {'queue': 'ingest'},
    'worker.tasks.drain_tenant_ingest_queue': {'queue': 'ingest'},
    'worker.tasks.refresh_aggregates_for_entity': {'queue': 'ingest'},
    'worker.tasks.refresh_sales_aggregates': {'queue': 'ingest'},
    'worker.tasks.reset_tenant_data_and_backfill': {'queue': 'delete'},
    'worker.tasks.delete_tenant_data_only': {'queue': 'delete'},
    'worker.tasks.generate_insights_for_tenant': {'queue': 'default'},
    'worker.tasks.generate_daily_insights_all_tenants': {'queue': 'default'},
    # Long-running maintenance: keep it off the ingest queue.
    'worker.tasks.purge_processed_staging_all_tenants': {'queue': 'default'},
}
celery.conf.beat_schedule = {
    'daily-insights-generation': {
        'task': 'worker.tasks.generate_daily_insights_all_tenants',
        'schedule': timedelta(days=1),
    },
    'incremental-sync-all-tenants': {
        'task': 'worker.tasks.enqueue_incremental_sync_all_tenants',
        'schedule': timedelta(minutes=1),
        'kwargs': {
            'limit': int(settings.incremental_sync_limit or 500),
            'max_tenants': int(settings.incremental_sync_max_tenants_per_run or 100),
        },
    },
    '3cx-call-center-db-sync': {
        'task': 'worker.tasks.sync_3cx_call_center_due_tenants',
        'schedule': timedelta(minutes=1),
    },
    'auto-recover-stuck-ingest': {
        'task': 'worker.tasks.auto_recover_stuck_ingest',
        'schedule': timedelta(seconds=max(30, int(settings.ingest_auto_recover_interval_seconds or 60))),
    },
    'daily-recovery-sync-all-tenants': {
        'task': 'worker.tasks.enqueue_daily_recovery_sync_all_tenants',
        'schedule': timedelta(days=1),
    },
    'daily-reconciliation-checks': {
        'task': 'worker.tasks.enqueue_daily_reconciliation_checks',
        'schedule': timedelta(minutes=5),
    },
    'audit-sync-completeness': {
        'task': 'worker.tasks.audit_sync_completeness',
        'schedule': timedelta(minutes=30),
    },
    # Nightly 03:30 Europe/Athens — drop processed staging rows past the retention
    # window. Without it the stg_* tables grow without bound (34 GB on one tenant).
    'purge-processed-staging': {
        'task': 'worker.tasks.purge_processed_staging_all_tenants',
        'schedule': crontab(hour=3, minute=30),
    },
    # Nightly 22:00 Europe/Athens — re-pull SoftOne net stock so the daily snapshot is
    # accurate (incremental syncs never refresh the balance). Timezone set below.
    'nightly-inventory-stock-snapshot': {
        'task': 'worker.tasks.refresh_inventory_snapshots_all_tenants',
        'schedule': crontab(hour=22, minute=0),
    },
    # Every 5 min — send each tenant's scheduled Co-Pilot daily report once its
    # configured time (Europe/Athens) has passed and it hasn't been sent today.
    'copilot-daily-reports': {
        'task': 'worker.tasks.send_scheduled_copilot_reports',
        'schedule': timedelta(minutes=5),
    },
}
celery.conf.timezone = 'Europe/Athens'
celery.conf.enable_utc = True

celery.autodiscover_tasks(['worker'])
