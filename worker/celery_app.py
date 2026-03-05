from celery import Celery
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
celery.conf.broker_transport_options = {'visibility_timeout': 3600}
celery.conf.task_routes = {
    'worker.tasks.sync_pharmacyone_sales': {'queue': 'ingest'},
    'worker.tasks.sync_pharmacyone_purchases': {'queue': 'ingest'},
    'worker.tasks.enqueue_external_ingest': {'queue': 'ingest'},
    'worker.tasks.drain_tenant_ingest_queue': {'queue': 'ingest'},
    'worker.tasks.refresh_aggregates_for_entity': {'queue': 'ingest'},
    'worker.tasks.refresh_sales_aggregates': {'queue': 'ingest'},
    'worker.tasks.generate_insights_for_tenant': {'queue': 'ingest'},
    'worker.tasks.generate_daily_insights_all_tenants': {'queue': 'ingest'},
}
celery.conf.beat_schedule = {
    'daily-insights-generation': {
        'task': 'worker.tasks.generate_daily_insights_all_tenants',
        'schedule': timedelta(days=1),
    }
}

celery.autodiscover_tasks(['worker'])
