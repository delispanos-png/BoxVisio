from celery import Celery

from app.core.config import settings


TASK_ROUTES = {
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
}


def make_celery_sender(name: str) -> Celery:
    celery = Celery(name)
    celery.conf.broker_url = settings.celery_broker_url
    celery.conf.result_backend = settings.celery_result_backend
    celery.conf.task_default_queue = 'default'
    celery.conf.task_default_exchange = 'default'
    celery.conf.task_default_routing_key = 'default'
    celery.conf.task_routes = TASK_ROUTES
    celery.conf.task_create_missing_queues = True
    return celery
