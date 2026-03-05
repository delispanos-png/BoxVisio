from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

http_requests_total = Counter(
    'cloudon_http_requests_total',
    'Total HTTP requests',
    ['method', 'path', 'status_code'],
)
http_request_duration_seconds = Histogram(
    'cloudon_http_request_duration_seconds',
    'HTTP request latency seconds',
    ['method', 'path'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

app_errors_total = Counter(
    'cloudon_app_errors_total',
    'Application errors by class/path',
    ['error_type', 'path'],
)

ingest_jobs_total = Counter(
    'cloudon_ingest_jobs_total',
    'Ingestion jobs processed',
    ['connector', 'entity', 'status'],
)
ingest_jobs_tenant_total = Counter(
    'cloudon_ingest_jobs_tenant_total',
    'Ingestion jobs processed by tenant',
    ['tenant', 'connector', 'entity', 'status'],
)
ingest_retries_total = Counter(
    'cloudon_ingest_retries_total',
    'Ingestion job retries enqueued',
    ['connector', 'entity'],
)
ingest_dead_letters_total = Counter(
    'cloudon_ingest_dead_letters_total',
    'Ingestion jobs moved to dead-letter queue',
    ['connector', 'entity'],
)
ingest_job_duration_seconds = Histogram(
    'cloudon_ingest_job_duration_seconds',
    'Ingestion job processing duration',
    ['connector', 'entity', 'status'],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

sync_duration_seconds = Histogram(
    'cloudon_sync_duration_seconds',
    'Sync/aggregation task duration',
    ['task', 'entity', 'status'],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

db_pool_checked_in = Gauge('cloudon_db_pool_checked_in', 'DB pool checked-in connections', ['pool'])
db_pool_checked_out = Gauge('cloudon_db_pool_checked_out', 'DB pool checked-out connections', ['pool'])
db_pool_size = Gauge('cloudon_db_pool_size', 'DB pool configured size', ['pool'])
db_pool_overflow = Gauge('cloudon_db_pool_overflow', 'DB pool overflow size', ['pool'])
tenant_engine_cache_size = Gauge('cloudon_tenant_engine_cache_size', 'Current tenant engine cache size')
ingest_queue_depth = Gauge('cloudon_ingest_queue_depth', 'Current ingest queue depth per tenant', ['tenant'])
ingest_dlq_depth = Gauge('cloudon_ingest_dlq_depth', 'Current DLQ depth per tenant', ['tenant'])


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
