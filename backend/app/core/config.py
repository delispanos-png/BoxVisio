from functools import lru_cache
import base64
import time
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_REPO_ROOT = Path(__file__).resolve().parents[3]
_BUILD_INFO_PATH = Path(__file__).resolve().parents[1] / 'build_info.json'
_VERSION_PATH = _REPO_ROOT / 'VERSION'

#  Directories whose contents make up the deployed application. Used only to
#  answer "is the running code newer than the last commit?".
_SOURCE_DIRS = ('backend/app', 'scripts', 'worker')
_SOURCE_SUFFIXES = ('.py', '.html', '.css', '.js', '.json', '.sql')


@lru_cache(maxsize=1)
def _build_info() -> dict[str, str | int]:
    """Version identity, written at commit time by scripts/bump_build.py.

    Read from a file rather than asked of git: the API container mounts the repo
    but has no `git` binary, so anything git-derived would silently degrade to
    'dev' in production.
    """
    try:
        import json

        data = json.loads(_BUILD_INFO_PATH.read_text(encoding='utf-8'))
        if data.get('version'):
            return data
    except Exception:
        pass
    #  No stamp yet — fall back to the series alone so the app still reports
    #  something meaningful instead of a bare hash.
    try:
        series = _VERSION_PATH.read_text(encoding='utf-8').strip()
    except Exception:
        series = ''
    return {'series': series or '0.0', 'build': 0, 'version': f'{series or "0.0"}.0', 'commit': '', 'branch': ''}


def _detect_app_version() -> str:
    return str(_build_info().get('version') or '0.0.0')


def _working_tree_is_dirty() -> bool:
    """True when application files are newer than the last staged git state.

    `.git/index` is rewritten by commit and by `git add`, so anything modified
    afterwards is work that is running but not recorded. This is a heuristic —
    it cannot see reverted edits — but it answers the question that matters:
    "is what I am looking at the same as what was committed?"

    Deliberately mtime-based: computing a real dirty state needs the git binary,
    which the container does not have.
    """
    try:
        index_mtime = (_REPO_ROOT / '.git' / 'index').stat().st_mtime
    except OSError:
        return False
    for rel in _SOURCE_DIRS:
        base = _REPO_ROOT / rel
        if not base.is_dir():
            continue
        for path in base.rglob('*'):
            if path.suffix not in _SOURCE_SUFFIXES:
                continue
            try:
                if path.stat().st_mtime > index_mtime:
                    return True
            except OSError:
                continue
    return False


_DIRTY_TTL_SECONDS = 30
_dirty_cache: dict[str, float | bool] = {'checked_at': 0.0, 'dirty': False}


def app_version_detailed() -> str:
    """Version plus build provenance, for operators — never shown to tenants.

    Adds `+dev` when the running code has uncommitted changes, which is the
    usual reason a version looks stuck.
    """
    info = _build_info()
    parts = [str(info.get('version') or '0.0.0')]
    now = time.time()
    if now - float(_dirty_cache['checked_at']) > _DIRTY_TTL_SECONDS:
        _dirty_cache['dirty'] = _working_tree_is_dirty()
        _dirty_cache['checked_at'] = now
    if _dirty_cache['dirty']:
        parts.append('+dev')
    commit = str(info.get('commit') or '')
    if commit:
        parts.append(f' · {commit}')
    return ''.join(parts)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    project_name: str = 'CloudOn BI'
    app_version: str = ''
    environment: str = 'dev'
    secret_key: str
    bi_secret_key: str
    access_token_expire_minutes: int = 720
    refresh_token_expire_days: int = 30
    default_trial_days: int = 14
    past_due_grace_days: int = 7

    control_database_url: str
    control_database_url_sync: str

    tenant_db_host: str = 'postgres'
    tenant_db_port: int = 5432
    tenant_db_superuser: str = 'postgres'
    tenant_db_superpass: str
    tenant_db_prefix: str = 'bi_tenant_'
    tenant_database_url_template: str = 'postgresql+asyncpg://{user}:{password}@postgres:5432/{db_name}'
    tenant_database_url_template_sync: str = 'postgresql+psycopg://{user}:{password}@postgres:5432/{db_name}'

    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800
    tenant_engine_cache_size: int = 64

    redis_url: str = 'redis://redis:6379/0'
    celery_broker_url: str = 'redis://redis:6379/0'
    celery_result_backend: str = 'redis://redis:6379/1'
    ingest_job_max_retries: int = 3
    ingest_retry_backoff_seconds: int = 5
    ingest_job_timeout_seconds: int = 300
    ingest_drain_max_jobs: int = 10
    ingest_stuck_heartbeat_seconds: int = 180
    ingest_backfill_job_timeout_seconds: int = 7200
    ingest_backfill_sales_chunk_days: int = 1
    ingest_backfill_purchases_chunk_days: int = 1
    ingest_backfill_inventory_chunk_days: int = 1
    ingest_auto_recover_enabled: bool = True
    ingest_auto_recover_interval_seconds: int = 60
    ingest_auto_recover_force_seconds: int = 600
    ingest_auto_recover_max_tenants_per_run: int = 10
    incremental_sync_all_tenants_enabled: bool = True
    incremental_sync_interval_minutes: int = 5
    incremental_sync_limit: int = 500
    incremental_sync_max_tenants_per_run: int = 100
    incremental_sync_overlap_minutes: int = 5
    incremental_sync_scheduler_round_robin_enabled: bool = True
    incremental_sync_scheduler_candidate_multiplier: int = 3
    # Staging rows are a write-only audit trail: written on ingest, marked
    # processed, never read back. Left unbounded they reached 34 GB on one tenant.
    # 'failed' rows are always kept, regardless of age.
    staging_retention_enabled: bool = True
    staging_retention_days: int = 14
    staging_retention_batch_rows: int = 50000
    # Every drain cycle used to enqueue its own aggregate refresh, so the
    # aggregates were rebuilt roughly twice a minute (2,884 runs/day). Refreshes
    # for the same tenant+entity are now coalesced into one run per window, with
    # the pending date ranges unioned together.
    aggregate_refresh_debounce_seconds: int = 120
    # A refresh that arrives without an explicit range used to rebuild aggregates
    # back to the earliest document on record. Capped unless the caller asks for
    # a full rebuild (end of a backfill), which is the only case that needs it.
    aggregate_refresh_max_window_days: int = 120
    auto_sync_max_queue_depth_per_tenant: int = 100
    auto_sync_live_dimension_streams_enabled: bool = False
    auto_sync_live_dimension_streams_csv: str = 'item_master'
    auto_sync_live_max_jobs_per_tenant: int = 4
    auto_sync_heavy_stream_min_interval_minutes: int = 1440
    ingest_recovery_enabled: bool = True
    ingest_recovery_on_incremental: bool = False
    ingest_recovery_on_backfill: bool = True
    ingest_recovery_incremental_days: int = 2
    ingest_daily_recovery_enabled: bool = True
    ingest_daily_recovery_days: int = 7
    ingest_recovery_chunk_days: int = 1
    ingest_recovery_limit: int = 500
    ingest_recovery_max_jobs: int = 5000
    ingest_recovery_streams_csv: str = 'sales_documents,purchase_documents,inventory_documents,cash_transactions,operating_expenses,supplier_orders'
    rate_limit_per_minute: int = 120
    ingest_tenant_lock_ttl_seconds: int = 300
    ingest_throttle_jobs_per_window: int = 120
    ingest_throttle_window_seconds: int = 60
    sqlserver_retry_sleep_seconds: int = 2
    sqlserver_query_timeout_seconds: int = 120
    sqlserver_query_retries: int = 1
    sqlserver_ingest_job_max_retries: int = 0
    sqlserver_lock_timeout_ms: int = 15000
    sqlserver_read_uncommitted: bool = True
    sqlserver_fetch_batch_size: int = 1000
    sqlserver_default_fetch_limit: int = 4000
    sqlserver_incremental_exhaustive_fetch: bool = True
    sqlserver_period_sync_exhaustive_fetch: bool = True
    sqlserver_period_sync_max_pages: int = 20000
    sqlserver_bulk_upsert_enabled: bool = True
    sqlserver_bulk_upsert_batch_size: int = 200

    celery_worker_concurrency: int = 4
    celery_worker_prefetch_multiplier: int = 1
    celery_worker_max_tasks_per_child: int = 1000

    whmcs_webhook_secret: str = ''
    default_admin_email: str = 'admin@boxvisio.com'
    default_admin_password: str = ''
    app_public_base_url: str = ''
    tenant_domain_root: str = ''

    smtp_host: str = ''
    smtp_port: int = 587
    smtp_username: str = ''
    smtp_password: str = ''
    smtp_from_email: str = ''
    smtp_from_name: str = 'CloudOn BI'
    smtp_use_tls: bool = True

    tenant_subdomain_auto_dns_enabled: bool = False
    cloudflare_api_token: str = ''
    cloudflare_zone_id: str = ''
    cloudflare_dns_record_type: str = 'CNAME'
    cloudflare_dns_target: str = ''
    cloudflare_dns_proxied: bool = True
    cloudflare_dns_ttl: int = 1

    odbc_driver: str = 'ODBC Driver 18 for SQL Server'
    tenant_portal_host: str = 'bi.boxvisio.com'
    admin_portal_host: str = 'adminpanel.boxvisio.com'
    server_public_ip: str = ''
    sqlserver_default_port: int = 1433
    log_level: str = 'INFO'
    sentry_dsn: str = ''
    sentry_environment: str = 'production'
    sentry_traces_sample_rate: float = 0.0

    @field_validator('secret_key', 'tenant_db_superpass', 'bi_secret_key')
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError('must be set via environment variable')
        return value

    @field_validator('bi_secret_key')
    @classmethod
    def _valid_bi_secret_key(cls, value: str) -> str:
        try:
            raw = base64.urlsafe_b64decode(value.encode('utf-8'))
        except Exception as exc:
            raise ValueError('BI_SECRET_KEY must be valid base64') from exc
        if len(raw) != 32:
            raise ValueError('BI_SECRET_KEY must decode to 32 bytes')
        return value

    @field_validator('app_version')
    @classmethod
    def _default_app_version(cls, value: str) -> str:
        cleaned = str(value or '').strip()
        return cleaned or _detect_app_version()

    @model_validator(mode='after')
    def _fail_fast_insecure_env(self):
        checks = {
            'secret_key': self.secret_key,
            'bi_secret_key': self.bi_secret_key,
            'tenant_db_superpass': self.tenant_db_superpass,
            'control_database_url': self.control_database_url,
            'control_database_url_sync': self.control_database_url_sync,
        }
        for key, value in checks.items():
            if 'CHANGE_ME' in str(value):
                raise ValueError(f'insecure env value detected for {key}')
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
