from functools import lru_cache
import base64
import subprocess
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _detect_app_version() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    git_dir = repo_root / '.git'
    try:
        head_raw = (git_dir / 'HEAD').read_text(encoding='utf-8').strip()
        if head_raw.startswith('ref:'):
            ref_path = head_raw.split(':', 1)[1].strip()
            ref_file = git_dir / ref_path
            if ref_file.exists():
                commit_sha = ref_file.read_text(encoding='utf-8').strip()
                if commit_sha:
                    return commit_sha[:7]
        elif head_raw:
            return head_raw[:7]
    except Exception:
        pass
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=str(repo_root),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        short_sha = str(result.stdout or '').strip()
        if short_sha:
            return short_sha
    except Exception:
        pass
    return 'dev'


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
    ingest_drain_max_jobs: int = 100
    ingest_stuck_heartbeat_seconds: int = 180
    ingest_auto_recover_enabled: bool = True
    ingest_auto_recover_interval_seconds: int = 60
    ingest_auto_recover_force_seconds: int = 600
    ingest_auto_recover_max_tenants_per_run: int = 10
    incremental_sync_all_tenants_enabled: bool = True
    incremental_sync_interval_minutes: int = 5
    incremental_sync_limit: int = 500
    incremental_sync_max_tenants_per_run: int = 100
    rate_limit_per_minute: int = 120
    ingest_tenant_lock_ttl_seconds: int = 300
    ingest_throttle_jobs_per_window: int = 120
    ingest_throttle_window_seconds: int = 60
    sqlserver_retry_sleep_seconds: int = 2

    celery_worker_concurrency: int = 4
    celery_worker_prefetch_multiplier: int = 1
    celery_worker_max_tasks_per_child: int = 1000

    whmcs_webhook_secret: str = ''
    default_admin_email: str = 'admin@boxvisio.com'
    default_admin_password: str = ''

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
