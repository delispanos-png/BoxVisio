import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator
from threading import Lock
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

_tenant_engines: OrderedDict[str, Any] = OrderedDict()
_tenant_factories: OrderedDict[str, Any] = OrderedDict()
_cache_lock = Lock()


def tenant_db_name(slug: str) -> str:
    return f"{settings.tenant_db_prefix}{slug}"


def build_tenant_urls(db_name: str, db_user: str, db_password: str) -> tuple[str, str]:
    async_url = settings.tenant_database_url_template.format(user=db_user, password=db_password, db_name=db_name)
    sync_url = settings.tenant_database_url_template_sync.format(user=db_user, password=db_password, db_name=db_name)
    return async_url, sync_url


def get_tenant_session_factory(tenant_key: str, db_name: str, db_user: str, db_password: str):
    with _cache_lock:
        if tenant_key in _tenant_factories:
            _tenant_engines.move_to_end(tenant_key)
            _tenant_factories.move_to_end(tenant_key)
            return _tenant_factories[tenant_key]

    async_url, _ = build_tenant_urls(db_name=db_name, db_user=db_user, db_password=db_password)
    engine = create_async_engine(
        async_url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
    )
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    with _cache_lock:
        _tenant_engines[tenant_key] = engine
        _tenant_factories[tenant_key] = factory
        _evict_if_needed()
    return factory


async def get_tenant_db_session(tenant_key: str, db_name: str, db_user: str, db_password: str) -> AsyncGenerator[AsyncSession, None]:
    factory = get_tenant_session_factory(tenant_key=tenant_key, db_name=db_name, db_user=db_user, db_password=db_password)
    async with factory() as session:
        yield session


def _evict_if_needed() -> None:
    while len(_tenant_engines) > settings.tenant_engine_cache_size:
        old_key, old_engine = _tenant_engines.popitem(last=False)
        _tenant_factories.pop(old_key, None)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(old_engine.dispose())
        except RuntimeError:
            asyncio.run(old_engine.dispose())


def get_tenant_pool_stats() -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = {}
    with _cache_lock:
        for tenant_key, engine in _tenant_engines.items():
            pool = engine.sync_engine.pool
            stats[tenant_key] = {
                'checked_in': int(getattr(pool, 'checkedin', lambda: 0)()),
                'checked_out': int(getattr(pool, 'checkedout', lambda: 0)()),
                'size': int(getattr(pool, 'size', lambda: 0)()),
                'overflow': int(getattr(pool, 'overflow', lambda: 0)()),
            }
        stats['_cache'] = {'size': len(_tenant_engines)}
    return stats
