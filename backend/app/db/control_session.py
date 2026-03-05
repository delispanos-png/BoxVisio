from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

control_engine = create_async_engine(
    settings.control_database_url,
    pool_pre_ping=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout,
    pool_recycle=settings.db_pool_recycle,
)
ControlSessionLocal = async_sessionmaker(control_engine, class_=AsyncSession, expire_on_commit=False)


async def get_control_db() -> AsyncSession:
    async with ControlSessionLocal() as session:
        yield session


def get_control_pool_stats() -> dict[str, int]:
    pool = control_engine.sync_engine.pool
    return {
        'checked_in': int(getattr(pool, 'checkedin', lambda: 0)()),
        'checked_out': int(getattr(pool, 'checkedout', lambda: 0)()),
        'size': int(getattr(pool, 'size', lambda: 0)()),
        'overflow': int(getattr(pool, 'overflow', lambda: 0)()),
    }
