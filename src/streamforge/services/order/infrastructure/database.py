import logging
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from streamforge.shared.config import get_settings

logger = logging.getLogger(__name__)

_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the application-level AsyncEngine singleton, creating it if necessary."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.async_database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=settings.db_pool_pre_ping,
            echo=settings.app_env == "development",
        )
    return _engine


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    """Return the application-level async_sessionmaker singleton, creating it if necessary."""
    global _session_maker
    if _session_maker is None:
        _session_maker = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_maker


def init_database() -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Initialize and return database engine and session maker singletons."""
    return get_engine(), get_session_maker()


async def close_database() -> None:
    """Dispose of the database engine and reset singletons."""
    global _engine, _session_maker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_maker = None


async def get_db_session() -> AsyncIterator[AsyncSession]:
    """Yield an AsyncSession for request or task processing without swallowing errors."""
    session_maker = get_session_maker()
    async with session_maker() as session:
        yield session


async def check_database_connection(engine: AsyncEngine | None = None) -> bool:
    """Check connectivity against the provided or default database engine."""
    target_engine = engine or get_engine()
    try:
        async with target_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.warning("Database connectivity check failed: %s", e)
        return False
