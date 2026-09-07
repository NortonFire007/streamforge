from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

from streamforge.services.order.infrastructure import database
from streamforge.services.order.infrastructure.database import (
    check_database_connection,
    close_database,
    get_db_session,
    get_engine,
    get_session_maker,
    init_database,
)
from streamforge.services.order.main import lifespan
from streamforge.shared.config.settings import get_settings


@pytest.fixture(autouse=True)
async def cleanup_database_state() -> AsyncGenerator[None]:
    """Ensure database state is clean before and after each test."""
    await close_database()
    get_settings.cache_clear()
    yield
    await close_database()
    get_settings.cache_clear()


def test_get_engine_singleton() -> None:
    engine1 = get_engine()
    engine2 = get_engine()
    assert engine1 is engine2
    assert isinstance(engine1, AsyncEngine)


def test_get_engine_pool_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_POOL_SIZE", "7")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "3")
    monkeypatch.setenv("DB_POOL_TIMEOUT", "12.0")
    monkeypatch.setenv("DB_POOL_RECYCLE", "900")
    monkeypatch.setenv("DB_POOL_PRE_PING", "true")
    get_settings.cache_clear()

    engine = get_engine()
    assert isinstance(engine.pool, QueuePool)
    pool: QueuePool = engine.pool
    assert pool.size() == 7
    assert pool._max_overflow == 3
    assert pool._timeout == 12.0
    assert pool._recycle == 900
    assert pool._pre_ping is True


def test_get_session_maker_singleton_and_binds_engine() -> None:
    sm1 = get_session_maker()
    sm2 = get_session_maker()
    assert sm1 is sm2

    engine = get_engine()
    assert sm1.kw["bind"] is engine
    assert sm1.kw["expire_on_commit"] is False
    assert sm1.kw["autoflush"] is False


def test_init_database() -> None:
    engine, session_maker = init_database()
    assert engine is get_engine()
    assert session_maker is get_session_maker()


@pytest.mark.asyncio
async def test_close_database_disposes_engine() -> None:
    get_engine()
    with patch.object(AsyncEngine, "dispose", new_callable=AsyncMock) as mock_dispose:
        await close_database()
        mock_dispose.assert_awaited_once()

    assert database._engine is None
    assert database._session_maker is None


@pytest.mark.asyncio
async def test_check_database_connection_success() -> None:
    mock_conn = AsyncMock()
    mock_engine = MagicMock(spec=AsyncEngine)
    mock_engine.connect.return_value.__aenter__.return_value = mock_conn

    result = await check_database_connection(engine=mock_engine)
    assert result is True
    mock_conn.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_database_connection_failure() -> None:
    mock_engine = MagicMock(spec=AsyncEngine)
    mock_engine.connect.side_effect = ConnectionRefusedError("DB connection failed")

    result = await check_database_connection(engine=mock_engine)
    assert result is False


@pytest.mark.asyncio
async def test_lifespan_startup_and_shutdown() -> None:
    from fastapi import FastAPI

    app = FastAPI()
    with (
        patch(
            "streamforge.services.order.main.init_database", wraps=init_database
        ) as mock_init,
        patch(
            "streamforge.services.order.main.close_database", wraps=close_database
        ) as mock_close,
    ):
        async with lifespan(app):
            mock_init.assert_called_once()
            assert database._engine is not None
            assert database._session_maker is not None
            mock_close.assert_not_called()

        mock_close.assert_awaited_once()
        assert database._engine is None
        assert database._session_maker is None


@pytest.mark.asyncio
async def test_get_db_session_does_not_swallow_errors() -> None:
    class CustomDatabaseError(Exception):
        pass

    with pytest.raises(CustomDatabaseError):
        async for _session in get_db_session():
            raise CustomDatabaseError("Unhandled database error")
