from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from services.order.main import app


@pytest.mark.asyncio
async def test_readiness_endpoint_success() -> None:
    with patch(
        "services.order.api.routes.check_database_connection",
        new=AsyncMock(return_value=True),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/ready")

        assert response.status_code == 200
        assert response.json() == {"status": "ready", "database": "connected"}


@pytest.mark.asyncio
async def test_readiness_endpoint_database_unreachable() -> None:
    with patch(
        "services.order.api.routes.check_database_connection",
        new=AsyncMock(return_value=False),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/ready")

        assert response.status_code == 503
        assert response.json() == {"detail": {"status": "not_ready", "database": "unreachable"}}


@pytest.mark.asyncio
async def test_readiness_endpoint_live_database() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "connected"}
