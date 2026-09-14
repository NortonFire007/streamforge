"""Unit tests for Order Service Clean Architecture layers.

Covers domain, repository, publisher, service, and API routes.
"""

import logging
from collections.abc import AsyncIterator
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from confluent_kafka import Message, Producer
from fastapi import Request
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from streamforge.services.order.api.dependencies import (
    get_kafka_producer,
    get_order_service,
)
from streamforge.services.order.application.service import OrderService
from streamforge.services.order.domain.models import Order, OrderStatus
from streamforge.services.order.infrastructure.database import get_db_session
from streamforge.services.order.infrastructure.models import OrderModel
from streamforge.services.order.infrastructure.publisher import (
    OrderEventPublisher,
    delivery_report,
)
from streamforge.services.order.infrastructure.repository import OrderRepository
from streamforge.services.order.main import app, lifespan


@pytest.mark.asyncio
async def test_order_repository_creates_and_commits() -> None:
    mock_session = AsyncMock(spec=AsyncSession)
    repo = OrderRepository(mock_session)

    order = Order(
        order_id=uuid4(),
        customer_id=uuid4(),
        total_amount=Decimal("129.50"),
    )

    persisted = await repo.create(order)

    assert persisted is order
    mock_session.add.assert_called_once()
    added_model = mock_session.add.call_args[0][0]
    assert isinstance(added_model, OrderModel)
    assert added_model.id == str(order.order_id)
    assert added_model.customer_id == str(order.customer_id)
    assert added_model.total_amount == Decimal("129.50")
    assert added_model.status == "created"
    mock_session.commit.assert_awaited_once()


def test_order_event_publisher_publishes_exact_decimal() -> None:
    mock_producer = MagicMock(spec=Producer)
    publisher = OrderEventPublisher(mock_producer, topic="orders.events")

    order = Order(
        order_id=uuid4(),
        customer_id=uuid4(),
        total_amount=Decimal("129.50"),
    )

    publisher.publish_order_created(order)

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs["topic"] == "orders.events"
    assert call_kwargs["key"] == str(order.customer_id)
    assert call_kwargs["callback"] is delivery_report

    # Check exact Decimal string in JSON bytes
    payload_str = call_kwargs["value"].decode("utf-8")
    assert '"total_amount":"129.50"' in payload_str
    mock_producer.poll.assert_called_once_with(0)


def test_order_event_publisher_buffer_error_logged(caplog: pytest.LogCaptureFixture) -> None:
    mock_producer = MagicMock(spec=Producer)
    mock_producer.produce.side_effect = BufferError("Local queue full")
    publisher = OrderEventPublisher(mock_producer)

    order = Order(
        order_id=uuid4(),
        customer_id=uuid4(),
        total_amount=Decimal("50.00"),
    )

    with caplog.at_level(logging.WARNING):
        publisher.publish_order_created(order)

    assert "Kafka local producer buffer full" in caplog.text
    mock_producer.poll.assert_not_called()


@pytest.mark.asyncio
async def test_order_service_orchestrates_persistence_and_publishing() -> None:
    mock_repo = AsyncMock(spec=OrderRepository)
    mock_publisher = MagicMock(spec=OrderEventPublisher)
    service = OrderService(repository=mock_repo, publisher=mock_publisher)

    customer_id = uuid4()
    amount = Decimal("99.99")

    async def fake_create(o: Order) -> Order:
        return o

    mock_repo.create.side_effect = fake_create

    result = await service.create_order(customer_id=customer_id, total_amount=amount)

    assert isinstance(result, Order)
    assert result.customer_id == customer_id
    assert result.total_amount == amount
    assert result.status == OrderStatus.CREATED
    mock_repo.create.assert_awaited_once_with(result)
    mock_publisher.publish_order_created.assert_called_once_with(result)


@pytest.mark.asyncio
async def test_create_order_http_endpoint_success() -> None:
    mock_service = AsyncMock(spec=OrderService)
    customer_id = uuid4()
    order_id = uuid4()

    fake_order = Order(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=Decimal("79.95"),
        status=OrderStatus.CREATED,
    )
    mock_service.create_order.return_value = fake_order

    app.dependency_overrides[get_order_service] = lambda: mock_service

    payload = {
        "customer_id": str(customer_id),
        "total_amount": "79.95",
    }

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/orders", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["order_id"] == str(order_id)
        assert data["customer_id"] == str(customer_id)
        assert data["total_amount"] == "79.95"
        assert data["status"] == "created"
        mock_service.create_order.assert_awaited_once_with(
            customer_id=customer_id,
            total_amount=Decimal("79.95"),
            order_id=None,
        )
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_create_order_full_dependency_wiring() -> None:
    mock_producer = MagicMock(spec=Producer)
    mock_session = AsyncMock(spec=AsyncSession)

    customer_id = uuid4()
    payload = {
        "customer_id": str(customer_id),
        "total_amount": "150.00",
    }

    async def override_get_db_session() -> AsyncIterator[AsyncSession]:
        yield mock_session

    app.dependency_overrides[get_kafka_producer] = lambda: mock_producer
    app.dependency_overrides[get_db_session] = override_get_db_session

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/orders", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["customer_id"] == str(customer_id)
        assert data["total_amount"] == "150.00"

        # Database transaction committed
        mock_session.commit.assert_awaited_once()

        # Producer produce and poll(0) invoked
        mock_producer.produce.assert_called_once()
        mock_producer.poll.assert_called_once_with(0)
    finally:
        app.dependency_overrides.clear()


def test_get_kafka_producer_missing_raises_error() -> None:
    mock_request = MagicMock(spec=Request)
    mock_request.app.state = MagicMock(spec=[])  # No producer attribute

    with pytest.raises(RuntimeError, match="Kafka producer is not initialized"):
        get_kafka_producer(mock_request)


def test_delivery_report_success(caplog: pytest.LogCaptureFixture) -> None:
    mock_msg = MagicMock(spec=Message)
    mock_msg.topic.return_value = "orders.events"
    mock_msg.partition.return_value = 2
    mock_msg.offset.return_value = 42

    with caplog.at_level(logging.INFO):
        delivery_report(None, mock_msg)

    assert "Kafka message delivered to orders.events [2] at offset 42" in caplog.text


def test_delivery_report_error(caplog: pytest.LogCaptureFixture) -> None:
    class DummyKafkaError:
        def __str__(self) -> str:
            return "Broker: Message timed out"

    mock_msg = MagicMock(spec=Message)
    mock_err = DummyKafkaError()

    with caplog.at_level(logging.ERROR):
        delivery_report(mock_err, mock_msg)

    assert "Kafka message delivery failed: Broker: Message timed out" in caplog.text


@pytest.mark.asyncio
async def test_lifespan_kafka_producer_startup_and_flush() -> None:
    mock_producer = MagicMock(spec=Producer)

    with (
        patch("streamforge.services.order.main.init_database"),
        patch("streamforge.services.order.main.init_tables", new_callable=AsyncMock),
        patch("streamforge.services.order.main.close_database", new_callable=AsyncMock),
        patch("streamforge.services.order.main.build_producer", return_value=mock_producer),
    ):
        async with lifespan(app):
            assert getattr(app.state, "producer", None) is mock_producer
            mock_producer.flush.assert_not_called()

        mock_producer.flush.assert_called_once_with(10)
