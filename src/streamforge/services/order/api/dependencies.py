"""FastAPI dependency composition and wiring for the Order Service."""

from confluent_kafka import Producer
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from streamforge.services.order.application.service import OrderService
from streamforge.services.order.infrastructure.database import get_db_session
from streamforge.services.order.infrastructure.publisher import OrderEventPublisher
from streamforge.services.order.infrastructure.repository import OrderRepository


def get_kafka_producer(request: Request) -> Producer:
    """Retrieve the application-level Kafka Producer singleton from app.state.

    Raises:
        RuntimeError: If the producer was not initialized during lifespan startup.
            Never creates a second producer lazily.
    """
    producer = getattr(request.app.state, "producer", None)
    if not isinstance(producer, Producer):
        raise RuntimeError("Kafka producer is not initialized in application state.")
    return producer


def get_order_repository(
    session: AsyncSession = Depends(get_db_session),
) -> OrderRepository:
    """Provide an OrderRepository bound to the request's database session."""
    return OrderRepository(session)


def get_order_publisher(
    producer: Producer = Depends(get_kafka_producer),
) -> OrderEventPublisher:
    """Provide an OrderEventPublisher bound to the application's Kafka producer."""
    return OrderEventPublisher(producer)


def get_order_service(
    repository: OrderRepository = Depends(get_order_repository),
    publisher: OrderEventPublisher = Depends(get_order_publisher),
) -> OrderService:
    """Provide an OrderService use-case orchestrator with wired dependencies."""
    return OrderService(repository=repository, publisher=publisher)
