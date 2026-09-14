"""Unit tests for OrderCreatedEvent schema and serializer."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from streamforge.services.order.events import OrderCreatedEvent, to_kafka_value


def test_order_created_event_defaults_and_serialization() -> None:
    order_id = uuid4()
    customer_id = uuid4()
    amount = Decimal("149.99")

    event = OrderCreatedEvent(
        order_id=order_id,
        customer_id=customer_id,
        total_amount=amount,
    )

    # Verify model fields
    assert isinstance(event.event_id, UUID)
    assert event.event_type == "order.created"
    assert event.event_version == 1
    assert event.order_id == order_id
    assert event.customer_id == customer_id
    assert event.total_amount == amount
    assert event.occurred_at.tzinfo is not None

    # Serialize via to_kafka_value
    raw_bytes = to_kafka_value(event)
    assert isinstance(raw_bytes, bytes)

    # Parse serialized JSON and assert structure
    data = json.loads(raw_bytes.decode("utf-8"))
    assert data["event_id"] == str(event.event_id)
    assert data["event_type"] == "order.created"
    assert data["event_version"] == 1
    assert data["order_id"] == str(order_id)
    assert data["customer_id"] == str(customer_id)
    # Exact monetary representation preserved as string
    assert data["total_amount"] == "149.99"

    # Verify occurred_at is a valid UTC ISO-8601 timestamp
    parsed_timestamp = datetime.fromisoformat(data["occurred_at"])
    assert parsed_timestamp.tzinfo is not None
    assert parsed_timestamp.utcoffset() == UTC.utcoffset(None)


def test_order_created_event_accepts_uuid_and_decimal_strings() -> None:
    event = OrderCreatedEvent(
        order_id=UUID("12345678-1234-5678-1234-567812345678"),
        customer_id=UUID("87654321-4321-8765-4321-876543210987"),
        total_amount=Decimal("10.00"),
    )
    raw_bytes = to_kafka_value(event)
    data = json.loads(raw_bytes.decode("utf-8"))
    assert data["order_id"] == "12345678-1234-5678-1234-567812345678"
    assert data["customer_id"] == "87654321-4321-8765-4321-876543210987"
    assert data["total_amount"] == "10.00"


def test_order_created_event_validation_errors() -> None:
    # Missing required order_id and customer_id
    with pytest.raises(ValidationError):
        OrderCreatedEvent.model_validate({"total_amount": "10.00"})

    # Negative total_amount
    with pytest.raises(ValidationError):
        OrderCreatedEvent(
            order_id=uuid4(),
            customer_id=uuid4(),
            total_amount=Decimal("-5.00"),
        )
