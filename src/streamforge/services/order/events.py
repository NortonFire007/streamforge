"""Domain events and serialization for the Order Service."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class OrderCreatedEvent(BaseModel):
    """Event model representing the creation of a new order.

    Mirrors the standard JSON event envelope contract for the 'orders.events' topic:
    - event_id: Unique UUIDv4 event identifier.
    - event_type: Constant 'order.created'.
    - event_version: Schema version number (default 1).
    - occurred_at: UTC timestamp in ISO-8601 format when the event was recorded.
    - order_id: Unique order identifier.
    - customer_id: Identifier of the customer placing the order.
    - total_amount: Exact monetary purchase amount for the order (stored as Decimal,
      serialized to exact string representation in JSON to eliminate floating-point drift).
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique UUIDv4 identifier for the event",
    )
    event_type: str = Field(default="order.created", description="Event type discriminator")
    event_version: int = Field(default=1, description="Event envelope schema version")
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the event occurred",
    )
    order_id: UUID = Field(..., description="Unique identifier of the created order")
    customer_id: UUID = Field(..., description="Unique customer identifier")
    total_amount: Decimal = Field(
        ...,
        ge=Decimal("0.00"),
        description="Exact monetary total amount of the order",
    )


def to_kafka_value(event: OrderCreatedEvent) -> bytes:
    """Serialize an OrderCreatedEvent into UTF-8 encoded JSON bytes for Kafka transmission.

    Args:
        event: The OrderCreatedEvent instance to serialize.

    Returns:
        UTF-8 encoded JSON bytes representation of the event envelope.
    """
    return event.model_dump_json().encode("utf-8")
