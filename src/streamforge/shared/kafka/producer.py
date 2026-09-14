"""Kafka producer factory and event builders."""

import uuid
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from streamforge.shared.config.settings import Settings, get_settings
from streamforge.shared.kafka.config import kafka_producer_config


def build_producer(settings: Settings | None = None) -> Producer:
    """Build and return a configured confluent_kafka.Producer instance.

    Configures client.id, delivery timeout, acks, idempotence, and retries
    based on application settings.

    Args:
        settings: Application settings containing Kafka producer options.
            If None, default settings are loaded via get_settings().

    Returns:
        A configured confluent_kafka.Producer instance.
    """
    if settings is None:
        settings = get_settings()

    config = kafka_producer_config(settings)
    return Producer(config)


def build_order_created_event(order: Any) -> dict[str, Any]:
    """Construct a standard OrderCreated event envelope dictionary from an order.

    The envelope includes metadata (event_id, event_type, event_version, occurred_at)
    and order payload fields (order_id, customer_id, total_amount).

    Args:
        order: An object or mapping containing order_id (or id), customer_id, and total_amount.

    Returns:
        A dictionary conforming to the OrderCreated event envelope specification.

    Raises:
        ValueError: If required order fields are missing or None.
    """
    if isinstance(order, dict):
        order_id = order.get("order_id") or order.get("id")
        customer_id = order.get("customer_id")
        total_amount = order.get("total_amount")
    else:
        order_id = getattr(order, "order_id", getattr(order, "id", None))
        customer_id = getattr(order, "customer_id", None)
        total_amount = getattr(order, "total_amount", None)

    if order_id is None or customer_id is None or total_amount is None:
        raise ValueError(
            "Order must provide order_id (or id), customer_id, and total_amount attributes."
        )

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.created",
        "event_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": str(order_id),

        "customer_id": str(customer_id),
        "total_amount": float(total_amount),
    }
