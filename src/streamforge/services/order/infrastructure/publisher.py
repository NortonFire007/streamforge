"""Kafka event publisher adapter for the Order Service."""

import logging
from typing import Any

from confluent_kafka import Producer

from streamforge.services.order.domain.models import Order
from streamforge.services.order.events import OrderCreatedEvent, to_kafka_value

logger = logging.getLogger(__name__)


def delivery_report(err: Any, msg: Any) -> None:
    """Delivery report callback triggered by librdkafka upon message delivery or failure.

    Args:
        err: KafkaError instance on failure, None on success.
        msg: Message object containing delivery metadata.
    """
    if err is not None:
        logger.error("Kafka message delivery failed: %s", err)
    else:
        logger.info(
            "Kafka message delivered to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class OrderEventPublisher:
    """Kafka messaging adapter encapsulating event production and error handling.

    Keeps librdkafka mechanics (produce, poll(0), delivery callbacks, and BufferError)
    isolated within the infrastructure layer.
    """

    def __init__(self, producer: Producer, topic: str = "orders.events") -> None:
        self._producer = producer
        self._topic = topic

    def publish_order_created(self, order: Order) -> None:
        """Construct, serialize, and publish an OrderCreated event to Kafka.

        Retains exact Decimal precision for monetary total_amount to avoid
        floating-point drift.

        Args:
            order: Pure domain Order entity for which the event is emitted.
        """
        event = OrderCreatedEvent(
            order_id=order.order_id,
            customer_id=order.customer_id,
            total_amount=order.total_amount,
        )
        json_bytes = to_kafka_value(event)

        try:
            self._producer.produce(
                topic=self._topic,
                key=str(order.customer_id),
                value=json_bytes,
                callback=delivery_report,
            )
            self._producer.poll(0)
        except BufferError as exc:
            logger.warning(
                "Kafka local producer buffer full; message was dropped: %s",
                exc,
            )
