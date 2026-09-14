"""Shared Kafka infrastructure and client modules."""

from streamforge.shared.kafka.config import (
    ConfigurationError,
    kafka_consumer_config,
    kafka_producer_config,
)
from streamforge.shared.kafka.producer import (
    build_order_created_event,
    build_producer,
)

__all__ = [
    "ConfigurationError",
    "build_order_created_event",
    "build_producer",
    "kafka_consumer_config",
    "kafka_producer_config",
]
