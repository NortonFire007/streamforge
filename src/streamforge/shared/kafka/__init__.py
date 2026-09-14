"""Shared Kafka infrastructure and client modules."""

from streamforge.shared.kafka.admin import (
    build_admin_client,
    create_topics,
    describe_cluster,
    describe_topics,
    get_default_topics,
)
from streamforge.shared.kafka.config import (
    ConfigurationError,
    kafka_consumer_config,
    kafka_producer_config,
)
from streamforge.shared.kafka.consumer import build_consumer
from streamforge.shared.kafka.producer import (
    build_order_created_event,
    build_producer,
)

__all__ = [
    "ConfigurationError",
    "build_admin_client",
    "build_consumer",
    "build_order_created_event",
    "build_producer",
    "create_topics",
    "describe_cluster",
    "describe_topics",
    "get_default_topics",
    "kafka_consumer_config",
    "kafka_producer_config",
]
