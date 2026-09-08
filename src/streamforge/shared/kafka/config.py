"""Kafka configuration builders and validation for confluent-kafka."""

from typing import Any

from streamforge.shared.config.settings import Settings


class ConfigurationError(ValueError):
    """Raised when Kafka configuration parameters are invalid or incompatible."""


def kafka_producer_config(settings: Settings) -> dict[str, Any]:
    """Build a configuration dictionary for confluent_kafka.Producer.

    Validates that idempotent producers require 'acks=all' (or '-1').

    Args:
        settings: Application settings containing Kafka producer options.

    Returns:
        A dictionary of librdkafka configuration options.

    Raises:
        ConfigurationError: If enable_idempotence is True but acks is not 'all' or '-1'.
    """
    acks_str = str(settings.kafka_producer_acks).strip().lower()
    if settings.kafka_producer_enable_idempotence and acks_str not in ("all", "-1"):
        raise ConfigurationError(
            "Producer idempotence requires acks='all' (or '-1'), "
            f"but got acks={settings.kafka_producer_acks!r}"
        )

    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": settings.kafka_producer_client_id,
        "acks": settings.kafka_producer_acks,
        "enable.idempotence": settings.kafka_producer_enable_idempotence,
        "retries": settings.kafka_producer_retries,
        "linger.ms": settings.kafka_producer_linger_ms,
        "delivery.timeout.ms": settings.kafka_producer_delivery_timeout_ms,
        "compression.type": settings.kafka_producer_compression_type,
    }


def kafka_consumer_config(settings: Settings) -> dict[str, Any]:
    """Build a configuration dictionary for confluent_kafka.Consumer.

    Enforces manual offset commits by hardcoding 'enable.auto.commit' to False.

    Args:
        settings: Application settings containing Kafka consumer options.

    Returns:
        A dictionary of librdkafka configuration options.
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": settings.kafka_consumer_client_id,
        "group.id": settings.kafka_consumer_group_id,
        "auto.offset.reset": settings.kafka_consumer_auto_offset_reset,
        "enable.auto.commit": False,
    }
