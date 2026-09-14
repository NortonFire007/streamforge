"""Kafka consumer factory."""

from typing import Any

from confluent_kafka import Consumer

from streamforge.shared.config.settings import Settings, get_settings
from streamforge.shared.kafka.config import kafka_consumer_config


def build_consumer(
    settings: Settings | None = None,
    *,
    extra_config: dict[str, Any] | None = None,
) -> Consumer:
    """Build and return a configured confluent_kafka.Consumer instance.

    Enforces manual offset commits by hardcoding 'enable.auto.commit=False'.

    Args:
        settings: Application settings containing Kafka consumer options.
            If None, default settings are loaded via get_settings().
        extra_config: Optional additional librdkafka configuration options to merge.
            'enable.auto.commit' cannot be overridden to True.

    Returns:
        A configured confluent_kafka.Consumer instance.
    """
    if settings is None:
        settings = get_settings()

    config = kafka_consumer_config(settings)
    if extra_config:
        config.update(extra_config)

    # Invariant: manual offset commits are always enforced directly in code
    config["enable.auto.commit"] = False

    return Consumer(config)
