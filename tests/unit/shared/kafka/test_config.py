import pytest

from streamforge.shared.config.settings import Settings
from streamforge.shared.kafka.config import (
    ConfigurationError,
    kafka_consumer_config,
    kafka_producer_config,
)


def test_kafka_producer_config_generation() -> None:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_producer_client_id="test-producer",
        kafka_producer_acks="all",
        kafka_producer_enable_idempotence=True,
        kafka_producer_retries=7,
        kafka_producer_linger_ms=10,
        kafka_producer_delivery_timeout_ms=60000,
        kafka_producer_compression_type="zstd",
    )

    config = kafka_producer_config(settings)

    assert config == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "test-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 7,
        "linger.ms": 10,
        "delivery.timeout.ms": 60000,
        "compression.type": "zstd",
    }


def test_kafka_producer_config_idempotence_acks_minus_one() -> None:
    settings = Settings(
        kafka_producer_enable_idempotence=True,
        kafka_producer_acks="-1",
    )
    config = kafka_producer_config(settings)
    assert config["enable.idempotence"] is True
    assert config["acks"] == "-1"


@pytest.mark.parametrize("invalid_acks", ["0", "1", "none"])
def test_kafka_producer_config_idempotence_conflict_raises_error(invalid_acks: str) -> None:
    settings = Settings(
        kafka_producer_enable_idempotence=True,
        kafka_producer_acks=invalid_acks,
    )
    with pytest.raises(ConfigurationError, match="Producer idempotence requires acks='all'"):
        kafka_producer_config(settings)


def test_kafka_producer_config_non_idempotent_allows_acks_zero() -> None:
    settings = Settings(
        kafka_producer_enable_idempotence=False,
        kafka_producer_acks="0",
    )
    config = kafka_producer_config(settings)
    assert config["enable.idempotence"] is False
    assert config["acks"] == "0"


def test_kafka_consumer_config_generation() -> None:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_consumer_client_id="test-consumer",
        kafka_consumer_group_id="test-group",
        kafka_consumer_auto_offset_reset="latest",
    )

    config = kafka_consumer_config(settings)

    assert config == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "test-consumer",
        "group.id": "test-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }


def test_kafka_consumer_config_always_disables_auto_commit() -> None:
    settings = Settings()
    config = kafka_consumer_config(settings)
    assert config["enable.auto.commit"] is False
