"""Unit tests for Kafka producer factory and event builder."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from confluent_kafka import Producer

from streamforge.shared.config.settings import Settings
from streamforge.shared.kafka.producer import build_order_created_event, build_producer


def test_build_producer_returns_configured_producer() -> None:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_producer_client_id="custom-order-producer",
        kafka_producer_acks="all",
        kafka_producer_enable_idempotence=True,
        kafka_producer_retries=3,
        kafka_producer_linger_ms=10,
        kafka_producer_delivery_timeout_ms=60000,
        kafka_producer_compression_type="gzip",
    )

    with patch("streamforge.shared.kafka.producer.Producer") as mock_producer_cls:
        mock_instance = MagicMock(spec=Producer)
        mock_producer_cls.return_value = mock_instance

        producer = build_producer(settings)

        assert producer is mock_instance
        mock_producer_cls.assert_called_once_with(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "custom-order-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 3,
                "linger.ms": 10,
                "delivery.timeout.ms": 60000,
                "compression.type": "gzip",
            }
        )


def test_build_producer_with_default_settings() -> None:
    with patch("streamforge.shared.kafka.producer.Producer") as mock_producer_cls:
        mock_instance = MagicMock(spec=Producer)
        mock_producer_cls.return_value = mock_instance

        producer = build_producer()

        assert producer is mock_instance
        mock_producer_cls.assert_called_once()
        config = mock_producer_cls.call_args[0][0]
        assert "bootstrap.servers" in config
        assert "client.id" in config
        assert config["client.id"] == "order-service"


def test_build_order_created_event_from_dict() -> None:
    order_dict = {
        "order_id": "11111111-1111-1111-1111-111111111111",
        "customer_id": "22222222-2222-2222-2222-222222222222",
        "total_amount": 99.50,
    }

    event = build_order_created_event(order_dict)

    assert isinstance(event, dict)
    UUID(event["event_id"])  # Must be valid UUID
    assert event["event_type"] == "order.created"
    assert event["event_version"] == 1
    assert event["order_id"] == "11111111-1111-1111-1111-111111111111"
    assert event["customer_id"] == "22222222-2222-2222-2222-222222222222"
    assert event["total_amount"] == 99.50

    # Verify occurred_at is valid ISO-8601 UTC timestamp
    dt = datetime.fromisoformat(event["occurred_at"])
    assert dt.tzinfo is not None


def test_build_order_created_event_from_object() -> None:
    order_obj = SimpleNamespace(
        id="33333333-3333-3333-3333-333333333333",
        customer_id="44444444-4444-4444-4444-444444444444",
        total_amount=150,
    )

    event = build_order_created_event(order_obj)

    assert event["order_id"] == "33333333-3333-3333-3333-333333333333"
    assert event["customer_id"] == "44444444-4444-4444-4444-444444444444"
    assert event["total_amount"] == 150.0
    assert event["event_type"] == "order.created"
    assert event["event_version"] == 1


def test_build_order_created_event_missing_fields_raises_error() -> None:
    with pytest.raises(ValueError, match="Order must provide"):
        build_order_created_event({"customer_id": "cust-1", "total_amount": 10.0})

    with pytest.raises(ValueError, match="Order must provide"):
        build_order_created_event({"order_id": "ord-1", "total_amount": 10.0})

    with pytest.raises(ValueError, match="Order must provide"):
        build_order_created_event({"order_id": "ord-1", "customer_id": "cust-1"})
