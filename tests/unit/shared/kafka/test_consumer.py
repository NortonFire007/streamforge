"""Unit tests for Kafka consumer factory and OrderEventConsumer."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import Consumer, Message, TopicPartition

from streamforge.services.order.consumer import OrderEventConsumer, setup_signal_handlers
from streamforge.shared.config.settings import Settings
from streamforge.shared.kafka.consumer import build_consumer


def test_build_consumer_returns_configured_consumer() -> None:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_consumer_client_id="custom-order-consumer",
        kafka_consumer_group_id="custom-group",
        kafka_consumer_auto_offset_reset="earliest",
    )

    with patch("streamforge.shared.kafka.consumer.Consumer") as mock_consumer_cls:
        mock_instance = MagicMock(spec=Consumer)
        mock_consumer_cls.return_value = mock_instance

        consumer = build_consumer(settings)

        assert consumer is mock_instance
        mock_consumer_cls.assert_called_once_with(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "custom-order-consumer",
                "group.id": "custom-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )


def test_build_consumer_with_default_settings() -> None:
    with patch("streamforge.shared.kafka.consumer.Consumer") as mock_consumer_cls:
        mock_instance = MagicMock(spec=Consumer)
        mock_consumer_cls.return_value = mock_instance

        consumer = build_consumer()

        assert consumer is mock_instance
        mock_consumer_cls.assert_called_once()
        config = mock_consumer_cls.call_args[0][0]
        assert "bootstrap.servers" in config
        assert config["enable.auto.commit"] is False


def test_build_consumer_extra_config_cannot_override_auto_commit() -> None:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_consumer_client_id="test-client",
        kafka_consumer_group_id="test-group",
    )

    with patch("streamforge.shared.kafka.consumer.Consumer") as mock_consumer_cls:
        mock_instance = MagicMock(spec=Consumer)
        mock_consumer_cls.return_value = mock_instance

        # Even if extra_config attempts to enable auto commit, it must be forced False
        build_consumer(
            settings,
            extra_config={"enable.auto.commit": True, "session.timeout.ms": 6000},
        )

        config = mock_consumer_cls.call_args[0][0]
        assert config["enable.auto.commit"] is False
        assert config["session.timeout.ms"] == 6000


def test_order_consumer_initialization_and_properties() -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    settings = Settings(
        kafka_consumer_client_id="test-consumer-id",
        kafka_consumer_group_id="test-group-id",
    )

    order_consumer = OrderEventConsumer(
        consumer=mock_kafka_consumer,
        settings=settings,
        topic="orders.events",
    )

    assert order_consumer.client_id == "test-consumer-id"
    assert order_consumer.topic == "orders.events"
    assert order_consumer.consumer is mock_kafka_consumer
    assert isinstance(order_consumer.process_id, int)


def test_order_consumer_rebalance_callbacks(caplog: pytest.LogCaptureFixture) -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    settings = Settings(kafka_consumer_client_id="observability-consumer")
    order_consumer = OrderEventConsumer(consumer=mock_kafka_consumer, settings=settings)

    partitions = [
        TopicPartition("orders.events", 0),
        TopicPartition("orders.events", 1),
    ]

    with caplog.at_level(logging.INFO):
        order_consumer.on_assign(mock_kafka_consumer, partitions)
        assert "Rebalance partitions assigned" in caplog.text
        assert "observability-consumer" in caplog.text
        assert "orders.events" in caplog.text

        caplog.clear()
        order_consumer.on_revoke(mock_kafka_consumer, partitions)
        assert "Rebalance partitions revoked" in caplog.text
        assert "observability-consumer" in caplog.text


def test_order_consumer_process_valid_message(caplog: pytest.LogCaptureFixture) -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    order_consumer = OrderEventConsumer(consumer=mock_kafka_consumer)

    payload = {
        "event_id": "c5d1e672-1b1e-45fa-b64d-cceea9f00101",
        "event_type": "order.created",
        "order_id": "b3f02e64-51a8-422c-9a4f-561bcf7047f0",
        "customer_id": "7cf73aa6-271d-4eb7-a790-a33762883f3e",
        "total_amount": "99.99",
    }
    mock_msg = MagicMock(spec=Message)
    mock_msg.value.return_value = json.dumps(payload).encode("utf-8")
    mock_msg.topic.return_value = "orders.events"
    mock_msg.partition.return_value = 2
    mock_msg.offset.return_value = 42

    with caplog.at_level(logging.INFO):
        result = order_consumer.process_message(mock_msg)

    assert result == payload
    assert "Consumed event: event_id=c5d1e672-1b1e-45fa-b64d-cceea9f00101" in caplog.text
    assert "order_id=b3f02e64-51a8-422c-9a4f-561bcf7047f0" in caplog.text
    assert "offset=42" in caplog.text


def test_order_consumer_poll_loop_processes_and_commits_synchronously() -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    order_consumer = OrderEventConsumer(
        consumer=mock_kafka_consumer,
        topic="orders.events",
    )

    payload = {
        "event_id": "evt-123",
        "event_type": "order.created",
        "order_id": "ord-456",
        "customer_id": "cust-789",
    }
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.value.return_value = json.dumps(payload).encode("utf-8")
    mock_msg.topic.return_value = "orders.events"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 10

    # First poll returns message, second poll stops consumer
    def poll_side_effect(timeout: float) -> Message | None:
        order_consumer.stop()
        return mock_msg

    mock_kafka_consumer.poll.side_effect = poll_side_effect

    order_consumer.start(poll_timeout=0.1)

    mock_kafka_consumer.subscribe.assert_called_once_with(
        ["orders.events"],
        on_assign=order_consumer.on_assign,
        on_revoke=order_consumer.on_revoke,
    )
    mock_kafka_consumer.commit.assert_called_once_with(asynchronous=False)
    mock_kafka_consumer.close.assert_called_once()


def test_order_consumer_handles_corrupt_message_without_commit() -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    order_consumer = OrderEventConsumer(consumer=mock_kafka_consumer)

    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b"invalid-non-json-data"
    mock_msg.topic.return_value = "orders.events"
    mock_msg.partition.return_value = 1
    mock_msg.offset.return_value = 5

    def poll_side_effect(timeout: float) -> Message | None:
        order_consumer.stop()
        return mock_msg

    mock_kafka_consumer.poll.side_effect = poll_side_effect

    order_consumer.start(poll_timeout=0.1)

    # Corrupt message should not trigger a commit
    mock_kafka_consumer.commit.assert_not_called()
    mock_kafka_consumer.close.assert_called_once()


def test_setup_signal_handlers_invokes_stop() -> None:
    mock_kafka_consumer = MagicMock(spec=Consumer)
    order_consumer = OrderEventConsumer(consumer=mock_kafka_consumer)

    with (
        patch("signal.signal") as mock_signal,
    ):
        setup_signal_handlers(order_consumer)
        assert mock_signal.call_count == 2
