"""Unit tests for Kafka admin client module and CLI."""

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    BrokerMetadata,
    ClusterMetadata,
    PartitionMetadata,
    TopicMetadata,
)

from streamforge.shared.config.settings import Settings
from streamforge.shared.kafka.admin import (
    build_admin_client,
    create_topics,
    describe_cluster,
    describe_topics,
    get_default_topics,
    main,
)


def test_build_admin_client_uses_settings() -> None:
    settings = Settings(
        kafka_bootstrap_servers="broker1:9092,broker2:9092",
        kafka_producer_client_id="test-producer",
    )
    with patch("streamforge.shared.kafka.admin.AdminClient") as mock_cls:
        mock_instance = MagicMock(spec=AdminClient)
        mock_cls.return_value = mock_instance

        client = build_admin_client(settings)

        assert client is mock_instance
        mock_cls.assert_called_once_with(
            {
                "bootstrap.servers": "broker1:9092,broker2:9092",
                "client.id": "test-producer-admin",
            }
        )


def test_build_admin_client_custom_bootstrap_servers() -> None:
    with patch("streamforge.shared.kafka.admin.AdminClient") as mock_cls:
        mock_instance = MagicMock(spec=AdminClient)
        mock_cls.return_value = mock_instance

        client = build_admin_client(bootstrap_servers="custom-broker:9092")

        assert client is mock_instance
        called_config = mock_cls.call_args[0][0]
        assert called_config["bootstrap.servers"] == "custom-broker:9092"


def test_get_default_topics_specifications() -> None:
    topics = get_default_topics()
    assert len(topics) == 1
    t = topics[0]
    assert t.topic == "orders.events"
    assert t.num_partitions == 6
    assert t.replication_factor == 3
    assert t.config == {
        "min.insync.replicas": "2",
        "cleanup.policy": "delete",
        "retention.ms": "604800000",
    }


def test_create_topics_success() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin.create_topics.return_value = {"orders.events": mock_future}

    results = create_topics(mock_admin)

    assert results == {"orders.events": "created"}
    mock_admin.create_topics.assert_called_once()


def test_create_topics_idempotent_already_exists() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_future = MagicMock()
    kafka_err = MagicMock(spec=KafkaError)
    kafka_err.code.return_value = KafkaError.TOPIC_ALREADY_EXISTS
    mock_future.result.side_effect = KafkaException(kafka_err)
    mock_admin.create_topics.return_value = {"orders.events": mock_future}

    results = create_topics(mock_admin)

    assert results == {"orders.events": "exists"}


def test_create_topics_unrecoverable_error_raises() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_future = MagicMock()
    kafka_err = MagicMock(spec=KafkaError)
    kafka_err.code.return_value = KafkaError._AUTHENTICATION
    mock_future.result.side_effect = KafkaException(kafka_err)
    mock_admin.create_topics.return_value = {"orders.events": mock_future}

    with pytest.raises(KafkaException):
        create_topics(mock_admin)


def test_describe_topics_success() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_cluster_meta = MagicMock(spec=ClusterMetadata)

    mock_part0 = MagicMock(spec=PartitionMetadata)
    mock_part0.id = 0
    mock_part0.leader = 1
    mock_part0.replicas = [1, 2, 3]
    mock_part0.isrs = [1, 2, 3]

    mock_topic_meta = MagicMock(spec=TopicMetadata)
    mock_topic_meta.topic = "orders.events"
    mock_topic_meta.error = None
    mock_topic_meta.partitions = {0: mock_part0}

    mock_cluster_meta.topics = {"orders.events": mock_topic_meta}
    mock_admin.list_topics.return_value = mock_cluster_meta

    result = describe_topics(mock_admin, ["orders.events"])

    assert "orders.events" in result
    topic_data = result["orders.events"]
    assert topic_data["topic"] == "orders.events"
    assert topic_data["partition_count"] == 1
    assert topic_data["replication_factor"] == 3
    assert topic_data["partitions"] == [
        {"partition_id": 0, "leader": 1, "replicas": [1, 2, 3], "isrs": [1, 2, 3]}
    ]


def test_describe_topics_topic_not_found() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_cluster_meta = MagicMock(spec=ClusterMetadata)
    mock_cluster_meta.topics = {}
    mock_admin.list_topics.return_value = mock_cluster_meta

    result = describe_topics(mock_admin, ["non.existent.topic"])
    assert result == {"non.existent.topic": {"error": "Topic not found"}}


def test_describe_cluster_success() -> None:
    mock_admin = MagicMock(spec=AdminClient)
    mock_cluster_meta = MagicMock(spec=ClusterMetadata)
    mock_cluster_meta.cluster_id = "test-cluster-123"
    mock_cluster_meta.controller_id = 1

    broker1 = MagicMock(spec=BrokerMetadata)
    broker1.id = 1
    broker1.host = "localhost"
    broker1.port = 9092

    broker2 = MagicMock(spec=BrokerMetadata)
    broker2.id = 2
    broker2.host = "localhost"
    broker2.port = 9093

    mock_cluster_meta.brokers = {1: broker1, 2: broker2}
    mock_admin.list_topics.return_value = mock_cluster_meta

    cluster_info = describe_cluster(mock_admin)

    assert cluster_info == {
        "cluster_id": "test-cluster-123",
        "controller_id": 1,
        "broker_count": 2,
        "brokers": [
            {"id": 1, "host": "localhost", "port": 9092},
            {"id": 2, "host": "localhost", "port": 9093},
        ],
    }


def test_cli_create_topics_command(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch("streamforge.shared.kafka.admin.build_admin_client"),
        patch("streamforge.shared.kafka.admin.create_topics") as mock_create,
    ):
        mock_create.return_value = {"orders.events": "created"}
        exit_code = main(["create-topics"])
        assert exit_code == 0
        captured = capsys.readouterr().out
        assert "=== Topic Creation Results ===" in captured
        assert "orders.events: created" in captured


def test_cli_describe_cluster_command(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch("streamforge.shared.kafka.admin.build_admin_client"),
        patch("streamforge.shared.kafka.admin.describe_cluster") as mock_desc,
    ):
        mock_desc.return_value = {
            "cluster_id": "cl-1",
            "controller_id": 2,
            "broker_count": 1,
            "brokers": [{"id": 1, "host": "localhost", "port": 9092}],
        }
        exit_code = main(["describe-cluster"])
        assert exit_code == 0
        captured = capsys.readouterr().out
        assert "=== Kafka Cluster Metadata ===" in captured
        assert "Cluster ID: cl-1" in captured


def test_cli_describe_topics_command(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch("streamforge.shared.kafka.admin.build_admin_client"),
        patch("streamforge.shared.kafka.admin.describe_topics") as mock_desc,
    ):
        mock_desc.return_value = {
            "orders.events": {
                "topic": "orders.events",
                "partition_count": 1,
                "replication_factor": 3,
                "partitions": [
                    {"partition_id": 0, "leader": 1, "replicas": [1, 2, 3], "isrs": [1, 2, 3]}
                ],
            }
        }
        exit_code = main(["describe-topics", "orders.events"])
        assert exit_code == 0
        captured = capsys.readouterr().out
        assert "=== Topic Descriptions ===" in captured
        assert "Topic: orders.events" in captured
        assert "Partition 0: leader=1" in captured
