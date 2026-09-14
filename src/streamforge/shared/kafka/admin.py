"""Kafka cluster and topic administration module."""

import argparse
import logging
import sys
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from streamforge.shared.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


def build_admin_client(
    settings: Settings | None = None,
    *,
    bootstrap_servers: str | None = None,
) -> AdminClient:
    """Build and return a configured confluent_kafka.admin.AdminClient.

    Args:
        settings: Application settings. If None, loaded via get_settings().
        bootstrap_servers: Optional explicit bootstrap server string overriding settings.

    Returns:
        Configured AdminClient instance.
    """
    if settings is None:
        settings = get_settings()

    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    config: dict[str, Any] = {
        "bootstrap.servers": servers,
        "client.id": f"{settings.kafka_producer_client_id}-admin",
    }
    return AdminClient(config)


def get_default_topics() -> list[NewTopic]:
    """Return the default topic specifications for StreamForge.

    Creates 'orders.events' with 6 partitions, replication factor 3,
    and min.insync.replicas=2.
    """
    return [
        NewTopic(
            topic="orders.events",
            num_partitions=6,
            replication_factor=3,
            config={
                "min.insync.replicas": "2",
                "cleanup.policy": "delete",
                "retention.ms": "604800000",
            },
        )
    ]


def create_topics(
    admin_client: AdminClient,
    topics: list[NewTopic] | None = None,
) -> dict[str, str]:
    """Idempotently create topics in Kafka.

    If a topic already exists, it is logged and treated as a success.

    Args:
        admin_client: Confluent Kafka AdminClient instance.
        topics: List of NewTopic specifications to create. Defaults to get_default_topics().

    Returns:
        A dictionary mapping topic names to their outcome ('created' or 'exists').

    Raises:
        KafkaException: If creation fails with an unrecoverable error.
    """
    if topics is None:
        topics = get_default_topics()

    topic_futures = admin_client.create_topics(topics)
    results: dict[str, str] = {}

    for topic_name, future in topic_futures.items():
        try:
            future.result()
            results[topic_name] = "created"
            logger.info("Topic '%s' successfully created.", topic_name)
        except KafkaException as exc:
            kafka_err = exc.args[0]
            if kafka_err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                results[topic_name] = "exists"
                logger.info("Topic '%s' already exists (idempotent skip).", topic_name)
            else:
                logger.error("Failed to create topic '%s': %s", topic_name, kafka_err)
                raise

    return results


def describe_topics(
    admin_client: AdminClient,
    topic_names: list[str],
) -> dict[str, Any]:
    """Retrieve metadata for specified topic names.

    Args:
        admin_client: Confluent Kafka AdminClient instance.
        topic_names: List of topic names to describe.

    Returns:
        A dictionary mapping topic names to their partition, leader, replica, and ISR metadata.
    """
    cluster_metadata = admin_client.list_topics(timeout=10.0)
    topic_descriptions: dict[str, Any] = {}

    for name in topic_names:
        topic_meta = cluster_metadata.topics.get(name)
        if topic_meta is None:
            topic_descriptions[name] = {"error": "Topic not found"}
            continue

        if topic_meta.error is not None:
            topic_descriptions[name] = {"error": str(topic_meta.error)}
            continue

        partitions_data = []
        replication_factor = 0

        for part_id in sorted(topic_meta.partitions.keys()):
            part = topic_meta.partitions[part_id]
            partitions_data.append(
                {
                    "partition_id": part.id,
                    "leader": part.leader,
                    "replicas": list(part.replicas),
                    "isrs": list(part.isrs),
                }
            )
            if not replication_factor:
                replication_factor = len(part.replicas)

        topic_descriptions[name] = {
            "topic": name,
            "partition_count": len(partitions_data),
            "replication_factor": replication_factor,
            "partitions": partitions_data,
        }

    return topic_descriptions


def describe_cluster(admin_client: AdminClient) -> dict[str, Any]:
    """Retrieve metadata about the connected Kafka cluster.

    Args:
        admin_client: Confluent Kafka AdminClient instance.

    Returns:
        A dictionary containing cluster ID, controller ID, and broker information.
    """
    cluster_metadata = admin_client.list_topics(timeout=10.0)
    brokers_data = [
        {"id": broker.id, "host": broker.host, "port": broker.port}
        for broker in cluster_metadata.brokers.values()
    ]
    brokers_data.sort(key=lambda b: int(b["id"]) if b["id"] is not None else 0)

    return {
        "cluster_id": cluster_metadata.cluster_id,
        "controller_id": cluster_metadata.controller_id,
        "broker_count": len(brokers_data),
        "brokers": brokers_data,
    }


def _build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser for Kafka admin CLI."""
    parser = argparse.ArgumentParser(
        prog="streamforge.kafka.admin",
        description="StreamForge Kafka Cluster Administration CLI",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers connection string (e.g. localhost:9092)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: create-topics
    subparsers.add_parser(
        "create-topics",
        help="Idempotently create default StreamForge topics (e.g. orders.events)",
    )

    # Subcommand: describe-topics
    desc_topics_parser = subparsers.add_parser(
        "describe-topics",
        help="Describe partitions, replication factor, leaders, and ISRs for topics",
    )
    desc_topics_parser.add_argument(
        "topic_names",
        nargs="+",
        help="One or more topic names to describe",
    )

    # Subcommand: describe-cluster
    subparsers.add_parser(
        "describe-cluster",
        help="Display cluster ID, active controller ID, and connected broker list",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute Kafka admin CLI subcommand."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = get_settings()
    admin_client = build_admin_client(settings, bootstrap_servers=args.bootstrap_servers)

    if args.command == "create-topics":
        results = create_topics(admin_client)
        print("=== Topic Creation Results ===")
        for topic, status in results.items():
            print(f" - {topic}: {status}")
        return 0

    if args.command == "describe-topics":
        topic_info = describe_topics(admin_client, args.topic_names)
        print("=== Topic Descriptions ===")
        for name, data in topic_info.items():
            if "error" in data:
                print(f"Topic '{name}': ERROR -> {data['error']}")
                continue
            print(f"Topic: {data['topic']}")
            print(f"  Partitions: {data['partition_count']}")
            print(f"  Replication Factor: {data['replication_factor']}")
            for p in data["partitions"]:
                print(
                    f"  Partition {p['partition_id']}: "
                    f"leader={p['leader']}, replicas={p['replicas']}, isrs={p['isrs']}"
                )
        return 0

    if args.command == "describe-cluster":
        cluster_info = describe_cluster(admin_client)
        print("=== Kafka Cluster Metadata ===")
        print(f"  Cluster ID: {cluster_info['cluster_id']}")
        print(f"  Controller ID: {cluster_info['controller_id']}")
        print(f"  Brokers ({cluster_info['broker_count']}):")
        for b in cluster_info["brokers"]:
            print(f"    Broker ID {b['id']}: {b['host']}:{b['port']}")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
