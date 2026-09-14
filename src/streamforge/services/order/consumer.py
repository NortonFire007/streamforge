"""Order event consumer process.

Consumes OrderCreated events from Kafka topic 'orders.events',
processes them, and performs synchronous manual offset commits.
"""

import json
import logging
import os
import signal
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from streamforge.shared.config.settings import Settings, get_settings
from streamforge.shared.kafka.consumer import build_consumer

logger = logging.getLogger(__name__)

DEFAULT_ORDERS_TOPIC = "orders.events"


class OrderEventConsumer:
    """Standalone Kafka consumer for order events.

    Enforces manual synchronous offset commits after event processing
    and handles group rebalancing and graceful shutdown signals.
    """

    def __init__(
        self,
        consumer: Consumer | None = None,
        settings: Settings | None = None,
        topic: str = DEFAULT_ORDERS_TOPIC,
    ) -> None:
        self._settings = settings or get_settings()
        self._client_id = self._settings.kafka_consumer_client_id
        self._process_id = os.getpid()
        self._topic = topic
        self._consumer = consumer or build_consumer(self._settings)
        self._is_running = False

    @property
    def client_id(self) -> str:
        """Return consumer client identifier."""
        return self._client_id

    @property
    def process_id(self) -> int:
        """Return current process ID."""
        return self._process_id

    @property
    def topic(self) -> str:
        """Return target topic name."""
        return self._topic

    @property
    def consumer(self) -> Consumer:
        """Return underlying Kafka consumer instance."""
        return self._consumer

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance callback triggered when partitions are assigned to this consumer.

        Args:
            consumer: Consumer instance triggering the rebalance.
            partitions: List of assigned TopicPartition instances.
        """
        partition_info = [(tp.topic, tp.partition) for tp in partitions]
        logger.info(
            "Rebalance partitions assigned: client_id=%s, pid=%s, partitions=%s",
            self._client_id,
            self._process_id,
            partition_info,
        )

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance callback triggered when partitions are revoked from this consumer.

        Args:
            consumer: Consumer instance triggering the rebalance.
            partitions: List of revoked TopicPartition instances.
        """
        partition_info = [(tp.topic, tp.partition) for tp in partitions]
        logger.info(
            "Rebalance partitions revoked: client_id=%s, pid=%s, partitions=%s",
            self._client_id,
            self._process_id,
            partition_info,
        )

    def process_message(self, message: Message) -> dict[str, Any] | None:
        """Parse and process an event payload from a consumed Kafka message.

        Args:
            message: Confluent Kafka Message object.

        Returns:
            Parsed event dictionary or None if payload is empty or invalid.
        """
        raw_payload = message.value()
        if raw_payload is None:
            logger.warning(
                "Received empty payload: topic=%s, partition=%s, offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            return None

        try:
            event_data = json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            logger.error(
                "Failed to decode message JSON at topic=%s, partition=%s, offset=%s: %s",
                message.topic(),
                message.partition(),
                message.offset(),
                err,
            )
            return None

        if not isinstance(event_data, dict):
            logger.warning(
                "Message payload is not a JSON object at topic=%s, partition=%s, offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            return None

        event_id = event_data.get("event_id")
        event_type = event_data.get("event_type")
        order_id = event_data.get("order_id")
        customer_id = event_data.get("customer_id")

        logger.info(
            "Consumed event: event_id=%s, event_type=%s, order_id=%s, customer_id=%s, "
            "topic=%s, partition=%s, offset=%s",
            event_id,
            event_type,
            order_id,
            customer_id,
            message.topic(),
            message.partition(),
            message.offset(),
        )
        return event_data

    def start(self, poll_timeout: float = 1.0) -> None:
        """Subscribe to target topic and enter the continuous polling loop.

        Args:
            poll_timeout: Maximum duration in seconds to block per poll() call.
        """
        self._is_running = True
        self._consumer.subscribe(
            [self._topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
        )
        logger.info(
            "Order consumer started: client_id=%s, pid=%s, group_id=%s, topic=%s",
            self._client_id,
            self._process_id,
            self._settings.kafka_consumer_group_id,
            self._topic,
        )

        try:
            while self._is_running:
                message = self._consumer.poll(timeout=poll_timeout)
                if message is None:
                    continue

                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition; harmless informational event
                        continue
                    logger.error("Kafka consumer poll error: %s", error)
                    continue

                processed = self.process_message(message)
                if processed is not None:
                    # Synchronous manual offset commit strictly after successful processing
                    self._consumer.commit(asynchronous=False)
                    logger.info(
                        "Committed offset: topic=%s, partition=%s, offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping consumer...")
        finally:
            self.close()

    def stop(self) -> None:
        """Signal the polling loop to terminate after the current cycle."""
        self._is_running = False

    def close(self) -> None:
        """Close consumer connection and release broker resources cleanly."""
        logger.info(
            "Closing Kafka consumer: client_id=%s, pid=%s",
            self._client_id,
            self._process_id,
        )
        try:
            self._consumer.close()
            logger.info("Kafka consumer closed successfully.")
        except KafkaException as err:
            logger.error("Error during Kafka consumer close: %s", err)


def setup_signal_handlers(consumer_instance: OrderEventConsumer) -> None:
    """Register signal handlers for graceful shutdown on SIGINT and SIGTERM."""

    def handle_shutdown(signum: int, frame: FrameType | None) -> None:
        signame = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info("Received termination signal %s, initiating clean exit...", signame)
        consumer_instance.stop()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)


def main() -> None:
    """Main entry point for running the order consumer as a standalone process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s (pid=%(process)d): %(message)s",
    )
    settings = get_settings()
    consumer = OrderEventConsumer(settings=settings)
    setup_signal_handlers(consumer)
    consumer.start()


if __name__ == "__main__":
    main()
