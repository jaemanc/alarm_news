"""
Event Publisher for the Alarm News Scheduler.

Publishes NotificationEvent objects to Kafka for downstream processing
by the worker component. Uses the shared Kafka producer infrastructure
with acks='all', user_id-based partitioning, and retry logic.

Key behaviors:
- Publishes to the 'notification-events' Kafka topic
- Partitions by user_id for ordered processing per user
- Retries failed publishes up to 3 times with 5-second intervals
- Logs and discards events after retry exhaustion
- Targets publishing within 10 seconds of time match
"""
import logging
import time
from typing import Optional

from src.shared.kafka_producer import ProducerInterface
from src.shared.models import NotificationEvent

logger = logging.getLogger(__name__)

# Default configuration constants
DEFAULT_TOPIC = "notification-events"
DEFAULT_PUBLISH_TIMEOUT_SECONDS = 10


class EventPublisher:
    """
    Publishes notification events to Kafka for the scheduler.

    Wraps the shared ProducerInterface to provide a scheduler-specific
    interface for publishing NotificationEvent objects. The underlying
    producer handles acks='all', idempotence, retry logic (3 attempts,
    5-second intervals), and user_id-based partitioning.
    """

    def __init__(
        self,
        producer: ProducerInterface,
        topic: str = DEFAULT_TOPIC,
    ) -> None:
        """
        Initialize the EventPublisher.

        Args:
            producer: A ProducerInterface instance (e.g., AlarmNewsKafkaProducer
                or InMemoryProducer for testing).
            topic: The Kafka topic to publish notification events to.
                Defaults to 'notification-events'.
        """
        self._producer = producer
        self._topic = topic

    @property
    def topic(self) -> str:
        """The Kafka topic this publisher writes to."""
        return self._topic

    def publish_event(self, event: NotificationEvent) -> bool:
        """
        Publish a notification event to Kafka.

        Uses the event's user_id as the partition key to ensure ordered
        processing per user. The underlying producer handles retry logic
        (3 attempts with 5-second intervals) and acks='all'.

        If all retry attempts fail, the event is logged and discarded.

        Args:
            event: The NotificationEvent to publish.

        Returns:
            True if the event was published successfully, False if all
            retries were exhausted and the event was discarded.
        """
        start_time = time.monotonic()

        logger.info(
            "Publishing notification event: event_id=%s user_id=%s "
            "notification_timestamp=%s topic=%s",
            event.event_id,
            event.user_id,
            event.notification_timestamp.isoformat()
            if hasattr(event.notification_timestamp, "isoformat")
            else event.notification_timestamp,
            self._topic,
        )

        success = self._producer.publish_event(self._topic, event)

        elapsed = time.monotonic() - start_time

        if success:
            logger.info(
                "Successfully published event: event_id=%s user_id=%s "
                "elapsed=%.2fs",
                event.event_id,
                event.user_id,
                elapsed,
            )
            if elapsed > DEFAULT_PUBLISH_TIMEOUT_SECONDS:
                logger.warning(
                    "Event publish exceeded %d-second target: event_id=%s "
                    "elapsed=%.2fs",
                    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
                    event.event_id,
                    elapsed,
                )
        else:
            logger.error(
                "Failed to publish event after all retries, discarding: "
                "event_id=%s user_id=%s elapsed=%.2fs",
                event.event_id,
                event.user_id,
                elapsed,
            )

        return success

    def health_check(self) -> bool:
        """
        Check if the underlying producer is healthy.

        Returns:
            True if the producer can communicate with Kafka brokers.
        """
        return self._producer.health_check()

    def close(self) -> None:
        """Close the underlying producer and release resources."""
        logger.info("Closing event publisher for topic=%s", self._topic)
        self._producer.close()
