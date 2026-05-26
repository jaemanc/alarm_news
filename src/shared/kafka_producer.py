"""
Kafka producer for the Alarm News System.

This module provides a reusable Kafka producer with:
- Abstract ProducerInterface for extensibility and testing
- Idempotent production with acks='all' for exactly-once semantics
- User_ID-based partitioning for ordered processing
- Retry logic: 3 attempts with 5-second intervals
- Request timeout of 30 seconds
- Health check for monitoring

Used by both the scheduler (publishing notification events) and
the worker (publishing email notifications).
"""
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from kafka import KafkaProducer as KafkaProducerClient
from kafka.errors import KafkaError, NoBrokersAvailable

from src.shared.models import NotificationEvent

logger = logging.getLogger(__name__)

# Default configuration constants
DEFAULT_RETRIES = 3
DEFAULT_RETRY_INTERVAL_SECONDS = 5
DEFAULT_REQUEST_TIMEOUT_MS = 30000


class ProducerInterface(ABC):
    """
    Abstract interface for Kafka producers.

    Provides extensibility for testing (mock producers) and
    alternative implementations (e.g., in-memory for local dev).
    """

    @abstractmethod
    def publish_event(self, topic: str, event: NotificationEvent) -> bool:
        """
        Publish a notification event to a Kafka topic.

        Args:
            topic: The Kafka topic to publish to.
            event: The NotificationEvent to publish.

        Returns:
            True if the event was published successfully, False otherwise.
        """
        pass

    @abstractmethod
    def publish_message(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        """
        Publish a generic message to a Kafka topic.

        Args:
            topic: The Kafka topic to publish to.
            key: The message key (used for partitioning).
            value: The message value as a dictionary.

        Returns:
            True if the message was published successfully, False otherwise.
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the producer is connected and healthy.

        Returns:
            True if the producer can communicate with Kafka brokers.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the producer and release resources."""
        pass


class AlarmNewsKafkaProducer(ProducerInterface):
    """
    Kafka producer implementation for the Alarm News system.

    Configured with:
    - acks='all': Wait for all in-sync replicas to acknowledge
    - enable_idempotence=True: Prevent duplicate messages
    - Retry logic: 3 attempts with 5-second intervals
    - Request timeout: 30 seconds
    - User_ID-based partitioning for ordered processing
    """

    def __init__(
        self,
        bootstrap_servers: str,
        retries: int = DEFAULT_RETRIES,
        retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
        request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
    ) -> None:
        """
        Initialize the Kafka producer.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses.
            retries: Number of retry attempts for failed publishes.
            retry_interval_seconds: Seconds to wait between retry attempts.
            request_timeout_ms: Request timeout in milliseconds.
        """
        self._bootstrap_servers = bootstrap_servers
        self._retries = retries
        self._retry_interval_seconds = retry_interval_seconds
        self._request_timeout_ms = request_timeout_ms
        self._producer: Optional[KafkaProducerClient] = None

        self._connect()

    def _connect(self) -> None:
        """Create the underlying KafkaProducer client."""
        try:
            self._producer = KafkaProducerClient(
                bootstrap_servers=self._bootstrap_servers.split(","),
                acks="all",
                enable_idempotence=True,
                request_timeout_ms=self._request_timeout_ms,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=self._retries,
                max_in_flight_requests_per_connection=1,
            )
            logger.info(
                "Kafka producer connected to %s", self._bootstrap_servers
            )
        except NoBrokersAvailable as e:
            logger.error(
                "Failed to connect to Kafka brokers at %s: %s",
                self._bootstrap_servers,
                e,
            )
            raise

    def publish_event(self, topic: str, event: NotificationEvent) -> bool:
        """
        Publish a notification event to a Kafka topic with user_id partitioning.

        Uses the event's user_id as the partition key to ensure ordered
        processing per user. Retries up to 3 times with 5-second intervals.

        Args:
            topic: The Kafka topic to publish to.
            event: The NotificationEvent to publish.

        Returns:
            True if the event was published successfully, False otherwise.
        """
        return self.publish_message(
            topic=topic,
            key=event.user_id,
            value=event.to_dict(),
        )

    def publish_message(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        """
        Publish a generic message to a Kafka topic with retry logic.

        Retries up to 3 times with 5-second intervals on failure.
        Uses the key for partition assignment (user_id-based partitioning).

        Args:
            topic: The Kafka topic to publish to.
            key: The message key (used for partitioning).
            value: The message value as a dictionary.

        Returns:
            True if the message was published successfully, False otherwise.
        """
        if self._producer is None:
            logger.error("Kafka producer is not initialized")
            return False

        for attempt in range(1, self._retries + 1):
            try:
                future = self._producer.send(topic, key=key, value=value)
                # Block until the message is acknowledged or timeout
                record_metadata = future.get(
                    timeout=self._request_timeout_ms / 1000
                )
                logger.info(
                    "Published message to topic=%s partition=%d offset=%d key=%s",
                    record_metadata.topic,
                    record_metadata.partition,
                    record_metadata.offset,
                    key,
                )
                return True
            except KafkaError as e:
                logger.warning(
                    "Failed to publish message to topic=%s key=%s "
                    "(attempt %d/%d): %s",
                    topic,
                    key,
                    attempt,
                    self._retries,
                    e,
                )
                if attempt < self._retries:
                    time.sleep(self._retry_interval_seconds)
            except Exception as e:
                logger.error(
                    "Unexpected error publishing to topic=%s key=%s "
                    "(attempt %d/%d): %s",
                    topic,
                    key,
                    attempt,
                    self._retries,
                    e,
                )
                if attempt < self._retries:
                    time.sleep(self._retry_interval_seconds)

        logger.error(
            "All %d retry attempts exhausted for topic=%s key=%s. "
            "Message discarded.",
            self._retries,
            topic,
            key,
        )
        return False

    def health_check(self) -> bool:
        """
        Check if the producer is connected and can communicate with brokers.

        Verifies the producer's internal metadata by requesting broker
        metadata with a 5-second timeout.

        Returns:
            True if the producer can reach Kafka brokers, False otherwise.
        """
        if self._producer is None:
            return False
        try:
            # bootstrap_connected() checks if the client has an active
            # connection to at least one broker
            metadata = self._producer.bootstrap_connected()
            return metadata
        except Exception as e:
            logger.warning("Kafka producer health check failed: %s", e)
            return False

    def close(self) -> None:
        """Close the producer and flush any pending messages."""
        if self._producer is not None:
            try:
                self._producer.flush(timeout=10)
                self._producer.close(timeout=10)
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.warning("Error closing Kafka producer: %s", e)
            finally:
                self._producer = None


class InMemoryProducer(ProducerInterface):
    """
    In-memory producer for testing and local development.

    Stores published messages in a list for inspection in tests.
    """

    def __init__(self) -> None:
        self.messages: list = []
        self._closed = False

    def publish_event(self, topic: str, event: NotificationEvent) -> bool:
        """Store the event in memory."""
        if self._closed:
            return False
        self.messages.append({
            "topic": topic,
            "key": event.user_id,
            "value": event.to_dict(),
        })
        return True

    def publish_message(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        """Store the message in memory."""
        if self._closed:
            return False
        self.messages.append({
            "topic": topic,
            "key": key,
            "value": value,
        })
        return True

    def health_check(self) -> bool:
        """Always healthy for in-memory producer."""
        return not self._closed

    def close(self) -> None:
        """Mark as closed."""
        self._closed = True


def create_kafka_producer(
    bootstrap_servers: Optional[str] = None,
    retries: int = DEFAULT_RETRIES,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
    request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
) -> ProducerInterface:
    """
    Factory function to create a Kafka producer.

    Loads broker addresses from the KAFKA_BROKERS environment variable
    if bootstrap_servers is not provided. Falls back to
    KAFKA_BOOTSTRAP_SERVERS for compatibility with existing config.

    Args:
        bootstrap_servers: Comma-separated broker addresses. If None,
            reads from KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS env var.
        retries: Number of retry attempts (default: 3).
        retry_interval_seconds: Seconds between retries (default: 5).
        request_timeout_ms: Request timeout in ms (default: 30000).

    Returns:
        A configured ProducerInterface instance.

    Raises:
        NoBrokersAvailable: If connection to Kafka brokers fails.
        ValueError: If no broker addresses are configured.
    """
    import os

    if bootstrap_servers is None:
        bootstrap_servers = os.environ.get(
            "KAFKA_BROKERS",
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
        )

    if not bootstrap_servers:
        raise ValueError(
            "Kafka broker addresses not configured. "
            "Set KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS environment variable."
        )

    return AlarmNewsKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        retries=retries,
        retry_interval_seconds=retry_interval_seconds,
        request_timeout_ms=request_timeout_ms,
    )
