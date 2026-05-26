"""
Kafka consumer for the Alarm News System.

This module provides a reusable Kafka consumer with:
- Abstract ConsumerInterface for extensibility and testing
- Manual offset commit (enable_auto_commit=False) for at-least-once delivery
- Configurable session timeout and max poll interval
- Graceful shutdown: stop consuming on SIGTERM, complete in-flight messages
- Offset commit only after successful processing

Used by:
- Worker (consuming notification events from 'notification-events' topic)
- Email Delivery Worker (consuming email notifications from 'email-delivery' topic)
"""
import json
import logging
import signal
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from kafka import KafkaConsumer as KafkaConsumerClient
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.consumer.fetcher import ConsumerRecord

logger = logging.getLogger(__name__)

# Default configuration constants
DEFAULT_SESSION_TIMEOUT_MS = 30000
DEFAULT_MAX_POLL_INTERVAL_MS = 300000  # 5 minutes
DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS = 60
DEFAULT_POLL_TIMEOUT_MS = 1000


class ConsumerInterface(ABC):
    """
    Abstract interface for Kafka consumers.

    Provides extensibility for testing (mock consumers) and
    alternative implementations (e.g., in-memory for local dev).
    """

    @abstractmethod
    def consume(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Start consuming messages and pass each to the handler callback.

        The handler is responsible for processing the message. Offsets
        are committed only after the handler returns successfully (no exception).

        Args:
            handler: Callback function that processes each message.
                     Receives the deserialized message value as a dict.
                     Should raise an exception if processing fails.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """
        Initiate graceful shutdown.

        Stops consuming new messages and waits for in-flight messages
        to complete within the configured grace period.
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the consumer is connected and healthy.

        Returns:
            True if the consumer can communicate with Kafka brokers.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the consumer and release resources."""
        pass


class AlarmNewsKafkaConsumer(ConsumerInterface):
    """
    Kafka consumer implementation for the Alarm News system.

    Configured with:
    - enable_auto_commit=False: Manual offset commit for at-least-once delivery
    - session_timeout_ms=30000: Fast rebalancing on consumer failure
    - max_poll_interval_ms=300000: 5 minutes for message processing
    - Graceful shutdown with configurable grace period (default 60s)
    - Offset commit only after successful handler execution

    This consumer is used by both the notification event worker and
    the email delivery worker with different topics and group IDs.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        session_timeout_ms: int = DEFAULT_SESSION_TIMEOUT_MS,
        max_poll_interval_ms: int = DEFAULT_MAX_POLL_INTERVAL_MS,
        shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
    ) -> None:
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses.
            topic: The Kafka topic to consume from.
            group_id: The consumer group ID.
            session_timeout_ms: Session timeout for consumer group membership.
            max_poll_interval_ms: Maximum time between polls before rebalance.
            shutdown_grace_period_seconds: Time to wait for in-flight messages
                during graceful shutdown.
        """
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._session_timeout_ms = session_timeout_ms
        self._max_poll_interval_ms = max_poll_interval_ms
        self._shutdown_grace_period_seconds = shutdown_grace_period_seconds
        self._consumer: Optional[KafkaConsumerClient] = None
        self._running = False
        self._shutting_down = False
        self._lock = threading.Lock()

        self._connect()

    def _connect(self) -> None:
        """Create the underlying KafkaConsumer client."""
        try:
            self._consumer = KafkaConsumerClient(
                self._topic,
                bootstrap_servers=self._bootstrap_servers.split(","),
                group_id=self._group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                session_timeout_ms=self._session_timeout_ms,
                max_poll_interval_ms=self._max_poll_interval_ms,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info(
                "Kafka consumer connected to %s, topic=%s, group_id=%s",
                self._bootstrap_servers,
                self._topic,
                self._group_id,
            )
        except NoBrokersAvailable as e:
            logger.error(
                "Failed to connect to Kafka brokers at %s: %s",
                self._bootstrap_servers,
                e,
            )
            raise

    def consume(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Start consuming messages with manual offset commit.

        Messages are passed to the handler callback one at a time.
        Offsets are committed only after the handler returns successfully.
        If the handler raises an exception, the offset is NOT committed,
        allowing the message to be redelivered.

        On SIGTERM, stops consuming new messages and completes the
        current in-flight message within the grace period.

        Args:
            handler: Callback function that processes each message.
                     Receives the deserialized message value as a dict.
                     Should raise an exception if processing fails.
        """
        self._running = True
        self._register_signal_handlers()

        logger.info(
            "Starting to consume from topic=%s with group_id=%s",
            self._topic,
            self._group_id,
        )

        try:
            while self._running and not self._shutting_down:
                if self._consumer is None:
                    break

                # Poll for messages with a 1-second timeout
                records = self._consumer.poll(timeout_ms=1000)

                for topic_partition, messages in records.items():
                    for message in messages:
                        if self._shutting_down:
                            logger.info(
                                "Shutdown in progress, completing in-flight message"
                            )

                        try:
                            handler(message.value)
                            # Commit offset only after successful processing
                            self._consumer.commit()
                            logger.debug(
                                "Committed offset for topic=%s partition=%d offset=%d",
                                message.topic,
                                message.partition,
                                message.offset,
                            )
                        except Exception as e:
                            logger.error(
                                "Handler failed for message at topic=%s "
                                "partition=%d offset=%d: %s",
                                message.topic,
                                message.partition,
                                message.offset,
                                e,
                            )
                            # Do NOT commit offset - allow redelivery
                            # Break out of inner loop to avoid processing
                            # more messages from this partition
                            break

                        # Check if we should stop after completing this message
                        if self._shutting_down:
                            logger.info(
                                "Shutdown: completed in-flight message, stopping"
                            )
                            self._running = False
                            break

                    if not self._running:
                        break

        except Exception as e:
            logger.error("Consumer loop error: %s", e)
        finally:
            self._running = False
            logger.info("Consumer loop stopped for topic=%s", self._topic)

    def shutdown(self) -> None:
        """
        Initiate graceful shutdown.

        Sets the shutdown flag to stop consuming new messages.
        The consume loop will complete the current in-flight message
        before exiting.
        """
        logger.info(
            "Graceful shutdown initiated for consumer topic=%s, "
            "grace period=%ds",
            self._topic,
            self._shutdown_grace_period_seconds,
        )
        self._shutting_down = True

        # Wait for the consume loop to finish within grace period
        deadline = time.time() + self._shutdown_grace_period_seconds
        while self._running and time.time() < deadline:
            time.sleep(0.1)

        if self._running:
            logger.warning(
                "Consumer did not stop within grace period of %ds, "
                "forcing shutdown",
                self._shutdown_grace_period_seconds,
            )
            self._running = False

    def health_check(self) -> bool:
        """
        Check if the consumer is connected and subscribed.

        Returns:
            True if the consumer has an active subscription.
        """
        if self._consumer is None:
            return False
        try:
            subscription = self._consumer.subscription()
            return subscription is not None and len(subscription) > 0
        except Exception as e:
            logger.warning("Kafka consumer health check failed: %s", e)
            return False

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._running = False
        if self._consumer is not None:
            try:
                self._consumer.close()
                logger.info(
                    "Kafka consumer closed for topic=%s", self._topic
                )
            except Exception as e:
                logger.warning("Error closing Kafka consumer: %s", e)
            finally:
                self._consumer = None

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM handler for graceful shutdown."""
        try:
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
        except (ValueError, OSError):
            # Signal handlers can only be set in the main thread
            logger.debug(
                "Cannot register signal handlers (not in main thread)"
            )

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, initiating graceful shutdown", sig_name)
        self._shutting_down = True


class InMemoryConsumer(ConsumerInterface):
    """
    In-memory consumer for testing and local development.

    Allows tests to push messages and verify handler behavior.
    """

    def __init__(self) -> None:
        self._messages: List[Dict[str, Any]] = []
        self._handler: Optional[Callable[[Dict[str, Any]], None]] = None
        self._running = False
        self._closed = False
        self._committed_count = 0

    def add_message(self, message: Dict[str, Any]) -> None:
        """Add a message to be consumed."""
        self._messages.append(message)

    def consume(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Process all queued messages with the handler.

        For testing, processes all messages synchronously.
        """
        if self._closed:
            return

        self._handler = handler
        self._running = True

        for message in self._messages:
            if not self._running:
                break
            try:
                handler(message)
                self._committed_count += 1
            except Exception as e:
                logger.error("InMemoryConsumer handler error: %s", e)
                # Don't commit, stop processing
                break

        self._running = False
        self._messages.clear()

    def shutdown(self) -> None:
        """Stop consuming."""
        self._running = False

    def health_check(self) -> bool:
        """Always healthy for in-memory consumer."""
        return not self._closed

    def close(self) -> None:
        """Mark as closed."""
        self._closed = True
        self._running = False

    @property
    def committed_count(self) -> int:
        """Number of successfully committed messages."""
        return self._committed_count


def create_kafka_consumer(
    topic: str,
    group_id: str,
    bootstrap_servers: Optional[str] = None,
    session_timeout_ms: int = DEFAULT_SESSION_TIMEOUT_MS,
    max_poll_interval_ms: int = DEFAULT_MAX_POLL_INTERVAL_MS,
    shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
) -> ConsumerInterface:
    """
    Factory function to create a Kafka consumer.

    Loads broker addresses from the KAFKA_BOOTSTRAP_SERVERS environment
    variable if bootstrap_servers is not provided.

    Args:
        topic: The Kafka topic to consume from.
        group_id: The consumer group ID.
        bootstrap_servers: Comma-separated broker addresses. If None,
            reads from KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS env var.
        session_timeout_ms: Session timeout in ms (default: 30000).
        max_poll_interval_ms: Max poll interval in ms (default: 300000).
        shutdown_grace_period_seconds: Grace period for shutdown (default: 60).

    Returns:
        A configured ConsumerInterface instance.

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

    return AlarmNewsKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        session_timeout_ms=session_timeout_ms,
        max_poll_interval_ms=max_poll_interval_ms,
        shutdown_grace_period_seconds=shutdown_grace_period_seconds,
    )


def create_email_delivery_consumer(
    bootstrap_servers: Optional[str] = None,
    shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
) -> ConsumerInterface:
    """
    Factory function to create a Kafka consumer for the email delivery worker.

    Pre-configured with:
    - Topic: 'email-delivery' (from KAFKA_EMAIL_TOPIC env var)
    - Group ID: 'alarm-news-email-workers' (from KAFKA_CONSUMER_GROUP_EMAIL env var)
    - enable_auto_commit=False (manual commit after successful delivery or DLQ)

    Args:
        bootstrap_servers: Comma-separated broker addresses. If None,
            reads from environment variables.
        shutdown_grace_period_seconds: Grace period for shutdown (default: 60).

    Returns:
        A configured ConsumerInterface instance for email delivery.

    Raises:
        NoBrokersAvailable: If connection to Kafka brokers fails.
        ValueError: If no broker addresses are configured.
    """
    import os

    topic = os.environ.get("KAFKA_EMAIL_TOPIC", "email-delivery")
    group_id = os.environ.get(
        "KAFKA_CONSUMER_GROUP_EMAIL", "alarm-news-email-workers"
    )

    return create_kafka_consumer(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        shutdown_grace_period_seconds=shutdown_grace_period_seconds,
    )
