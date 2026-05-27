"""
Event consumer for the Worker component.

This module provides the EventConsumer class that consumes NotificationEvent
messages from the 'notification-events' Kafka topic. It wraps the shared
AlarmNewsKafkaConsumer with worker-specific configuration:

- Topic: 'notification-events'
- Consumer group: 'alarm-news-workers'
- Manual offset commit (enable_auto_commit=False)
- Graceful shutdown on SIGTERM with 60-second grace period
- Stops consuming new messages on shutdown signal
- Completes in-flight notifications within the grace period
"""
import logging
import os
import signal
import threading
import time
from typing import Any, Callable, Dict, Optional

from src.shared.kafka_consumer import (
    AlarmNewsKafkaConsumer,
    ConsumerInterface,
    create_kafka_consumer,
    DEFAULT_SESSION_TIMEOUT_MS,
    DEFAULT_MAX_POLL_INTERVAL_MS,
)
from src.shared.models import NotificationEvent

logger = logging.getLogger(__name__)

# Default configuration for the worker event consumer
DEFAULT_TOPIC = "notification-events"
DEFAULT_GROUP_ID = "alarm-news-workers"
DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS = 60


class EventConsumer:
    """
    Event consumer for the notification worker.

    Consumes NotificationEvent messages from the 'notification-events' Kafka
    topic using a consumer group with manual offset commit. Handles graceful
    shutdown on SIGTERM by stopping new message consumption and allowing
    in-flight notifications to complete within a 60-second grace period.

    Usage:
        consumer = EventConsumer()
        consumer.consume_events(handler_callback)

    The handler callback receives a NotificationEvent and should raise an
    exception if processing fails (offset will not be committed, allowing
    redelivery).
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
        consumer: Optional[ConsumerInterface] = None,
    ) -> None:
        """
        Initialize the event consumer.

        Args:
            bootstrap_servers: Comma-separated Kafka broker addresses.
                If None, reads from KAFKA_BOOTSTRAP_SERVERS or KAFKA_BROKERS env var.
            topic: Kafka topic to consume from. Defaults to 'notification-events'
                or KAFKA_NOTIFICATION_TOPIC env var.
            group_id: Consumer group ID. Defaults to 'alarm-news-workers'
                or KAFKA_CONSUMER_GROUP_WORKER env var.
            shutdown_grace_period_seconds: Time to wait for in-flight messages
                during graceful shutdown. Defaults to 60 seconds.
            consumer: Optional pre-configured ConsumerInterface instance
                (useful for testing with InMemoryConsumer).
        """
        self._topic = topic or os.environ.get(
            "KAFKA_NOTIFICATION_TOPIC", DEFAULT_TOPIC
        )
        self._group_id = group_id or os.environ.get(
            "KAFKA_CONSUMER_GROUP_WORKER", DEFAULT_GROUP_ID
        )
        self._shutdown_grace_period_seconds = shutdown_grace_period_seconds
        self._shutting_down = False
        self._running = False
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()

        if consumer is not None:
            self._consumer = consumer
        else:
            self._consumer = create_kafka_consumer(
                topic=self._topic,
                group_id=self._group_id,
                bootstrap_servers=bootstrap_servers,
                shutdown_grace_period_seconds=shutdown_grace_period_seconds,
            )

    @property
    def is_running(self) -> bool:
        """Whether the consumer is currently running."""
        return self._running

    @property
    def is_shutting_down(self) -> bool:
        """Whether the consumer is in the process of shutting down."""
        return self._shutting_down

    def consume_events(
        self, handler: Callable[[NotificationEvent], None]
    ) -> None:
        """
        Start consuming NotificationEvent messages from Kafka.

        Deserializes each message into a NotificationEvent and passes it
        to the handler callback. Offsets are committed only after the handler
        returns successfully (no exception raised).

        On SIGTERM, stops consuming new messages and completes the current
        in-flight notification within the configured grace period (default 60s).

        Args:
            handler: Callback that processes each NotificationEvent.
                Should raise an exception if processing fails (offset
                will not be committed, allowing redelivery).
        """
        self._running = True
        self._register_signal_handlers()

        logger.info(
            "EventConsumer starting: topic=%s, group_id=%s, "
            "grace_period=%ds",
            self._topic,
            self._group_id,
            self._shutdown_grace_period_seconds,
        )

        def _message_handler(message: Dict[str, Any]) -> None:
            """Deserialize message and invoke the event handler."""
            event = NotificationEvent.from_dict(message)
            logger.debug(
                "Processing event: event_id=%s, user_id=%s",
                event.event_id,
                event.user_id,
            )
            handler(event)
            logger.debug(
                "Event processed successfully: event_id=%s", event.event_id
            )

        try:
            self._consumer.consume(_message_handler)
        except Exception as e:
            logger.error("EventConsumer error: %s", e)
            raise
        finally:
            self._running = False
            logger.info("EventConsumer stopped")

    def shutdown(self) -> None:
        """
        Initiate graceful shutdown.

        Stops consuming new messages and waits for in-flight notifications
        to complete within the configured grace period (default 60 seconds).
        If the grace period expires, forces shutdown.

        This method is safe to call from signal handlers and other threads.
        """
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True

        logger.info(
            "EventConsumer shutdown initiated, grace_period=%ds",
            self._shutdown_grace_period_seconds,
        )

        # Delegate to the underlying consumer's shutdown mechanism
        self._consumer.shutdown()
        self._shutdown_event.set()

    def close(self) -> None:
        """Close the consumer and release all resources."""
        self._running = False
        self._consumer.close()
        logger.info("EventConsumer closed")

    def health_check(self) -> bool:
        """
        Check if the consumer is connected and healthy.

        Returns:
            True if the underlying Kafka consumer is healthy.
        """
        return self._consumer.health_check()

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM and SIGINT handlers for graceful shutdown."""
        try:
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
            logger.debug("Signal handlers registered for SIGTERM and SIGINT")
        except (ValueError, OSError):
            # Signal handlers can only be set in the main thread
            logger.debug(
                "Cannot register signal handlers (not in main thread)"
            )

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        sig_name = signal.Signals(signum).name
        logger.info(
            "EventConsumer received %s, initiating graceful shutdown",
            sig_name,
        )
        self.shutdown()


def create_event_consumer(
    bootstrap_servers: Optional[str] = None,
    shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
) -> EventConsumer:
    """
    Factory function to create an EventConsumer with default configuration.

    Reads configuration from environment variables:
    - KAFKA_BOOTSTRAP_SERVERS / KAFKA_BROKERS: Broker addresses
    - KAFKA_NOTIFICATION_TOPIC: Topic name (default: 'notification-events')
    - KAFKA_CONSUMER_GROUP_WORKER: Group ID (default: 'alarm-news-workers')

    Args:
        bootstrap_servers: Optional broker addresses override.
        shutdown_grace_period_seconds: Grace period for shutdown (default: 60).

    Returns:
        Configured EventConsumer instance.

    Raises:
        NoBrokersAvailable: If connection to Kafka brokers fails.
        ValueError: If no broker addresses are configured.
    """
    return EventConsumer(
        bootstrap_servers=bootstrap_servers,
        shutdown_grace_period_seconds=shutdown_grace_period_seconds,
    )
