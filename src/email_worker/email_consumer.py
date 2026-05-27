"""
Email consumer for the Email Delivery Worker component.

This module provides the EmailConsumer class that consumes EmailNotification
messages from the 'email-delivery' Kafka topic. It wraps the shared
AlarmNewsKafkaConsumer with email-worker-specific configuration:

- Topic: 'email-delivery'
- Consumer group: 'alarm-news-email-workers'
- Manual offset commit (enable_auto_commit=False)
- Commit offsets only after successful delivery or DLQ storage
- Graceful shutdown on SIGTERM with 60-second grace period
- Stops consuming new messages on shutdown signal
- Completes in-flight email deliveries within the grace period
"""
import logging
import os
import signal
import threading
import time
from typing import Any, Callable, Dict, Optional

from src.shared.kafka_consumer import (
    ConsumerInterface,
    create_kafka_consumer,
)
from src.shared.models import EmailNotification

logger = logging.getLogger(__name__)

# Default configuration for the email delivery consumer
DEFAULT_TOPIC = "email-delivery"
DEFAULT_GROUP_ID = "alarm-news-email-workers"
DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS = 60


class EmailConsumer:
    """
    Email consumer for the email delivery worker.

    Consumes EmailNotification messages from the 'email-delivery' Kafka
    topic using a consumer group with manual offset commit. Offsets are
    committed only after successful email delivery or DLQ storage.

    Handles graceful shutdown on SIGTERM by stopping new message consumption
    and allowing in-flight email deliveries to complete within a 60-second
    grace period.

    Usage:
        consumer = EmailConsumer()
        consumer.consume_emails(handler_callback)

    The handler callback receives an EmailNotification and should raise an
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
        Initialize the email consumer.

        Args:
            bootstrap_servers: Comma-separated Kafka broker addresses.
                If None, reads from KAFKA_BOOTSTRAP_SERVERS or KAFKA_BROKERS env var.
            topic: Kafka topic to consume from. Defaults to 'email-delivery'
                or KAFKA_EMAIL_TOPIC env var.
            group_id: Consumer group ID. Defaults to 'alarm-news-email-workers'
                or KAFKA_CONSUMER_GROUP_EMAIL env var.
            shutdown_grace_period_seconds: Time to wait for in-flight messages
                during graceful shutdown. Defaults to 60 seconds.
            consumer: Optional pre-configured ConsumerInterface instance
                (useful for testing with InMemoryConsumer).
        """
        self._topic = topic or os.environ.get(
            "KAFKA_EMAIL_TOPIC", DEFAULT_TOPIC
        )
        self._group_id = group_id or os.environ.get(
            "KAFKA_CONSUMER_GROUP_EMAIL", DEFAULT_GROUP_ID
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

    def consume_emails(
        self, handler: Callable[[EmailNotification], None]
    ) -> None:
        """
        Start consuming EmailNotification messages from Kafka.

        Deserializes each message into an EmailNotification and passes it
        to the handler callback. Offsets are committed only after the handler
        returns successfully (no exception raised). This ensures offsets are
        committed only after successful delivery or DLQ storage.

        On SIGTERM, stops consuming new messages and completes the current
        in-flight email delivery within the configured grace period (default 60s).

        Args:
            handler: Callback that processes each EmailNotification.
                Should raise an exception if processing fails (offset
                will not be committed, allowing redelivery).
        """
        self._running = True
        self._register_signal_handlers()

        logger.info(
            "EmailConsumer starting: topic=%s, group_id=%s, "
            "grace_period=%ds",
            self._topic,
            self._group_id,
            self._shutdown_grace_period_seconds,
        )

        def _message_handler(message: Dict[str, Any]) -> None:
            """Deserialize message and invoke the email handler."""
            email = EmailNotification.from_dict(message)
            logger.debug(
                "Processing email notification: to=%s, subject=%s",
                email.to_email,
                email.subject,
            )
            handler(email)
            logger.debug(
                "Email notification processed successfully: to=%s",
                email.to_email,
            )

        try:
            self._consumer.consume(_message_handler)
        except Exception as e:
            logger.error("EmailConsumer error: %s", e)
            raise
        finally:
            self._running = False
            logger.info("EmailConsumer stopped")

    def shutdown(self) -> None:
        """
        Initiate graceful shutdown.

        Stops consuming new messages and waits for in-flight email deliveries
        to complete within the configured grace period (default 60 seconds).
        If the grace period expires, forces shutdown.

        This method is safe to call from signal handlers and other threads.
        """
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True

        logger.info(
            "EmailConsumer shutdown initiated, grace_period=%ds",
            self._shutdown_grace_period_seconds,
        )

        # Delegate to the underlying consumer's shutdown mechanism
        self._consumer.shutdown()
        self._shutdown_event.set()

    def close(self) -> None:
        """Close the consumer and release all resources."""
        self._running = False
        self._consumer.close()
        logger.info("EmailConsumer closed")

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
            "EmailConsumer received %s, initiating graceful shutdown",
            sig_name,
        )
        self.shutdown()


def create_email_consumer(
    bootstrap_servers: Optional[str] = None,
    shutdown_grace_period_seconds: int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
) -> EmailConsumer:
    """
    Factory function to create an EmailConsumer with default configuration.

    Reads configuration from environment variables:
    - KAFKA_BOOTSTRAP_SERVERS / KAFKA_BROKERS: Broker addresses
    - KAFKA_EMAIL_TOPIC: Topic name (default: 'email-delivery')
    - KAFKA_CONSUMER_GROUP_EMAIL: Group ID (default: 'alarm-news-email-workers')

    Args:
        bootstrap_servers: Optional broker addresses override.
        shutdown_grace_period_seconds: Grace period for shutdown (default: 60).

    Returns:
        Configured EmailConsumer instance.

    Raises:
        NoBrokersAvailable: If connection to Kafka brokers fails.
        ValueError: If no broker addresses are configured.
    """
    return EmailConsumer(
        bootstrap_servers=bootstrap_servers,
        shutdown_grace_period_seconds=shutdown_grace_period_seconds,
    )
