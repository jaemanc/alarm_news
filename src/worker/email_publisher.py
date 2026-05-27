"""
Email Publisher for the Worker component.

Publishes formatted EmailNotification objects to the Kafka 'email-delivery'
topic for downstream processing by the email delivery worker. If publishing
fails after all retries, the email is stored in the dead letter queue
(notification-dlq topic) with failure reason and attempt count.

Key behaviors:
- Publishes to the 'email-delivery' Kafka topic
- Partitions by to_email for ordered delivery per user
- Retries failed publishes up to 3 times with 5-second intervals
- Stores failed emails in dead letter queue after retry exhaustion
- Includes failure reason and attempt count in DLQ messages
- Publishes within 10 seconds
- Logs failures with correlation IDs
"""
import logging
import time
import uuid
from typing import Optional

from src.shared.kafka_producer import ProducerInterface
from src.shared.models import EmailNotification

logger = logging.getLogger(__name__)

# Default configuration constants
DEFAULT_EMAIL_TOPIC = "email-delivery"
DEFAULT_DLQ_TOPIC = "notification-dlq"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL_SECONDS = 5
DEFAULT_PUBLISH_TIMEOUT_SECONDS = 10


class EmailPublisher:
    """
    Publishes formatted email notifications to Kafka for delivery.

    Wraps the shared ProducerInterface to provide a worker-specific
    interface for publishing EmailNotification objects. Handles retry
    logic and dead letter queue storage for failed publishes.
    """

    def __init__(
        self,
        producer: ProducerInterface,
        topic: str = DEFAULT_EMAIL_TOPIC,
        dlq_topic: str = DEFAULT_DLQ_TOPIC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
    ) -> None:
        """
        Initialize the EmailPublisher.

        Args:
            producer: A ProducerInterface instance (e.g., AlarmNewsKafkaProducer
                or InMemoryProducer for testing).
            topic: The Kafka topic to publish email notifications to.
                Defaults to 'email-delivery'.
            dlq_topic: The Kafka topic for dead letter queue messages.
                Defaults to 'notification-dlq'.
            max_retries: Maximum number of publish attempts.
                Defaults to 3.
            retry_interval_seconds: Seconds to wait between retry attempts.
                Defaults to 5.
        """
        self._producer = producer
        self._topic = topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries
        self._retry_interval_seconds = retry_interval_seconds

    @property
    def topic(self) -> str:
        """The Kafka topic this publisher writes email notifications to."""
        return self._topic

    @property
    def dlq_topic(self) -> str:
        """The Kafka topic for dead letter queue messages."""
        return self._dlq_topic

    def publish_email(
        self, email: EmailNotification, correlation_id: Optional[str] = None
    ) -> bool:
        """
        Publish a formatted email notification to Kafka.

        Retries up to max_retries times with retry_interval_seconds between
        attempts. If all retries fail, the email is stored in the dead letter
        queue with the failure reason and attempt count.

        Args:
            email: The EmailNotification to publish.
            correlation_id: Optional correlation ID for tracing. If not
                provided, a new UUID is generated.

        Returns:
            True if the email was published successfully, False if all
            retries were exhausted (email stored in DLQ).
        """
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        start_time = time.monotonic()
        last_error: Optional[str] = None

        logger.info(
            "Publishing email notification: to_email=%s subject=%s "
            "correlation_id=%s topic=%s",
            email.to_email,
            email.subject,
            correlation_id,
            self._topic,
        )

        for attempt in range(1, self._max_retries + 1):
            success = self._producer.publish_message(
                topic=self._topic,
                key=email.to_email,
                value=email.to_dict(),
            )

            if success:
                elapsed = time.monotonic() - start_time
                logger.info(
                    "Successfully published email: to_email=%s "
                    "correlation_id=%s attempt=%d/%d elapsed=%.2fs",
                    email.to_email,
                    correlation_id,
                    attempt,
                    self._max_retries,
                    elapsed,
                )
                if elapsed > DEFAULT_PUBLISH_TIMEOUT_SECONDS:
                    logger.warning(
                        "Email publish exceeded %d-second target: "
                        "correlation_id=%s elapsed=%.2fs",
                        DEFAULT_PUBLISH_TIMEOUT_SECONDS,
                        correlation_id,
                        elapsed,
                    )
                return True

            last_error = (
                f"Kafka publish failed on attempt {attempt}/{self._max_retries}"
            )
            logger.warning(
                "Failed to publish email (attempt %d/%d): to_email=%s "
                "correlation_id=%s",
                attempt,
                self._max_retries,
                email.to_email,
                correlation_id,
            )

            if attempt < self._max_retries:
                time.sleep(self._retry_interval_seconds)

        # All retries exhausted - send to DLQ
        elapsed = time.monotonic() - start_time
        logger.error(
            "All %d retry attempts exhausted for email publish: "
            "to_email=%s correlation_id=%s elapsed=%.2fs. "
            "Sending to dead letter queue.",
            self._max_retries,
            email.to_email,
            correlation_id,
            elapsed,
        )

        self.send_to_dlq(
            email=email,
            reason=last_error or "Unknown failure",
            attempt_count=self._max_retries,
            correlation_id=correlation_id,
        )

        return False

    def send_to_dlq(
        self,
        email: EmailNotification,
        reason: str,
        attempt_count: int = 0,
        correlation_id: Optional[str] = None,
    ) -> bool:
        """
        Store a failed email in the dead letter queue.

        Includes the failure reason and attempt count in the DLQ message
        for debugging and potential manual retry.

        Args:
            email: The EmailNotification that failed to publish.
            reason: The reason for the failure.
            attempt_count: The number of publish attempts made.
            correlation_id: Optional correlation ID for tracing.

        Returns:
            True if the DLQ message was stored successfully, False otherwise.
        """
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        dlq_message = {
            "original_email": email.to_dict(),
            "failure_reason": reason,
            "attempt_count": attempt_count,
            "correlation_id": correlation_id,
            "dlq_timestamp": time.time(),
        }

        logger.info(
            "Storing failed email in DLQ: to_email=%s reason=%s "
            "attempt_count=%d correlation_id=%s dlq_topic=%s",
            email.to_email,
            reason,
            attempt_count,
            correlation_id,
            self._dlq_topic,
        )

        success = self._producer.publish_message(
            topic=self._dlq_topic,
            key=email.to_email,
            value=dlq_message,
        )

        if success:
            logger.info(
                "Successfully stored email in DLQ: correlation_id=%s",
                correlation_id,
            )
        else:
            logger.error(
                "Failed to store email in DLQ: to_email=%s "
                "correlation_id=%s. Email lost.",
                email.to_email,
                correlation_id,
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
        logger.info(
            "Closing email publisher for topic=%s dlq_topic=%s",
            self._topic,
            self._dlq_topic,
        )
        self._producer.close()
