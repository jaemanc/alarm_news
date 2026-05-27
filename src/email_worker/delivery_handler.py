"""
Email delivery handler for the Email Delivery Worker.

This module provides the DeliveryHandler class that orchestrates email delivery:
1. Receives EmailNotification messages from the EmailConsumer
2. Connects to the SMTP server and sends the email via SMTPClient
3. On successful delivery, returns without exception (offset auto-committed)
4. On failure after all SMTP retries, stores the failed email in the
   dead letter queue (notification-dlq topic) via ProducerInterface
5. After DLQ storage, returns without exception so the offset is committed
   (preventing infinite redelivery)
6. DLQ messages include failure reason and attempt count

Requirements: 10.1, 10.2, 10.6, 10.8, 10.9
"""
import logging
import uuid
from typing import Optional

from src.email_worker.smtp_client import (
    SMTPClient,
    SMTPClientError,
    SMTPConnectionError,
    SMTPAuthenticationError,
    SMTPDeliveryError,
    DEFAULT_MAX_RETRIES,
)
from src.shared.kafka_producer import ProducerInterface
from src.shared.models import EmailNotification

logger = logging.getLogger(__name__)

# Default dead letter queue topic
DEFAULT_DLQ_TOPIC = "notification-dlq"


class DeliveryHandler:
    """
    Orchestrates email delivery for the email delivery worker.

    Uses the SMTPClient to send emails and the ProducerInterface to store
    failed emails in the dead letter queue after retry exhaustion.

    The handler is designed to be passed as a callback to
    EmailConsumer.consume_emails(). It should never raise an exception
    after DLQ storage, ensuring the Kafka offset is always committed
    (preventing infinite redelivery).

    Args:
        smtp_client: Configured SMTPClient instance for sending emails.
        dlq_producer: ProducerInterface for publishing to the dead letter queue.
        dlq_topic: Kafka topic for the dead letter queue.
    """

    def __init__(
        self,
        smtp_client: SMTPClient,
        dlq_producer: ProducerInterface,
        dlq_topic: str = DEFAULT_DLQ_TOPIC,
    ) -> None:
        self._smtp_client = smtp_client
        self._dlq_producer = dlq_producer
        self._dlq_topic = dlq_topic

    def _get_max_retries(self) -> int:
        """Get the max retries from the SMTP client, with a safe default."""
        try:
            return self._smtp_client._max_retries
        except AttributeError:
            return DEFAULT_MAX_RETRIES

    def handle(self, email: EmailNotification) -> None:
        """
        Handle an EmailNotification message.

        Attempts to send the email via SMTP. On success, returns normally
        so the consumer commits the offset. On failure after all retries,
        stores the email in the dead letter queue and returns normally
        (offset still committed to prevent infinite redelivery).

        Args:
            email: The EmailNotification to deliver.
        """
        correlation_id = str(uuid.uuid4())

        try:
            self._ensure_connected(correlation_id)
            self._smtp_client.send_email(email)

            logger.info(
                "Email delivered successfully",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "subject": email.subject,
                },
            )

        except SMTPAuthenticationError as e:
            logger.error(
                "SMTP authentication failed, sending to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "error": str(e),
                },
            )
            self._send_to_dlq(
                email, reason=str(e), correlation_id=correlation_id, attempt_count=1
            )

        except SMTPConnectionError as e:
            logger.error(
                "SMTP connection failed after retries, sending to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "error": str(e),
                },
            )
            self._send_to_dlq(
                email,
                reason=str(e),
                correlation_id=correlation_id,
                attempt_count=self._get_max_retries(),
            )

        except SMTPDeliveryError as e:
            logger.error(
                "Email delivery failed after retries, sending to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "error": str(e),
                },
            )
            self._send_to_dlq(
                email,
                reason=str(e),
                correlation_id=correlation_id,
                attempt_count=self._get_max_retries(),
            )

        except SMTPClientError as e:
            logger.error(
                "Unexpected SMTP error, sending to DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "error": str(e),
                },
            )
            self._send_to_dlq(
                email, reason=str(e), correlation_id=correlation_id, attempt_count=1
            )

    def _ensure_connected(self, correlation_id: str) -> None:
        """
        Ensure the SMTP client is connected and authenticated.

        If not connected, attempts to connect and authenticate.

        Args:
            correlation_id: Correlation ID for logging.

        Raises:
            SMTPConnectionError: If connection fails after retries.
            SMTPAuthenticationError: If authentication fails.
        """
        if not self._smtp_client.is_connected:
            logger.info(
                "SMTP client not connected, establishing connection",
                extra={"correlation_id": correlation_id},
            )
            self._smtp_client.connect()
            self._smtp_client.authenticate()

    def _send_to_dlq(
        self,
        email: EmailNotification,
        reason: str,
        correlation_id: str,
        attempt_count: int = 1,
    ) -> None:
        """
        Store a failed email in the dead letter queue.

        Publishes the email data along with failure metadata to the
        notification-dlq Kafka topic. If DLQ publishing also fails,
        logs the error but does NOT raise (to prevent infinite redelivery).

        Args:
            email: The failed EmailNotification.
            reason: The failure reason string.
            correlation_id: Correlation ID for tracing.
            attempt_count: Number of delivery attempts made before failure.
        """
        dlq_message = {
            "to_email": email.to_email,
            "subject": email.subject,
            "body_html": email.body_html,
            "timestamp": email.timestamp.isoformat() if email.timestamp else None,
            "failure_reason": reason,
            "attempt_count": attempt_count,
            "correlation_id": correlation_id,
        }

        success = self._dlq_producer.publish_message(
            topic=self._dlq_topic,
            key=email.to_email,
            value=dlq_message,
        )

        if success:
            logger.info(
                "Failed email stored in DLQ",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "dlq_topic": self._dlq_topic,
                    "attempt_count": attempt_count,
                },
            )
        else:
            logger.error(
                "Failed to store email in DLQ - message may be lost",
                extra={
                    "correlation_id": correlation_id,
                    "to_email": email.to_email,
                    "dlq_topic": self._dlq_topic,
                    "failure_reason": reason,
                    "attempt_count": attempt_count,
                },
            )
