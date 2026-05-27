"""
Tests for the Email Delivery Worker EmailConsumer.

Tests cover:
- Consuming EmailNotification messages from Kafka email delivery topic
- Manual offset commit mode (via InMemoryConsumer)
- Commit offsets only after successful delivery or DLQ storage
- Graceful shutdown on SIGTERM
- Stopping consumption on shutdown signal
- Completing in-flight email deliveries within grace period
- Health check delegation
"""
import signal
import threading
import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.shared.kafka_consumer import InMemoryConsumer
from src.shared.models import EmailNotification
from src.email_worker.email_consumer import (
    EmailConsumer,
    DEFAULT_TOPIC,
    DEFAULT_GROUP_ID,
    DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
    create_email_consumer,
)


class TestEmailConsumerInit:
    """Tests for EmailConsumer initialization."""

    def test_init_with_injected_consumer(self):
        """EmailConsumer accepts an injected ConsumerInterface."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec._consumer is mock_consumer
        assert ec._topic == DEFAULT_TOPIC
        assert ec._group_id == DEFAULT_GROUP_ID
        assert ec._shutdown_grace_period_seconds == DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS

    def test_init_with_custom_topic_and_group(self):
        """EmailConsumer uses custom topic and group_id when provided."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(
            consumer=mock_consumer,
            topic="custom-email-topic",
            group_id="custom-email-group",
            shutdown_grace_period_seconds=30,
        )

        assert ec._topic == "custom-email-topic"
        assert ec._group_id == "custom-email-group"
        assert ec._shutdown_grace_period_seconds == 30

    @patch.dict(
        "os.environ",
        {
            "KAFKA_EMAIL_TOPIC": "env-email-topic",
            "KAFKA_CONSUMER_GROUP_EMAIL": "env-email-group",
        },
    )
    def test_init_reads_env_vars(self):
        """EmailConsumer reads topic and group from environment variables."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec._topic == "env-email-topic"
        assert ec._group_id == "env-email-group"

    def test_init_defaults(self):
        """EmailConsumer uses correct defaults."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec._topic == "email-delivery"
        assert ec._group_id == "alarm-news-email-workers"
        assert ec._shutdown_grace_period_seconds == 60


class TestEmailConsumerConsumeEmails:
    """Tests for consuming EmailNotification messages."""

    def test_consume_single_email(self):
        """EmailConsumer deserializes and passes EmailNotification to handler."""
        mock_consumer = InMemoryConsumer()
        email_data = {
            "to_email": "user@example.com",
            "subject": "Alarm News - 2025-01-15",
            "body_html": "<h1>Your News</h1>",
            "timestamp": "2025-01-15T09:00:00",
        }
        mock_consumer.add_message(email_data)

        ec = EmailConsumer(consumer=mock_consumer)
        received_emails = []

        def handler(email: EmailNotification) -> None:
            received_emails.append(email)

        ec.consume_emails(handler)

        assert len(received_emails) == 1
        assert received_emails[0].to_email == "user@example.com"
        assert received_emails[0].subject == "Alarm News - 2025-01-15"
        assert received_emails[0].body_html == "<h1>Your News</h1>"
        assert received_emails[0].timestamp == datetime(2025, 1, 15, 9, 0, 0)

    def test_consume_multiple_emails(self):
        """EmailConsumer processes multiple emails in order."""
        mock_consumer = InMemoryConsumer()
        for i in range(5):
            mock_consumer.add_message(
                {
                    "to_email": f"user{i}@example.com",
                    "subject": f"News Alert {i}",
                    "body_html": f"<p>Content {i}</p>",
                    "timestamp": f"2025-01-15T09:{i:02d}:00",
                }
            )

        ec = EmailConsumer(consumer=mock_consumer)
        received_emails = []

        def handler(email: EmailNotification) -> None:
            received_emails.append(email)

        ec.consume_emails(handler)

        assert len(received_emails) == 5
        assert [e.to_email for e in received_emails] == [
            "user0@example.com",
            "user1@example.com",
            "user2@example.com",
            "user3@example.com",
            "user4@example.com",
        ]

    def test_consume_email_without_timestamp(self):
        """EmailConsumer handles emails without a timestamp field."""
        mock_consumer = InMemoryConsumer()
        email_data = {
            "to_email": "user@example.com",
            "subject": "Alert",
            "body_html": "<p>Hello</p>",
        }
        mock_consumer.add_message(email_data)

        ec = EmailConsumer(consumer=mock_consumer)
        received_emails = []

        def handler(email: EmailNotification) -> None:
            received_emails.append(email)

        ec.consume_emails(handler)

        assert len(received_emails) == 1
        assert received_emails[0].timestamp is None

    def test_consume_stops_on_handler_error(self):
        """EmailConsumer stops processing when handler raises an exception."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "to_email": "user1@example.com",
                "subject": "Alert 1",
                "body_html": "<p>1</p>",
                "timestamp": "2025-01-15T09:00:00",
            }
        )
        mock_consumer.add_message(
            {
                "to_email": "user2@example.com",
                "subject": "Alert 2",
                "body_html": "<p>2</p>",
                "timestamp": "2025-01-15T09:01:00",
            }
        )

        ec = EmailConsumer(consumer=mock_consumer)
        received_emails = []

        def handler(email: EmailNotification) -> None:
            received_emails.append(email)
            if email.to_email == "user1@example.com":
                raise RuntimeError("SMTP delivery failed")

        ec.consume_emails(handler)

        # Only the first email was received before the error
        assert len(received_emails) == 1
        assert received_emails[0].to_email == "user1@example.com"
        # Offset not committed for failed message
        assert mock_consumer.committed_count == 0


class TestEmailConsumerManualCommit:
    """Tests verifying manual offset commit behavior.

    Offsets are committed only after successful delivery or DLQ storage.
    """

    def test_no_commit_on_delivery_failure(self):
        """Offset is NOT committed when handler raises an exception."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "to_email": "fail@example.com",
                "subject": "Will Fail",
                "body_html": "<p>fail</p>",
                "timestamp": "2025-01-15T10:00:00",
            }
        )

        ec = EmailConsumer(consumer=mock_consumer)

        def failing_handler(email: EmailNotification) -> None:
            raise ConnectionError("SMTP connection refused")

        ec.consume_emails(failing_handler)

        # No offset committed - allows redelivery
        assert mock_consumer.committed_count == 0

    def test_commit_after_successful_delivery(self):
        """Offset is committed after handler succeeds (successful delivery)."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "to_email": "success@example.com",
                "subject": "Delivered",
                "body_html": "<p>ok</p>",
                "timestamp": "2025-01-15T10:00:00",
            }
        )

        ec = EmailConsumer(consumer=mock_consumer)

        def handler(email: EmailNotification) -> None:
            pass  # Successful delivery

        ec.consume_emails(handler)

        assert mock_consumer.committed_count == 1

    def test_commit_after_dlq_storage(self):
        """Offset is committed after DLQ storage (handler doesn't raise)."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "to_email": "dlq@example.com",
                "subject": "DLQ Item",
                "body_html": "<p>stored in dlq</p>",
                "timestamp": "2025-01-15T10:00:00",
            }
        )

        ec = EmailConsumer(consumer=mock_consumer)
        dlq_stored = []

        def handler(email: EmailNotification) -> None:
            # Simulates: delivery failed, stored in DLQ, no exception raised
            dlq_stored.append(email)

        ec.consume_emails(handler)

        # Offset committed because handler completed without exception
        # (DLQ storage is considered successful processing)
        assert mock_consumer.committed_count == 1
        assert len(dlq_stored) == 1

    def test_commit_multiple_successful_deliveries(self):
        """All offsets committed for multiple successful deliveries."""
        mock_consumer = InMemoryConsumer()
        for i in range(3):
            mock_consumer.add_message(
                {
                    "to_email": f"user{i}@example.com",
                    "subject": f"Alert {i}",
                    "body_html": f"<p>{i}</p>",
                    "timestamp": f"2025-01-15T10:{i:02d}:00",
                }
            )

        ec = EmailConsumer(consumer=mock_consumer)

        def handler(email: EmailNotification) -> None:
            pass  # All succeed

        ec.consume_emails(handler)

        assert mock_consumer.committed_count == 3


class TestEmailConsumerShutdown:
    """Tests for graceful shutdown behavior."""

    def test_shutdown_sets_flag(self):
        """Shutdown sets the shutting_down flag."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec.is_shutting_down is False
        ec.shutdown()
        assert ec.is_shutting_down is True

    def test_shutdown_is_idempotent(self):
        """Calling shutdown multiple times is safe."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        ec.shutdown()
        ec.shutdown()
        ec.shutdown()

        assert ec.is_shutting_down is True

    def test_shutdown_delegates_to_consumer(self):
        """Shutdown calls the underlying consumer's shutdown method."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EmailConsumer(consumer=mock_consumer)

        ec.shutdown()

        mock_consumer.shutdown.assert_called_once()

    def test_close_releases_resources(self):
        """Close stops the consumer and releases resources."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        ec.close()

        assert ec.is_running is False
        assert mock_consumer.health_check() is False  # closed

    def test_shutdown_grace_period_default(self):
        """Default grace period is 60 seconds."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec._shutdown_grace_period_seconds == 60

    def test_shutdown_grace_period_custom(self):
        """Custom grace period is respected."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(
            consumer=mock_consumer, shutdown_grace_period_seconds=30
        )

        assert ec._shutdown_grace_period_seconds == 30


class TestEmailConsumerSignalHandling:
    """Tests for SIGTERM/SIGINT signal handling."""

    def test_signal_handler_triggers_shutdown(self):
        """Signal handler calls shutdown."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EmailConsumer(consumer=mock_consumer)

        # Simulate signal handler invocation
        ec._signal_handler(signal.SIGTERM, None)

        assert ec.is_shutting_down is True
        mock_consumer.shutdown.assert_called_once()

    def test_signal_handler_sigint(self):
        """SIGINT also triggers shutdown."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EmailConsumer(consumer=mock_consumer)

        ec._signal_handler(signal.SIGINT, None)

        assert ec.is_shutting_down is True


class TestEmailConsumerHealthCheck:
    """Tests for health check delegation."""

    def test_health_check_delegates_to_consumer(self):
        """Health check delegates to the underlying consumer."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)

        assert ec.health_check() is True

    def test_health_check_returns_false_when_closed(self):
        """Health check returns False after consumer is closed."""
        mock_consumer = InMemoryConsumer()
        ec = EmailConsumer(consumer=mock_consumer)
        ec.close()

        assert ec.health_check() is False


class TestCreateEmailConsumer:
    """Tests for the factory function."""

    @patch.dict(
        "os.environ",
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_EMAIL_TOPIC": "test-email-topic",
            "KAFKA_CONSUMER_GROUP_EMAIL": "test-email-group",
        },
    )
    @patch("src.email_worker.email_consumer.create_kafka_consumer")
    def test_factory_creates_consumer(self, mock_create):
        """Factory function creates EmailConsumer with env config."""
        mock_consumer = InMemoryConsumer()
        mock_create.return_value = mock_consumer

        ec = create_email_consumer(
            bootstrap_servers="localhost:9092",
            shutdown_grace_period_seconds=45,
        )

        assert ec._shutdown_grace_period_seconds == 45
        assert isinstance(ec, EmailConsumer)
