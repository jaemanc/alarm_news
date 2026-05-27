"""
Tests for the Email Delivery Handler.

Tests cover:
- Successful email delivery via SMTP (offset committed)
- SMTP connection failure → DLQ storage (offset committed)
- SMTP authentication failure → DLQ storage (offset committed)
- SMTP delivery failure after retries → DLQ storage (offset committed)
- DLQ message structure includes failure reason and correlation ID
- Auto-reconnection when SMTP client is disconnected
- DLQ publish failure is logged but does not raise
"""
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.email_worker.delivery_handler import DeliveryHandler, DEFAULT_DLQ_TOPIC
from src.email_worker.smtp_client import (
    SMTPClient,
    SMTPConnectionError,
    SMTPAuthenticationError,
    SMTPDeliveryError,
)
from src.shared.kafka_producer import InMemoryProducer
from src.shared.models import EmailNotification


def _make_email(
    to_email: str = "user@example.com",
    subject: str = "Alarm News - 2025-01-15",
    body_html: str = "<h1>Your News</h1>",
    timestamp: datetime = None,
) -> EmailNotification:
    """Helper to create an EmailNotification for testing."""
    return EmailNotification(
        to_email=to_email,
        subject=subject,
        body_html=body_html,
        timestamp=timestamp or datetime(2025, 1, 15, 9, 0, 0),
    )


class TestDeliveryHandlerSuccess:
    """Tests for successful email delivery."""

    def test_successful_delivery_does_not_raise(self):
        """Handler returns normally on successful SMTP delivery."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.return_value = True
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = _make_email()

        # Should not raise - allows offset commit
        handler.handle(email)

        smtp_client.send_email.assert_called_once_with(email)
        # Nothing sent to DLQ
        assert len(dlq_producer.messages) == 0

    def test_successful_delivery_does_not_store_in_dlq(self):
        """No DLQ message is produced on successful delivery."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.return_value = True
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        assert len(dlq_producer.messages) == 0


class TestDeliveryHandlerConnectionFailure:
    """Tests for SMTP connection failures."""

    def test_connection_failure_sends_to_dlq(self):
        """When SMTP connection fails after retries, email goes to DLQ."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = False
        smtp_client.connect.side_effect = SMTPConnectionError(
            "Failed to connect to SMTP server after 3 attempts"
        )
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = _make_email()

        # Should NOT raise - DLQ storage allows offset commit
        handler.handle(email)

        assert len(dlq_producer.messages) == 1
        dlq_msg = dlq_producer.messages[0]
        assert dlq_msg["topic"] == DEFAULT_DLQ_TOPIC
        assert dlq_msg["key"] == email.to_email
        assert "Failed to connect" in dlq_msg["value"]["failure_reason"]

    def test_connection_failure_does_not_raise(self):
        """Handler does not raise on connection failure (offset committed)."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = False
        smtp_client.connect.side_effect = SMTPConnectionError("Connection refused")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        # No exception raised
        handler.handle(_make_email())


class TestDeliveryHandlerAuthenticationFailure:
    """Tests for SMTP authentication failures."""

    def test_auth_failure_sends_to_dlq(self):
        """When SMTP authentication fails, email goes to DLQ."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = False
        smtp_client.connect.return_value = True
        smtp_client.authenticate.side_effect = SMTPAuthenticationError(
            "SMTP authentication failed for user test@smtp.com"
        )
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = _make_email()

        handler.handle(email)

        assert len(dlq_producer.messages) == 1
        dlq_msg = dlq_producer.messages[0]
        assert "authentication failed" in dlq_msg["value"]["failure_reason"]

    def test_auth_failure_does_not_raise(self):
        """Handler does not raise on auth failure (offset committed)."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = False
        smtp_client.connect.return_value = True
        smtp_client.authenticate.side_effect = SMTPAuthenticationError("Auth failed")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        handler.handle(_make_email())


class TestDeliveryHandlerDeliveryFailure:
    """Tests for SMTP delivery failures after retries."""

    def test_delivery_failure_sends_to_dlq(self):
        """When SMTP delivery fails after retries, email goes to DLQ."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError(
            "Failed to deliver email to user@example.com after 3 attempts"
        )
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = _make_email()

        handler.handle(email)

        assert len(dlq_producer.messages) == 1
        dlq_msg = dlq_producer.messages[0]
        assert dlq_msg["topic"] == DEFAULT_DLQ_TOPIC
        assert "Failed to deliver" in dlq_msg["value"]["failure_reason"]

    def test_delivery_failure_does_not_raise(self):
        """Handler does not raise on delivery failure (offset committed)."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Delivery failed")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        handler.handle(_make_email())


class TestDeliveryHandlerDLQMessage:
    """Tests for DLQ message structure."""

    def test_dlq_message_contains_email_data(self):
        """DLQ message includes original email fields."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Timeout")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = _make_email(
            to_email="test@example.com",
            subject="Test Subject",
            body_html="<p>Body</p>",
            timestamp=datetime(2025, 1, 15, 10, 30, 0),
        )

        handler.handle(email)

        dlq_value = dlq_producer.messages[0]["value"]
        assert dlq_value["to_email"] == "test@example.com"
        assert dlq_value["subject"] == "Test Subject"
        assert dlq_value["body_html"] == "<p>Body</p>"
        assert dlq_value["timestamp"] == "2025-01-15T10:30:00"

    def test_dlq_message_contains_failure_reason(self):
        """DLQ message includes the failure reason."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError(
            "Network timeout after 3 attempts"
        )
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        dlq_value = dlq_producer.messages[0]["value"]
        assert "Network timeout after 3 attempts" in dlq_value["failure_reason"]

    def test_dlq_message_contains_correlation_id(self):
        """DLQ message includes a correlation ID for tracing."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Error")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        dlq_value = dlq_producer.messages[0]["value"]
        assert "correlation_id" in dlq_value
        assert len(dlq_value["correlation_id"]) > 0

    def test_dlq_message_contains_attempt_count(self):
        """DLQ message includes the number of delivery attempts made."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client._max_retries = 3
        smtp_client.send_email.side_effect = SMTPDeliveryError(
            "Failed to deliver after 3 attempts"
        )
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        dlq_value = dlq_producer.messages[0]["value"]
        assert "attempt_count" in dlq_value
        assert dlq_value["attempt_count"] == 3

    def test_dlq_message_key_is_recipient_email(self):
        """DLQ message key is the recipient email for partitioning."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Error")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email(to_email="recipient@test.com"))

        assert dlq_producer.messages[0]["key"] == "recipient@test.com"

    def test_dlq_uses_configured_topic(self):
        """DLQ messages are published to the configured topic."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Error")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(
            smtp_client=smtp_client,
            dlq_producer=dlq_producer,
            dlq_topic="custom-dlq-topic",
        )
        handler.handle(_make_email())

        assert dlq_producer.messages[0]["topic"] == "custom-dlq-topic"

    def test_dlq_message_handles_none_timestamp(self):
        """DLQ message handles email with no timestamp."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Error")
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        email = EmailNotification(
            to_email="user@example.com",
            subject="Test",
            body_html="<p>Hi</p>",
            timestamp=None,
        )
        handler.handle(email)

        dlq_value = dlq_producer.messages[0]["value"]
        assert dlq_value["timestamp"] is None


class TestDeliveryHandlerReconnection:
    """Tests for SMTP auto-reconnection."""

    def test_reconnects_when_disconnected(self):
        """Handler reconnects to SMTP when client is not connected."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = False
        smtp_client.connect.return_value = True
        smtp_client.authenticate.return_value = True
        smtp_client.send_email.return_value = True
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        smtp_client.connect.assert_called_once()
        smtp_client.authenticate.assert_called_once()
        smtp_client.send_email.assert_called_once()

    def test_does_not_reconnect_when_connected(self):
        """Handler skips reconnection when client is already connected."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.return_value = True
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)
        handler.handle(_make_email())

        smtp_client.connect.assert_not_called()
        smtp_client.authenticate.assert_not_called()


class TestDeliveryHandlerDLQFailure:
    """Tests for when DLQ publishing itself fails."""

    def test_dlq_publish_failure_does_not_raise(self):
        """If DLQ publish fails, handler still does not raise."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Delivery failed")

        dlq_producer = MagicMock()
        dlq_producer.publish_message.return_value = False  # DLQ publish fails

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        # Should NOT raise even when DLQ fails
        handler.handle(_make_email())

    def test_dlq_publish_failure_logs_error(self, caplog):
        """DLQ publish failure is logged as an error."""
        smtp_client = MagicMock(spec=SMTPClient)
        smtp_client.is_connected = True
        smtp_client.send_email.side_effect = SMTPDeliveryError("Delivery failed")

        dlq_producer = MagicMock()
        dlq_producer.publish_message.return_value = False

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        import logging
        with caplog.at_level(logging.ERROR):
            handler.handle(_make_email())

        assert any("Failed to store email in DLQ" in record.message for record in caplog.records)


class TestDeliveryHandlerDefaults:
    """Tests for default configuration."""

    def test_default_dlq_topic(self):
        """Default DLQ topic is 'notification-dlq'."""
        smtp_client = MagicMock(spec=SMTPClient)
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(smtp_client=smtp_client, dlq_producer=dlq_producer)

        assert handler._dlq_topic == "notification-dlq"

    def test_custom_dlq_topic(self):
        """Custom DLQ topic is respected."""
        smtp_client = MagicMock(spec=SMTPClient)
        dlq_producer = InMemoryProducer()

        handler = DeliveryHandler(
            smtp_client=smtp_client,
            dlq_producer=dlq_producer,
            dlq_topic="my-custom-dlq",
        )

        assert handler._dlq_topic == "my-custom-dlq"
