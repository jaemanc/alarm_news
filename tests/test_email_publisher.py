"""
Unit tests for the worker email publisher.

Tests cover:
- Publishing email notifications to Kafka email-delivery topic
- Retry logic: 3 attempts with 5-second intervals
- Dead letter queue storage after retry exhaustion
- DLQ messages include failure reason and attempt count
- Publishing within 10-second target
- Logging failures with correlation IDs
- Health check delegation
- Close/cleanup behavior

Requirements: 9.9, 9.10, 9.11
"""
import logging
import time
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pytest

from src.shared.kafka_producer import InMemoryProducer, ProducerInterface
from src.shared.models import EmailNotification
from src.worker.email_publisher import (
    EmailPublisher,
    DEFAULT_EMAIL_TOPIC,
    DEFAULT_DLQ_TOPIC,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_INTERVAL_SECONDS,
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
)


# --- Helper Fixtures ---


@pytest.fixture
def sample_email():
    """Create a sample EmailNotification for testing."""
    return EmailNotification(
        to_email="user@example.com",
        subject="Alarm News - 2024-06-15 - technology, AI",
        body_html="<html><body><h1>News</h1></body></html>",
        timestamp=datetime(2024, 6, 15, 9, 0, 0),
    )


@pytest.fixture
def in_memory_producer():
    """Create an InMemoryProducer for testing."""
    return InMemoryProducer()


@pytest.fixture
def email_publisher(in_memory_producer):
    """Create an EmailPublisher with an InMemoryProducer."""
    return EmailPublisher(producer=in_memory_producer)


# --- EmailPublisher Basic Tests ---


class TestEmailPublisherConfig:
    """Tests for EmailPublisher configuration."""

    def test_default_topic_is_email_delivery(self, in_memory_producer):
        """Verify default topic is 'email-delivery'."""
        publisher = EmailPublisher(producer=in_memory_producer)
        assert publisher.topic == "email-delivery"

    def test_default_dlq_topic_is_notification_dlq(self, in_memory_producer):
        """Verify default DLQ topic is 'notification-dlq'."""
        publisher = EmailPublisher(producer=in_memory_producer)
        assert publisher.dlq_topic == "notification-dlq"

    def test_custom_topic(self, in_memory_producer):
        """Verify custom topic can be configured."""
        publisher = EmailPublisher(
            producer=in_memory_producer, topic="custom-email-topic"
        )
        assert publisher.topic == "custom-email-topic"

    def test_custom_dlq_topic(self, in_memory_producer):
        """Verify custom DLQ topic can be configured."""
        publisher = EmailPublisher(
            producer=in_memory_producer, dlq_topic="custom-dlq"
        )
        assert publisher.dlq_topic == "custom-dlq"


# --- Publish Email Tests ---


class TestPublishEmail:
    """Tests for the publish_email method."""

    def test_publish_email_returns_true_on_success(
        self, email_publisher, sample_email
    ):
        """Verify publish_email returns True when publish succeeds."""
        result = email_publisher.publish_email(sample_email)
        assert result is True

    def test_publish_email_sends_to_correct_topic(
        self, email_publisher, in_memory_producer, sample_email
    ):
        """Verify email is published to the email-delivery topic."""
        email_publisher.publish_email(sample_email)

        assert len(in_memory_producer.messages) == 1
        assert in_memory_producer.messages[0]["topic"] == DEFAULT_EMAIL_TOPIC

    def test_publish_email_uses_to_email_as_partition_key(
        self, email_publisher, in_memory_producer, sample_email
    ):
        """Verify to_email is used as the Kafka partition key."""
        email_publisher.publish_email(sample_email)

        msg = in_memory_producer.messages[0]
        assert msg["key"] == "user@example.com"

    def test_publish_email_serializes_email_data(
        self, email_publisher, in_memory_producer, sample_email
    ):
        """Verify email is serialized correctly via to_dict()."""
        email_publisher.publish_email(sample_email)

        msg = in_memory_producer.messages[0]
        assert msg["value"]["to_email"] == "user@example.com"
        assert msg["value"]["subject"] == "Alarm News - 2024-06-15 - technology, AI"
        assert msg["value"]["body_html"] == "<html><body><h1>News</h1></body></html>"
        assert msg["value"]["timestamp"] == "2024-06-15T09:00:00"

    def test_publish_email_with_correlation_id(
        self, email_publisher, sample_email, caplog
    ):
        """Verify correlation_id is used in logging."""
        with caplog.at_level(logging.INFO):
            email_publisher.publish_email(
                sample_email, correlation_id="corr-123"
            )

        assert any(
            "corr-123" in record.message for record in caplog.records
        )

    def test_publish_email_generates_correlation_id_if_not_provided(
        self, email_publisher, sample_email, caplog
    ):
        """Verify a correlation_id is generated when not provided."""
        with caplog.at_level(logging.INFO):
            email_publisher.publish_email(sample_email)

        # Should have logged with some correlation_id
        assert any(
            "correlation_id=" in record.message for record in caplog.records
        )

    def test_publish_multiple_emails(
        self, email_publisher, in_memory_producer
    ):
        """Verify multiple emails can be published sequentially."""
        emails = [
            EmailNotification(
                to_email=f"user{i}@example.com",
                subject=f"Subject {i}",
                body_html=f"<html>{i}</html>",
                timestamp=datetime(2024, 6, 15, 9, i, 0),
            )
            for i in range(3)
        ]

        for email in emails:
            result = email_publisher.publish_email(email)
            assert result is True

        assert len(in_memory_producer.messages) == 3
        for i, msg in enumerate(in_memory_producer.messages):
            assert msg["key"] == f"user{i}@example.com"


# --- Retry Logic Tests ---


class TestRetryLogic:
    """Tests for retry behavior on publish failure."""

    def test_retries_up_to_3_times_on_failure(self, sample_email):
        """Verify publish retries 3 times before giving up."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        result = publisher.publish_email(sample_email)

        assert result is False
        # 3 attempts for email-delivery + 1 for DLQ
        assert mock_producer.publish_message.call_count == 4

    def test_succeeds_on_second_attempt(self, sample_email):
        """Verify publish succeeds if second attempt works."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.side_effect = [False, True]

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        result = publisher.publish_email(sample_email)

        assert result is True
        assert mock_producer.publish_message.call_count == 2

    def test_succeeds_on_third_attempt(self, sample_email):
        """Verify publish succeeds if third attempt works."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.side_effect = [False, False, True]

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        result = publisher.publish_email(sample_email)

        assert result is True
        assert mock_producer.publish_message.call_count == 3

    @patch("src.worker.email_publisher.time.sleep")
    def test_waits_5_seconds_between_retries(self, mock_sleep, sample_email):
        """Verify 5-second intervals between retry attempts."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer,
            max_retries=3,
            retry_interval_seconds=5,
        )
        publisher.publish_email(sample_email)

        # Should sleep between attempts 1->2 and 2->3 (not after last attempt)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(5)

    @patch("src.worker.email_publisher.time.sleep")
    def test_no_sleep_after_last_retry(self, mock_sleep, sample_email):
        """Verify no sleep after the final retry attempt."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer,
            max_retries=3,
            retry_interval_seconds=5,
        )
        publisher.publish_email(sample_email)

        # 2 sleeps: between attempt 1->2 and 2->3
        assert mock_sleep.call_count == 2

    @patch("src.worker.email_publisher.time.sleep")
    def test_no_sleep_on_first_success(self, mock_sleep, sample_email):
        """Verify no sleep when first attempt succeeds."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = True

        publisher = EmailPublisher(
            producer=mock_producer,
            max_retries=3,
            retry_interval_seconds=5,
        )
        publisher.publish_email(sample_email)

        mock_sleep.assert_not_called()


# --- Dead Letter Queue Tests ---


class TestDeadLetterQueue:
    """Tests for dead letter queue behavior."""

    def test_sends_to_dlq_after_retry_exhaustion(self, sample_email):
        """Verify email is sent to DLQ after all retries fail."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email, correlation_id="corr-dlq-test")

        # Last call should be to DLQ topic
        dlq_call = mock_producer.publish_message.call_args_list[-1]
        assert dlq_call[1]["topic"] == DEFAULT_DLQ_TOPIC
        assert dlq_call[1]["key"] == "user@example.com"

    def test_dlq_message_includes_failure_reason(self, sample_email):
        """Verify DLQ message includes the failure reason."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email)

        dlq_call = mock_producer.publish_message.call_args_list[-1]
        dlq_value = dlq_call[1]["value"]
        assert "failure_reason" in dlq_value
        assert "failed" in dlq_value["failure_reason"].lower() or "Kafka" in dlq_value["failure_reason"]

    def test_dlq_message_includes_attempt_count(self, sample_email):
        """Verify DLQ message includes the attempt count."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email)

        dlq_call = mock_producer.publish_message.call_args_list[-1]
        dlq_value = dlq_call[1]["value"]
        assert dlq_value["attempt_count"] == 3

    def test_dlq_message_includes_original_email(self, sample_email):
        """Verify DLQ message includes the original email data."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email)

        dlq_call = mock_producer.publish_message.call_args_list[-1]
        dlq_value = dlq_call[1]["value"]
        assert "original_email" in dlq_value
        assert dlq_value["original_email"]["to_email"] == "user@example.com"

    def test_dlq_message_includes_correlation_id(self, sample_email):
        """Verify DLQ message includes the correlation ID."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email, correlation_id="corr-abc-123")

        dlq_call = mock_producer.publish_message.call_args_list[-1]
        dlq_value = dlq_call[1]["value"]
        assert dlq_value["correlation_id"] == "corr-abc-123"

    def test_dlq_message_includes_timestamp(self, sample_email):
        """Verify DLQ message includes a dlq_timestamp."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )
        publisher.publish_email(sample_email)

        dlq_call = mock_producer.publish_message.call_args_list[-1]
        dlq_value = dlq_call[1]["value"]
        assert "dlq_timestamp" in dlq_value
        assert isinstance(dlq_value["dlq_timestamp"], float)

    def test_send_to_dlq_directly(self, in_memory_producer, sample_email):
        """Verify send_to_dlq can be called directly."""
        publisher = EmailPublisher(producer=in_memory_producer)
        result = publisher.send_to_dlq(
            email=sample_email,
            reason="Manual DLQ test",
            attempt_count=2,
            correlation_id="corr-direct",
        )

        assert result is True
        assert len(in_memory_producer.messages) == 1
        msg = in_memory_producer.messages[0]
        assert msg["topic"] == DEFAULT_DLQ_TOPIC
        assert msg["value"]["failure_reason"] == "Manual DLQ test"
        assert msg["value"]["attempt_count"] == 2
        assert msg["value"]["correlation_id"] == "corr-direct"

    def test_send_to_dlq_returns_false_on_failure(self, sample_email):
        """Verify send_to_dlq returns False when DLQ publish fails."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(producer=mock_producer)
        result = publisher.send_to_dlq(
            email=sample_email,
            reason="Test failure",
            attempt_count=3,
        )

        assert result is False


# --- Logging Tests ---


class TestEmailPublisherLogging:
    """Tests for logging behavior of the EmailPublisher."""

    def test_logs_success_on_publish(self, email_publisher, sample_email, caplog):
        """Verify successful publish is logged."""
        with caplog.at_level(logging.INFO):
            email_publisher.publish_email(
                sample_email, correlation_id="corr-log-success"
            )

        assert any(
            "Successfully published email" in record.message
            and "corr-log-success" in record.message
            for record in caplog.records
        )

    def test_logs_error_on_all_retries_exhausted(self, sample_email, caplog):
        """Verify error is logged when all retries are exhausted."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )

        with caplog.at_level(logging.ERROR):
            publisher.publish_email(
                sample_email, correlation_id="corr-log-error"
            )

        assert any(
            "retry attempts exhausted" in record.message
            and "corr-log-error" in record.message
            for record in caplog.records
        )

    def test_logs_warning_on_each_failed_attempt(self, sample_email, caplog):
        """Verify warning is logged for each failed attempt."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )

        with caplog.at_level(logging.WARNING):
            publisher.publish_email(
                sample_email, correlation_id="corr-log-warn"
            )

        warning_records = [
            r for r in caplog.records
            if r.levelno == logging.WARNING
            and "Failed to publish email" in r.message
        ]
        assert len(warning_records) == 3

    def test_logs_correlation_id_on_failure(self, sample_email, caplog):
        """Verify correlation ID is included in failure logs."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = False

        publisher = EmailPublisher(
            producer=mock_producer, max_retries=3, retry_interval_seconds=0
        )

        with caplog.at_level(logging.WARNING):
            publisher.publish_email(
                sample_email, correlation_id="corr-trace-id"
            )

        assert any(
            "corr-trace-id" in record.message for record in caplog.records
        )

    @patch("src.worker.email_publisher.time.monotonic")
    def test_logs_warning_when_publish_exceeds_timeout(
        self, mock_monotonic, sample_email, caplog
    ):
        """Verify warning is logged when publish takes > 10 seconds."""
        mock_monotonic.side_effect = [0.0, 12.0]

        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_message.return_value = True

        publisher = EmailPublisher(producer=mock_producer)

        with caplog.at_level(logging.WARNING):
            publisher.publish_email(sample_email)

        assert any(
            "exceeded" in record.message
            and "10-second" in record.message
            for record in caplog.records
        )


# --- Health Check and Close Tests ---


class TestEmailPublisherHealthCheck:
    """Tests for health check delegation."""

    def test_health_check_delegates_to_producer(self):
        """Verify health_check calls the producer's health_check."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.health_check.return_value = True

        publisher = EmailPublisher(producer=mock_producer)
        assert publisher.health_check() is True
        mock_producer.health_check.assert_called_once()

    def test_health_check_returns_false_when_unhealthy(self):
        """Verify health_check returns False when producer is unhealthy."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.health_check.return_value = False

        publisher = EmailPublisher(producer=mock_producer)
        assert publisher.health_check() is False


class TestEmailPublisherClose:
    """Tests for close/cleanup behavior."""

    def test_close_calls_producer_close(self):
        """Verify close() delegates to the producer."""
        mock_producer = MagicMock(spec=ProducerInterface)
        publisher = EmailPublisher(producer=mock_producer)

        publisher.close()

        mock_producer.close.assert_called_once()

    def test_publish_after_close_returns_false(
        self, email_publisher, sample_email
    ):
        """Verify publishing fails after the publisher is closed."""
        email_publisher.close()
        result = email_publisher.publish_email(sample_email)
        assert result is False
