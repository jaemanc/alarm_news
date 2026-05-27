"""
Unit tests for the scheduler event publisher.

Tests cover:
- Publishing notification events to Kafka via ProducerInterface
- User_id-based partitioning (key = user_id)
- Retry logic delegation to the underlying producer
- Logging and discarding events after retry exhaustion
- Health check delegation
- Close/cleanup behavior
"""
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.scheduler.event_publisher import (
    EventPublisher,
    DEFAULT_TOPIC,
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
)
from src.shared.kafka_producer import InMemoryProducer, ProducerInterface
from src.shared.models import NotificationEvent


# --- Helper Fixtures ---


@pytest.fixture
def sample_event():
    """Create a sample NotificationEvent for testing."""
    return NotificationEvent(
        event_id="evt-test-001",
        user_id="user-abc-123",
        notification_timestamp=datetime(2024, 6, 15, 9, 0, 0),
    )


@pytest.fixture
def in_memory_producer():
    """Create an InMemoryProducer for testing."""
    return InMemoryProducer()


@pytest.fixture
def event_publisher(in_memory_producer):
    """Create an EventPublisher with an InMemoryProducer."""
    return EventPublisher(producer=in_memory_producer)


# --- EventPublisher Tests ---


class TestEventPublisher:
    """Tests for the EventPublisher class."""

    def test_default_topic_is_notification_events(self, in_memory_producer):
        """Verify default topic is 'notification-events'."""
        publisher = EventPublisher(producer=in_memory_producer)
        assert publisher.topic == "notification-events"

    def test_custom_topic(self, in_memory_producer):
        """Verify custom topic can be configured."""
        publisher = EventPublisher(
            producer=in_memory_producer, topic="custom-topic"
        )
        assert publisher.topic == "custom-topic"

    def test_publish_event_returns_true_on_success(
        self, event_publisher, sample_event
    ):
        """Verify publish_event returns True when publish succeeds."""
        result = event_publisher.publish_event(sample_event)
        assert result is True

    def test_publish_event_sends_to_correct_topic(
        self, event_publisher, in_memory_producer, sample_event
    ):
        """Verify event is published to the configured topic."""
        event_publisher.publish_event(sample_event)

        assert len(in_memory_producer.messages) == 1
        assert in_memory_producer.messages[0]["topic"] == DEFAULT_TOPIC

    def test_publish_event_uses_user_id_as_partition_key(
        self, event_publisher, in_memory_producer, sample_event
    ):
        """Verify user_id is used as the Kafka partition key."""
        event_publisher.publish_event(sample_event)

        msg = in_memory_producer.messages[0]
        assert msg["key"] == "user-abc-123"

    def test_publish_event_serializes_event_data(
        self, event_publisher, in_memory_producer, sample_event
    ):
        """Verify event is serialized correctly via to_dict()."""
        event_publisher.publish_event(sample_event)

        msg = in_memory_producer.messages[0]
        assert msg["value"]["event_id"] == "evt-test-001"
        assert msg["value"]["user_id"] == "user-abc-123"
        assert msg["value"]["notification_timestamp"] == "2024-06-15T09:00:00"

    def test_publish_event_returns_false_on_failure(self, sample_event):
        """Verify publish_event returns False when producer fails."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_event.return_value = False

        publisher = EventPublisher(producer=mock_producer)
        result = publisher.publish_event(sample_event)

        assert result is False

    def test_publish_event_calls_producer_with_correct_args(self, sample_event):
        """Verify the producer is called with topic and event."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_event.return_value = True

        publisher = EventPublisher(producer=mock_producer)
        publisher.publish_event(sample_event)

        mock_producer.publish_event.assert_called_once_with(
            DEFAULT_TOPIC, sample_event
        )

    def test_publish_multiple_events(
        self, event_publisher, in_memory_producer
    ):
        """Verify multiple events can be published sequentially."""
        events = [
            NotificationEvent(
                event_id=f"evt-{i}",
                user_id=f"user-{i}",
                notification_timestamp=datetime(2024, 6, 15, 9, i, 0),
            )
            for i in range(5)
        ]

        for event in events:
            result = event_publisher.publish_event(event)
            assert result is True

        assert len(in_memory_producer.messages) == 5
        for i, msg in enumerate(in_memory_producer.messages):
            assert msg["key"] == f"user-{i}"
            assert msg["value"]["event_id"] == f"evt-{i}"

    def test_publish_event_after_close_returns_false(
        self, event_publisher, sample_event
    ):
        """Verify publishing fails after the publisher is closed."""
        event_publisher.close()
        result = event_publisher.publish_event(sample_event)
        assert result is False


class TestEventPublisherLogging:
    """Tests for logging behavior of the EventPublisher."""

    def test_logs_success_on_publish(self, event_publisher, sample_event, caplog):
        """Verify successful publish is logged."""
        with caplog.at_level(logging.INFO):
            event_publisher.publish_event(sample_event)

        assert any(
            "Successfully published event" in record.message
            and "evt-test-001" in record.message
            for record in caplog.records
        )

    def test_logs_error_on_failure(self, sample_event, caplog):
        """Verify failed publish is logged as error."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_event.return_value = False

        publisher = EventPublisher(producer=mock_producer)

        with caplog.at_level(logging.ERROR):
            publisher.publish_event(sample_event)

        assert any(
            "Failed to publish event" in record.message
            and "discarding" in record.message
            for record in caplog.records
        )

    @patch("src.scheduler.event_publisher.time.monotonic")
    def test_logs_warning_when_publish_exceeds_timeout(
        self, mock_monotonic, sample_event, caplog
    ):
        """Verify warning is logged when publish takes > 10 seconds."""
        # Simulate 12 seconds elapsed
        mock_monotonic.side_effect = [0.0, 12.0]

        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.publish_event.return_value = True

        publisher = EventPublisher(producer=mock_producer)

        with caplog.at_level(logging.WARNING):
            publisher.publish_event(sample_event)

        assert any(
            "exceeded" in record.message
            and "10-second" in record.message
            for record in caplog.records
        )


class TestEventPublisherHealthCheck:
    """Tests for health check delegation."""

    def test_health_check_delegates_to_producer(self):
        """Verify health_check calls the producer's health_check."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.health_check.return_value = True

        publisher = EventPublisher(producer=mock_producer)
        assert publisher.health_check() is True
        mock_producer.health_check.assert_called_once()

    def test_health_check_returns_false_when_unhealthy(self):
        """Verify health_check returns False when producer is unhealthy."""
        mock_producer = MagicMock(spec=ProducerInterface)
        mock_producer.health_check.return_value = False

        publisher = EventPublisher(producer=mock_producer)
        assert publisher.health_check() is False

    def test_health_check_with_in_memory_producer(self, in_memory_producer):
        """Verify health_check works with InMemoryProducer."""
        publisher = EventPublisher(producer=in_memory_producer)
        assert publisher.health_check() is True

        in_memory_producer.close()
        assert publisher.health_check() is False


class TestEventPublisherClose:
    """Tests for close/cleanup behavior."""

    def test_close_calls_producer_close(self):
        """Verify close() delegates to the producer."""
        mock_producer = MagicMock(spec=ProducerInterface)
        publisher = EventPublisher(producer=mock_producer)

        publisher.close()

        mock_producer.close.assert_called_once()

    def test_close_with_in_memory_producer(self, in_memory_producer):
        """Verify close works with InMemoryProducer."""
        publisher = EventPublisher(producer=in_memory_producer)
        publisher.close()

        assert in_memory_producer.health_check() is False
