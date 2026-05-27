"""
Tests for the Worker EventConsumer.

Tests cover:
- Consuming NotificationEvent messages from Kafka
- Manual offset commit mode (via InMemoryConsumer)
- Graceful shutdown on SIGTERM
- Stopping consumption on shutdown signal
- Completing in-flight notifications within grace period
- Health check delegation
"""
import signal
import threading
import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.shared.kafka_consumer import InMemoryConsumer
from src.shared.models import NotificationEvent
from src.worker.event_consumer import (
    EventConsumer,
    DEFAULT_TOPIC,
    DEFAULT_GROUP_ID,
    DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
    create_event_consumer,
)


class TestEventConsumerInit:
    """Tests for EventConsumer initialization."""

    def test_init_with_injected_consumer(self):
        """EventConsumer accepts an injected ConsumerInterface."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec._consumer is mock_consumer
        assert ec._topic == DEFAULT_TOPIC
        assert ec._group_id == DEFAULT_GROUP_ID
        assert ec._shutdown_grace_period_seconds == DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS

    def test_init_with_custom_topic_and_group(self):
        """EventConsumer uses custom topic and group_id when provided."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(
            consumer=mock_consumer,
            topic="custom-topic",
            group_id="custom-group",
            shutdown_grace_period_seconds=30,
        )

        assert ec._topic == "custom-topic"
        assert ec._group_id == "custom-group"
        assert ec._shutdown_grace_period_seconds == 30

    @patch.dict(
        "os.environ",
        {
            "KAFKA_NOTIFICATION_TOPIC": "env-topic",
            "KAFKA_CONSUMER_GROUP_WORKER": "env-group",
        },
    )
    def test_init_reads_env_vars(self):
        """EventConsumer reads topic and group from environment variables."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec._topic == "env-topic"
        assert ec._group_id == "env-group"

    def test_init_defaults(self):
        """EventConsumer uses correct defaults."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec._topic == "notification-events"
        assert ec._group_id == "alarm-news-workers"
        assert ec._shutdown_grace_period_seconds == 60


class TestEventConsumerConsumeEvents:
    """Tests for consuming NotificationEvent messages."""

    def test_consume_single_event(self):
        """EventConsumer deserializes and passes NotificationEvent to handler."""
        mock_consumer = InMemoryConsumer()
        event_data = {
            "event_id": "evt-001",
            "user_id": "user-123",
            "notification_timestamp": "2025-01-15T09:00:00",
        }
        mock_consumer.add_message(event_data)

        ec = EventConsumer(consumer=mock_consumer)
        received_events = []

        def handler(event: NotificationEvent) -> None:
            received_events.append(event)

        ec.consume_events(handler)

        assert len(received_events) == 1
        assert received_events[0].event_id == "evt-001"
        assert received_events[0].user_id == "user-123"
        assert received_events[0].notification_timestamp == datetime(
            2025, 1, 15, 9, 0, 0
        )

    def test_consume_multiple_events(self):
        """EventConsumer processes multiple events in order."""
        mock_consumer = InMemoryConsumer()
        for i in range(5):
            mock_consumer.add_message(
                {
                    "event_id": f"evt-{i:03d}",
                    "user_id": f"user-{i}",
                    "notification_timestamp": f"2025-01-15T09:{i:02d}:00",
                }
            )

        ec = EventConsumer(consumer=mock_consumer)
        received_events = []

        def handler(event: NotificationEvent) -> None:
            received_events.append(event)

        ec.consume_events(handler)

        assert len(received_events) == 5
        assert [e.event_id for e in received_events] == [
            "evt-000",
            "evt-001",
            "evt-002",
            "evt-003",
            "evt-004",
        ]

    def test_consume_stops_on_handler_error(self):
        """EventConsumer stops processing when handler raises an exception."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "event_id": "evt-001",
                "user_id": "user-1",
                "notification_timestamp": "2025-01-15T09:00:00",
            }
        )
        mock_consumer.add_message(
            {
                "event_id": "evt-002",
                "user_id": "user-2",
                "notification_timestamp": "2025-01-15T09:01:00",
            }
        )

        ec = EventConsumer(consumer=mock_consumer)
        received_events = []

        def handler(event: NotificationEvent) -> None:
            received_events.append(event)
            if event.event_id == "evt-001":
                raise RuntimeError("Processing failed")

        ec.consume_events(handler)

        # Only the first event was received before the error
        assert len(received_events) == 1
        assert received_events[0].event_id == "evt-001"
        # Offset not committed for failed message
        assert mock_consumer.committed_count == 0

    def test_consume_commits_offset_on_success(self):
        """EventConsumer commits offset after successful handler execution."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "event_id": "evt-001",
                "user_id": "user-1",
                "notification_timestamp": "2025-01-15T09:00:00",
            }
        )

        ec = EventConsumer(consumer=mock_consumer)

        def handler(event: NotificationEvent) -> None:
            pass  # Success

        ec.consume_events(handler)

        assert mock_consumer.committed_count == 1


class TestEventConsumerShutdown:
    """Tests for graceful shutdown behavior."""

    def test_shutdown_sets_flag(self):
        """Shutdown sets the shutting_down flag."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec.is_shutting_down is False
        ec.shutdown()
        assert ec.is_shutting_down is True

    def test_shutdown_is_idempotent(self):
        """Calling shutdown multiple times is safe."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        ec.shutdown()
        ec.shutdown()
        ec.shutdown()

        assert ec.is_shutting_down is True

    def test_shutdown_delegates_to_consumer(self):
        """Shutdown calls the underlying consumer's shutdown method."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EventConsumer(consumer=mock_consumer)

        ec.shutdown()

        mock_consumer.shutdown.assert_called_once()

    def test_close_releases_resources(self):
        """Close stops the consumer and releases resources."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        ec.close()

        assert ec.is_running is False
        assert mock_consumer.health_check() is False  # closed

    def test_shutdown_grace_period_default(self):
        """Default grace period is 60 seconds."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec._shutdown_grace_period_seconds == 60

    def test_shutdown_grace_period_custom(self):
        """Custom grace period is respected."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(
            consumer=mock_consumer, shutdown_grace_period_seconds=30
        )

        assert ec._shutdown_grace_period_seconds == 30


class TestEventConsumerSignalHandling:
    """Tests for SIGTERM/SIGINT signal handling."""

    def test_signal_handler_triggers_shutdown(self):
        """Signal handler calls shutdown."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EventConsumer(consumer=mock_consumer)

        # Simulate signal handler invocation
        ec._signal_handler(signal.SIGTERM, None)

        assert ec.is_shutting_down is True
        mock_consumer.shutdown.assert_called_once()

    def test_signal_handler_sigint(self):
        """SIGINT also triggers shutdown."""
        mock_consumer = MagicMock()
        mock_consumer.health_check.return_value = True
        ec = EventConsumer(consumer=mock_consumer)

        ec._signal_handler(signal.SIGINT, None)

        assert ec.is_shutting_down is True


class TestEventConsumerHealthCheck:
    """Tests for health check delegation."""

    def test_health_check_delegates_to_consumer(self):
        """Health check delegates to the underlying consumer."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)

        assert ec.health_check() is True

    def test_health_check_returns_false_when_closed(self):
        """Health check returns False after consumer is closed."""
        mock_consumer = InMemoryConsumer()
        ec = EventConsumer(consumer=mock_consumer)
        ec.close()

        assert ec.health_check() is False


class TestEventConsumerManualCommit:
    """Tests verifying manual offset commit behavior."""

    def test_no_auto_commit_on_failure(self):
        """Offset is NOT committed when handler raises an exception."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "event_id": "evt-fail",
                "user_id": "user-x",
                "notification_timestamp": "2025-01-15T10:00:00",
            }
        )

        ec = EventConsumer(consumer=mock_consumer)

        def failing_handler(event: NotificationEvent) -> None:
            raise ValueError("Simulated failure")

        ec.consume_events(failing_handler)

        # No offset committed
        assert mock_consumer.committed_count == 0

    def test_commit_only_after_success(self):
        """Offset is committed only after handler succeeds."""
        mock_consumer = InMemoryConsumer()
        mock_consumer.add_message(
            {
                "event_id": "evt-ok-1",
                "user_id": "user-a",
                "notification_timestamp": "2025-01-15T10:00:00",
            }
        )
        mock_consumer.add_message(
            {
                "event_id": "evt-ok-2",
                "user_id": "user-b",
                "notification_timestamp": "2025-01-15T10:01:00",
            }
        )

        ec = EventConsumer(consumer=mock_consumer)

        def handler(event: NotificationEvent) -> None:
            pass  # Success

        ec.consume_events(handler)

        assert mock_consumer.committed_count == 2


class TestCreateEventConsumer:
    """Tests for the factory function."""

    @patch.dict(
        "os.environ",
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_NOTIFICATION_TOPIC": "test-topic",
            "KAFKA_CONSUMER_GROUP_WORKER": "test-group",
        },
    )
    @patch("src.worker.event_consumer.create_kafka_consumer")
    def test_factory_creates_consumer(self, mock_create):
        """Factory function creates EventConsumer with env config."""
        mock_consumer = InMemoryConsumer()
        mock_create.return_value = mock_consumer

        ec = create_event_consumer(
            bootstrap_servers="localhost:9092",
            shutdown_grace_period_seconds=45,
        )

        assert ec._shutdown_grace_period_seconds == 45
        assert isinstance(ec, EventConsumer)
