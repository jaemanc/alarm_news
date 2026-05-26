"""
Unit tests for the Kafka producer module.

Tests cover:
- ProducerInterface contract (InMemoryProducer)
- publish_event with user_id partitioning
- Retry logic: 3 attempts with 5-second intervals
- Health check behavior
- Factory function configuration loading
"""
import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.shared.kafka_producer import (
    AlarmNewsKafkaProducer,
    InMemoryProducer,
    ProducerInterface,
    create_kafka_producer,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_INTERVAL_SECONDS,
    DEFAULT_REQUEST_TIMEOUT_MS,
)
from src.shared.models import NotificationEvent


# --- InMemoryProducer Tests ---


class TestInMemoryProducer:
    """Tests for the InMemoryProducer (test double)."""

    def test_implements_producer_interface(self):
        producer = InMemoryProducer()
        assert isinstance(producer, ProducerInterface)

    def test_publish_event_stores_message(self):
        producer = InMemoryProducer()
        event = NotificationEvent(
            event_id="evt-001",
            user_id="user-123",
            notification_timestamp=datetime(2024, 1, 15, 9, 0, 0),
        )

        result = producer.publish_event("notification-events", event)

        assert result is True
        assert len(producer.messages) == 1
        msg = producer.messages[0]
        assert msg["topic"] == "notification-events"
        assert msg["key"] == "user-123"
        assert msg["value"]["event_id"] == "evt-001"
        assert msg["value"]["user_id"] == "user-123"

    def test_publish_message_stores_generic_message(self):
        producer = InMemoryProducer()
        value = {"to_email": "test@example.com", "subject": "Hello"}

        result = producer.publish_message("email-delivery", "user-456", value)

        assert result is True
        assert len(producer.messages) == 1
        assert producer.messages[0]["key"] == "user-456"
        assert producer.messages[0]["value"] == value

    def test_health_check_returns_true_when_open(self):
        producer = InMemoryProducer()
        assert producer.health_check() is True

    def test_health_check_returns_false_when_closed(self):
        producer = InMemoryProducer()
        producer.close()
        assert producer.health_check() is False

    def test_publish_fails_after_close(self):
        producer = InMemoryProducer()
        producer.close()

        event = NotificationEvent(
            event_id="evt-002",
            user_id="user-789",
            notification_timestamp=datetime(2024, 1, 15, 10, 0, 0),
        )
        result = producer.publish_event("notification-events", event)
        assert result is False
        assert len(producer.messages) == 0

    def test_multiple_messages_stored_in_order(self):
        producer = InMemoryProducer()
        for i in range(5):
            producer.publish_message("test-topic", f"key-{i}", {"index": i})

        assert len(producer.messages) == 5
        for i, msg in enumerate(producer.messages):
            assert msg["key"] == f"key-{i}"
            assert msg["value"]["index"] == i


# --- AlarmNewsKafkaProducer Tests (with mocked KafkaProducer) ---


class TestAlarmNewsKafkaProducer:
    """Tests for the real Kafka producer with mocked kafka-python client."""

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_producer_configured_with_acks_all(self, mock_kafka_class):
        """Verify producer is created with acks='all'."""
        AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")

        mock_kafka_class.assert_called_once()
        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["acks"] == "all"

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_producer_configured_with_idempotence(self, mock_kafka_class):
        """Verify producer has enable_idempotence=True."""
        AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")

        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["enable_idempotence"] is True

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_producer_configured_with_request_timeout(self, mock_kafka_class):
        """Verify producer has request_timeout_ms=30000."""
        AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")

        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["request_timeout_ms"] == 30000

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_producer_configured_with_custom_timeout(self, mock_kafka_class):
        """Verify custom request timeout is passed through."""
        AlarmNewsKafkaProducer(
            bootstrap_servers="localhost:9092",
            request_timeout_ms=60000,
        )

        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["request_timeout_ms"] == 60000

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_publish_event_uses_user_id_as_key(self, mock_kafka_class):
        """Verify publish_event partitions by user_id."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "notification-events"
        mock_metadata.partition = 3
        mock_metadata.offset = 42
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        event = NotificationEvent(
            event_id="evt-100",
            user_id="user-abc",
            notification_timestamp=datetime(2024, 6, 1, 8, 30, 0),
        )

        result = producer.publish_event("notification-events", event)

        assert result is True
        mock_producer.send.assert_called_once_with(
            "notification-events",
            key="user-abc",
            value=event.to_dict(),
        )

    @patch("src.shared.kafka_producer.time.sleep")
    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_retry_logic_3_attempts(self, mock_kafka_class, mock_sleep):
        """Verify retry logic: 3 attempts with 5-second intervals."""
        from kafka.errors import KafkaError

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("Connection failed")
        mock_producer.send.return_value = mock_future
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        event = NotificationEvent(
            event_id="evt-fail",
            user_id="user-retry",
            notification_timestamp=datetime(2024, 6, 1, 9, 0, 0),
        )

        result = producer.publish_event("notification-events", event)

        assert result is False
        assert mock_producer.send.call_count == 3
        # Should sleep between retries (2 sleeps for 3 attempts)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(5)

    @patch("src.shared.kafka_producer.time.sleep")
    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_retry_succeeds_on_second_attempt(self, mock_kafka_class, mock_sleep):
        """Verify message is published if retry succeeds."""
        from kafka.errors import KafkaError

        mock_producer = MagicMock()
        mock_future_fail = MagicMock()
        mock_future_fail.get.side_effect = KafkaError("Temporary failure")

        mock_future_success = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "notification-events"
        mock_metadata.partition = 0
        mock_metadata.offset = 10
        mock_future_success.get.return_value = mock_metadata

        mock_producer.send.side_effect = [mock_future_fail, mock_future_success]
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        event = NotificationEvent(
            event_id="evt-retry-ok",
            user_id="user-retry-ok",
            notification_timestamp=datetime(2024, 6, 1, 10, 0, 0),
        )

        result = producer.publish_event("notification-events", event)

        assert result is True
        assert mock_producer.send.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_health_check_returns_true_when_connected(self, mock_kafka_class):
        """Verify health_check returns True when broker is reachable."""
        mock_producer = MagicMock()
        mock_producer.bootstrap_connected.return_value = True
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        assert producer.health_check() is True

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_health_check_returns_false_when_disconnected(self, mock_kafka_class):
        """Verify health_check returns False when broker is unreachable."""
        mock_producer = MagicMock()
        mock_producer.bootstrap_connected.return_value = False
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        assert producer.health_check() is False

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_health_check_returns_false_after_close(self, mock_kafka_class):
        """Verify health_check returns False after producer is closed."""
        mock_producer = MagicMock()
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        producer.close()
        assert producer.health_check() is False

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_close_flushes_and_closes(self, mock_kafka_class):
        """Verify close() flushes pending messages and closes the client."""
        mock_producer = MagicMock()
        mock_kafka_class.return_value = mock_producer

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        producer.close()

        mock_producer.flush.assert_called_once_with(timeout=10)
        mock_producer.close.assert_called_once_with(timeout=10)

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_publish_returns_false_when_producer_none(self, mock_kafka_class):
        """Verify publish returns False if producer is not initialized."""
        mock_kafka_class.return_value = MagicMock()

        producer = AlarmNewsKafkaProducer(bootstrap_servers="localhost:9092")
        producer._producer = None

        result = producer.publish_message("topic", "key", {"data": "test"})
        assert result is False


# --- Factory Function Tests ---


class TestCreateKafkaProducer:
    """Tests for the create_kafka_producer factory function."""

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_uses_kafka_brokers_env_var(self, mock_kafka_class):
        """Verify factory reads KAFKA_BROKERS env var."""
        mock_kafka_class.return_value = MagicMock()

        with patch.dict(
            os.environ,
            {"KAFKA_BROKERS": "broker1:9092,broker2:9092"},
            clear=False,
        ):
            # Remove KAFKA_BOOTSTRAP_SERVERS if present
            os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
            producer = create_kafka_producer()

        assert isinstance(producer, AlarmNewsKafkaProducer)
        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == [
            "broker1:9092",
            "broker2:9092",
        ]

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_falls_back_to_bootstrap_servers_env(self, mock_kafka_class):
        """Verify factory falls back to KAFKA_BOOTSTRAP_SERVERS."""
        mock_kafka_class.return_value = MagicMock()

        with patch.dict(
            os.environ,
            {"KAFKA_BOOTSTRAP_SERVERS": "fallback:9092"},
            clear=False,
        ):
            os.environ.pop("KAFKA_BROKERS", None)
            producer = create_kafka_producer()

        assert isinstance(producer, AlarmNewsKafkaProducer)
        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == ["fallback:9092"]

    def test_raises_value_error_when_no_brokers_configured(self):
        """Verify factory raises ValueError when no brokers are set."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Kafka broker addresses not configured"):
                create_kafka_producer()

    @patch("src.shared.kafka_producer.KafkaProducerClient")
    def test_explicit_bootstrap_servers_takes_precedence(self, mock_kafka_class):
        """Verify explicit parameter overrides env vars."""
        mock_kafka_class.return_value = MagicMock()

        with patch.dict(
            os.environ,
            {"KAFKA_BROKERS": "env-broker:9092"},
            clear=False,
        ):
            producer = create_kafka_producer(
                bootstrap_servers="explicit-broker:9092"
            )

        call_kwargs = mock_kafka_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == ["explicit-broker:9092"]
