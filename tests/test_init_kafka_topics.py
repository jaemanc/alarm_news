"""
Unit tests for the Kafka topic initialization script.

Tests cover:
- Broker address loading from environment variables
- Topic creation logic with idempotency (skip existing topics)
- Handling of already-existing topics
- Topic configuration correctness
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from scripts.init_kafka_topics import (
    TOPICS,
    create_topics,
    get_broker_addresses,
    get_existing_topics,
)


class TestGetBrokerAddresses:
    """Tests for broker address resolution from environment variables."""

    def test_returns_kafka_brokers_env_var(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert get_broker_addresses() == "broker1:9092,broker2:9092"

    def test_falls_back_to_kafka_bootstrap_servers(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BROKERS", raising=False)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fallback:9092")
        assert get_broker_addresses() == "fallback:9092"

    def test_kafka_brokers_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKERS", "primary:9092")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "secondary:9092")
        assert get_broker_addresses() == "primary:9092"

    def test_defaults_to_localhost(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BROKERS", raising=False)
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert get_broker_addresses() == "localhost:9092"


class TestTopicDefinitions:
    """Tests for topic configuration correctness."""

    def test_notification_events_topic_config(self):
        topic = next(t for t in TOPICS if t["name"] == "notification-events")
        assert topic["num_partitions"] == 12
        assert topic["replication_factor"] == 3
        assert topic["topic_configs"]["min.insync.replicas"] == "2"
        # 24 hours in ms
        assert topic["topic_configs"]["retention.ms"] == str(86400000)

    def test_email_delivery_topic_config(self):
        topic = next(t for t in TOPICS if t["name"] == "email-delivery")
        assert topic["num_partitions"] == 12
        assert topic["replication_factor"] == 3
        assert topic["topic_configs"]["min.insync.replicas"] == "2"
        # 24 hours in ms
        assert topic["topic_configs"]["retention.ms"] == str(86400000)

    def test_notification_dlq_topic_config(self):
        topic = next(t for t in TOPICS if t["name"] == "notification-dlq")
        assert topic["num_partitions"] == 3
        assert topic["replication_factor"] == 3
        assert topic["topic_configs"]["min.insync.replicas"] == "2"
        # 7 days in ms
        assert topic["topic_configs"]["retention.ms"] == str(604800000)

    def test_all_three_topics_defined(self):
        topic_names = [t["name"] for t in TOPICS]
        assert "notification-events" in topic_names
        assert "email-delivery" in topic_names
        assert "notification-dlq" in topic_names
        assert len(TOPICS) == 3


class TestGetExistingTopics:
    """Tests for retrieving existing topics from Kafka."""

    def test_returns_set_of_existing_topics(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = [
            "notification-events",
            "other-topic",
        ]
        result = get_existing_topics(mock_client)
        assert result == {"notification-events", "other-topic"}

    def test_returns_empty_set_when_no_topics(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = []
        result = get_existing_topics(mock_client)
        assert result == set()


class TestCreateTopics:
    """Tests for topic creation with idempotency."""

    def test_creates_all_topics_when_none_exist(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = []

        create_topics(mock_client)

        mock_client.create_topics.assert_called_once()
        call_args = mock_client.create_topics.call_args
        new_topics = call_args[1]["new_topics"] if "new_topics" in call_args[1] else call_args[0][0]
        # Should attempt to create all 3 topics
        assert len(new_topics) == 3
        topic_names = [t.name for t in new_topics]
        assert "notification-events" in topic_names
        assert "email-delivery" in topic_names
        assert "notification-dlq" in topic_names

    def test_skips_existing_topics(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = [
            "notification-events",
            "email-delivery",
        ]

        create_topics(mock_client)

        mock_client.create_topics.assert_called_once()
        call_args = mock_client.create_topics.call_args
        new_topics = call_args[1]["new_topics"] if "new_topics" in call_args[1] else call_args[0][0]
        # Should only create the DLQ topic
        assert len(new_topics) == 1
        assert new_topics[0].name == "notification-dlq"

    def test_skips_all_when_all_exist(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = [
            "notification-events",
            "email-delivery",
            "notification-dlq",
        ]

        create_topics(mock_client)

        # Should not attempt to create any topics
        mock_client.create_topics.assert_not_called()

    def test_handles_topic_already_exists_error(self):
        from kafka.errors import TopicAlreadyExistsError

        mock_client = MagicMock()
        mock_client.list_topics.return_value = []
        mock_client.create_topics.side_effect = TopicAlreadyExistsError(
            "Topic already exists"
        )

        # Should not raise — handles the race condition gracefully
        create_topics(mock_client)

    def test_raises_on_unexpected_error(self):
        mock_client = MagicMock()
        mock_client.list_topics.return_value = []
        mock_client.create_topics.side_effect = RuntimeError("Connection lost")

        with pytest.raises(RuntimeError, match="Connection lost"):
            create_topics(mock_client)
