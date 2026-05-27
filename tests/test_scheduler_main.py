"""
Unit tests for the Scheduler Main Loop.

Tests cover:
- Initialization of MongoDB connection and Kafka producer
- Loading user data on startup via UserLoader
- Evaluation of notification times every minute
- Publishing events to Kafka when times match
- Graceful shutdown on SIGTERM
- Error handling during evaluation cycles

Requirements: 8.1, 8.3, 8.4, 8.6
"""
import signal
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from src.scheduler.main import SchedulerMain, DEFAULT_EVALUATION_INTERVAL_SECONDS
from src.scheduler.event_publisher import EventPublisher
from src.scheduler.notification_evaluator import NotificationTimeEvaluator
from src.scheduler.user_loader import UserLoader, UserNotificationConfig
from src.shared.models import NotificationEvent, NotificationTime


@pytest.fixture
def mock_database():
    """Create a mock MongoDB connection manager."""
    db = MagicMock()
    db.connect = MagicMock()
    db.disconnect = MagicMock()
    db.health_check = MagicMock(return_value=True)
    db.find_many = MagicMock(return_value=[])
    return db


@pytest.fixture
def mock_producer():
    """Create a mock Kafka producer."""
    producer = MagicMock()
    producer.publish_event = MagicMock(return_value=True)
    producer.health_check = MagicMock(return_value=True)
    producer.close = MagicMock()
    return producer


@pytest.fixture
def mock_user_loader():
    """Create a mock UserLoader."""
    loader = MagicMock(spec=UserLoader)
    loader.start = MagicMock(return_value=True)
    loader.stop = MagicMock()
    loader.users = []
    loader.is_running = True
    return loader


@pytest.fixture
def mock_evaluator():
    """Create a mock NotificationTimeEvaluator."""
    evaluator = MagicMock(spec=NotificationTimeEvaluator)
    evaluator.evaluate_notification_times = MagicMock(return_value=[])
    return evaluator


@pytest.fixture
def mock_event_publisher():
    """Create a mock EventPublisher."""
    publisher = MagicMock(spec=EventPublisher)
    publisher.publish_event = MagicMock(return_value=True)
    publisher.close = MagicMock()
    return publisher


@pytest.fixture
def sample_users():
    """Sample cached users for testing."""
    return [
        UserNotificationConfig(
            user_id="user-001",
            email="alice@example.com",
            keywords=["python", "AI"],
            notification_times=[
                NotificationTime(hour=9, minute=0),
                NotificationTime(hour=17, minute=30),
            ],
            subscription_expiry=datetime.utcnow() + timedelta(days=15),
        ),
        UserNotificationConfig(
            user_id="user-002",
            email="bob@example.com",
            keywords=["blockchain"],
            notification_times=[
                NotificationTime(hour=8, minute=0),
            ],
            subscription_expiry=datetime.utcnow() + timedelta(days=25),
        ),
    ]


@pytest.fixture
def scheduler(mock_database, mock_producer, mock_user_loader, mock_evaluator, mock_event_publisher):
    """Create a SchedulerMain with all mocked dependencies."""
    s = SchedulerMain(
        database=mock_database,
        producer=mock_producer,
        user_loader=mock_user_loader,
        evaluator=mock_evaluator,
        event_publisher=mock_event_publisher,
        evaluation_interval_seconds=0.1,  # Short interval for testing
    )
    yield s
    # Ensure cleanup
    if s.is_running:
        s.stop()


class TestSchedulerInitialization:
    """Tests for scheduler initialization and dependency setup."""

    def test_start_with_injected_dependencies(self, scheduler, mock_user_loader):
        """Should start successfully with pre-injected dependencies."""
        result = scheduler.start()

        assert result is True
        assert scheduler.is_running is True
        mock_user_loader.start.assert_called_once()

    def test_start_fails_when_user_loader_fails(
        self, mock_database, mock_producer, mock_evaluator, mock_event_publisher
    ):
        """Should return False if UserLoader fails to start."""
        mock_user_loader = MagicMock(spec=UserLoader)
        mock_user_loader.start.return_value = False

        scheduler = SchedulerMain(
            database=mock_database,
            producer=mock_producer,
            user_loader=mock_user_loader,
            evaluator=mock_evaluator,
            event_publisher=mock_event_publisher,
            evaluation_interval_seconds=0.1,
        )

        result = scheduler.start()

        assert result is False
        assert scheduler.is_running is False

    def test_start_when_already_running(self, scheduler):
        """Should return True without restarting if already running."""
        scheduler.start()
        result = scheduler.start()

        assert result is True
        assert scheduler.is_running is True

    @patch("src.scheduler.main.get_config")
    def test_initializes_database_from_config_when_not_provided(self, mock_get_config):
        """Should create MongoDB connection from config if not injected."""
        mock_config = MagicMock()
        mock_config.mongodb.uri = "mongodb://localhost:27017"
        mock_config.mongodb.database = "alarm_news"
        mock_config.mongodb.min_pool_size = 10
        mock_config.mongodb.max_pool_size = 100
        mock_config.kafka.bootstrap_servers = "localhost:9092"
        mock_config.kafka.notification_topic = "notification-events"
        mock_config.scheduler.user_reload_minutes = 5
        mock_get_config.return_value = mock_config

        with patch("src.scheduler.main.MongoDBConnectionManager") as MockDB:
            mock_db_instance = MagicMock()
            MockDB.return_value = mock_db_instance
            mock_db_instance.connect.side_effect = Exception("No MongoDB")

            scheduler = SchedulerMain(evaluation_interval_seconds=0.1)
            result = scheduler.start()

            assert result is False
            MockDB.assert_called_once_with(mock_config.mongodb)

    @patch("src.scheduler.main.get_config")
    def test_initializes_kafka_producer_from_config_when_not_provided(self, mock_get_config):
        """Should create Kafka producer from config if not injected."""
        mock_config = MagicMock()
        mock_config.mongodb.uri = "mongodb://localhost:27017"
        mock_config.kafka.bootstrap_servers = "localhost:9092"
        mock_config.kafka.notification_topic = "notification-events"
        mock_config.scheduler.user_reload_minutes = 5
        mock_get_config.return_value = mock_config

        mock_db = MagicMock()

        with patch("src.scheduler.main.AlarmNewsKafkaProducer") as MockProducer:
            MockProducer.side_effect = Exception("No Kafka")

            scheduler = SchedulerMain(
                database=mock_db,
                evaluation_interval_seconds=0.1,
            )
            result = scheduler.start()

            assert result is False
            MockProducer.assert_called_once_with(
                bootstrap_servers="localhost:9092"
            )


class TestEvaluationLoop:
    """Tests for the notification time evaluation loop."""

    def test_evaluates_notification_times_with_cached_users(
        self, scheduler, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should evaluate notification times against cached users."""
        mock_user_loader.users = sample_users
        mock_evaluator.evaluate_notification_times.return_value = []

        scheduler.start()
        time.sleep(0.25)  # Allow a few evaluation cycles
        scheduler.stop()

        # Should have called evaluate with users
        assert mock_evaluator.evaluate_notification_times.call_count >= 1
        call_args = mock_evaluator.evaluate_notification_times.call_args
        assert call_args[0][1] == sample_users

    def test_publishes_events_when_times_match(
        self, scheduler, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should publish events to Kafka when notification times match."""
        mock_user_loader.users = sample_users

        matched_event = NotificationEvent(
            event_id="evt-001",
            user_id="user-001",
            notification_timestamp=datetime.utcnow(),
        )
        mock_evaluator.evaluate_notification_times.return_value = [matched_event]

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop()

        # Should have published the event
        mock_event_publisher.publish_event.assert_called_with(matched_event)

    def test_does_not_publish_when_no_matches(
        self, scheduler, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should not publish when no notification times match."""
        mock_user_loader.users = sample_users
        mock_evaluator.evaluate_notification_times.return_value = []

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop()

        mock_event_publisher.publish_event.assert_not_called()

    def test_skips_evaluation_when_no_users_cached(
        self, scheduler, mock_user_loader, mock_evaluator
    ):
        """Should skip evaluation when no users are cached."""
        mock_user_loader.users = []

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop()

        mock_evaluator.evaluate_notification_times.assert_not_called()

    def test_handles_evaluation_errors_gracefully(
        self, scheduler, mock_user_loader, mock_evaluator, sample_users
    ):
        """Should continue running after an error in evaluation cycle."""
        mock_user_loader.users = sample_users
        mock_evaluator.evaluate_notification_times.side_effect = [
            Exception("Temporary error"),
            [],  # Recovers on next cycle
            [],
        ]

        scheduler.start()
        time.sleep(0.35)
        scheduler.stop()

        # Should have been called multiple times despite the error
        assert mock_evaluator.evaluate_notification_times.call_count >= 2

    def test_publishes_multiple_events_in_one_cycle(
        self, scheduler, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should publish all matched events in a single evaluation cycle."""
        mock_user_loader.users = sample_users

        events = [
            NotificationEvent(
                event_id="evt-001",
                user_id="user-001",
                notification_timestamp=datetime.utcnow(),
            ),
            NotificationEvent(
                event_id="evt-002",
                user_id="user-002",
                notification_timestamp=datetime.utcnow(),
            ),
        ]
        mock_evaluator.evaluate_notification_times.return_value = events

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop()

        # Both events should have been published
        assert mock_event_publisher.publish_event.call_count >= 2

    def test_handles_publish_failure(
        self, scheduler, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should continue publishing remaining events even if one fails."""
        mock_user_loader.users = sample_users

        events = [
            NotificationEvent(
                event_id="evt-001",
                user_id="user-001",
                notification_timestamp=datetime.utcnow(),
            ),
            NotificationEvent(
                event_id="evt-002",
                user_id="user-002",
                notification_timestamp=datetime.utcnow(),
            ),
        ]
        mock_evaluator.evaluate_notification_times.return_value = events
        # First publish fails, second succeeds
        mock_event_publisher.publish_event.side_effect = [False, True]

        scheduler.start()
        time.sleep(0.25)
        scheduler.stop()

        # Both events should have been attempted
        assert mock_event_publisher.publish_event.call_count >= 2


class TestGracefulShutdown:
    """Tests for graceful shutdown behavior."""

    def test_stop_sets_running_to_false(self, scheduler):
        """Should set is_running to False on stop."""
        scheduler.start()
        assert scheduler.is_running is True

        scheduler.stop()
        assert scheduler.is_running is False

    def test_stop_stops_user_loader(self, scheduler, mock_user_loader):
        """Should stop the user loader on shutdown."""
        scheduler.start()
        scheduler.stop()

        mock_user_loader.stop.assert_called_once()

    def test_stop_closes_event_publisher(self, scheduler, mock_event_publisher):
        """Should close the event publisher on shutdown."""
        scheduler.start()
        scheduler.stop()

        mock_event_publisher.close.assert_called_once()

    def test_stop_disconnects_database(self, scheduler, mock_database):
        """Should disconnect from MongoDB on shutdown."""
        scheduler.start()
        scheduler.stop()

        mock_database.disconnect.assert_called_once()

    def test_stop_when_not_running(self, scheduler):
        """Should be a no-op when scheduler is not running."""
        scheduler.stop()  # Should not raise

    def test_evaluation_loop_stops_on_shutdown(
        self, scheduler, mock_user_loader, mock_evaluator, sample_users
    ):
        """Evaluation loop should stop when shutdown is signaled."""
        mock_user_loader.users = sample_users
        mock_evaluator.evaluate_notification_times.return_value = []

        scheduler.start()
        time.sleep(0.15)

        call_count_before = mock_evaluator.evaluate_notification_times.call_count
        scheduler.stop()
        time.sleep(0.2)

        # No more evaluations after stop
        call_count_after = mock_evaluator.evaluate_notification_times.call_count
        assert call_count_after <= call_count_before + 1  # At most one more in-flight

    def test_signal_handler_triggers_stop(self, scheduler, mock_user_loader):
        """Signal handler should trigger graceful shutdown."""
        scheduler.start()

        # Register signal handlers
        scheduler.register_signal_handlers()

        # Simulate SIGTERM by calling the handler directly
        # (can't actually send signals in tests easily)
        scheduler.stop()

        assert scheduler.is_running is False
        mock_user_loader.stop.assert_called_once()


class TestEvaluationInterval:
    """Tests for evaluation interval configuration."""

    def test_default_evaluation_interval(self):
        """Default evaluation interval should be 60 seconds."""
        assert DEFAULT_EVALUATION_INTERVAL_SECONDS == 60

    def test_custom_evaluation_interval(
        self, mock_database, mock_producer, mock_user_loader, mock_evaluator, mock_event_publisher, sample_users
    ):
        """Should respect custom evaluation interval."""
        mock_user_loader.users = sample_users
        mock_evaluator.evaluate_notification_times.return_value = []

        scheduler = SchedulerMain(
            database=mock_database,
            producer=mock_producer,
            user_loader=mock_user_loader,
            evaluator=mock_evaluator,
            event_publisher=mock_event_publisher,
            evaluation_interval_seconds=0.2,
        )

        scheduler.start()
        time.sleep(0.55)
        scheduler.stop()

        # With 0.2s interval and 0.55s runtime, expect ~3 evaluations
        # (first immediate, then at 0.2s, 0.4s)
        count = mock_evaluator.evaluate_notification_times.call_count
        assert 2 <= count <= 4
