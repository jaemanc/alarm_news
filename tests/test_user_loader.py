"""
Unit tests for the User Loader component.

Tests cover:
- Loading active users from MongoDB (subscription_expiry > now)
- Startup retry logic (10 attempts, 10-second intervals)
- Periodic reload every 5 minutes
- Caching users in memory between reloads
- Graceful degradation when MongoDB unavailable during reload
- Warning logs on reload failures
"""
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from src.scheduler.user_loader import (
    UserLoader,
    UserNotificationConfig,
)
from src.shared.models import NotificationTime


@pytest.fixture
def mock_database():
    """Create a mock database interface."""
    db = MagicMock()
    db.find_many = MagicMock(return_value=[])
    return db


@pytest.fixture
def sample_user_docs():
    """Sample user documents as returned from MongoDB."""
    return [
        {
            "user_id": "user-001",
            "hashed_password": "$2b$12$hash1",
            "email": "alice@example.com",
            "keywords": ["python", "AI"],
            "notification_times": [
                {"hour": 9, "minute": 0},
                {"hour": 17, "minute": 30},
            ],
            "subscription_expiry": datetime.utcnow() + timedelta(days=15),
        },
        {
            "user_id": "user-002",
            "hashed_password": "$2b$12$hash2",
            "email": "bob@example.com",
            "keywords": ["blockchain", "stocks"],
            "notification_times": [
                {"hour": 8, "minute": 0},
            ],
            "subscription_expiry": datetime.utcnow() + timedelta(days=25),
        },
        {
            "user_id": "user-003",
            "hashed_password": "$2b$12$hash3",
            "email": "carol@example.com",
            "keywords": ["technology"],
            "notification_times": [],
            "subscription_expiry": datetime.utcnow() + timedelta(days=5),
        },
    ]


@pytest.fixture
def loader(mock_database):
    """Create a UserLoader with mock database and short intervals for testing."""
    return UserLoader(
        database=mock_database,
        reload_interval_seconds=1,
        startup_retry_interval_seconds=0.01,  # Very short for testing
        startup_max_retries=3,
    )


class TestLoadActiveUsers:
    """Tests for load_active_users method."""

    def test_returns_empty_list_when_no_active_users(self, loader, mock_database):
        """Should return empty list when no users have valid subscriptions."""
        mock_database.find_many.return_value = []

        users = loader.load_active_users()

        assert users == []
        mock_database.find_many.assert_called_once()

    def test_queries_users_with_valid_subscription(self, loader, mock_database):
        """Should query for users with subscription_expiry > now."""
        mock_database.find_many.return_value = []

        loader.load_active_users()

        call_args = mock_database.find_many.call_args
        assert call_args[0][0] == "users"
        query = call_args[0][1]
        assert "subscription_expiry" in query
        assert "$gt" in query["subscription_expiry"]
        # The $gt value should be a datetime close to now
        gt_value = query["subscription_expiry"]["$gt"]
        assert isinstance(gt_value, datetime)

    def test_loads_users_with_all_fields(self, loader, mock_database, sample_user_docs):
        """Should correctly parse all user fields from MongoDB documents."""
        mock_database.find_many.return_value = sample_user_docs

        users = loader.load_active_users()

        assert len(users) == 3

        # Check first user
        alice = users[0]
        assert alice.user_id == "user-001"
        assert alice.email == "alice@example.com"
        assert alice.keywords == ["python", "AI"]
        assert len(alice.notification_times) == 2
        assert alice.notification_times[0].hour == 9
        assert alice.notification_times[0].minute == 0
        assert alice.notification_times[1].hour == 17
        assert alice.notification_times[1].minute == 30
        assert alice.subscription_expiry is not None

    def test_handles_user_with_no_notification_times(self, loader, mock_database):
        """Should handle users with empty notification_times."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-001",
                "email": "test@example.com",
                "keywords": ["test"],
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        users = loader.load_active_users()

        assert len(users) == 1
        assert users[0].notification_times == []

    def test_handles_user_with_no_keywords(self, loader, mock_database):
        """Should handle users with no keywords field."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-001",
                "email": "test@example.com",
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        users = loader.load_active_users()

        assert len(users) == 1
        assert users[0].keywords == []

    def test_skips_invalid_user_documents(self, loader, mock_database):
        """Should skip documents missing required fields and log a warning."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-001",
                "email": "valid@example.com",
                "keywords": ["test"],
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
            {
                # Missing user_id and email - should be skipped
                "keywords": ["invalid"],
            },
        ]

        users = loader.load_active_users()

        assert len(users) == 1
        assert users[0].user_id == "user-001"

    def test_raises_on_database_error(self, loader, mock_database):
        """Should propagate database exceptions."""
        mock_database.find_many.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            loader.load_active_users()


class TestInitialLoad:
    """Tests for initial_load method with retry logic."""

    def test_successful_first_attempt(self, loader, mock_database, sample_user_docs):
        """Should load users on first attempt and return True."""
        mock_database.find_many.return_value = sample_user_docs

        result = loader.initial_load()

        assert result is True
        assert len(loader.users) == 3
        assert mock_database.find_many.call_count == 1

    def test_retries_on_failure_then_succeeds(self, loader, mock_database, sample_user_docs):
        """Should retry on failure and succeed on subsequent attempt."""
        mock_database.find_many.side_effect = [
            Exception("Connection timeout"),
            Exception("Connection timeout"),
            sample_user_docs,  # Third attempt succeeds
        ]

        result = loader.initial_load()

        assert result is True
        assert len(loader.users) == 3
        assert mock_database.find_many.call_count == 3

    def test_fails_after_max_retries(self, loader, mock_database):
        """Should return False after exhausting all retry attempts."""
        mock_database.find_many.side_effect = Exception("Connection refused")

        result = loader.initial_load()

        assert result is False
        assert len(loader.users) == 0
        assert mock_database.find_many.call_count == 3  # startup_max_retries=3

    def test_sets_last_reload_time_on_success(self, loader, mock_database, sample_user_docs):
        """Should set last_reload_time on successful initial load."""
        mock_database.find_many.return_value = sample_user_docs

        before = datetime.utcnow()
        loader.initial_load()
        after = datetime.utcnow()

        assert loader.last_reload_time is not None
        assert before <= loader.last_reload_time <= after

    def test_retry_interval_is_respected(self, mock_database):
        """Should wait the configured interval between retries."""
        mock_database.find_many.side_effect = Exception("Connection refused")

        loader = UserLoader(
            database=mock_database,
            startup_retry_interval_seconds=0.05,
            startup_max_retries=3,
        )

        start = time.time()
        loader.initial_load()
        elapsed = time.time() - start

        # Should have waited at least 2 intervals (between attempts 1-2 and 2-3)
        assert elapsed >= 0.09


class TestReloadUsers:
    """Tests for reload_users method."""

    def test_updates_cached_users_on_success(self, loader, mock_database, sample_user_docs):
        """Should update cached users when reload succeeds."""
        # Initial state: empty
        assert len(loader.users) == 0

        mock_database.find_many.return_value = sample_user_docs

        loader.reload_users()

        assert len(loader.users) == 3

    def test_keeps_cached_data_on_failure(self, loader, mock_database, sample_user_docs):
        """Should keep previously cached data when reload fails."""
        # First load succeeds
        mock_database.find_many.return_value = sample_user_docs
        loader.reload_users()
        assert len(loader.users) == 3

        # Second load fails
        mock_database.find_many.side_effect = Exception("Connection lost")
        loader.reload_users()

        # Should still have the cached users
        assert len(loader.users) == 3

    def test_logs_warning_on_failure(self, loader, mock_database, caplog):
        """Should log a warning when reload fails."""
        mock_database.find_many.side_effect = Exception("Network error")

        import logging
        with caplog.at_level(logging.WARNING):
            loader.reload_users()

        assert any("MongoDB reload failed" in record.message for record in caplog.records)

    def test_updates_last_reload_time_on_success(self, loader, mock_database, sample_user_docs):
        """Should update last_reload_time on successful reload."""
        mock_database.find_many.return_value = sample_user_docs

        before = datetime.utcnow()
        loader.reload_users()
        after = datetime.utcnow()

        assert loader.last_reload_time is not None
        assert before <= loader.last_reload_time <= after

    def test_does_not_update_last_reload_time_on_failure(self, loader, mock_database):
        """Should not update last_reload_time when reload fails."""
        mock_database.find_many.side_effect = Exception("Connection lost")

        loader.reload_users()

        assert loader.last_reload_time is None

    def test_replaces_old_users_with_new_data(self, loader, mock_database):
        """Should completely replace cached users on successful reload."""
        # First load: 2 users
        mock_database.find_many.return_value = [
            {
                "user_id": "user-001",
                "email": "a@example.com",
                "keywords": ["test"],
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
            {
                "user_id": "user-002",
                "email": "b@example.com",
                "keywords": ["test"],
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
        ]
        loader.reload_users()
        assert len(loader.users) == 2

        # Second load: 1 user (one expired)
        mock_database.find_many.return_value = [
            {
                "user_id": "user-001",
                "email": "a@example.com",
                "keywords": ["test"],
                "notification_times": [],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
        ]
        loader.reload_users()
        assert len(loader.users) == 1


class TestStartStop:
    """Tests for start/stop lifecycle and periodic reload."""

    def test_start_performs_initial_load(self, mock_database, sample_user_docs):
        """Starting should perform initial load and cache users."""
        mock_database.find_many.return_value = sample_user_docs

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=60,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        result = loader.start()

        assert result is True
        assert loader.is_running is True
        assert len(loader.users) == 3
        loader.stop()

    def test_start_returns_false_on_initial_load_failure(self, mock_database):
        """Should return False and not start if initial load fails."""
        mock_database.find_many.side_effect = Exception("Connection refused")

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=60,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=2,
        )

        result = loader.start()

        assert result is False
        assert loader.is_running is False

    def test_stop_clears_running_flag(self, mock_database, sample_user_docs):
        """Stopping should set is_running to False."""
        mock_database.find_many.return_value = sample_user_docs

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=60,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        loader.start()
        loader.stop()

        assert loader.is_running is False

    def test_start_when_already_running(self, mock_database, sample_user_docs):
        """Starting when already running should not create duplicate timers."""
        mock_database.find_many.return_value = sample_user_docs

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=60,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        result1 = loader.start()
        result2 = loader.start()

        assert result1 is True
        assert result2 is True
        assert loader.is_running is True
        loader.stop()

    def test_periodic_reload_executes(self, mock_database, sample_user_docs):
        """Should reload users periodically after start."""
        mock_database.find_many.return_value = sample_user_docs

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=0.1,  # Very short for testing
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        loader.start()
        time.sleep(0.35)  # Wait for a few reload cycles
        loader.stop()

        # Initial load + at least 2 periodic reloads
        assert mock_database.find_many.call_count >= 3

    def test_periodic_reload_continues_on_failure(self, mock_database, sample_user_docs):
        """Should continue periodic reloads even if one fails."""
        # First call succeeds (initial load), then fail, then succeed
        mock_database.find_many.side_effect = [
            sample_user_docs,  # Initial load
            Exception("Temporary failure"),  # First reload fails
            sample_user_docs,  # Second reload succeeds
        ]

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=0.1,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        loader.start()
        time.sleep(0.35)
        loader.stop()

        # Should have attempted multiple calls despite failure
        assert mock_database.find_many.call_count >= 3


class TestUserNotificationConfig:
    """Tests for UserNotificationConfig dataclass."""

    def test_from_user_dict_full_document(self):
        """Should create config from a complete user document."""
        doc = {
            "user_id": "user-123",
            "hashed_password": "$2b$12$hash",
            "email": "test@example.com",
            "keywords": ["python", "AI"],
            "notification_times": [
                {"hour": 9, "minute": 0},
                {"hour": 17, "minute": 30},
            ],
            "subscription_expiry": datetime(2025, 3, 15, 12, 0, 0),
        }

        config = UserNotificationConfig.from_user_dict(doc)

        assert config.user_id == "user-123"
        assert config.email == "test@example.com"
        assert config.keywords == ["python", "AI"]
        assert len(config.notification_times) == 2
        assert config.notification_times[0].hour == 9
        assert config.notification_times[0].minute == 0
        assert config.subscription_expiry == datetime(2025, 3, 15, 12, 0, 0)

    def test_from_user_dict_minimal_document(self):
        """Should handle document with only required fields."""
        doc = {
            "user_id": "user-456",
            "email": "minimal@example.com",
        }

        config = UserNotificationConfig.from_user_dict(doc)

        assert config.user_id == "user-456"
        assert config.email == "minimal@example.com"
        assert config.keywords == []
        assert config.notification_times == []
        assert config.subscription_expiry is None

    def test_from_user_dict_raises_on_missing_user_id(self):
        """Should raise KeyError when user_id is missing."""
        doc = {
            "email": "test@example.com",
        }

        with pytest.raises(KeyError):
            UserNotificationConfig.from_user_dict(doc)

    def test_from_user_dict_raises_on_missing_email(self):
        """Should raise KeyError when email is missing."""
        doc = {
            "user_id": "user-123",
        }

        with pytest.raises(KeyError):
            UserNotificationConfig.from_user_dict(doc)


class TestThreadSafety:
    """Tests for thread-safe access to cached users."""

    def test_users_property_returns_copy(self, loader, mock_database, sample_user_docs):
        """The users property should return a copy, not the internal list."""
        mock_database.find_many.return_value = sample_user_docs
        loader.reload_users()

        users1 = loader.users
        users2 = loader.users

        # Should be equal but not the same object
        assert users1 == users2
        assert users1 is not users2

    def test_concurrent_reload_and_read(self, mock_database, sample_user_docs):
        """Should handle concurrent reload and read operations safely."""
        mock_database.find_many.return_value = sample_user_docs

        loader = UserLoader(
            database=mock_database,
            reload_interval_seconds=0.05,
            startup_retry_interval_seconds=0.01,
            startup_max_retries=3,
        )

        loader.start()

        # Read users concurrently during reloads
        errors = []

        def read_users():
            try:
                for _ in range(20):
                    users = loader.users
                    # Should always be a valid list
                    assert isinstance(users, list)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_users) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        loader.stop()
        assert errors == []
