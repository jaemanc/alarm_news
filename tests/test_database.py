"""
Unit tests for the MongoDB connection manager.

Tests the retry decorator, abstract interface, connection configuration,
and index creation logic without requiring a live MongoDB instance.
"""
import time
from unittest.mock import MagicMock, patch, call

import pytest
from pymongo.errors import (
    ConnectionFailure,
    AutoReconnect,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)

from src.shared.config import MongoDBConfig
from src.shared.database import (
    DatabaseInterface,
    MongoDBConnectionManager,
    retry_on_write_failure,
    create_database,
    WRITE_RETRY_ATTEMPTS,
    WRITE_RETRY_INTERVAL_SECONDS,
)


class TestRetryDecorator:
    """Tests for the retry_on_write_failure decorator."""

    @pytest.mark.unit
    def test_succeeds_on_first_attempt(self):
        """Function succeeds without retries."""
        call_count = 0

        @retry_on_write_failure(attempts=3, interval=0)
        def successful_op():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_op()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.unit
    def test_retries_on_connection_failure(self):
        """Retries on ConnectionFailure and succeeds on second attempt."""
        call_count = 0

        @retry_on_write_failure(attempts=3, interval=0)
        def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionFailure("Connection lost")
            return "recovered"

        result = flaky_op()
        assert result == "recovered"
        assert call_count == 2

    @pytest.mark.unit
    def test_retries_on_auto_reconnect(self):
        """Retries on AutoReconnect error."""
        call_count = 0

        @retry_on_write_failure(attempts=3, interval=0)
        def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AutoReconnect("Auto reconnect")
            return "recovered"

        result = flaky_op()
        assert result == "recovered"
        assert call_count == 3

    @pytest.mark.unit
    def test_retries_on_network_timeout(self):
        """Retries on NetworkTimeout error."""
        call_count = 0

        @retry_on_write_failure(attempts=3, interval=0)
        def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise NetworkTimeout("Timeout")
            return "recovered"

        result = flaky_op()
        assert result == "recovered"
        assert call_count == 2

    @pytest.mark.unit
    def test_raises_after_all_attempts_exhausted(self):
        """Raises the last exception after all retry attempts fail."""

        @retry_on_write_failure(attempts=3, interval=0)
        def always_fails():
            raise ConnectionFailure("Persistent failure")

        with pytest.raises(ConnectionFailure, match="Persistent failure"):
            always_fails()

    @pytest.mark.unit
    def test_does_not_retry_on_non_connection_errors(self):
        """Does not retry on errors that are not connection-related."""
        call_count = 0

        @retry_on_write_failure(attempts=3, interval=0)
        def value_error_op():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not a connection error")

        with pytest.raises(ValueError, match="Not a connection error"):
            value_error_op()
        assert call_count == 1

    @pytest.mark.unit
    def test_default_retry_constants(self):
        """Verify default retry constants match requirements."""
        assert WRITE_RETRY_ATTEMPTS == 3
        assert WRITE_RETRY_INTERVAL_SECONDS == 5


class TestDatabaseInterface:
    """Tests for the abstract DatabaseInterface."""

    @pytest.mark.unit
    def test_cannot_instantiate_abstract_class(self):
        """DatabaseInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            DatabaseInterface()

    @pytest.mark.unit
    def test_concrete_implementation_must_implement_all_methods(self):
        """A subclass must implement all abstract methods."""

        class IncompleteDB(DatabaseInterface):
            def connect(self):
                pass

        with pytest.raises(TypeError):
            IncompleteDB()


class TestMongoDBConnectionManager:
    """Tests for MongoDBConnectionManager using mocked pymongo."""

    def _make_config(self) -> MongoDBConfig:
        """Create a test MongoDBConfig."""
        return MongoDBConfig(
            uri="mongodb://localhost:27017",
            database="alarm_news",
            min_pool_size=10,
            max_pool_size=100,
        )

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_connect_creates_client_with_correct_params(self, mock_client_cls):
        """connect() creates MongoClient with pooling, write concern, and read preference."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        mock_client_cls.assert_called_once_with(
            "mongodb://localhost:27017",
            minPoolSize=10,
            maxPoolSize=100,
            w="majority",
            readPreference="primary",
            serverSelectionTimeoutMS=5000,
        )

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_connect_verifies_connectivity_with_ping(self, mock_client_cls):
        """connect() sends a ping command to verify connectivity."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        mock_client.admin.command.assert_called_with("ping")

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_connect_creates_indexes(self, mock_client_cls):
        """connect() creates unique index on user_id and index on subscription_expiry."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        # Verify create_index was called for user_id (unique) and subscription_expiry
        calls = mock_collection.create_index.call_args_list
        assert len(calls) == 2

        # First call: unique index on user_id
        assert calls[0] == call(
            [("user_id", 1)],
            unique=True,
            name="idx_user_id_unique",
        )

        # Second call: index on subscription_expiry
        assert calls[1] == call(
            [("subscription_expiry", 1)],
            name="idx_subscription_expiry",
        )

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_disconnect_closes_client(self, mock_client_cls):
        """disconnect() closes the MongoClient and resets state."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()
        manager.disconnect()

        mock_client.close.assert_called_once()
        assert manager._client is None
        assert manager._db is None

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_health_check_returns_true_when_connected(self, mock_client_cls):
        """health_check() returns True when ping succeeds."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        assert manager.health_check() is True

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_health_check_returns_false_when_ping_fails(self, mock_client_cls):
        """health_check() returns False when ping raises an error."""
        mock_client = MagicMock()
        # First ping succeeds (during connect), subsequent pings fail
        mock_client.admin.command.side_effect = [{"ok": 1}, ConnectionFailure("down")]
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        assert manager.health_check() is False

    @pytest.mark.unit
    def test_health_check_returns_false_when_not_connected(self):
        """health_check() returns False when client is None."""
        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager._client = None

        assert manager.health_check() is False

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_insert_one_delegates_to_collection(self, mock_client_cls):
        """insert_one() inserts document and returns ID."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.insert_one.return_value = MagicMock(inserted_id="abc123")
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        result = manager.insert_one("users", {"user_id": "u1", "email": "a@b.com"})
        assert result == "abc123"

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_find_one_delegates_to_collection(self, mock_client_cls):
        """find_one() queries and returns matching document."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = {"user_id": "u1", "email": "a@b.com"}
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        result = manager.find_one("users", {"user_id": "u1"})
        assert result == {"user_id": "u1", "email": "a@b.com"}

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_update_one_returns_true_on_modification(self, mock_client_cls):
        """update_one() returns True when a document is modified."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.update_one.return_value = MagicMock(modified_count=1)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        result = manager.update_one("users", {"user_id": "u1"}, {"$set": {"email": "new@b.com"}})
        assert result is True

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_delete_one_returns_true_on_deletion(self, mock_client_cls):
        """delete_one() returns True when a document is deleted."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.delete_one.return_value = MagicMock(deleted_count=1)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        result = manager.delete_one("users", {"user_id": "u1"})
        assert result is True

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_find_many_returns_list_of_documents(self, mock_client_cls):
        """find_many() returns all matching documents as a list."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"user_id": "u1"},
            {"user_id": "u2"},
        ]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        result = manager.find_many("users", {})
        assert len(result) == 2
        assert result[0]["user_id"] == "u1"

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_get_collection_returns_collection_reference(self, mock_client_cls):
        """get_collection() returns a pymongo Collection reference."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        collection = manager.get_collection("users")
        assert collection is mock_collection

    @pytest.mark.unit
    @patch("src.shared.database.MongoClient")
    def test_insert_one_retries_on_connection_failure(self, mock_client_cls):
        """insert_one() retries on ConnectionFailure."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1}
        mock_db = MagicMock()
        mock_collection = MagicMock()
        # First call fails, second succeeds
        mock_collection.insert_one.side_effect = [
            ConnectionFailure("Connection lost"),
            MagicMock(inserted_id="abc123"),
        ]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        config = self._make_config()
        manager = MongoDBConnectionManager(config=config)
        manager.connect()

        # Patch sleep to avoid waiting in tests
        with patch("src.shared.database.time.sleep"):
            result = manager.insert_one("users", {"user_id": "u1"})

        assert result == "abc123"
        assert mock_collection.insert_one.call_count == 2

    @pytest.mark.unit
    def test_create_database_returns_new_instance(self):
        """create_database() returns a new MongoDBConnectionManager."""
        config = self._make_config()
        manager = create_database(config=config)
        assert isinstance(manager, MongoDBConnectionManager)
        assert manager._config == config
