"""
Unit tests for Redis connection manager.

Tests cover:
- Connection with configurable host, port, password
- Retry logic: 10 attempts with 10-second intervals
- Connection health check method
- Singleton pattern and factory functions
"""
import pytest
from unittest.mock import patch, MagicMock
import redis

from src.shared.config import RedisConfig
from src.shared.redis_client import (
    RedisConnectionManager,
    get_redis_connection_manager,
    get_redis_client,
    reset_redis_connection,
    MAX_RETRY_ATTEMPTS,
    RETRY_INTERVAL_SECONDS,
)


@pytest.fixture
def redis_config():
    """Create a test Redis configuration."""
    return RedisConfig(
        host="test-host",
        port=6380,
        password="test-password",
        db=1,
    )


@pytest.fixture
def redis_config_no_password():
    """Create a test Redis configuration without password."""
    return RedisConfig(
        host="localhost",
        port=6379,
        password=None,
        db=0,
    )


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton before and after each test."""
    reset_redis_connection()
    yield
    reset_redis_connection()


class TestRedisConnectionManager:
    """Tests for RedisConnectionManager class."""

    @patch("src.shared.redis_client.redis.Redis")
    def test_connect_success_first_attempt(self, mock_redis_cls, redis_config):
        """Test successful connection on first attempt."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        mock_redis_cls.assert_called_once_with(
            host="test-host",
            port=6380,
            password="test-password",
            db=1,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        mock_client.ping.assert_called_once()

    @patch("src.shared.redis_client.time.sleep")
    @patch("src.shared.redis_client.redis.Redis")
    def test_connect_retries_on_failure(self, mock_redis_cls, mock_sleep, redis_config):
        """Test retry logic when connection fails initially."""
        mock_client = MagicMock()
        # Fail twice, then succeed
        mock_client.ping.side_effect = [
            redis.ConnectionError("Connection refused"),
            redis.ConnectionError("Connection refused"),
            True,
        ]
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        assert mock_client.ping.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(RETRY_INTERVAL_SECONDS)

    @patch("src.shared.redis_client.time.sleep")
    @patch("src.shared.redis_client.redis.Redis")
    def test_connect_exhausts_all_retries(self, mock_redis_cls, mock_sleep, redis_config):
        """Test that ConnectionError is raised after all retries exhausted."""
        mock_client = MagicMock()
        mock_client.ping.side_effect = redis.ConnectionError("Connection refused")
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)

        with pytest.raises(redis.ConnectionError, match="Failed to connect to Redis after 10 attempts"):
            manager.connect()

        assert mock_client.ping.call_count == MAX_RETRY_ATTEMPTS
        assert mock_sleep.call_count == MAX_RETRY_ATTEMPTS - 1

    @patch("src.shared.redis_client.time.sleep")
    @patch("src.shared.redis_client.redis.Redis")
    def test_connect_handles_timeout_error(self, mock_redis_cls, mock_sleep, redis_config):
        """Test retry on TimeoutError."""
        mock_client = MagicMock()
        mock_client.ping.side_effect = [
            redis.TimeoutError("Timed out"),
            True,
        ]
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        assert mock_client.ping.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("src.shared.redis_client.redis.Redis")
    def test_connect_uses_config_values(self, mock_redis_cls, redis_config_no_password):
        """Test that connection uses configuration values correctly."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config_no_password)
        manager.connect()

        mock_redis_cls.assert_called_once_with(
            host="localhost",
            port=6379,
            password=None,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

    @patch("src.shared.redis_client.redis.Redis")
    def test_get_client_returns_connected_client(self, mock_redis_cls, redis_config):
        """Test get_client returns the Redis client after connection."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        client = manager.get_client()
        assert client is mock_client

    def test_get_client_raises_if_not_connected(self, redis_config):
        """Test get_client raises RuntimeError if not connected."""
        manager = RedisConnectionManager(redis_config)

        with pytest.raises(RuntimeError, match="Redis client is not connected"):
            manager.get_client()

    @patch("src.shared.redis_client.redis.Redis")
    def test_health_check_returns_true_when_healthy(self, mock_redis_cls, redis_config):
        """Test health_check returns True when Redis responds to PING."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        assert manager.health_check() is True

    @patch("src.shared.redis_client.redis.Redis")
    def test_health_check_returns_false_on_connection_error(self, mock_redis_cls, redis_config):
        """Test health_check returns False when Redis connection fails."""
        mock_client = MagicMock()
        # First ping succeeds (connect), second fails (health_check)
        mock_client.ping.side_effect = [True, redis.ConnectionError("Lost connection")]
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        assert manager.health_check() is False

    @patch("src.shared.redis_client.redis.Redis")
    def test_health_check_returns_false_on_timeout(self, mock_redis_cls, redis_config):
        """Test health_check returns False on timeout."""
        mock_client = MagicMock()
        mock_client.ping.side_effect = [True, redis.TimeoutError("Timed out")]
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()

        assert manager.health_check() is False

    def test_health_check_returns_false_when_not_connected(self, redis_config):
        """Test health_check returns False when client is None."""
        manager = RedisConnectionManager(redis_config)
        assert manager.health_check() is False

    @patch("src.shared.redis_client.redis.Redis")
    def test_close_disconnects_client(self, mock_redis_cls, redis_config):
        """Test close() disconnects and clears the client."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager = RedisConnectionManager(redis_config)
        manager.connect()
        manager.close()

        mock_client.close.assert_called_once()
        assert manager.health_check() is False

    @patch("src.shared.redis_client.redis.Redis")
    def test_close_when_not_connected(self, mock_redis_cls, redis_config):
        """Test close() is safe to call when not connected."""
        manager = RedisConnectionManager(redis_config)
        manager.close()  # Should not raise


class TestSingletonFactory:
    """Tests for singleton factory functions."""

    @patch("src.shared.redis_client.redis.Redis")
    def test_get_redis_connection_manager_creates_singleton(self, mock_redis_cls, redis_config):
        """Test that get_redis_connection_manager returns the same instance."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager1 = get_redis_connection_manager(redis_config)
        manager2 = get_redis_connection_manager(redis_config)

        assert manager1 is manager2

    @patch("src.shared.redis_client.redis.Redis")
    def test_get_redis_client_returns_client(self, mock_redis_cls, redis_config):
        """Test that get_redis_client returns the Redis client."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        client = get_redis_client(redis_config)
        assert client is mock_client

    @patch("src.shared.redis_client.redis.Redis")
    def test_reset_redis_connection_clears_singleton(self, mock_redis_cls, redis_config):
        """Test that reset_redis_connection clears the singleton."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        manager1 = get_redis_connection_manager(redis_config)
        reset_redis_connection()

        # Create a new mock for the second instance
        mock_client2 = MagicMock()
        mock_client2.ping.return_value = True
        mock_redis_cls.return_value = mock_client2

        manager2 = get_redis_connection_manager(redis_config)

        assert manager1 is not manager2
        mock_client.close.assert_called_once()


class TestRetryConstants:
    """Tests for retry configuration constants."""

    def test_max_retry_attempts_is_10(self):
        """Verify retry attempts constant is 10."""
        assert MAX_RETRY_ATTEMPTS == 10

    def test_retry_interval_is_10_seconds(self):
        """Verify retry interval constant is 10 seconds."""
        assert RETRY_INTERVAL_SECONDS == 10
