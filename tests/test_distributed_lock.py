"""
Unit tests for distributed lock manager.

Tests cover:
- Lock acquisition with SET NX EX (Requirements 11.1, 11.9)
- Lock release with Lua script (Requirements 11.5, 11.6)
- Lock timeout behavior (Requirements 11.1, 11.2)
- Lock already held scenario (Requirements 11.3)
- Lock TTL for deadlock prevention (Requirement 11.7)
- Lock key format and ownership tracking (Requirements 11.4)
- InMemoryLock for development/testing
- Factory function create_lock_manager
"""
import pytest
from unittest.mock import patch, MagicMock, call
import time
import threading

from src.shared.locking import (
    LockInterface,
    InMemoryLock,
    RedisLock,
    create_lock_manager,
)


class TestInMemoryLock:
    """Tests for InMemoryLock implementation."""

    def test_acquire_lock_success(self):
        """Test successful lock acquisition when lock is free."""
        lock = InMemoryLock()
        result = lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        assert result is True

    def test_acquire_lock_already_held_returns_false(self):
        """Test that acquiring an already-held lock returns False after timeout."""
        lock = InMemoryLock()
        # First worker acquires
        lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        # Second worker tries to acquire - should timeout
        result = lock.acquire("event-123", "worker-2", ttl_seconds=300, timeout_seconds=0.3)
        assert result is False

    def test_acquire_lock_after_expiry(self):
        """Test that a lock can be acquired after TTL expires."""
        lock = InMemoryLock()
        # Acquire with very short TTL
        lock.acquire("event-123", "worker-1", ttl_seconds=1, timeout_seconds=1)
        # Wait for expiry
        time.sleep(1.1)
        # Should be able to acquire now
        result = lock.acquire("event-123", "worker-2", ttl_seconds=300, timeout_seconds=1)
        assert result is True

    def test_release_lock_by_owner(self):
        """Test that the lock owner can release the lock."""
        lock = InMemoryLock()
        lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        result = lock.release("event-123", "worker-1")
        assert result is True

    def test_release_lock_by_non_owner_fails(self):
        """Test that a non-owner cannot release the lock."""
        lock = InMemoryLock()
        lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        result = lock.release("event-123", "worker-2")
        assert result is False

    def test_release_nonexistent_lock(self):
        """Test releasing a lock that doesn't exist returns False."""
        lock = InMemoryLock()
        result = lock.release("event-999", "worker-1")
        assert result is False

    def test_is_held_returns_true_when_locked(self):
        """Test is_held returns True for an active lock."""
        lock = InMemoryLock()
        lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        assert lock.is_held("event-123") is True

    def test_is_held_returns_false_when_not_locked(self):
        """Test is_held returns False when no lock exists."""
        lock = InMemoryLock()
        assert lock.is_held("event-123") is False

    def test_is_held_returns_false_after_expiry(self):
        """Test is_held returns False after TTL expires."""
        lock = InMemoryLock()
        lock.acquire("event-123", "worker-1", ttl_seconds=1, timeout_seconds=1)
        time.sleep(1.1)
        assert lock.is_held("event-123") is False

    def test_acquire_after_release(self):
        """Test that a lock can be acquired after it is released."""
        lock = InMemoryLock()
        lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=1)
        lock.release("event-123", "worker-1")
        result = lock.acquire("event-123", "worker-2", ttl_seconds=300, timeout_seconds=1)
        assert result is True

    def test_concurrent_acquire_only_one_wins(self):
        """Test that only one thread can acquire the lock at a time."""
        lock = InMemoryLock()
        results = []

        def try_acquire(worker_id):
            result = lock.acquire("event-123", worker_id, ttl_seconds=300, timeout_seconds=0.5)
            results.append((worker_id, result))

        t1 = threading.Thread(target=try_acquire, args=("worker-1",))
        t2 = threading.Thread(target=try_acquire, args=("worker-2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one should succeed
        successes = [r for r in results if r[1] is True]
        failures = [r for r in results if r[1] is False]
        assert len(successes) == 1
        assert len(failures) == 1


class TestRedisLock:
    """Tests for RedisLock implementation."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = MagicMock()
        mock.register_script.return_value = MagicMock()
        return mock

    @pytest.fixture
    def redis_lock(self, mock_redis):
        """Create a RedisLock instance with mock Redis."""
        return RedisLock(mock_redis)

    def test_acquire_lock_success(self, redis_lock, mock_redis):
        """Test successful lock acquisition using SET NX EX."""
        mock_redis.set.return_value = True

        result = redis_lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=10)

        assert result is True
        mock_redis.set.assert_called_with(
            "lock:event:event-123", "worker-1", nx=True, ex=300
        )

    def test_acquire_lock_key_format(self, redis_lock, mock_redis):
        """Test that lock key follows format lock:event:{event_id}."""
        mock_redis.set.return_value = True

        redis_lock.acquire("my-event-456", "worker-1")

        mock_redis.set.assert_called_with(
            "lock:event:my-event-456", "worker-1", nx=True, ex=300
        )

    def test_acquire_lock_stores_worker_id_as_value(self, redis_lock, mock_redis):
        """Test that the lock value is the worker_id for ownership tracking."""
        mock_redis.set.return_value = True

        redis_lock.acquire("event-123", "worker-abc-789")

        mock_redis.set.assert_called_with(
            "lock:event:event-123", "worker-abc-789", nx=True, ex=300
        )

    def test_acquire_lock_uses_5_minute_ttl(self, redis_lock, mock_redis):
        """Test that default TTL is 5 minutes (300 seconds) to prevent deadlocks."""
        mock_redis.set.return_value = True

        redis_lock.acquire("event-123", "worker-1")

        mock_redis.set.assert_called_with(
            "lock:event:event-123", "worker-1", nx=True, ex=300
        )

    def test_acquire_lock_custom_ttl(self, redis_lock, mock_redis):
        """Test lock acquisition with custom TTL."""
        mock_redis.set.return_value = True

        redis_lock.acquire("event-123", "worker-1", ttl_seconds=600)

        mock_redis.set.assert_called_with(
            "lock:event:event-123", "worker-1", nx=True, ex=600
        )

    @patch("src.shared.locking.time.sleep")
    @patch("src.shared.locking.time.time")
    def test_acquire_lock_already_held_returns_false(self, mock_time, mock_sleep, redis_lock, mock_redis):
        """Test that acquiring an already-held lock returns False after timeout."""
        mock_redis.set.return_value = None  # SET NX returns None when key exists
        # Simulate time passing beyond the 10-second timeout
        mock_time.side_effect = [0.0, 0.1, 0.2, 0.3, 10.1]

        result = redis_lock.acquire("event-123", "worker-2", ttl_seconds=300, timeout_seconds=10)

        assert result is False

    @patch("src.shared.locking.time.sleep")
    @patch("src.shared.locking.time.time")
    def test_acquire_lock_retries_until_timeout(self, mock_time, mock_sleep, redis_lock, mock_redis):
        """Test that lock acquisition retries within the 10-second timeout."""
        # First attempts fail, then succeed
        mock_redis.set.side_effect = [None, None, True]
        mock_time.side_effect = [0.0, 0.1, 0.2, 0.3]

        result = redis_lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=10)

        assert result is True
        assert mock_redis.set.call_count == 3
        assert mock_sleep.call_count == 2  # Slept between retries

    @patch("src.shared.locking.time.sleep")
    @patch("src.shared.locking.time.time")
    def test_acquire_lock_10_second_timeout(self, mock_time, mock_sleep, redis_lock, mock_redis):
        """Test that lock acquisition times out after 10 seconds."""
        mock_redis.set.return_value = None
        # Simulate: start at 0, then jump past 10 seconds
        mock_time.side_effect = [0.0, 5.0, 10.1]

        result = redis_lock.acquire("event-123", "worker-1", ttl_seconds=300, timeout_seconds=10)

        assert result is False

    def test_release_lock_uses_lua_script(self, redis_lock, mock_redis):
        """Test that release uses Lua script for atomic check-and-delete."""
        release_script = mock_redis.register_script.return_value
        release_script.return_value = 1

        result = redis_lock.release("event-123", "worker-1")

        assert result is True
        release_script.assert_called_once_with(
            keys=["lock:event:event-123"], args=["worker-1"]
        )

    def test_release_lock_only_by_owner(self, redis_lock, mock_redis):
        """Test that release fails if caller is not the lock owner."""
        release_script = mock_redis.register_script.return_value
        release_script.return_value = 0  # Lua script returns 0 if not owner

        result = redis_lock.release("event-123", "worker-2")

        assert result is False

    def test_release_lock_key_format(self, redis_lock, mock_redis):
        """Test that release uses correct lock key format."""
        release_script = mock_redis.register_script.return_value
        release_script.return_value = 1

        redis_lock.release("my-event-789", "worker-1")

        release_script.assert_called_once_with(
            keys=["lock:event:my-event-789"], args=["worker-1"]
        )

    def test_is_held_returns_true_when_key_exists(self, redis_lock, mock_redis):
        """Test is_held returns True when lock key exists in Redis."""
        mock_redis.exists.return_value = 1

        result = redis_lock.is_held("event-123")

        assert result is True
        mock_redis.exists.assert_called_with("lock:event:event-123")

    def test_is_held_returns_false_when_key_missing(self, redis_lock, mock_redis):
        """Test is_held returns False when lock key does not exist."""
        mock_redis.exists.return_value = 0

        result = redis_lock.is_held("event-123")

        assert result is False

    def test_lua_release_script_content(self, mock_redis):
        """Test that the Lua release script checks ownership before deleting."""
        # Verify the script content matches expected pattern
        expected_script = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
    """
        assert RedisLock.RELEASE_SCRIPT.strip() == expected_script.strip()

    def test_register_script_called_on_init(self, mock_redis):
        """Test that the Lua script is registered with Redis on initialization."""
        RedisLock(mock_redis)
        mock_redis.register_script.assert_called_once_with(RedisLock.RELEASE_SCRIPT)


class TestCreateLockManager:
    """Tests for the create_lock_manager factory function."""

    def test_create_memory_lock(self):
        """Test creating an InMemoryLock via factory."""
        lock = create_lock_manager("memory")
        assert isinstance(lock, InMemoryLock)

    def test_create_redis_lock(self):
        """Test creating a RedisLock via factory."""
        mock_redis = MagicMock()
        mock_redis.register_script.return_value = MagicMock()
        lock = create_lock_manager("redis", redis_client=mock_redis)
        assert isinstance(lock, RedisLock)

    def test_create_redis_lock_without_client_raises(self):
        """Test that creating RedisLock without redis_client raises ValueError."""
        with pytest.raises(ValueError, match="redis_client is required"):
            create_lock_manager("redis")

    def test_create_unknown_type_raises(self):
        """Test that unknown lock type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown lock type"):
            create_lock_manager("unknown")

    def test_lock_interface_compliance(self):
        """Test that both implementations satisfy the LockInterface."""
        memory_lock = create_lock_manager("memory")
        assert isinstance(memory_lock, LockInterface)

        mock_redis = MagicMock()
        mock_redis.register_script.return_value = MagicMock()
        redis_lock = create_lock_manager("redis", redis_client=mock_redis)
        assert isinstance(redis_lock, LockInterface)


class TestLockInterface:
    """Tests verifying the LockInterface contract."""

    def test_interface_has_acquire_method(self):
        """Test that LockInterface defines acquire method."""
        assert hasattr(LockInterface, "acquire")

    def test_interface_has_release_method(self):
        """Test that LockInterface defines release method."""
        assert hasattr(LockInterface, "release")

    def test_interface_has_is_held_method(self):
        """Test that LockInterface defines is_held method."""
        assert hasattr(LockInterface, "is_held")

    def test_cannot_instantiate_interface_directly(self):
        """Test that LockInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            LockInterface()
