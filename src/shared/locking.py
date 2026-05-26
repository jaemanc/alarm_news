"""
Distributed locking abstraction layer for extensibility.

This module provides an abstract interface for distributed locking operations,
allowing easy integration of Redis or other locking backends.
The default in-memory implementation is suitable for single-instance development.
For production with multiple instances, use the Redis implementation.
"""
from abc import ABC, abstractmethod
from typing import Optional
import threading
import time


class LockInterface(ABC):
    """Abstract interface for distributed lock operations."""

    @abstractmethod
    def acquire(self, key: str, owner: str, ttl_seconds: int = 300, timeout_seconds: int = 10) -> bool:
        """
        Attempt to acquire a distributed lock.

        Args:
            key: Lock key identifier (e.g., event_id)
            owner: Owner identifier (e.g., worker_id) for ownership tracking
            ttl_seconds: Time-to-live for the lock to prevent deadlocks
            timeout_seconds: Maximum time to wait for lock acquisition

        Returns:
            True if lock acquired, False otherwise
        """
        pass

    @abstractmethod
    def release(self, key: str, owner: str) -> bool:
        """
        Release a distributed lock (only if owned by the caller).

        Args:
            key: Lock key identifier
            owner: Owner identifier (must match the lock holder)

        Returns:
            True if lock released, False if not owned or not found
        """
        pass

    @abstractmethod
    def is_held(self, key: str) -> bool:
        """
        Check if a lock is currently held.

        Args:
            key: Lock key identifier

        Returns:
            True if lock is held, False otherwise
        """
        pass


class InMemoryLock(LockInterface):
    """
    In-memory lock implementation for single-instance development and testing.

    WARNING: This does NOT provide distributed locking across multiple processes.
    Use RedisLock for production deployments with multiple worker instances.
    """

    def __init__(self):
        self._locks: dict = {}
        self._mutex = threading.Lock()

    def acquire(self, key: str, owner: str, ttl_seconds: int = 300, timeout_seconds: int = 10) -> bool:
        """Attempt to acquire an in-memory lock with timeout."""
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            with self._mutex:
                # Check if lock exists and is not expired
                if key in self._locks:
                    lock_owner, expiry = self._locks[key]
                    if time.time() >= expiry:
                        # Lock expired, can acquire
                        self._locks[key] = (owner, time.time() + ttl_seconds)
                        return True
                    else:
                        # Lock held by someone else
                        pass
                else:
                    # Lock not held, acquire it
                    self._locks[key] = (owner, time.time() + ttl_seconds)
                    return True

            # Wait briefly before retrying
            time.sleep(0.1)

        return False

    def release(self, key: str, owner: str) -> bool:
        """Release an in-memory lock if owned by the caller."""
        with self._mutex:
            if key in self._locks:
                lock_owner, _ = self._locks[key]
                if lock_owner == owner:
                    del self._locks[key]
                    return True
            return False

    def is_held(self, key: str) -> bool:
        """Check if a lock is currently held."""
        with self._mutex:
            if key in self._locks:
                _, expiry = self._locks[key]
                if time.time() < expiry:
                    return True
                else:
                    # Expired, clean up
                    del self._locks[key]
            return False


class RedisLock(LockInterface):
    """
    Redis-backed distributed lock implementation.

    Uses Redis SET NX EX for atomic lock acquisition and Lua scripts
    for atomic release (check-and-delete pattern).

    Suitable for production deployments with multiple worker instances.
    """

    # Lua script for atomic release: only delete if we own the lock
    RELEASE_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
    """

    def __init__(self, redis_client):
        """
        Initialize Redis lock manager.

        Args:
            redis_client: Redis client instance
        """
        self._redis = redis_client
        self._release_script = self._redis.register_script(self.RELEASE_SCRIPT)

    def acquire(self, key: str, owner: str, ttl_seconds: int = 300, timeout_seconds: int = 10) -> bool:
        """
        Acquire a distributed lock using Redis SET NX EX.

        Lock key format: lock:event:{key}
        """
        lock_key = f"lock:event:{key}"
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            # SET NX EX: set if not exists with expiry
            result = self._redis.set(lock_key, owner, nx=True, ex=ttl_seconds)
            if result:
                return True

            # Wait briefly before retrying
            time.sleep(0.1)

        return False

    def release(self, key: str, owner: str) -> bool:
        """
        Release a distributed lock atomically using Lua script.

        Only releases if the lock is owned by the specified owner.
        """
        lock_key = f"lock:event:{key}"
        result = self._release_script(keys=[lock_key], args=[owner])
        return result == 1

    def is_held(self, key: str) -> bool:
        """Check if a lock is currently held in Redis."""
        lock_key = f"lock:event:{key}"
        return self._redis.exists(lock_key) > 0


def create_lock_manager(lock_type: str = "memory", **kwargs) -> LockInterface:
    """
    Factory function to create lock manager instances.

    Args:
        lock_type: Type of lock manager ("memory" or "redis")
        **kwargs: Additional arguments for lock manager initialization

    Returns:
        Lock manager instance

    Example:
        # In-memory lock for development
        lock_mgr = create_lock_manager("memory")

        # Redis lock for production
        lock_mgr = create_lock_manager("redis", redis_client=redis_client)
    """
    if lock_type == "memory":
        return InMemoryLock()
    elif lock_type == "redis":
        redis_client = kwargs.get("redis_client")
        if not redis_client:
            raise ValueError("redis_client is required for RedisLock")
        return RedisLock(redis_client)
    else:
        raise ValueError(f"Unknown lock type: {lock_type}")
