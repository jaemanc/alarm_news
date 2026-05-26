"""
Redis connection manager for Alarm News System.

This module provides a singleton Redis connection manager with:
- Configurable host, port, password from environment variables
- Retry logic: 10 attempts with 10-second intervals on startup
- Connection health check method for readiness probes

The connection manager is used by RedisLock and RedisCache components.
"""
import logging
import time
from typing import Optional

import redis

from src.shared.config import get_config, RedisConfig

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRY_ATTEMPTS = 10
RETRY_INTERVAL_SECONDS = 10


class RedisConnectionManager:
    """
    Manages a Redis connection with retry logic and health checking.

    Designed as a singleton - use get_redis_client() factory function
    to obtain the shared instance.
    """

    def __init__(self, config: Optional[RedisConfig] = None):
        """
        Initialize the Redis connection manager.

        Args:
            config: Redis configuration. If None, loads from environment.
        """
        if config is None:
            config = get_config().redis

        self._config = config
        self._client: Optional[redis.Redis] = None

    def connect(self) -> None:
        """
        Establish connection to Redis with retry logic.

        Attempts to connect up to 10 times with 10-second intervals.

        Raises:
            redis.ConnectionError: If all retry attempts are exhausted.
        """
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                self._client = redis.Redis(
                    host=self._config.host,
                    port=self._config.port,
                    password=self._config.password,
                    db=self._config.db,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )
                # Verify the connection is alive
                self._client.ping()
                logger.info(
                    "Redis connection established on attempt %d/%d (host=%s, port=%d)",
                    attempt,
                    MAX_RETRY_ATTEMPTS,
                    self._config.host,
                    self._config.port,
                )
                return
            except (redis.ConnectionError, redis.TimeoutError) as e:
                last_error = e
                logger.warning(
                    "Redis connection attempt %d/%d failed: %s",
                    attempt,
                    MAX_RETRY_ATTEMPTS,
                    str(e),
                )
                if attempt < MAX_RETRY_ATTEMPTS:
                    time.sleep(RETRY_INTERVAL_SECONDS)

        raise redis.ConnectionError(
            f"Failed to connect to Redis after {MAX_RETRY_ATTEMPTS} attempts. "
            f"Last error: {last_error}"
        )

    def get_client(self) -> redis.Redis:
        """
        Get the Redis client instance.

        Returns:
            The connected Redis client.

        Raises:
            RuntimeError: If connect() has not been called or connection failed.
        """
        if self._client is None:
            raise RuntimeError(
                "Redis client is not connected. Call connect() first."
            )
        return self._client

    def health_check(self) -> bool:
        """
        Check if the Redis connection is healthy.

        Sends a PING command to verify connectivity.

        Returns:
            True if Redis responds to PING, False otherwise.
        """
        if self._client is None:
            return False
        try:
            return self._client.ping()
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError):
            return False

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.info("Redis connection closed.")


# Singleton instance
_instance: Optional[RedisConnectionManager] = None


def get_redis_connection_manager(config: Optional[RedisConfig] = None) -> RedisConnectionManager:
    """
    Get the singleton RedisConnectionManager instance.

    On first call, creates and connects the manager. Subsequent calls
    return the same instance.

    Args:
        config: Optional Redis configuration override.

    Returns:
        The singleton RedisConnectionManager instance.
    """
    global _instance
    if _instance is None:
        _instance = RedisConnectionManager(config)
        _instance.connect()
    return _instance


def get_redis_client(config: Optional[RedisConfig] = None) -> redis.Redis:
    """
    Convenience function to get the Redis client instance.

    Args:
        config: Optional Redis configuration override.

    Returns:
        The connected Redis client.
    """
    manager = get_redis_connection_manager(config)
    return manager.get_client()


def reset_redis_connection() -> None:
    """
    Reset the singleton instance. Useful for testing or reconnection.

    Closes the existing connection and clears the singleton so the next
    call to get_redis_connection_manager() creates a fresh instance.
    """
    global _instance
    if _instance is not None:
        _instance.close()
        _instance = None
