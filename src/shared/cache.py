"""
Cache abstraction layer for extensibility.

This module provides an abstract interface for caching operations,
allowing easy integration of Redis or other caching backends in the future.
"""
from abc import ABC, abstractmethod
from typing import Optional, Any
from datetime import timedelta


class CacheInterface(ABC):
    """Abstract interface for cache operations."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> bool:
        """
        Store a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live for the cached value
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Delete a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists, False otherwise
        """
        pass


class InMemoryCache(CacheInterface):
    """
    In-memory cache implementation.
    
    This is a simple implementation for development and testing.
    For production, consider using RedisCache for distributed caching.
    """
    
    def __init__(self):
        self._cache = {}
        self._expiry = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from in-memory cache."""
        if key in self._cache:
            # Check if expired
            if key in self._expiry:
                from datetime import datetime
                if datetime.now() > self._expiry[key]:
                    self.delete(key)
                    return None
            return self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> bool:
        """Store a value in in-memory cache."""
        self._cache[key] = value
        if ttl:
            from datetime import datetime
            self._expiry[key] = datetime.now() + ttl
        return True
    
    def delete(self, key: str) -> bool:
        """Delete a value from in-memory cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._expiry:
            del self._expiry[key]
        return True
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in in-memory cache."""
        return key in self._cache


class RedisCache(CacheInterface):
    """
    Redis-backed cache implementation.
    
    This implementation can be used when Redis is available for distributed caching.
    To enable: Update configuration to use RedisCache instead of InMemoryCache.
    """
    
    def __init__(self, redis_client):
        """
        Initialize Redis cache.
        
        Args:
            redis_client: Redis client instance
        """
        self._redis = redis_client
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from Redis cache."""
        import pickle
        value = self._redis.get(key)
        if value:
            return pickle.loads(value)
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> bool:
        """Store a value in Redis cache."""
        import pickle
        serialized = pickle.dumps(value)
        if ttl:
            return self._redis.setex(key, ttl, serialized)
        else:
            return self._redis.set(key, serialized)
    
    def delete(self, key: str) -> bool:
        """Delete a value from Redis cache."""
        return self._redis.delete(key) > 0
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in Redis cache."""
        return self._redis.exists(key) > 0


# Factory function for creating cache instances
def create_cache(cache_type: str = "memory", **kwargs) -> CacheInterface:
    """
    Factory function to create cache instances.
    
    Args:
        cache_type: Type of cache ("memory" or "redis")
        **kwargs: Additional arguments for cache initialization
        
    Returns:
        Cache instance
        
    Example:
        # In-memory cache for development
        cache = create_cache("memory")
        
        # Redis cache for production
        cache = create_cache("redis", redis_client=redis_client)
    """
    if cache_type == "memory":
        return InMemoryCache()
    elif cache_type == "redis":
        redis_client = kwargs.get("redis_client")
        if not redis_client:
            raise ValueError("redis_client is required for RedisCache")
        return RedisCache(redis_client)
    else:
        raise ValueError(f"Unknown cache type: {cache_type}")
