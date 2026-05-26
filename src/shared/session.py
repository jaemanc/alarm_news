"""
Session management abstraction layer for extensibility.

This module provides an abstract interface for session storage,
allowing easy integration of Redis or other session backends in the future.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime, timedelta


class SessionInterface(ABC):
    """Abstract interface for session management."""
    
    @abstractmethod
    def create_session(self, user_id: str, data: Dict[str, Any], ttl: timedelta) -> str:
        """
        Create a new session.
        
        Args:
            user_id: User identifier
            data: Session data
            ttl: Session time-to-live
            
        Returns:
            Session token
        """
        pass
    
    @abstractmethod
    def get_session(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve session data.
        
        Args:
            token: Session token
            
        Returns:
            Session data or None if not found/expired
        """
        pass
    
    @abstractmethod
    def invalidate_session(self, token: str) -> bool:
        """
        Invalidate a session.
        
        Args:
            token: Session token
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def invalidate_user_sessions(self, user_id: str) -> int:
        """
        Invalidate all sessions for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Number of sessions invalidated
        """
        pass


class JWTSessionManager(SessionInterface):
    """
    JWT-based session management.
    
    This implementation uses JWT tokens for stateless sessions.
    Suitable for development and small-scale deployments.
    """
    
    def __init__(self, secret_key: str):
        """
        Initialize JWT session manager.
        
        Args:
            secret_key: Secret key for JWT signing
        """
        self._secret_key = secret_key
        self._invalidated_tokens = set()  # In-memory blacklist
    
    def create_session(self, user_id: str, data: Dict[str, Any], ttl: timedelta) -> str:
        """Create a JWT token."""
        import jwt
        
        payload = {
            "user_id": user_id,
            "exp": datetime.utcnow() + ttl,
            "iat": datetime.utcnow(),
            **data
        }
        
        token = jwt.encode(payload, self._secret_key, algorithm="HS256")
        return token
    
    def get_session(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode and validate JWT token."""
        import jwt
        
        if token in self._invalidated_tokens:
            return None
        
        try:
            payload = jwt.decode(token, self._secret_key, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
    
    def invalidate_session(self, token: str) -> bool:
        """Add token to blacklist."""
        self._invalidated_tokens.add(token)
        return True
    
    def invalidate_user_sessions(self, user_id: str) -> int:
        """
        Invalidate all sessions for a user.
        
        Note: This is limited in JWT-based implementation.
        For better support, use RedisSessionManager.
        """
        # In JWT implementation, we can't easily invalidate all user sessions
        # This would require storing all tokens per user
        # For now, return 0 to indicate limitation
        return 0


class RedisSessionManager(SessionInterface):
    """
    Redis-backed session management.
    
    This implementation uses Redis for distributed session storage.
    Suitable for production deployments with multiple instances.
    To enable: Update configuration to use RedisSessionManager instead of JWTSessionManager.
    """
    
    def __init__(self, redis_client, secret_key: str):
        """
        Initialize Redis session manager.
        
        Args:
            redis_client: Redis client instance
            secret_key: Secret key for token generation
        """
        self._redis = redis_client
        self._secret_key = secret_key
    
    def create_session(self, user_id: str, data: Dict[str, Any], ttl: timedelta) -> str:
        """Create a session in Redis."""
        import secrets
        import json
        
        # Generate secure random token
        token = secrets.token_urlsafe(32)
        
        # Store session data in Redis
        session_key = f"session:{token}"
        user_sessions_key = f"user_sessions:{user_id}"
        
        session_data = {
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat(),
            **data
        }
        
        # Store session with TTL
        self._redis.setex(
            session_key,
            ttl,
            json.dumps(session_data)
        )
        
        # Track user sessions
        self._redis.sadd(user_sessions_key, token)
        self._redis.expire(user_sessions_key, ttl)
        
        return token
    
    def get_session(self, token: str) -> Optional[Dict[str, Any]]:
        """Retrieve session from Redis."""
        import json
        
        session_key = f"session:{token}"
        data = self._redis.get(session_key)
        
        if data:
            return json.loads(data)
        return None
    
    def invalidate_session(self, token: str) -> bool:
        """Delete session from Redis."""
        session_key = f"session:{token}"
        
        # Get user_id before deleting
        session_data = self.get_session(token)
        if session_data:
            user_id = session_data.get("user_id")
            if user_id:
                user_sessions_key = f"user_sessions:{user_id}"
                self._redis.srem(user_sessions_key, token)
        
        return self._redis.delete(session_key) > 0
    
    def invalidate_user_sessions(self, user_id: str) -> int:
        """Invalidate all sessions for a user."""
        user_sessions_key = f"user_sessions:{user_id}"
        
        # Get all tokens for user
        tokens = self._redis.smembers(user_sessions_key)
        
        count = 0
        for token in tokens:
            session_key = f"session:{token}"
            if self._redis.delete(session_key) > 0:
                count += 1
        
        # Clear user sessions set
        self._redis.delete(user_sessions_key)
        
        return count


# Factory function for creating session managers
def create_session_manager(session_type: str = "jwt", **kwargs) -> SessionInterface:
    """
    Factory function to create session manager instances.
    
    Args:
        session_type: Type of session manager ("jwt" or "redis")
        **kwargs: Additional arguments for session manager initialization
        
    Returns:
        Session manager instance
        
    Example:
        # JWT-based sessions for development
        session_mgr = create_session_manager("jwt", secret_key="your-secret")
        
        # Redis-backed sessions for production
        session_mgr = create_session_manager(
            "redis",
            redis_client=redis_client,
            secret_key="your-secret"
        )
    """
    secret_key = kwargs.get("secret_key")
    if not secret_key:
        raise ValueError("secret_key is required")
    
    if session_type == "jwt":
        return JWTSessionManager(secret_key)
    elif session_type == "redis":
        redis_client = kwargs.get("redis_client")
        if not redis_client:
            raise ValueError("redis_client is required for RedisSessionManager")
        return RedisSessionManager(redis_client, secret_key)
    else:
        raise ValueError(f"Unknown session type: {session_type}")
