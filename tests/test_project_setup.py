"""
Smoke tests to verify project structure and dependencies are properly configured.
"""
import pytest


class TestProjectSetup:
    """Verify project dependencies and structure."""

    @pytest.mark.unit
    def test_pymongo_importable(self):
        """Verify pymongo is installed and importable."""
        import pymongo
        assert pymongo.version is not None

    @pytest.mark.unit
    def test_kafka_importable(self):
        """Verify kafka-python is installed and importable."""
        import kafka
        assert kafka is not None

    @pytest.mark.unit
    def test_redis_importable(self):
        """Verify redis is installed and importable."""
        import redis
        assert redis is not None

    @pytest.mark.unit
    def test_bcrypt_importable(self):
        """Verify bcrypt is installed and importable."""
        import bcrypt
        assert bcrypt is not None

    @pytest.mark.unit
    def test_jwt_importable(self):
        """Verify pyjwt is installed and importable."""
        import jwt
        assert jwt is not None

    @pytest.mark.unit
    def test_beautifulsoup_importable(self):
        """Verify beautifulsoup4 is installed and importable."""
        from bs4 import BeautifulSoup
        assert BeautifulSoup is not None

    @pytest.mark.unit
    def test_requests_importable(self):
        """Verify requests is installed and importable."""
        import requests
        assert requests is not None

    @pytest.mark.unit
    def test_dotenv_importable(self):
        """Verify python-dotenv is installed and importable."""
        from dotenv import load_dotenv
        assert load_dotenv is not None

    @pytest.mark.unit
    def test_smtplib_available(self):
        """Verify smtplib (built-in) is available."""
        import smtplib
        assert smtplib is not None

    @pytest.mark.unit
    def test_shared_cache_interface(self):
        """Verify shared cache abstraction is importable."""
        from src.shared.cache import CacheInterface, InMemoryCache, create_cache

        cache = create_cache("memory")
        assert isinstance(cache, InMemoryCache)
        cache.set("test_key", "test_value")
        assert cache.get("test_key") == "test_value"
        assert cache.exists("test_key") is True
        cache.delete("test_key")
        assert cache.get("test_key") is None

    @pytest.mark.unit
    def test_shared_locking_interface(self):
        """Verify shared locking abstraction is importable."""
        from src.shared.locking import LockInterface, InMemoryLock, create_lock_manager

        lock_mgr = create_lock_manager("memory")
        assert isinstance(lock_mgr, InMemoryLock)

        # Acquire and release a lock
        acquired = lock_mgr.acquire("test-event-1", "worker-1", ttl_seconds=10, timeout_seconds=1)
        assert acquired is True
        assert lock_mgr.is_held("test-event-1") is True

        released = lock_mgr.release("test-event-1", "worker-1")
        assert released is True
        assert lock_mgr.is_held("test-event-1") is False

    @pytest.mark.unit
    def test_shared_session_interface(self):
        """Verify shared session abstraction is importable."""
        from src.shared.session import SessionInterface, JWTSessionManager, create_session_manager

        session_mgr = create_session_manager("jwt", secret_key="test-secret")
        assert isinstance(session_mgr, JWTSessionManager)
