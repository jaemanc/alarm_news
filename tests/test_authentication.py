"""
Unit tests for the authentication handler.

Tests cover:
- Password verification with correct and incorrect passwords
- Subscription expiry checking
- JWT token generation and expiry
- Rate limiting logic
- Full authentication flow
"""
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import bcrypt
import jwt
import pytest

from src.auth.authentication import (
    AuthenticationHandler,
    AuthenticationInterface,
    AuthResult,
    GENERIC_ERROR_MESSAGE,
    EXPIRED_SUBSCRIPTION_MESSAGE,
    RATE_LIMIT_MESSAGE,
)
from src.shared.cache import InMemoryCache
from src.shared.config import AuthConfig, RateLimitConfig
from src.shared.models import User


# Test fixtures

@pytest.fixture
def auth_config():
    """Create a test authentication config."""
    return AuthConfig(
        jwt_secret="test-secret-key-for-testing-only",
        jwt_expiry_hours=24,
        bcrypt_cost_factor=12,
    )


@pytest.fixture
def rate_limit_config():
    """Create a test rate limit config."""
    return RateLimitConfig(
        max_attempts=5,
        window_minutes=15,
    )


@pytest.fixture
def mock_db():
    """Create a mock database interface."""
    return MagicMock()


@pytest.fixture
def cache():
    """Create an in-memory cache for testing."""
    return InMemoryCache()


@pytest.fixture
def handler(mock_db, cache, auth_config, rate_limit_config):
    """Create an AuthenticationHandler instance for testing."""
    return AuthenticationHandler(
        db=mock_db,
        cache=cache,
        auth_config=auth_config,
        rate_limit_config=rate_limit_config,
    )


def _hash_password(password: str) -> str:
    """Helper to hash a password with bcrypt for test setup."""
    salt = bcrypt.gensalt(rounds=4)  # Low cost for fast tests
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def _create_user_doc(user_id: str, password: str, expiry: datetime) -> dict:
    """Helper to create a user document for mock DB responses."""
    return {
        "user_id": user_id,
        "hashed_password": _hash_password(password),
        "email": "test@example.com",
        "keywords": ["python"],
        "notification_times": [],
        "subscription_expiry": expiry,
    }


# Tests for verify_password

class TestVerifyPassword:
    def test_correct_password(self, handler):
        """Verify that a correct password returns True."""
        password = "MySecurePass123!"
        hashed = _hash_password(password)
        assert handler.verify_password(password, hashed) is True

    def test_incorrect_password(self, handler):
        """Verify that an incorrect password returns False."""
        hashed = _hash_password("CorrectPassword1!")
        assert handler.verify_password("WrongPassword1!", hashed) is False

    def test_empty_password(self, handler):
        """Verify that an empty password returns False against a valid hash."""
        hashed = _hash_password("SomePassword1!")
        assert handler.verify_password("", hashed) is False

    def test_invalid_hash_format(self, handler):
        """Verify that an invalid hash format returns False gracefully."""
        assert handler.verify_password("password", "not-a-valid-hash") is False


# Tests for check_subscription_valid

class TestCheckSubscriptionValid:
    def test_valid_subscription(self, handler):
        """Subscription expiry in the future should be valid."""
        future = datetime.now(timezone.utc) + timedelta(days=10)
        assert handler.check_subscription_valid(future) is True

    def test_expired_subscription(self, handler):
        """Subscription expiry in the past should be invalid."""
        past = datetime.now(timezone.utc) - timedelta(days=1)
        assert handler.check_subscription_valid(past) is False

    def test_none_expiry(self, handler):
        """None expiry should be invalid."""
        assert handler.check_subscription_valid(None) is False

    def test_naive_datetime_treated_as_utc(self, handler):
        """Naive datetime (no tzinfo) should be treated as UTC."""
        future = datetime.utcnow() + timedelta(days=10)
        assert handler.check_subscription_valid(future) is True

    def test_just_expired(self, handler):
        """Subscription that just expired (1 second ago) should be invalid."""
        just_past = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert handler.check_subscription_valid(just_past) is False


# Tests for generate_token

class TestGenerateToken:
    def test_token_is_valid_jwt(self, handler, auth_config):
        """Generated token should be a valid JWT decodable with the secret."""
        user_id = "test-user-123"
        token = handler.generate_token(user_id)

        payload = jwt.decode(token, auth_config.jwt_secret, algorithms=["HS256"])
        assert payload["sub"] == user_id

    def test_token_has_24_hour_expiry(self, handler, auth_config):
        """Token should expire approximately 24 hours from now."""
        user_id = "test-user-123"
        token = handler.generate_token(user_id)

        payload = jwt.decode(token, auth_config.jwt_secret, algorithms=["HS256"])
        exp = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        iat = datetime.fromtimestamp(payload["iat"], tz=timezone.utc)

        # Expiry should be ~24 hours after issued
        diff = exp - iat
        assert timedelta(hours=23, minutes=59) <= diff <= timedelta(hours=24, minutes=1)

    def test_token_contains_iat(self, handler, auth_config):
        """Token should contain an 'iat' (issued at) claim."""
        token = handler.generate_token("user-1")
        payload = jwt.decode(token, auth_config.jwt_secret, algorithms=["HS256"])
        assert "iat" in payload

    def test_token_uses_hs256(self, handler, auth_config):
        """Token should use HS256 algorithm."""
        token = handler.generate_token("user-1")
        header = jwt.get_unverified_header(token)
        assert header["alg"] == "HS256"


# Tests for rate_limit_check

class TestRateLimitCheck:
    def test_first_attempt_allowed(self, handler):
        """First authentication attempt should be allowed."""
        assert handler.rate_limit_check("user-1") is True

    def test_blocked_after_max_attempts(self, handler):
        """User should be blocked after 5 failed attempts."""
        user_id = "user-rate-limited"

        # Simulate 5 failed attempts
        for _ in range(5):
            handler._record_failed_attempt(user_id)

        assert handler.rate_limit_check(user_id) is False

    def test_not_blocked_before_max_attempts(self, handler):
        """User should not be blocked before reaching 5 failed attempts."""
        user_id = "user-not-blocked"

        # Simulate 4 failed attempts (below limit)
        for _ in range(4):
            handler._record_failed_attempt(user_id)

        assert handler.rate_limit_check(user_id) is True

    def test_reset_clears_block(self, handler):
        """Resetting attempts should clear the block."""
        user_id = "user-reset"

        # Block the user
        for _ in range(5):
            handler._record_failed_attempt(user_id)
        assert handler.rate_limit_check(user_id) is False

        # Reset
        handler._reset_attempts(user_id)
        assert handler.rate_limit_check(user_id) is True


# Tests for authenticate (full flow)

class TestAuthenticate:
    def test_successful_authentication(self, handler, mock_db, auth_config):
        """Successful auth should return a valid JWT token."""
        user_id = str(uuid.uuid4())
        password = "SecurePass123!"
        expiry = datetime.now(timezone.utc) + timedelta(days=15)
        user_doc = _create_user_doc(user_id, password, expiry)

        mock_db.find_one.return_value = user_doc

        result = handler.authenticate(user_id, password)

        assert result.success is True
        assert result.token is not None
        assert result.error_message is None

        # Verify token is valid
        payload = jwt.decode(result.token, auth_config.jwt_secret, algorithms=["HS256"])
        assert payload["sub"] == user_id

    def test_user_not_found(self, handler, mock_db):
        """Non-existent user should get generic error."""
        mock_db.find_one.return_value = None

        result = handler.authenticate("nonexistent-user", "password")

        assert result.success is False
        assert result.token is None
        assert result.error_message == GENERIC_ERROR_MESSAGE

    def test_wrong_password(self, handler, mock_db):
        """Wrong password should get generic error."""
        user_id = str(uuid.uuid4())
        expiry = datetime.now(timezone.utc) + timedelta(days=15)
        user_doc = _create_user_doc(user_id, "CorrectPassword1!", expiry)

        mock_db.find_one.return_value = user_doc

        result = handler.authenticate(user_id, "WrongPassword1!")

        assert result.success is False
        assert result.token is None
        assert result.error_message == GENERIC_ERROR_MESSAGE

    def test_expired_subscription(self, handler, mock_db):
        """Expired subscription should return subscription expired error."""
        user_id = str(uuid.uuid4())
        password = "SecurePass123!"
        expiry = datetime.now(timezone.utc) - timedelta(days=1)
        user_doc = _create_user_doc(user_id, password, expiry)

        mock_db.find_one.return_value = user_doc

        result = handler.authenticate(user_id, password)

        assert result.success is False
        assert result.token is None
        assert result.error_message == EXPIRED_SUBSCRIPTION_MESSAGE

    def test_rate_limited_user(self, handler, mock_db):
        """Rate-limited user should get rate limit error without DB lookup."""
        user_id = "rate-limited-user"

        # Block the user
        for _ in range(5):
            handler._record_failed_attempt(user_id)

        result = handler.authenticate(user_id, "any-password")

        assert result.success is False
        assert result.token is None
        assert result.error_message == RATE_LIMIT_MESSAGE
        # DB should not be called when rate limited
        mock_db.find_one.assert_not_called()

    def test_failed_attempts_increment(self, handler, mock_db):
        """Failed attempts should increment the counter."""
        user_id = str(uuid.uuid4())
        mock_db.find_one.return_value = None

        # Make 4 failed attempts
        for _ in range(4):
            result = handler.authenticate(user_id, "wrong")
            assert result.success is False

        # 5th attempt should still work (block happens after 5th failure)
        assert handler.rate_limit_check(user_id) is True

        # 5th failed attempt triggers block
        handler.authenticate(user_id, "wrong")
        assert handler.rate_limit_check(user_id) is False

    def test_successful_auth_resets_attempts(self, handler, mock_db, auth_config):
        """Successful authentication should reset the failed attempt counter."""
        user_id = str(uuid.uuid4())
        password = "SecurePass123!"
        expiry = datetime.now(timezone.utc) + timedelta(days=15)
        user_doc = _create_user_doc(user_id, password, expiry)

        mock_db.find_one.return_value = user_doc

        # Record some failed attempts
        for _ in range(3):
            handler._record_failed_attempt(user_id)

        # Successful auth
        result = handler.authenticate(user_id, password)
        assert result.success is True

        # Attempts should be reset
        assert handler.rate_limit_check(user_id) is True


# Tests for AuthResult dataclass

class TestAuthResult:
    def test_success_result(self):
        """AuthResult for success should have token and no error."""
        result = AuthResult(token="some-token", success=True)
        assert result.token == "some-token"
        assert result.success is True
        assert result.error_message is None

    def test_failure_result(self):
        """AuthResult for failure should have no token and an error."""
        result = AuthResult(token=None, success=False, error_message="Error")
        assert result.token is None
        assert result.success is False
        assert result.error_message == "Error"


# Tests for AuthenticationInterface

class TestAuthenticationInterface:
    def test_is_abstract(self):
        """AuthenticationInterface should not be instantiable directly."""
        with pytest.raises(TypeError):
            AuthenticationInterface()
