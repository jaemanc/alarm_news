"""
Authentication handler for the Alarm News System.

This module implements user authentication with:
- Password verification using bcrypt.checkpw()
- Subscription expiry checking
- JWT token generation (HS256, 24-hour expiry)
- Rate limiting via CacheInterface (5 attempts per 15 minutes)
- Generic error messages for security

Design:
    - Abstract AuthenticationInterface for extensibility
    - Concrete AuthenticationHandler using MongoDB + CacheInterface
    - AuthResult dataclass for structured responses
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt

from src.shared.cache import CacheInterface
from src.shared.config import AuthConfig, RateLimitConfig
from src.shared.database import DatabaseInterface
from src.shared.models import User

logger = logging.getLogger(__name__)

# Constants
GENERIC_ERROR_MESSAGE = "Invalid credentials"
EXPIRED_SUBSCRIPTION_MESSAGE = "Subscription expired"
RATE_LIMIT_MESSAGE = "Too many authentication attempts. Please try again later."
RATE_LIMIT_KEY_PREFIX = "auth:attempts:"
RATE_LIMIT_BLOCK_KEY_PREFIX = "auth:blocked:"


@dataclass
class AuthResult:
    """
    Result of an authentication attempt.

    Attributes:
        token: JWT authentication token (None on failure)
        success: Whether authentication succeeded
        error_message: Description of failure (None on success)
    """
    token: Optional[str]
    success: bool
    error_message: Optional[str] = None


class AuthenticationInterface(ABC):
    """
    Abstract interface for user authentication.

    Allows alternative implementations (e.g., OAuth, LDAP) to be
    substituted by implementing this interface.
    """

    @abstractmethod
    def authenticate(self, user_id: str, password: str) -> AuthResult:
        """
        Authenticate a user with user_id and password.

        Args:
            user_id: The user's unique identifier.
            password: The plaintext password to verify.

        Returns:
            AuthResult with token on success, or error on failure.
        """
        ...

    @abstractmethod
    def verify_password(self, password: str, hashed: str) -> bool:
        """
        Verify a plaintext password against a bcrypt hash.

        Args:
            password: Plaintext password to verify.
            hashed: Bcrypt-hashed password to compare against.

        Returns:
            True if the password matches, False otherwise.
        """
        ...

    @abstractmethod
    def check_subscription_valid(self, expiry: Optional[datetime]) -> bool:
        """
        Check if a subscription has not expired.

        Args:
            expiry: The subscription expiry timestamp.

        Returns:
            True if the subscription is still valid, False otherwise.
        """
        ...

    @abstractmethod
    def generate_token(self, user_id: str) -> str:
        """
        Generate a JWT authentication token.

        Args:
            user_id: The user's unique identifier to encode in the token.

        Returns:
            Signed JWT token string.
        """
        ...

    @abstractmethod
    def rate_limit_check(self, user_id: str) -> bool:
        """
        Check if a user has exceeded the authentication rate limit.

        Args:
            user_id: The user's unique identifier.

        Returns:
            True if the user is within the rate limit, False if blocked.
        """
        ...


class AuthenticationHandler(AuthenticationInterface):
    """
    Concrete authentication handler using MongoDB, bcrypt, JWT, and CacheInterface.

    Implements all authentication logic including password verification,
    subscription checking, token generation, and rate limiting.
    """

    def __init__(
        self,
        db: DatabaseInterface,
        cache: CacheInterface,
        auth_config: Optional[AuthConfig] = None,
        rate_limit_config: Optional[RateLimitConfig] = None,
    ) -> None:
        """
        Initialize the authentication handler.

        Args:
            db: Database interface for user retrieval.
            cache: Cache interface for rate limiting storage.
            auth_config: Authentication configuration (JWT secret, expiry).
                If None, loads from global config.
            rate_limit_config: Rate limiting configuration (max attempts, window).
                If None, loads from global config.
        """
        self._db = db
        self._cache = cache

        if auth_config is None:
            from src.shared.config import get_config
            auth_config = get_config().auth
        if rate_limit_config is None:
            from src.shared.config import get_config
            rate_limit_config = get_config().rate_limit

        self._jwt_secret = auth_config.jwt_secret
        self._jwt_expiry_hours = auth_config.jwt_expiry_hours
        self._max_attempts = rate_limit_config.max_attempts
        self._window_minutes = rate_limit_config.window_minutes

    def verify_password(self, password: str, hashed: str) -> bool:
        """
        Verify a plaintext password against a bcrypt hash using bcrypt.checkpw().

        Args:
            password: Plaintext password to verify.
            hashed: Bcrypt-hashed password string.

        Returns:
            True if the password matches the hash, False otherwise.
        """
        try:
            return bcrypt.checkpw(
                password.encode("utf-8"),
                hashed.encode("utf-8"),
            )
        except (ValueError, TypeError) as e:
            logger.warning("Password verification error: %s", str(e))
            return False

    def check_subscription_valid(self, expiry: Optional[datetime]) -> bool:
        """
        Check if the subscription expiry is in the future.

        Args:
            expiry: The subscription expiry timestamp.

        Returns:
            True if expiry is after the current UTC time, False otherwise.
        """
        if expiry is None:
            return False
        now = datetime.now(timezone.utc)
        # Ensure expiry is timezone-aware for comparison
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return now < expiry

    def generate_token(self, user_id: str) -> str:
        """
        Generate a JWT token with HS256 algorithm and 24-hour expiry.

        The token payload includes:
            - sub: user_id
            - iat: issued at timestamp
            - exp: expiration timestamp (24 hours from now)

        Args:
            user_id: The user's unique identifier.

        Returns:
            Signed JWT token string.
        """
        now = datetime.now(timezone.utc)
        payload = {
            "sub": user_id,
            "iat": now,
            "exp": now + timedelta(hours=self._jwt_expiry_hours),
        }
        return jwt.encode(payload, self._jwt_secret, algorithm="HS256")

    def rate_limit_check(self, user_id: str) -> bool:
        """
        Check if a user is within the authentication rate limit.

        Uses CacheInterface to track failed attempts per user_id.
        Allows 5 attempts per 15-minute window. After the limit is exceeded,
        the user_id is blocked for 15 minutes.

        Args:
            user_id: The user's unique identifier.

        Returns:
            True if the user is allowed to attempt authentication.
            False if the user is blocked due to too many attempts.
        """
        block_key = f"{RATE_LIMIT_BLOCK_KEY_PREFIX}{user_id}"

        # Check if user is currently blocked
        if self._cache.exists(block_key):
            return False

        return True

    def _record_failed_attempt(self, user_id: str) -> None:
        """
        Record a failed authentication attempt and block if limit exceeded.

        Increments the attempt counter for the user. If the counter reaches
        the maximum allowed attempts, blocks the user for the configured window.

        Args:
            user_id: The user's unique identifier.
        """
        attempts_key = f"{RATE_LIMIT_KEY_PREFIX}{user_id}"
        block_key = f"{RATE_LIMIT_BLOCK_KEY_PREFIX}{user_id}"
        window = timedelta(minutes=self._window_minutes)

        # Get current attempt count
        current_attempts = self._cache.get(attempts_key)
        if current_attempts is None:
            current_attempts = 0

        current_attempts += 1
        self._cache.set(attempts_key, current_attempts, ttl=window)

        if current_attempts >= self._max_attempts:
            # Block the user for the window duration
            self._cache.set(block_key, True, ttl=window)
            logger.warning(
                "User %s blocked for %d minutes after %d failed attempts",
                user_id,
                self._window_minutes,
                current_attempts,
            )

    def _reset_attempts(self, user_id: str) -> None:
        """
        Reset the failed attempt counter on successful authentication.

        Args:
            user_id: The user's unique identifier.
        """
        attempts_key = f"{RATE_LIMIT_KEY_PREFIX}{user_id}"
        block_key = f"{RATE_LIMIT_BLOCK_KEY_PREFIX}{user_id}"
        self._cache.delete(attempts_key)
        self._cache.delete(block_key)

    def authenticate(self, user_id: str, password: str) -> AuthResult:
        """
        Authenticate a user with user_id and password.

        Process:
            1. Check rate limit (block if exceeded)
            2. Retrieve user from MongoDB by user_id
            3. Verify password using bcrypt.checkpw()
            4. Check subscription_expiry > current timestamp
            5. Generate JWT token with HS256, 24-hour expiry
            6. Return token on success

        Security:
            - Returns generic "Invalid credentials" for user-not-found and
              password-mismatch to prevent user enumeration.
            - Blocks user_id for 15 minutes after 5 failed attempts.

        Args:
            user_id: The user's unique identifier.
            password: The plaintext password to verify.

        Returns:
            AuthResult with token on success, or error message on failure.
        """
        # Step 1: Check rate limit
        if not self.rate_limit_check(user_id):
            logger.info("Authentication blocked for user %s: rate limit exceeded", user_id)
            return AuthResult(
                token=None,
                success=False,
                error_message=RATE_LIMIT_MESSAGE,
            )

        # Step 2: Retrieve user from MongoDB
        user_doc = self._db.find_one("users", {"user_id": user_id})
        if user_doc is None:
            self._record_failed_attempt(user_id)
            logger.info("Authentication failed for user %s: user not found", user_id)
            return AuthResult(
                token=None,
                success=False,
                error_message=GENERIC_ERROR_MESSAGE,
            )

        user = User.from_dict(user_doc)

        # Step 3: Verify password
        if not self.verify_password(password, user.hashed_password):
            self._record_failed_attempt(user_id)
            logger.info("Authentication failed for user %s: invalid password", user_id)
            return AuthResult(
                token=None,
                success=False,
                error_message=GENERIC_ERROR_MESSAGE,
            )

        # Step 4: Check subscription expiry
        if not self.check_subscription_valid(user.subscription_expiry):
            logger.info("Authentication failed for user %s: subscription expired", user_id)
            return AuthResult(
                token=None,
                success=False,
                error_message=EXPIRED_SUBSCRIPTION_MESSAGE,
            )

        # Step 5: Generate JWT token
        token = self.generate_token(user_id)

        # Reset failed attempts on success
        self._reset_attempts(user_id)

        logger.info("Authentication successful for user %s", user_id)

        # Step 6: Return token
        return AuthResult(
            token=token,
            success=True,
        )
