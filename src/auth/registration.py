"""
User registration handler for the Alarm News System.

This module implements user registration with email validation, keyword validation,
secure password generation, bcrypt hashing, and MongoDB storage.

Design:
    - Abstract RegistrationInterface for extensibility
    - Concrete UserRegistrationHandler using MongoDB
    - RegistrationResult dataclass for structured responses
"""
import logging
import re
import secrets
import string
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import bcrypt

from src.shared.database import DatabaseInterface
from src.shared.models import User

logger = logging.getLogger(__name__)

# Validation constants
EMAIL_REGEX = re.compile(r"^[^@]+@[^@]+$")
MIN_KEYWORDS = 1
MAX_KEYWORDS = 20
MIN_KEYWORD_LENGTH = 1
MAX_KEYWORD_LENGTH = 100
PASSWORD_LENGTH = 16
SUBSCRIPTION_DAYS = 30
BCRYPT_COST_FACTOR = 12


@dataclass
class RegistrationResult:
    """
    Result of a user registration attempt.

    Attributes:
        user_id: The generated unique user ID (None on failure)
        password: The generated plaintext password (None on failure)
        subscription_expiry: When the subscription expires (None on failure)
        success: Whether registration succeeded
        error_message: Description of failure (None on success)
    """
    user_id: Optional[str]
    password: Optional[str]
    subscription_expiry: Optional[datetime]
    success: bool
    error_message: Optional[str] = None


class RegistrationInterface(ABC):
    """
    Abstract interface for user registration.

    Allows alternative implementations (e.g., different databases or
    registration flows) to be substituted.
    """

    @abstractmethod
    def register_user(self, email: str, keywords: List[str]) -> RegistrationResult:
        """
        Register a new user with email and keywords.

        Args:
            email: User's email address.
            keywords: List of keyword strings for news/stock matching.

        Returns:
            RegistrationResult with user_id, password, and subscription_expiry on success.
        """
        ...

    @abstractmethod
    def validate_email(self, email: str) -> bool:
        """
        Validate email format: [local-part]@[domain] where both parts are non-empty.

        Args:
            email: Email address to validate.

        Returns:
            True if valid, False otherwise.
        """
        ...

    @abstractmethod
    def validate_keywords(self, keywords: List[str]) -> bool:
        """
        Validate keywords: at least 1, each 1-100 characters, max 20 keywords.

        Args:
            keywords: List of keyword strings.

        Returns:
            True if valid, False otherwise.
        """
        ...

    @abstractmethod
    def generate_password(self) -> str:
        """
        Generate a random password with 12+ characters including uppercase,
        lowercase, numbers, and special characters.

        Returns:
            Generated plaintext password.
        """
        ...

    @abstractmethod
    def hash_password(self, password: str) -> str:
        """
        Hash password using bcrypt with cost factor 12.

        Args:
            password: Plaintext password to hash.

        Returns:
            Bcrypt-hashed password string.
        """
        ...


class UserRegistrationHandler(RegistrationInterface):
    """
    Concrete user registration handler using MongoDB for storage.

    Implements all registration logic including validation, password generation,
    hashing, and database persistence.
    """

    def __init__(self, db: DatabaseInterface) -> None:
        """
        Initialize the registration handler.

        Args:
            db: Database interface for user storage.
        """
        self._db = db

    def validate_email(self, email: str) -> bool:
        """
        Validate email format using regex pattern ^[^@]+@[^@]+$.

        The pattern ensures a non-empty local-part and non-empty domain
        separated by exactly one @ symbol.

        Args:
            email: Email address to validate.

        Returns:
            True if the email matches the pattern, False otherwise.
        """
        if not email or not isinstance(email, str):
            return False
        return EMAIL_REGEX.match(email) is not None

    def validate_keywords(self, keywords: List[str]) -> bool:
        """
        Validate keywords list.

        Rules:
            - At least 1 keyword required
            - Maximum 20 keywords allowed
            - Each keyword must be between 1 and 100 characters

        Args:
            keywords: List of keyword strings.

        Returns:
            True if all validation rules pass, False otherwise.
        """
        if not keywords or not isinstance(keywords, list):
            return False
        if len(keywords) < MIN_KEYWORDS or len(keywords) > MAX_KEYWORDS:
            return False
        for keyword in keywords:
            if not isinstance(keyword, str):
                return False
            if len(keyword) < MIN_KEYWORD_LENGTH or len(keyword) > MAX_KEYWORD_LENGTH:
                return False
        return True

    def generate_password(self) -> str:
        """
        Generate a cryptographically secure random password.

        The password is at least 12 characters (default 16) and guaranteed to
        contain at least one uppercase letter, one lowercase letter, one digit,
        and one special character. Uses the secrets module for secure randomness.

        Returns:
            Generated plaintext password string.
        """
        # Character pools
        uppercase = string.ascii_uppercase
        lowercase = string.ascii_lowercase
        digits = string.digits
        special = "!@#$%^&*()-_=+[]{}|;:,.<>?"

        # Guarantee at least one of each required type
        password_chars = [
            secrets.choice(uppercase),
            secrets.choice(lowercase),
            secrets.choice(digits),
            secrets.choice(special),
        ]

        # Fill remaining characters from all pools
        all_chars = uppercase + lowercase + digits + special
        remaining_length = PASSWORD_LENGTH - len(password_chars)
        for _ in range(remaining_length):
            password_chars.append(secrets.choice(all_chars))

        # Shuffle to avoid predictable positions
        # Use Fisher-Yates shuffle with secrets for secure randomization
        for i in range(len(password_chars) - 1, 0, -1):
            j = secrets.randbelow(i + 1)
            password_chars[i], password_chars[j] = password_chars[j], password_chars[i]

        return "".join(password_chars)

    def hash_password(self, password: str) -> str:
        """
        Hash password using bcrypt with cost factor 12.

        Args:
            password: Plaintext password to hash.

        Returns:
            Bcrypt-hashed password as a UTF-8 string.
        """
        salt = bcrypt.gensalt(rounds=BCRYPT_COST_FACTOR)
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed.decode("utf-8")

    def register_user(self, email: str, keywords: List[str]) -> RegistrationResult:
        """
        Register a new user with email and keywords.

        Process:
            1. Validate email format
            2. Validate keywords
            3. Generate unique user_id (UUID4)
            4. Generate secure random password
            5. Hash password with bcrypt (cost factor 12)
            6. Set subscription_expiry to 30 days from now
            7. Store user document in MongoDB
            8. Return user_id, password, subscription_expiry

        Args:
            email: User's email address.
            keywords: List of keyword strings for news/stock matching.

        Returns:
            RegistrationResult with credentials on success, or error on failure.
        """
        # Step 1: Validate email
        if not self.validate_email(email):
            logger.warning("Registration failed: invalid email format '%s'", email)
            return RegistrationResult(
                user_id=None,
                password=None,
                subscription_expiry=None,
                success=False,
                error_message="Invalid email format",
            )

        # Step 2: Validate keywords
        if not self.validate_keywords(keywords):
            logger.warning("Registration failed: invalid keywords")
            return RegistrationResult(
                user_id=None,
                password=None,
                subscription_expiry=None,
                success=False,
                error_message="Invalid keywords: at least 1 keyword required, each 1-100 characters, maximum 20 keywords",
            )

        # Step 3: Generate unique user_id
        user_id = str(uuid.uuid4())

        # Step 4: Generate secure password
        password = self.generate_password()

        # Step 5: Hash password
        hashed_password = self.hash_password(password)

        # Step 6: Set subscription expiry (30 days from now)
        subscription_expiry = datetime.now(timezone.utc) + timedelta(days=SUBSCRIPTION_DAYS)

        # Step 7: Store user in MongoDB
        user = User(
            user_id=user_id,
            hashed_password=hashed_password,
            email=email,
            keywords=keywords,
            notification_times=[],
            subscription_expiry=subscription_expiry,
        )

        try:
            self._db.insert_one("users", user.to_dict())
            logger.info("User registered successfully: user_id=%s", user_id)
        except Exception as e:
            logger.error("Registration failed: MongoDB write error: %s", str(e))
            return RegistrationResult(
                user_id=None,
                password=None,
                subscription_expiry=None,
                success=False,
                error_message="Storage failure: unable to save user data",
            )

        # Step 8: Return result
        return RegistrationResult(
            user_id=user_id,
            password=password,
            subscription_expiry=subscription_expiry,
            success=True,
        )
