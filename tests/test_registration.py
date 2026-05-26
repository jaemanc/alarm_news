"""
Unit tests for the user registration handler.

Tests cover:
- Email validation with valid and invalid formats
- Keyword validation with edge cases
- Password generation meets requirements
- Bcrypt hashing with cost factor 12
- Full registration flow with mock database
- Error handling for database failures
"""
import re
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import bcrypt
import pytest

from src.auth.registration import (
    RegistrationResult,
    UserRegistrationHandler,
)
from src.shared.database import DatabaseInterface


class FakeDatabaseInterface(DatabaseInterface):
    """In-memory fake database for testing."""

    def __init__(self):
        self.documents: Dict[str, List[Dict[str, Any]]] = {}
        self.should_fail = False

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def health_check(self) -> bool:
        return True

    def get_collection(self, name: str) -> Any:
        return self.documents.get(name, [])

    def insert_one(self, collection: str, document: Dict[str, Any]) -> Optional[str]:
        if self.should_fail:
            raise Exception("Database write failure")
        if collection not in self.documents:
            self.documents[collection] = []
        self.documents[collection].append(document)
        return "fake_id"

    def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for doc in self.documents.get(collection, []):
            if all(doc.get(k) == v for k, v in query.items()):
                return doc
        return None

    def update_one(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        return False

    def delete_one(self, collection: str, query: Dict[str, Any]) -> bool:
        return False

    def find_many(self, collection: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []


@pytest.fixture
def fake_db():
    """Create a fake database for testing."""
    return FakeDatabaseInterface()


@pytest.fixture
def handler(fake_db):
    """Create a registration handler with fake database."""
    return UserRegistrationHandler(db=fake_db)


class TestValidateEmail:
    """Tests for email validation."""

    def test_valid_email_simple(self, handler):
        assert handler.validate_email("user@example.com") is True

    def test_valid_email_with_dots(self, handler):
        assert handler.validate_email("user.name@example.co.uk") is True

    def test_valid_email_with_plus(self, handler):
        assert handler.validate_email("user+tag@example.com") is True

    def test_valid_email_minimal(self, handler):
        assert handler.validate_email("a@b") is True

    def test_invalid_email_no_at(self, handler):
        assert handler.validate_email("userexample.com") is False

    def test_invalid_email_empty_local(self, handler):
        assert handler.validate_email("@example.com") is False

    def test_invalid_email_empty_domain(self, handler):
        assert handler.validate_email("user@") is False

    def test_invalid_email_empty_string(self, handler):
        assert handler.validate_email("") is False

    def test_invalid_email_none(self, handler):
        assert handler.validate_email(None) is False

    def test_invalid_email_multiple_at(self, handler):
        # The regex ^[^@]+@[^@]+$ rejects multiple @ symbols
        assert handler.validate_email("user@@example.com") is False

    def test_invalid_email_at_only(self, handler):
        assert handler.validate_email("@") is False


class TestValidateKeywords:
    """Tests for keyword validation."""

    def test_valid_single_keyword(self, handler):
        assert handler.validate_keywords(["python"]) is True

    def test_valid_multiple_keywords(self, handler):
        assert handler.validate_keywords(["python", "javascript", "rust"]) is True

    def test_valid_max_keywords(self, handler):
        keywords = [f"keyword{i}" for i in range(20)]
        assert handler.validate_keywords(keywords) is True

    def test_valid_keyword_min_length(self, handler):
        assert handler.validate_keywords(["a"]) is True

    def test_valid_keyword_max_length(self, handler):
        assert handler.validate_keywords(["a" * 100]) is True

    def test_invalid_empty_list(self, handler):
        assert handler.validate_keywords([]) is False

    def test_invalid_none(self, handler):
        assert handler.validate_keywords(None) is False

    def test_invalid_too_many_keywords(self, handler):
        keywords = [f"keyword{i}" for i in range(21)]
        assert handler.validate_keywords(keywords) is False

    def test_invalid_keyword_too_long(self, handler):
        assert handler.validate_keywords(["a" * 101]) is False

    def test_invalid_keyword_empty_string(self, handler):
        assert handler.validate_keywords([""]) is False

    def test_invalid_keyword_non_string(self, handler):
        assert handler.validate_keywords([123]) is False

    def test_invalid_mixed_valid_and_empty(self, handler):
        assert handler.validate_keywords(["valid", ""]) is False


class TestGeneratePassword:
    """Tests for password generation."""

    def test_password_minimum_length(self, handler):
        password = handler.generate_password()
        assert len(password) >= 12

    def test_password_contains_uppercase(self, handler):
        password = handler.generate_password()
        assert any(c in string.ascii_uppercase for c in password)

    def test_password_contains_lowercase(self, handler):
        password = handler.generate_password()
        assert any(c in string.ascii_lowercase for c in password)

    def test_password_contains_digit(self, handler):
        password = handler.generate_password()
        assert any(c in string.digits for c in password)

    def test_password_contains_special(self, handler):
        password = handler.generate_password()
        special = "!@#$%^&*()-_=+[]{}|;:,.<>?"
        assert any(c in special for c in password)

    def test_password_uniqueness(self, handler):
        """Generated passwords should be unique (probabilistic test)."""
        passwords = {handler.generate_password() for _ in range(100)}
        assert len(passwords) == 100


class TestHashPassword:
    """Tests for bcrypt password hashing."""

    def test_hash_produces_valid_bcrypt(self, handler):
        password = "TestPassword123!"
        hashed = handler.hash_password(password)
        assert hashed.startswith("$2b$")

    def test_hash_uses_cost_factor_12(self, handler):
        password = "TestPassword123!"
        hashed = handler.hash_password(password)
        # bcrypt format: $2b$12$...
        assert "$2b$12$" in hashed

    def test_hash_verifies_correctly(self, handler):
        password = "TestPassword123!"
        hashed = handler.hash_password(password)
        assert bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

    def test_hash_rejects_wrong_password(self, handler):
        password = "TestPassword123!"
        hashed = handler.hash_password(password)
        assert not bcrypt.checkpw("WrongPassword!".encode("utf-8"), hashed.encode("utf-8"))

    def test_hash_different_each_time(self, handler):
        """Same password should produce different hashes (different salts)."""
        password = "TestPassword123!"
        hash1 = handler.hash_password(password)
        hash2 = handler.hash_password(password)
        assert hash1 != hash2


class TestRegisterUser:
    """Tests for the full registration flow."""

    def test_successful_registration(self, handler, fake_db):
        result = handler.register_user("user@example.com", ["python", "news"])

        assert result.success is True
        assert result.user_id is not None
        assert result.password is not None
        assert result.subscription_expiry is not None
        assert result.error_message is None

    def test_registration_returns_valid_uuid(self, handler):
        result = handler.register_user("user@example.com", ["python"])

        # Validate UUID4 format
        import uuid
        parsed = uuid.UUID(result.user_id, version=4)
        assert str(parsed) == result.user_id

    def test_registration_sets_30_day_expiry(self, handler):
        before = datetime.now(timezone.utc)
        result = handler.register_user("user@example.com", ["python"])
        after = datetime.now(timezone.utc)

        expected_min = before + timedelta(days=30)
        expected_max = after + timedelta(days=30)

        assert expected_min <= result.subscription_expiry <= expected_max

    def test_registration_stores_user_in_db(self, handler, fake_db):
        result = handler.register_user("user@example.com", ["python", "news"])

        stored = fake_db.find_one("users", {"user_id": result.user_id})
        assert stored is not None
        assert stored["email"] == "user@example.com"
        assert stored["keywords"] == ["python", "news"]
        assert stored["user_id"] == result.user_id

    def test_registration_stores_hashed_password(self, handler, fake_db):
        result = handler.register_user("user@example.com", ["python"])

        stored = fake_db.find_one("users", {"user_id": result.user_id})
        # Verify the stored hash matches the returned plaintext password
        assert bcrypt.checkpw(
            result.password.encode("utf-8"),
            stored["hashed_password"].encode("utf-8"),
        )

    def test_registration_invalid_email(self, handler):
        result = handler.register_user("invalid-email", ["python"])

        assert result.success is False
        assert result.user_id is None
        assert result.password is None
        assert "email" in result.error_message.lower()

    def test_registration_invalid_keywords(self, handler):
        result = handler.register_user("user@example.com", [])

        assert result.success is False
        assert result.user_id is None
        assert "keyword" in result.error_message.lower()

    def test_registration_db_failure(self, handler, fake_db):
        fake_db.should_fail = True
        result = handler.register_user("user@example.com", ["python"])

        assert result.success is False
        assert result.user_id is None
        assert "storage" in result.error_message.lower()

    def test_registration_password_meets_requirements(self, handler):
        result = handler.register_user("user@example.com", ["python"])

        password = result.password
        assert len(password) >= 12
        assert any(c in string.ascii_uppercase for c in password)
        assert any(c in string.ascii_lowercase for c in password)
        assert any(c in string.digits for c in password)
