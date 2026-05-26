"""
Unit tests for the Subscription Manager.

Tests cover:
- Renewal calculation with expired and active subscriptions
- Early renewal (7 days before expiry)
- Renewal rejection when too early
- Cancellation deletes user data
- Token invalidation on cancellation
- Error handling for missing users and storage failures
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.auth.subscription import (
    CancellationResult,
    RenewalResult,
    SubscriptionManager,
    SUBSCRIPTION_DURATION_DAYS,
    EARLY_RENEWAL_DAYS,
    TOKEN_INVALIDATION_KEY_PREFIX,
    TOKEN_INVALIDATION_TTL_HOURS,
)


@pytest.fixture
def mock_db():
    """Create a mock database interface."""
    return MagicMock()


@pytest.fixture
def mock_cache():
    """Create a mock cache interface."""
    return MagicMock()


@pytest.fixture
def manager(mock_db, mock_cache):
    """Create a SubscriptionManager with mocked dependencies."""
    return SubscriptionManager(db=mock_db, cache=mock_cache)


class TestCalculateNewExpiry:
    """Tests for calculate_new_expiry method."""

    def test_expired_subscription_uses_now_as_base(self, manager):
        """When subscription is expired, new expiry = now + 30 days."""
        expired = datetime.now(timezone.utc) - timedelta(days=10)
        new_expiry = manager.calculate_new_expiry(expired)

        now = datetime.now(timezone.utc)
        expected_min = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) - timedelta(seconds=5)
        expected_max = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) + timedelta(seconds=5)

        assert expected_min <= new_expiry <= expected_max

    def test_active_subscription_uses_current_expiry_as_base(self, manager):
        """When subscription is active, new expiry = current_expiry + 30 days."""
        future_expiry = datetime.now(timezone.utc) + timedelta(days=3)
        new_expiry = manager.calculate_new_expiry(future_expiry)

        expected = future_expiry + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
        # Allow small time delta for test execution
        assert abs((new_expiry - expected).total_seconds()) < 1

    def test_none_expiry_uses_now_as_base(self, manager):
        """When current_expiry is None, new expiry = now + 30 days."""
        new_expiry = manager.calculate_new_expiry(None)

        now = datetime.now(timezone.utc)
        expected_min = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) - timedelta(seconds=5)
        expected_max = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) + timedelta(seconds=5)

        assert expected_min <= new_expiry <= expected_max

    def test_naive_datetime_treated_as_utc(self, manager):
        """Naive datetime (no tzinfo) is treated as UTC."""
        # A naive datetime in the future
        future_expiry = datetime.now() + timedelta(days=5)
        new_expiry = manager.calculate_new_expiry(future_expiry)

        # Should be future_expiry (as UTC) + 30 days
        expected_base = future_expiry.replace(tzinfo=timezone.utc)
        expected = expected_base + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
        assert abs((new_expiry - expected).total_seconds()) < 1


class TestCanRenew:
    """Tests for the _can_renew internal method."""

    def test_expired_subscription_can_renew(self, manager):
        """Expired subscriptions can always be renewed."""
        expired = datetime.now(timezone.utc) - timedelta(days=5)
        assert manager._can_renew(expired) is True

    def test_within_7_days_can_renew(self, manager):
        """Subscriptions expiring within 7 days can be renewed."""
        expiry = datetime.now(timezone.utc) + timedelta(days=6)
        assert manager._can_renew(expiry) is True

    def test_exactly_7_days_can_renew(self, manager):
        """Subscriptions expiring in exactly 7 days can be renewed."""
        expiry = datetime.now(timezone.utc) + timedelta(days=7)
        assert manager._can_renew(expiry) is True

    def test_more_than_7_days_cannot_renew(self, manager):
        """Subscriptions with more than 7 days remaining cannot be renewed."""
        expiry = datetime.now(timezone.utc) + timedelta(days=8)
        assert manager._can_renew(expiry) is False

    def test_none_expiry_can_renew(self, manager):
        """None expiry (no subscription) can be renewed."""
        assert manager._can_renew(None) is True


class TestRenewSubscription:
    """Tests for renew_subscription method."""

    def test_successful_renewal_active_subscription(self, manager, mock_db):
        """Renew an active subscription within the renewal window."""
        current_expiry = datetime.now(timezone.utc) + timedelta(days=3)
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "subscription_expiry": current_expiry,
        }
        mock_db.update_one.return_value = True

        result = manager.renew_subscription("user-123")

        assert result.success is True
        assert result.new_expiry is not None
        expected = current_expiry + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
        assert abs((result.new_expiry - expected).total_seconds()) < 1
        assert result.error_message is None

        # Verify MongoDB update was called
        mock_db.update_one.assert_called_once()
        call_args = mock_db.update_one.call_args
        assert call_args[0][0] == "users"
        assert call_args[0][1] == {"user_id": "user-123"}

    def test_successful_renewal_expired_subscription(self, manager, mock_db):
        """Renew an expired subscription (base from now)."""
        expired = datetime.now(timezone.utc) - timedelta(days=10)
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "subscription_expiry": expired,
        }
        mock_db.update_one.return_value = True

        result = manager.renew_subscription("user-123")

        assert result.success is True
        assert result.new_expiry is not None
        now = datetime.now(timezone.utc)
        expected_min = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) - timedelta(seconds=5)
        expected_max = now + timedelta(days=SUBSCRIPTION_DURATION_DAYS) + timedelta(seconds=5)
        assert expected_min <= result.new_expiry <= expected_max

    def test_renewal_rejected_too_early(self, manager, mock_db):
        """Renewal rejected when subscription has more than 7 days remaining."""
        future_expiry = datetime.now(timezone.utc) + timedelta(days=20)
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "subscription_expiry": future_expiry,
        }

        result = manager.renew_subscription("user-123")

        assert result.success is False
        assert result.new_expiry is None
        assert "7 days" in result.error_message
        mock_db.update_one.assert_not_called()

    def test_renewal_user_not_found(self, manager, mock_db):
        """Renewal fails when user does not exist."""
        mock_db.find_one.return_value = None

        result = manager.renew_subscription("nonexistent-user")

        assert result.success is False
        assert result.new_expiry is None
        assert "not found" in result.error_message

    def test_renewal_mongodb_update_fails(self, manager, mock_db):
        """Renewal fails when MongoDB update returns False."""
        current_expiry = datetime.now(timezone.utc) + timedelta(days=3)
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "subscription_expiry": current_expiry,
        }
        mock_db.update_one.return_value = False

        result = manager.renew_subscription("user-123")

        assert result.success is False
        assert "storage error" in result.error_message

    def test_renewal_mongodb_exception(self, manager, mock_db):
        """Renewal fails when MongoDB raises an exception."""
        current_expiry = datetime.now(timezone.utc) + timedelta(days=3)
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "subscription_expiry": current_expiry,
        }
        mock_db.update_one.side_effect = Exception("Connection lost")

        result = manager.renew_subscription("user-123")

        assert result.success is False
        assert "storage error" in result.error_message


class TestCancelSubscription:
    """Tests for cancel_subscription method."""

    def test_successful_cancellation(self, manager, mock_db, mock_cache):
        """Cancel subscription deletes user and invalidates tokens."""
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "email": "test@example.com",
        }
        mock_db.delete_one.return_value = True

        result = manager.cancel_subscription("user-123")

        assert result.success is True
        assert result.error_message is None

        # Verify user was deleted
        mock_db.delete_one.assert_called_once_with("users", {"user_id": "user-123"})

        # Verify tokens were invalidated
        expected_key = f"{TOKEN_INVALIDATION_KEY_PREFIX}user-123"
        mock_cache.set.assert_called_once()
        call_args = mock_cache.set.call_args
        assert call_args[0][0] == expected_key
        assert call_args[0][1] is True

    def test_cancellation_user_not_found(self, manager, mock_db, mock_cache):
        """Cancellation fails when user does not exist."""
        mock_db.find_one.return_value = None

        result = manager.cancel_subscription("nonexistent-user")

        assert result.success is False
        assert "not found" in result.error_message
        mock_db.delete_one.assert_not_called()
        mock_cache.set.assert_not_called()

    def test_cancellation_mongodb_delete_fails(self, manager, mock_db, mock_cache):
        """Cancellation fails when MongoDB delete returns False."""
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "email": "test@example.com",
        }
        mock_db.delete_one.return_value = False

        result = manager.cancel_subscription("user-123")

        assert result.success is False
        assert "storage error" in result.error_message

    def test_cancellation_mongodb_exception(self, manager, mock_db, mock_cache):
        """Cancellation fails when MongoDB raises an exception."""
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "email": "test@example.com",
        }
        mock_db.delete_one.side_effect = Exception("Connection lost")

        result = manager.cancel_subscription("user-123")

        assert result.success is False
        assert "storage error" in result.error_message


class TestInvalidateTokens:
    """Tests for invalidate_tokens method."""

    def test_invalidate_tokens_stores_in_cache(self, manager, mock_cache):
        """Token invalidation stores a marker in cache with TTL."""
        manager.invalidate_tokens("user-123")

        expected_key = f"{TOKEN_INVALIDATION_KEY_PREFIX}user-123"
        mock_cache.set.assert_called_once()
        call_args = mock_cache.set.call_args
        assert call_args[0][0] == expected_key
        assert call_args[0][1] is True
        # Verify TTL is set
        ttl = call_args[1].get("ttl") or call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("ttl")
        assert ttl == timedelta(hours=TOKEN_INVALIDATION_TTL_HOURS)

    def test_cancellation_invalidates_before_delete(self, manager, mock_db, mock_cache):
        """Tokens are invalidated before the user document is deleted."""
        mock_db.find_one.return_value = {
            "user_id": "user-123",
            "email": "test@example.com",
        }
        mock_db.delete_one.return_value = True

        # Track call order
        call_order = []
        mock_cache.set.side_effect = lambda *args, **kwargs: call_order.append("cache_set")
        mock_db.delete_one.side_effect = lambda *args, **kwargs: (call_order.append("db_delete"), True)[1]

        manager.cancel_subscription("user-123")

        assert call_order == ["cache_set", "db_delete"]
