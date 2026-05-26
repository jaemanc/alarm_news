"""
Subscription manager for the Alarm News System.

This module implements subscription renewal and cancellation with:
- Renewal calculation: max(current_expiry, now) + 30 days
- Early renewal allowed up to 7 days before expiry
- Cancellation: delete user document from MongoDB
- Token invalidation via CacheInterface (Redis with TTL)

Design:
    - Abstract SubscriptionManagerInterface for extensibility
    - Concrete SubscriptionManager using MongoDB + CacheInterface
    - RenewalResult and CancellationResult dataclasses for structured responses
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.shared.cache import CacheInterface
from src.shared.database import DatabaseInterface

logger = logging.getLogger(__name__)

# Constants
SUBSCRIPTION_DURATION_DAYS = 30
EARLY_RENEWAL_DAYS = 7
TOKEN_INVALIDATION_KEY_PREFIX = "token:invalidated:"
TOKEN_INVALIDATION_TTL_HOURS = 24


@dataclass
class RenewalResult:
    """
    Result of a subscription renewal attempt.

    Attributes:
        new_expiry: The new subscription expiry datetime (None on failure)
        success: Whether the renewal succeeded
        error_message: Description of failure (None on success)
    """
    new_expiry: Optional[datetime]
    success: bool
    error_message: Optional[str] = None


@dataclass
class CancellationResult:
    """
    Result of a subscription cancellation attempt.

    Attributes:
        success: Whether the cancellation succeeded
        error_message: Description of failure (None on success)
    """
    success: bool
    error_message: Optional[str] = None


class SubscriptionManagerInterface(ABC):
    """
    Abstract interface for subscription management.

    Allows alternative implementations to be substituted
    by implementing this interface.
    """

    @abstractmethod
    def renew_subscription(self, user_id: str) -> RenewalResult:
        """
        Renew a user's subscription for 30 days.

        Args:
            user_id: The user's unique identifier.

        Returns:
            RenewalResult with new expiry on success, or error on failure.
        """
        ...

    @abstractmethod
    def cancel_subscription(self, user_id: str) -> CancellationResult:
        """
        Cancel a subscription and delete user data.

        Args:
            user_id: The user's unique identifier.

        Returns:
            CancellationResult indicating success or failure.
        """
        ...

    @abstractmethod
    def calculate_new_expiry(self, current_expiry: Optional[datetime]) -> datetime:
        """
        Calculate new subscription expiry.

        New expiry = max(current_expiry, now) + 30 days.

        Args:
            current_expiry: The current subscription expiry timestamp.

        Returns:
            The new subscription expiry datetime.
        """
        ...

    @abstractmethod
    def invalidate_tokens(self, user_id: str) -> None:
        """
        Invalidate all authentication tokens for a user.

        Args:
            user_id: The user's unique identifier.
        """
        ...


class SubscriptionManager(SubscriptionManagerInterface):
    """
    Concrete subscription manager using MongoDB and CacheInterface.

    Implements subscription renewal and cancellation logic including
    expiry calculation, early renewal validation, and token invalidation.
    """

    def __init__(
        self,
        db: DatabaseInterface,
        cache: CacheInterface,
    ) -> None:
        """
        Initialize the subscription manager.

        Args:
            db: Database interface for user data operations.
            cache: Cache interface for token invalidation storage.
        """
        self._db = db
        self._cache = cache

    def calculate_new_expiry(self, current_expiry: Optional[datetime]) -> datetime:
        """
        Calculate new subscription expiry as max(current_expiry, now) + 30 days.

        If the subscription has already expired (current_expiry < now),
        the new expiry is calculated from the current timestamp.
        If the subscription is still active (current_expiry >= now),
        the new expiry is calculated from the current expiry.

        Args:
            current_expiry: The current subscription expiry timestamp.

        Returns:
            The new subscription expiry datetime (timezone-aware UTC).
        """
        now = datetime.now(timezone.utc)

        if current_expiry is None:
            base = now
        else:
            # Ensure timezone-aware comparison
            if current_expiry.tzinfo is None:
                current_expiry = current_expiry.replace(tzinfo=timezone.utc)
            base = max(current_expiry, now)

        return base + timedelta(days=SUBSCRIPTION_DURATION_DAYS)

    def _can_renew(self, current_expiry: Optional[datetime]) -> bool:
        """
        Check if renewal is allowed (up to 7 days before expiry).

        Renewal is allowed when:
        - The subscription has already expired (always allowed)
        - The subscription expires within 7 days (early renewal)

        Args:
            current_expiry: The current subscription expiry timestamp.

        Returns:
            True if renewal is allowed, False otherwise.
        """
        if current_expiry is None:
            return True

        now = datetime.now(timezone.utc)

        # Ensure timezone-aware comparison
        if current_expiry.tzinfo is None:
            current_expiry = current_expiry.replace(tzinfo=timezone.utc)

        # If already expired, renewal is always allowed
        if current_expiry <= now:
            return True

        # Allow renewal up to 7 days before expiry
        days_until_expiry = (current_expiry - now).total_seconds() / 86400
        return days_until_expiry <= EARLY_RENEWAL_DAYS

    def renew_subscription(self, user_id: str) -> RenewalResult:
        """
        Renew a user's subscription for 30 days.

        Process:
            1. Retrieve user from MongoDB by user_id
            2. Check if renewal is allowed (up to 7 days before expiry)
            3. Calculate new expiry: max(current_expiry, now) + 30 days
            4. Update subscription_expiry in MongoDB

        Args:
            user_id: The user's unique identifier.

        Returns:
            RenewalResult with new expiry on success, or error on failure.
        """
        # Step 1: Retrieve user from MongoDB
        user_doc = self._db.find_one("users", {"user_id": user_id})
        if user_doc is None:
            logger.warning("Renewal failed for user %s: user not found", user_id)
            return RenewalResult(
                new_expiry=None,
                success=False,
                error_message="User not found",
            )

        current_expiry = user_doc.get("subscription_expiry")

        # Step 2: Check if renewal is allowed
        if not self._can_renew(current_expiry):
            logger.info(
                "Renewal rejected for user %s: subscription not within renewal window",
                user_id,
            )
            return RenewalResult(
                new_expiry=None,
                success=False,
                error_message="Renewal not allowed yet. Renewal is available up to 7 days before expiry.",
            )

        # Step 3: Calculate new expiry
        new_expiry = self.calculate_new_expiry(current_expiry)

        # Step 4: Update subscription_expiry in MongoDB
        try:
            updated = self._db.update_one(
                "users",
                {"user_id": user_id},
                {"$set": {"subscription_expiry": new_expiry}},
            )
        except Exception as e:
            logger.error("Renewal failed for user %s: MongoDB error: %s", user_id, str(e))
            return RenewalResult(
                new_expiry=None,
                success=False,
                error_message="Renewal failed due to storage error",
            )

        if not updated:
            logger.error("Renewal failed for user %s: document not modified", user_id)
            return RenewalResult(
                new_expiry=None,
                success=False,
                error_message="Renewal failed due to storage error",
            )

        logger.info("Subscription renewed for user %s. New expiry: %s", user_id, new_expiry.isoformat())
        return RenewalResult(
            new_expiry=new_expiry,
            success=True,
        )

    def invalidate_tokens(self, user_id: str) -> None:
        """
        Invalidate all authentication tokens for a user by storing
        an invalidation marker in the cache with a TTL.

        The TTL matches the maximum token lifetime (24 hours) so that
        after the TTL expires, any previously issued tokens will have
        also expired naturally.

        Args:
            user_id: The user's unique identifier.
        """
        key = f"{TOKEN_INVALIDATION_KEY_PREFIX}{user_id}"
        ttl = timedelta(hours=TOKEN_INVALIDATION_TTL_HOURS)
        self._cache.set(key, True, ttl=ttl)
        logger.info("Tokens invalidated for user %s", user_id)

    def cancel_subscription(self, user_id: str) -> CancellationResult:
        """
        Cancel a subscription and delete user data from MongoDB.

        Process:
            1. Retrieve user from MongoDB to confirm existence
            2. Invalidate all authentication tokens
            3. Delete user document from MongoDB

        Args:
            user_id: The user's unique identifier.

        Returns:
            CancellationResult indicating success or failure.
        """
        # Step 1: Verify user exists
        user_doc = self._db.find_one("users", {"user_id": user_id})
        if user_doc is None:
            logger.warning("Cancellation failed for user %s: user not found", user_id)
            return CancellationResult(
                success=False,
                error_message="User not found",
            )

        # Step 2: Invalidate tokens before deletion
        self.invalidate_tokens(user_id)

        # Step 3: Delete user document from MongoDB
        try:
            deleted = self._db.delete_one("users", {"user_id": user_id})
        except Exception as e:
            logger.error("Cancellation failed for user %s: MongoDB error: %s", user_id, str(e))
            return CancellationResult(
                success=False,
                error_message="Cancellation failed due to storage error",
            )

        if not deleted:
            logger.error("Cancellation failed for user %s: document not deleted", user_id)
            return CancellationResult(
                success=False,
                error_message="Cancellation failed due to storage error",
            )

        logger.info("Subscription cancelled and user data deleted for user %s", user_id)
        return CancellationResult(success=True)
