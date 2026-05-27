"""
Notification Time Evaluator for the Alarm News Scheduler.

Evaluates which users should receive notifications at the current time.
Uses 1-minute precision for time matching and consistent hashing to
distribute users across multiple scheduler instances.
"""
import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from src.scheduler.user_loader import UserNotificationConfig
from src.shared.models import NotificationEvent, NotificationTime

logger = logging.getLogger(__name__)


class NotificationTimeEvaluator:
    """
    Evaluates user notification times and generates notification events.

    Matches the current time (hour, minute) against each user's configured
    notification times. Distributes users across scheduler instances using
    consistent hashing based on user_id. Skips users with expired subscriptions.
    Generates a UUID4 event_id for each matched notification for idempotency.
    """

    def __init__(
        self,
        instance_id: int = 0,
        total_instances: int = 1,
    ):
        """
        Initialize the NotificationTimeEvaluator.

        Args:
            instance_id: The index of this scheduler instance (0-based).
            total_instances: Total number of scheduler instances.
        """
        if total_instances < 1:
            raise ValueError("total_instances must be at least 1")
        if instance_id < 0 or instance_id >= total_instances:
            raise ValueError(
                f"instance_id must be between 0 and {total_instances - 1}"
            )
        self._instance_id = instance_id
        self._total_instances = total_instances

    @property
    def instance_id(self) -> int:
        """The index of this scheduler instance."""
        return self._instance_id

    @property
    def total_instances(self) -> int:
        """Total number of scheduler instances."""
        return self._total_instances

    def evaluate_notification_times(
        self,
        current_time: datetime,
        users: List[UserNotificationConfig],
    ) -> List[NotificationEvent]:
        """
        Evaluate which users should receive notifications at the current time.

        For each user:
        1. Check if the user is assigned to this instance (consistent hashing)
        2. Skip users with expired subscriptions
        3. Match current time (hour, minute) against user notification times
        4. Generate a NotificationEvent with UUID4 event_id for each match

        Args:
            current_time: The current time to evaluate against.
            users: List of cached user notification configurations.

        Returns:
            List of NotificationEvent objects for users whose notification
            times match the current time.
        """
        events: List[NotificationEvent] = []

        for user in users:
            # Distribute users across instances using consistent hashing
            if not self._is_assigned_to_instance(user.user_id):
                continue

            # Skip users with expired subscriptions
            if self._is_subscription_expired(user, current_time):
                logger.debug(
                    "Skipping user %s: subscription expired", user.user_id
                )
                continue

            # Check if any notification time matches the current time
            for notification_time in user.notification_times:
                if self._matches_notification_time(current_time, notification_time):
                    event = NotificationEvent(
                        event_id=str(uuid.uuid4()),
                        user_id=user.user_id,
                        notification_timestamp=current_time,
                    )
                    events.append(event)
                    logger.info(
                        "Notification time matched for user %s at %02d:%02d "
                        "(event_id=%s)",
                        user.user_id,
                        notification_time.hour,
                        notification_time.minute,
                        event.event_id,
                    )
                    # Only generate one event per user per evaluation cycle
                    # even if multiple notification times match (edge case)
                    break

        logger.info(
            "Evaluated %d users, generated %d notification events "
            "(instance %d/%d)",
            len(users),
            len(events),
            self._instance_id,
            self._total_instances,
        )
        return events

    def _matches_notification_time(
        self, current: datetime, target: NotificationTime
    ) -> bool:
        """
        Check if the current time matches the target notification time.

        Uses 1-minute precision: matches on hour and minute only.

        Args:
            current: The current datetime.
            target: The target notification time (hour, minute).

        Returns:
            True if current hour and minute match the target.
        """
        return current.hour == target.hour and current.minute == target.minute

    def _is_assigned_to_instance(self, user_id: str) -> bool:
        """
        Determine if a user is assigned to this scheduler instance.

        Uses consistent hashing based on user_id to distribute users
        evenly across scheduler instances.

        Args:
            user_id: The user's unique identifier.

        Returns:
            True if this user should be handled by this instance.
        """
        if self._total_instances == 1:
            return True

        hash_value = self._consistent_hash(user_id)
        assigned_instance = hash_value % self._total_instances
        return assigned_instance == self._instance_id

    def _consistent_hash(self, user_id: str) -> int:
        """
        Compute a consistent hash for a user_id.

        Uses SHA-256 to produce a deterministic, well-distributed hash value.

        Args:
            user_id: The user's unique identifier.

        Returns:
            An integer hash value.
        """
        hash_bytes = hashlib.sha256(user_id.encode("utf-8")).digest()
        # Use first 8 bytes for a 64-bit integer
        return int.from_bytes(hash_bytes[:8], byteorder="big")

    def _is_subscription_expired(
        self, user: UserNotificationConfig, current_time: datetime
    ) -> bool:
        """
        Check if a user's subscription has expired.

        Args:
            user: The user's notification configuration.
            current_time: The current time to check against.

        Returns:
            True if the subscription is expired or not set.
        """
        if user.subscription_expiry is None:
            return True
        return user.subscription_expiry <= current_time
