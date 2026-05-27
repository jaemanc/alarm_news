"""
Unit tests for the Notification Time Evaluator component.

Tests cover:
- Notification time matching with 1-minute precision
- Consistent hashing distribution across scheduler instances
- Subscription expiry filtering
- UUID4 event_id generation for idempotency
- Edge cases: no users, no notification times, multiple matches
"""
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.scheduler.notification_evaluator import NotificationTimeEvaluator
from src.scheduler.user_loader import UserNotificationConfig
from src.shared.models import NotificationTime


@pytest.fixture
def evaluator():
    """Create a single-instance evaluator (handles all users)."""
    return NotificationTimeEvaluator(instance_id=0, total_instances=1)


@pytest.fixture
def active_user():
    """Create a user with an active subscription and notification times."""
    return UserNotificationConfig(
        user_id="user-001",
        email="alice@example.com",
        keywords=["python", "AI"],
        notification_times=[
            NotificationTime(hour=9, minute=0),
            NotificationTime(hour=17, minute=30),
        ],
        subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
    )


@pytest.fixture
def sample_users():
    """Create a list of sample users with various configurations."""
    return [
        UserNotificationConfig(
            user_id="user-001",
            email="alice@example.com",
            keywords=["python", "AI"],
            notification_times=[
                NotificationTime(hour=9, minute=0),
                NotificationTime(hour=17, minute=30),
            ],
            subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
        ),
        UserNotificationConfig(
            user_id="user-002",
            email="bob@example.com",
            keywords=["blockchain"],
            notification_times=[
                NotificationTime(hour=8, minute=0),
                NotificationTime(hour=9, minute=0),
            ],
            subscription_expiry=datetime(2025, 7, 1, 0, 0, 0),
        ),
        UserNotificationConfig(
            user_id="user-003",
            email="carol@example.com",
            keywords=["technology"],
            notification_times=[
                NotificationTime(hour=12, minute=0),
            ],
            subscription_expiry=datetime(2025, 5, 1, 0, 0, 0),
        ),
    ]


class TestNotificationTimeMatching:
    """Tests for 1-minute precision time matching."""

    def test_matches_exact_hour_and_minute(self, evaluator, active_user):
        """Should match when current hour and minute equal notification time."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 1
        assert events[0].user_id == "user-001"

    def test_matches_second_notification_time(self, evaluator, active_user):
        """Should match the second notification time for a user."""
        current_time = datetime(2025, 1, 20, 17, 30, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 1
        assert events[0].user_id == "user-001"

    def test_no_match_wrong_hour(self, evaluator, active_user):
        """Should not match when hour differs."""
        current_time = datetime(2025, 1, 20, 10, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 0

    def test_no_match_wrong_minute(self, evaluator, active_user):
        """Should not match when minute differs."""
        current_time = datetime(2025, 1, 20, 9, 1, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 0

    def test_ignores_seconds(self, evaluator, active_user):
        """Should match regardless of seconds value (1-minute precision)."""
        current_time = datetime(2025, 1, 20, 9, 0, 45)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 1

    def test_no_notification_times_configured(self, evaluator):
        """Should not generate events for users with no notification times."""
        user = UserNotificationConfig(
            user_id="user-no-times",
            email="notime@example.com",
            keywords=["test"],
            notification_times=[],
            subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
        )
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 0

    def test_multiple_users_same_time(self, evaluator, sample_users):
        """Should generate events for all users matching the same time."""
        # Both user-001 and user-002 have 9:00
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, sample_users)

        # user-003 has subscription_expiry in the past relative to a future check,
        # but here current_time is 2025-01-20 which is before 2025-05-01
        matched_user_ids = {e.user_id for e in events}
        assert "user-001" in matched_user_ids
        assert "user-002" in matched_user_ids

    def test_midnight_time(self, evaluator):
        """Should correctly match midnight (hour=0, minute=0)."""
        user = UserNotificationConfig(
            user_id="user-midnight",
            email="midnight@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=0, minute=0)],
            subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
        )
        current_time = datetime(2025, 1, 20, 0, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 1

    def test_end_of_day_time(self, evaluator):
        """Should correctly match 23:59."""
        user = UserNotificationConfig(
            user_id="user-late",
            email="late@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=23, minute=59)],
            subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
        )
        current_time = datetime(2025, 1, 20, 23, 59, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 1


class TestSubscriptionExpiry:
    """Tests for subscription expiry filtering."""

    def test_skips_expired_subscription(self, evaluator):
        """Should skip users whose subscription has expired."""
        user = UserNotificationConfig(
            user_id="user-expired",
            email="expired@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=9, minute=0)],
            subscription_expiry=datetime(2025, 1, 1, 0, 0, 0),
        )
        # Current time is after subscription expiry
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 0

    def test_skips_user_with_no_subscription_expiry(self, evaluator):
        """Should skip users with None subscription_expiry."""
        user = UserNotificationConfig(
            user_id="user-no-sub",
            email="nosub@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=9, minute=0)],
            subscription_expiry=None,
        )
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 0

    def test_includes_user_with_future_expiry(self, evaluator):
        """Should include users whose subscription expires in the future."""
        user = UserNotificationConfig(
            user_id="user-active",
            email="active@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=9, minute=0)],
            subscription_expiry=datetime(2025, 2, 15, 0, 0, 0),
        )
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 1

    def test_skips_user_expiring_exactly_now(self, evaluator):
        """Should skip users whose subscription expires at exactly the current time."""
        expiry_time = datetime(2025, 1, 20, 9, 0, 0)
        user = UserNotificationConfig(
            user_id="user-edge",
            email="edge@example.com",
            keywords=["test"],
            notification_times=[NotificationTime(hour=9, minute=0)],
            subscription_expiry=expiry_time,
        )
        current_time = expiry_time

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 0


class TestConsistentHashing:
    """Tests for consistent hashing distribution across instances."""

    def test_single_instance_handles_all_users(self, sample_users):
        """A single instance should handle all users."""
        evaluator = NotificationTimeEvaluator(instance_id=0, total_instances=1)
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, sample_users)

        # user-001 and user-002 both have 9:00, user-003 doesn't
        matched_ids = {e.user_id for e in events}
        assert "user-001" in matched_ids
        assert "user-002" in matched_ids

    def test_users_distributed_across_instances(self, sample_users):
        """Users should be distributed across multiple instances."""
        # Use 2 instances and check that each user goes to exactly one
        evaluator_0 = NotificationTimeEvaluator(instance_id=0, total_instances=2)
        evaluator_1 = NotificationTimeEvaluator(instance_id=1, total_instances=2)

        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events_0 = evaluator_0.evaluate_notification_times(current_time, sample_users)
        events_1 = evaluator_1.evaluate_notification_times(current_time, sample_users)

        # Combined events should cover all matching users without duplicates
        all_user_ids = {e.user_id for e in events_0} | {e.user_id for e in events_1}
        # user-001 and user-002 match 9:00, user-003 has 12:00
        assert "user-001" in all_user_ids or "user-002" in all_user_ids

        # No user should appear in both instances
        ids_0 = {e.user_id for e in events_0}
        ids_1 = {e.user_id for e in events_1}
        assert ids_0.isdisjoint(ids_1)

    def test_consistent_hashing_is_deterministic(self):
        """Same user_id should always map to the same instance."""
        evaluator = NotificationTimeEvaluator(instance_id=0, total_instances=3)

        # Call multiple times - should always return the same result
        results = [
            evaluator._is_assigned_to_instance("user-test-123")
            for _ in range(10)
        ]

        assert all(r == results[0] for r in results)

    def test_all_users_assigned_to_exactly_one_instance(self):
        """Every user should be assigned to exactly one instance."""
        total_instances = 5
        user_ids = [f"user-{i:04d}" for i in range(100)]

        for user_id in user_ids:
            assigned_count = 0
            for instance_id in range(total_instances):
                evaluator = NotificationTimeEvaluator(
                    instance_id=instance_id, total_instances=total_instances
                )
                if evaluator._is_assigned_to_instance(user_id):
                    assigned_count += 1
            assert assigned_count == 1, (
                f"User {user_id} assigned to {assigned_count} instances"
            )

    def test_distribution_is_roughly_even(self):
        """Users should be roughly evenly distributed across instances."""
        total_instances = 4
        user_ids = [f"user-{i:04d}" for i in range(1000)]

        counts = [0] * total_instances
        for user_id in user_ids:
            for instance_id in range(total_instances):
                evaluator = NotificationTimeEvaluator(
                    instance_id=instance_id, total_instances=total_instances
                )
                if evaluator._is_assigned_to_instance(user_id):
                    counts[instance_id] += 1

        # Each instance should get roughly 250 users (25% of 1000)
        # Allow 15% deviation
        expected = 1000 / total_instances
        for count in counts:
            assert abs(count - expected) < expected * 0.15, (
                f"Distribution too uneven: {counts}"
            )


class TestEventGeneration:
    """Tests for event_id generation and event structure."""

    def test_generates_valid_uuid4_event_id(self, evaluator, active_user):
        """Should generate a valid UUID4 for event_id."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert len(events) == 1
        # Validate it's a valid UUID4
        parsed_uuid = uuid.UUID(events[0].event_id)
        assert parsed_uuid.version == 4

    def test_unique_event_ids_per_evaluation(self, evaluator, sample_users):
        """Each event should have a unique event_id."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, sample_users)

        event_ids = [e.event_id for e in events]
        assert len(event_ids) == len(set(event_ids))

    def test_event_contains_correct_user_id(self, evaluator, active_user):
        """Event should contain the correct user_id."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert events[0].user_id == "user-001"

    def test_event_contains_notification_timestamp(self, evaluator, active_user):
        """Event should contain the current time as notification_timestamp."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [active_user])

        assert events[0].notification_timestamp == current_time

    def test_different_event_ids_across_evaluations(self, evaluator, active_user):
        """Different evaluation cycles should produce different event_ids."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events1 = evaluator.evaluate_notification_times(current_time, [active_user])
        events2 = evaluator.evaluate_notification_times(current_time, [active_user])

        assert events1[0].event_id != events2[0].event_id


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_user_list(self, evaluator):
        """Should return empty list when no users provided."""
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [])

        assert events == []

    def test_invalid_total_instances(self):
        """Should raise ValueError for invalid total_instances."""
        with pytest.raises(ValueError):
            NotificationTimeEvaluator(instance_id=0, total_instances=0)

    def test_invalid_instance_id_negative(self):
        """Should raise ValueError for negative instance_id."""
        with pytest.raises(ValueError):
            NotificationTimeEvaluator(instance_id=-1, total_instances=3)

    def test_invalid_instance_id_too_large(self):
        """Should raise ValueError for instance_id >= total_instances."""
        with pytest.raises(ValueError):
            NotificationTimeEvaluator(instance_id=3, total_instances=3)

    def test_one_event_per_user_per_cycle(self, evaluator):
        """Should generate at most one event per user even if multiple times match."""
        # This is an edge case where a user has duplicate notification times
        # (shouldn't happen in practice but the evaluator should handle it)
        user = UserNotificationConfig(
            user_id="user-dup",
            email="dup@example.com",
            keywords=["test"],
            notification_times=[
                NotificationTime(hour=9, minute=0),
                NotificationTime(hour=9, minute=0),  # Duplicate
            ],
            subscription_expiry=datetime(2025, 6, 15, 12, 0, 0),
        )
        current_time = datetime(2025, 1, 20, 9, 0, 0)

        events = evaluator.evaluate_notification_times(current_time, [user])

        assert len(events) == 1
