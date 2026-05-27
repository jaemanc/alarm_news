"""
Subscription Expiry Cleanup Job for the Alarm News System.

Runs daily at midnight UTC to delete user records that have been
expired for more than 90 days.

Requirements: 17.3, 17.4, 17.5, 17.6
"""
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.shared.database import MongoDBConnectionManager

logger = logging.getLogger(__name__)

# Cleanup configuration
EXPIRY_GRACE_PERIOD_DAYS = 90
SECONDS_IN_A_DAY = 86400


class CleanupJob:
    """
    Scheduled cleanup job that removes user records with subscriptions
    expired for more than 90 days.

    The job runs daily at midnight UTC. It queries MongoDB for users
    whose subscription_expiry is more than 90 days in the past and
    deletes those records.

    Usage:
        cleanup = CleanupJob(database=db_manager)
        cleanup.start()   # Schedules the first run at next midnight UTC
        ...
        cleanup.stop()    # Cancels any pending scheduled run
    """

    def __init__(
        self,
        database: Optional[MongoDBConnectionManager] = None,
        grace_period_days: int = EXPIRY_GRACE_PERIOD_DAYS,
    ) -> None:
        """
        Initialize the cleanup job.

        Args:
            database: MongoDB connection manager. If None, will be created on start.
            grace_period_days: Number of days after expiry before deletion (default: 90).
        """
        self._database = database
        self._grace_period_days = grace_period_days
        self._timer: Optional[threading.Timer] = None
        self._running = False

    @property
    def is_running(self) -> bool:
        """Whether the cleanup job scheduler is active."""
        return self._running

    def start(self) -> None:
        """
        Start the cleanup job scheduler.

        Schedules the first execution at the next midnight UTC.
        Subsequent runs are scheduled after each execution completes.
        """
        if self._running:
            logger.warning("Cleanup job is already running.")
            return

        self._running = True
        self._schedule_next_run()
        logger.info(
            "Cleanup job started (grace period: %d days).",
            self._grace_period_days,
        )

    def stop(self) -> None:
        """
        Stop the cleanup job scheduler.

        Cancels any pending scheduled execution.
        """
        self._running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        logger.info("Cleanup job stopped.")

    def run_now(self) -> int:
        """
        Execute the cleanup job immediately.

        Queries MongoDB for users with subscription_expiry more than
        90 days in the past and deletes those records.

        Returns:
            Number of deleted user records.
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self._grace_period_days)

        logger.info(
            "Running cleanup job. Deleting users expired before %s.",
            cutoff_date.isoformat(),
        )

        try:
            # Query for users expired more than 90 days ago
            query = {"subscription_expiry": {"$lt": cutoff_date}}
            expired_users = self._database.find_many("users", query)

            if not expired_users:
                logger.info("Cleanup job complete. No expired users to delete.")
                return 0

            # Delete expired user records
            deleted_count = 0
            for user in expired_users:
                user_id = user.get("user_id", "unknown")
                success = self._database.delete_one("users", {"user_id": user_id})
                if success:
                    deleted_count += 1
                else:
                    logger.warning(
                        "Failed to delete expired user: %s", user_id
                    )

            logger.info(
                "Cleanup job complete. Deleted %d expired user records.",
                deleted_count,
            )
            return deleted_count

        except Exception as e:
            logger.error("Cleanup job failed: %s", e, exc_info=True)
            return 0

    def _schedule_next_run(self) -> None:
        """
        Schedule the next cleanup run at midnight UTC.

        Calculates the delay until the next midnight UTC and sets
        a threading.Timer to execute the job.
        """
        if not self._running:
            return

        delay_seconds = self._seconds_until_midnight_utc()
        logger.info(
            "Next cleanup job scheduled in %.0f seconds (midnight UTC).",
            delay_seconds,
        )

        self._timer = threading.Timer(delay_seconds, self._execute_and_reschedule)
        self._timer.daemon = True
        self._timer.start()

    def _execute_and_reschedule(self) -> None:
        """
        Execute the cleanup job and schedule the next run.
        """
        if not self._running:
            return

        self.run_now()
        self._schedule_next_run()

    @staticmethod
    def _seconds_until_midnight_utc() -> float:
        """
        Calculate the number of seconds until the next midnight UTC.

        Returns:
            Seconds until next midnight UTC (always > 0).
        """
        now = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        delta = (next_midnight - now).total_seconds()
        return delta
