"""
User Loader for the Alarm News Scheduler.

Loads active users from MongoDB and caches them in memory for the scheduler
to use when evaluating notification times. Handles startup retries and
periodic reloads with graceful degradation.
"""
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from src.shared.database import DatabaseInterface
from src.shared.models import User, NotificationTime

logger = logging.getLogger(__name__)


# Default configuration constants
DEFAULT_RELOAD_INTERVAL_SECONDS = 5 * 60  # 5 minutes
DEFAULT_STARTUP_RETRY_INTERVAL_SECONDS = 10
DEFAULT_STARTUP_MAX_RETRIES = 10


@dataclass
class UserNotificationConfig:
    """
    Cached user configuration for notification scheduling.

    Attributes:
        user_id: Unique user identifier.
        email: User's email address.
        keywords: List of keyword strings for news/stock matching.
        notification_times: List of configured notification times.
        subscription_expiry: Timestamp when subscription expires.
    """
    user_id: str
    email: str
    keywords: List[str] = field(default_factory=list)
    notification_times: List[NotificationTime] = field(default_factory=list)
    subscription_expiry: Optional[datetime] = None

    @classmethod
    def from_user_dict(cls, data: dict) -> "UserNotificationConfig":
        """
        Create a UserNotificationConfig from a MongoDB user document.

        Args:
            data: Raw MongoDB document dictionary.

        Returns:
            UserNotificationConfig instance.
        """
        notification_times = [
            NotificationTime.from_dict(nt)
            for nt in data.get("notification_times", [])
        ]
        return cls(
            user_id=data["user_id"],
            email=data["email"],
            keywords=data.get("keywords", []),
            notification_times=notification_times,
            subscription_expiry=data.get("subscription_expiry"),
        )


class UserLoader:
    """
    Loads and caches active users from MongoDB for the scheduler.

    On startup, loads all users with valid subscriptions (subscription_expiry > now).
    Retries MongoDB connection every 10 seconds (max 10 attempts) on startup failure.
    Reloads user data every 5 minutes. Continues with cached data if MongoDB is
    unavailable during reload.
    """

    def __init__(
        self,
        database: DatabaseInterface,
        reload_interval_seconds: int = DEFAULT_RELOAD_INTERVAL_SECONDS,
        startup_retry_interval_seconds: int = DEFAULT_STARTUP_RETRY_INTERVAL_SECONDS,
        startup_max_retries: int = DEFAULT_STARTUP_MAX_RETRIES,
    ):
        """
        Initialize the UserLoader.

        Args:
            database: Database interface for querying users.
            reload_interval_seconds: Interval between user reloads (default: 300s / 5 min).
            startup_retry_interval_seconds: Interval between startup retries (default: 10s).
            startup_max_retries: Maximum startup retry attempts (default: 10).
        """
        self._database = database
        self._reload_interval_seconds = reload_interval_seconds
        self._startup_retry_interval_seconds = startup_retry_interval_seconds
        self._startup_max_retries = startup_max_retries
        self._users: List[UserNotificationConfig] = []
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._lock = threading.Lock()
        self._last_reload_time: Optional[datetime] = None

    @property
    def is_running(self) -> bool:
        """Whether the periodic reload is currently running."""
        return self._running

    @property
    def users(self) -> List[UserNotificationConfig]:
        """Get the currently cached users."""
        with self._lock:
            return list(self._users)

    @property
    def last_reload_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful reload."""
        return self._last_reload_time

    def load_active_users(self) -> List[UserNotificationConfig]:
        """
        Load all users with valid subscriptions from MongoDB.

        Queries for users where subscription_expiry > current timestamp.

        Returns:
            List of UserNotificationConfig for active users.

        Raises:
            Exception: If MongoDB query fails.
        """
        now = datetime.utcnow()
        query = {"subscription_expiry": {"$gt": now}}

        logger.info(
            "Loading active users from MongoDB (subscription_expiry > %s)",
            now.isoformat(),
        )

        user_docs = self._database.find_many("users", query)

        users = []
        for doc in user_docs:
            try:
                user_config = UserNotificationConfig.from_user_dict(doc)
                users.append(user_config)
            except (KeyError, ValueError) as e:
                logger.warning(
                    "Skipping invalid user document (user_id=%s): %s",
                    doc.get("user_id", "unknown"),
                    str(e),
                )

        logger.info("Loaded %d active users from MongoDB", len(users))
        return users

    def initial_load(self) -> bool:
        """
        Perform the initial user load with retry logic on startup.

        Retries MongoDB connection every 10 seconds for up to 10 attempts.

        Returns:
            True if users were loaded successfully, False if all retries exhausted.
        """
        for attempt in range(1, self._startup_max_retries + 1):
            try:
                users = self.load_active_users()
                with self._lock:
                    self._users = users
                    self._last_reload_time = datetime.utcnow()
                logger.info(
                    "Initial user load successful on attempt %d/%d (%d users)",
                    attempt,
                    self._startup_max_retries,
                    len(users),
                )
                return True
            except Exception as e:
                logger.warning(
                    "Initial user load failed (attempt %d/%d): %s",
                    attempt,
                    self._startup_max_retries,
                    str(e),
                )
                if attempt < self._startup_max_retries:
                    logger.info(
                        "Retrying in %d seconds...",
                        self._startup_retry_interval_seconds,
                    )
                    time.sleep(self._startup_retry_interval_seconds)

        logger.error(
            "Failed to load users after %d attempts. Startup failed.",
            self._startup_max_retries,
        )
        return False

    def reload_users(self) -> None:
        """
        Reload user data from MongoDB.

        On success, updates the cached users. On failure, logs a warning
        and continues with the previously cached data.
        """
        try:
            users = self.load_active_users()
            with self._lock:
                self._users = users
                self._last_reload_time = datetime.utcnow()
            logger.info("User reload successful (%d active users)", len(users))
        except Exception as e:
            logger.warning(
                "MongoDB reload failed, continuing with cached data (%d users): %s",
                len(self._users),
                str(e),
            )

    def start(self) -> bool:
        """
        Start the user loader: perform initial load and begin periodic reloads.

        Returns:
            True if initial load succeeded and periodic reloads started,
            False if initial load failed after all retries.
        """
        with self._lock:
            if self._running:
                logger.warning("UserLoader is already running.")
                return True

        # Perform initial load with retries
        if not self.initial_load():
            return False

        with self._lock:
            self._running = True

        logger.info(
            "Starting periodic user reload with interval of %d seconds",
            self._reload_interval_seconds,
        )
        self._schedule_reload()
        return True

    def stop(self) -> None:
        """Stop the periodic user reload."""
        with self._lock:
            self._running = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

        logger.info("UserLoader stopped.")

    def _schedule_reload(self) -> None:
        """Schedule the next periodic reload."""
        if not self._running:
            return

        self._timer = threading.Timer(
            self._reload_interval_seconds, self._reload_cycle
        )
        self._timer.daemon = True
        self._timer.start()

    def _reload_cycle(self) -> None:
        """Execute one reload cycle and schedule the next."""
        if not self._running:
            return

        self.reload_users()

        # Schedule next reload
        if self._running:
            self._schedule_reload()
