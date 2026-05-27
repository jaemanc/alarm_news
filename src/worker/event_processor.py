"""
Worker event processor for the Alarm News system.

Orchestrates the full notification processing pipeline:
1. Consume NotificationEvent from Kafka
2. Acquire distributed lock using event_id
3. Retrieve user info and crawled data
4. Format email notification
5. Publish email to Kafka email-delivery topic
6. Release lock and manage offset commits

Key behaviors:
- If lock already held by another worker → skip (return normally, offset committed)
- If lock acquisition times out → raise exception (offset NOT committed, allows redelivery)
- On success: release lock, return normally (offset committed by consumer)
- On failure: release lock, raise exception (offset NOT committed, allows redelivery)

Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 9.1, 9.2, 9.9, 12.3
"""
import logging
import os
import uuid
from typing import Optional

from src.shared.locking import LockInterface
from src.shared.models import NotificationEvent
from src.worker.data_retriever import DataRetriever
from src.worker.email_formatter import EmailFormatter
from src.worker.email_publisher import EmailPublisher

logger = logging.getLogger(__name__)

# Lock configuration
DEFAULT_LOCK_TTL_SECONDS = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT_SECONDS = 10


class LockAlreadyHeldError(Exception):
    """Raised when the distributed lock is already held by another worker."""
    pass


class LockTimeoutError(Exception):
    """Raised when lock acquisition times out."""
    pass


class EventProcessingError(Exception):
    """Raised when event processing fails after lock acquisition."""
    pass


class EventProcessor:
    """
    Orchestrates notification event processing.

    Consumes NotificationEvent messages, acquires a distributed lock to
    prevent duplicate processing, retrieves user data and crawled content,
    formats an email notification, and publishes it to the email-delivery
    Kafka topic.

    Usage:
        processor = EventProcessor(
            lock_manager=lock_manager,
            data_retriever=data_retriever,
            email_formatter=email_formatter,
            email_publisher=email_publisher,
        )
        # Pass processor.process_event as the handler to EventConsumer
        consumer.consume_events(processor.process_event)
    """

    def __init__(
        self,
        lock_manager: LockInterface,
        data_retriever: DataRetriever,
        email_formatter: EmailFormatter,
        email_publisher: EmailPublisher,
        worker_id: Optional[str] = None,
        lock_ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
        lock_timeout_seconds: int = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        """
        Initialize the EventProcessor.

        Args:
            lock_manager: Distributed lock interface (Redis or in-memory).
            data_retriever: Retrieves user info and crawled data.
            email_formatter: Formats email notifications.
            email_publisher: Publishes emails to Kafka.
            worker_id: Unique identifier for this worker instance.
                If None, generates a UUID4.
            lock_ttl_seconds: TTL for the distributed lock (default: 300s / 5 min).
            lock_timeout_seconds: Timeout for lock acquisition (default: 10s).
        """
        self._lock_manager = lock_manager
        self._data_retriever = data_retriever
        self._email_formatter = email_formatter
        self._email_publisher = email_publisher
        self._worker_id = worker_id or os.environ.get(
            "WORKER_ID", str(uuid.uuid4())
        )
        self._lock_ttl_seconds = lock_ttl_seconds
        self._lock_timeout_seconds = lock_timeout_seconds

    @property
    def worker_id(self) -> str:
        """The unique identifier for this worker instance."""
        return self._worker_id

    def process_event(self, event: NotificationEvent) -> None:
        """
        Process a single notification event.

        This method is designed to be passed as the handler callback to
        EventConsumer.consume_events(). The consumer commits the offset
        only if this method returns normally (no exception raised).

        Flow:
        1. Acquire distributed lock using event_id
        2. If lock already held → return normally (skip, offset committed)
        3. If lock timeout → raise LockTimeoutError (offset NOT committed)
        4. Retrieve user info and crawled data
        5. Format email notification
        6. Publish email to Kafka
        7. On success: release lock, return normally
        8. On failure: release lock, raise exception

        Args:
            event: The NotificationEvent to process.

        Raises:
            LockTimeoutError: If lock acquisition times out (allows redelivery).
            EventProcessingError: If processing fails after lock acquired
                (allows redelivery).
        """
        correlation_id = event.event_id
        logger.info(
            "Processing notification event: event_id=%s user_id=%s "
            "worker_id=%s",
            event.event_id,
            event.user_id,
            self._worker_id,
        )

        # Step 1: Acquire distributed lock
        lock_acquired = self._acquire_lock(event.event_id)

        if not lock_acquired:
            # Lock is already held by another worker OR timed out.
            # We distinguish by checking if the lock is currently held.
            if self._lock_manager.is_held(event.event_id):
                # Lock held by another worker → skip processing,
                # return normally so offset is committed
                logger.info(
                    "Lock already held for event_id=%s, skipping "
                    "(another worker is processing). worker_id=%s",
                    event.event_id,
                    self._worker_id,
                )
                return
            else:
                # Lock acquisition timed out (transient failure) →
                # raise exception so offset is NOT committed, allowing redelivery
                logger.warning(
                    "Lock acquisition timed out for event_id=%s "
                    "worker_id=%s. Will not acknowledge message.",
                    event.event_id,
                    self._worker_id,
                )
                raise LockTimeoutError(
                    f"Lock acquisition timed out for event_id={event.event_id}"
                )

        # Lock acquired successfully - process the event
        try:
            self._do_process(event, correlation_id)
            # Success: release lock, return normally (offset committed)
            self._release_lock(event.event_id)
            logger.info(
                "Successfully processed event: event_id=%s user_id=%s "
                "worker_id=%s",
                event.event_id,
                event.user_id,
                self._worker_id,
            )
        except Exception as e:
            # Failure: release lock, raise exception (offset NOT committed)
            logger.error(
                "Failed to process event: event_id=%s user_id=%s "
                "worker_id=%s error=%s",
                event.event_id,
                event.user_id,
                self._worker_id,
                str(e),
            )
            self._release_lock(event.event_id)
            raise EventProcessingError(
                f"Failed to process event_id={event.event_id}: {e}"
            ) from e

    def _acquire_lock(self, event_id: str) -> bool:
        """
        Attempt to acquire the distributed lock for an event.

        Args:
            event_id: The event ID to use as the lock key.

        Returns:
            True if lock acquired, False if already held or timed out.
        """
        logger.debug(
            "Acquiring lock: event_id=%s worker_id=%s ttl=%ds timeout=%ds",
            event_id,
            self._worker_id,
            self._lock_ttl_seconds,
            self._lock_timeout_seconds,
        )
        return self._lock_manager.acquire(
            key=event_id,
            owner=self._worker_id,
            ttl_seconds=self._lock_ttl_seconds,
            timeout_seconds=self._lock_timeout_seconds,
        )

    def _release_lock(self, event_id: str) -> None:
        """
        Release the distributed lock for an event.

        Args:
            event_id: The event ID used as the lock key.
        """
        released = self._lock_manager.release(
            key=event_id,
            owner=self._worker_id,
        )
        if released:
            logger.debug(
                "Lock released: event_id=%s worker_id=%s",
                event_id,
                self._worker_id,
            )
        else:
            logger.warning(
                "Lock release failed (not owned or expired): "
                "event_id=%s worker_id=%s",
                event_id,
                self._worker_id,
            )

    def _do_process(self, event: NotificationEvent, correlation_id: str) -> None:
        """
        Execute the core processing pipeline after lock acquisition.

        Steps:
        1. Retrieve user info (skip if not found or expired)
        2. Retrieve crawled data for user keywords
        3. Format email notification
        4. Publish email to Kafka

        Args:
            event: The NotificationEvent being processed.
            correlation_id: Correlation ID for tracing (same as event_id).

        Raises:
            Exception: If any step in the pipeline fails.
        """
        # Step 2: Retrieve user info and crawled data
        result = self._data_retriever.retrieve_notification_data(event.user_id)

        if result is None:
            # User not found or subscription expired - skip processing
            # This is not an error; return normally so offset is committed
            logger.info(
                "Skipping event: user not found or subscription expired. "
                "event_id=%s user_id=%s",
                event.event_id,
                event.user_id,
            )
            return

        user_info, crawled_data = result

        # Step 3: Format email notification
        email_notification = self._email_formatter.format_email(
            user_info=user_info,
            data=crawled_data,
        )

        logger.info(
            "Email formatted: event_id=%s to_email=%s subject=%s",
            event.event_id,
            email_notification.to_email,
            email_notification.subject,
        )

        # Step 4: Publish email to Kafka email-delivery topic
        published = self._email_publisher.publish_email(
            email=email_notification,
            correlation_id=correlation_id,
        )

        if not published:
            raise EventProcessingError(
                f"Failed to publish email for event_id={event.event_id} "
                f"(all retries exhausted, stored in DLQ)"
            )


def create_event_processor(
    lock_manager: LockInterface,
    data_retriever: DataRetriever,
    email_formatter: Optional[EmailFormatter] = None,
    email_publisher: Optional[EmailPublisher] = None,
    worker_id: Optional[str] = None,
) -> EventProcessor:
    """
    Factory function to create an EventProcessor with default configuration.

    Args:
        lock_manager: Distributed lock interface.
        data_retriever: Data retriever for user info and crawled data.
        email_formatter: Email formatter instance. If None, creates a new one.
        email_publisher: Email publisher instance. Must be provided.
        worker_id: Optional worker ID override.

    Returns:
        Configured EventProcessor instance.
    """
    if email_formatter is None:
        email_formatter = EmailFormatter()

    if email_publisher is None:
        raise ValueError("email_publisher is required")

    return EventProcessor(
        lock_manager=lock_manager,
        data_retriever=data_retriever,
        email_formatter=email_formatter,
        email_publisher=email_publisher,
        worker_id=worker_id,
    )
