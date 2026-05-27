"""
Structured logging configuration for Alarm News System.

Provides:
- JSON-formatted structured logging for production
- Correlation ID support using contextvars for tracing across
  scheduler, worker, and email worker components
- Predefined log events for notification lifecycle:
  received, processed, delivered, failed

Usage:
    from src.shared.logging_config import setup_logging, get_correlation_id, set_correlation_id

    # Setup at application startup
    setup_logging()

    # Set correlation ID at the start of processing
    set_correlation_id("event-uuid-123")

    # Log notification events
    log_notification_event("received", user_id="user-1", event_id="evt-1")
    log_notification_event("processed", user_id="user-1", event_id="evt-1")
    log_notification_event("delivered", user_id="user-1", event_id="evt-1")
    log_notification_event("failed", user_id="user-1", event_id="evt-1", error="SMTP timeout")
"""
import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Context variable for correlation ID — propagates across async boundaries
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> Optional[str]:
    """
    Get the current correlation ID from context.

    Returns:
        The current correlation ID, or None if not set.
    """
    return _correlation_id.get()


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """
    Set the correlation ID in the current context.

    If no ID is provided, generates a new UUID4.

    Args:
        correlation_id: Optional correlation ID to set. Generates one if None.

    Returns:
        The correlation ID that was set.
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
    _correlation_id.set(correlation_id)
    return correlation_id


def clear_correlation_id() -> None:
    """Clear the correlation ID from the current context."""
    _correlation_id.set(None)


class CorrelationIdFilter(logging.Filter):
    """
    Logging filter that injects the correlation ID into log records.

    Adds a 'correlation_id' attribute to every log record, making it
    available for formatters and structured log output.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation_id to the log record."""
        record.correlation_id = get_correlation_id() or "none"  # type: ignore[attr-defined]
        return True


class StructuredJsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as single-line JSON objects with:
    - timestamp (ISO 8601)
    - level
    - logger name
    - message
    - correlation_id
    - any extra fields
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "none"),
        }

        # Include extra fields passed via the `extra` parameter
        # Exclude standard LogRecord attributes
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated",
            "thread", "threadName", "msecs", "filename", "funcName",
            "levelno", "lineno", "module", "exc_info", "exc_text",
            "stack_info", "pathname", "processName", "process",
            "message", "levelname", "correlation_id", "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                log_entry[key] = value

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
) -> None:
    """
    Configure structured logging for the application.

    Sets up the root logger with:
    - CorrelationIdFilter for automatic correlation ID injection
    - JSON formatter (production) or simple formatter (development)
    - Stream handler writing to stdout

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        format_type: "json" for structured JSON output, "text" for human-readable.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Add correlation ID filter
    correlation_filter = CorrelationIdFilter()
    handler.addFilter(correlation_filter)

    # Set formatter based on format type
    if format_type == "json":
        handler.setFormatter(StructuredJsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(correlation_id)s] %(name)s: %(message)s"
        ))

    root_logger.addHandler(handler)


def log_notification_event(
    event_type: str,
    user_id: Optional[str] = None,
    event_id: Optional[str] = None,
    error: Optional[str] = None,
    **extra: Any,
) -> None:
    """
    Log a notification lifecycle event with structured data.

    Supported event types:
    - "received": Notification event consumed from Kafka
    - "processed": Notification data retrieved and email formatted
    - "delivered": Email successfully sent to user
    - "failed": Processing or delivery failed

    Args:
        event_type: One of "received", "processed", "delivered", "failed".
        user_id: The user ID associated with the notification.
        event_id: The notification event ID.
        error: Error message (for "failed" events).
        **extra: Additional context fields to include in the log.
    """
    notification_logger = logging.getLogger("alarm_news.notification")

    log_data: Dict[str, Any] = {
        "notification_event": event_type,
    }
    if user_id:
        log_data["user_id"] = user_id
    if event_id:
        log_data["event_id"] = event_id
    if error:
        log_data["error"] = error
    log_data.update(extra)

    message = f"notification.{event_type}"

    if event_type == "failed":
        notification_logger.error(message, extra=log_data)
    else:
        notification_logger.info(message, extra=log_data)
