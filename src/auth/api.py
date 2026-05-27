"""
REST API endpoint handlers for notification time and keyword management.

This module provides lightweight handler functions for:
- PUT /users/{user_id}/notification-times
- PUT /users/{user_id}/keywords

These handlers perform validation and MongoDB updates. They are designed
as framework-agnostic functions that return structured results, making them
easy to mount on any HTTP framework (Flask, FastAPI, etc.).
"""
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.shared.database import DatabaseInterface
from src.shared.models import NotificationTime

logger = logging.getLogger(__name__)

# Validation constants
MAX_NOTIFICATION_TIMES = 5
MIN_HOUR = 0
MAX_HOUR = 23
MIN_MINUTE = 0
MAX_MINUTE = 59

MIN_KEYWORDS = 1
MAX_KEYWORDS = 20
MIN_KEYWORD_LENGTH = 1
MAX_KEYWORD_LENGTH = 100


@dataclass
class ApiResult:
    """
    Structured result from an API handler.

    Attributes:
        success: Whether the operation succeeded.
        data: Response payload on success.
        error_message: Description of failure on error.
        status_code: HTTP status code to return.
    """
    success: bool
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    status_code: int = 200


def validate_notification_times(notification_times: List[Dict[str, Any]]) -> Optional[str]:
    """
    Validate a list of notification time entries.

    Each entry must have integer 'hour' (0-23) and 'minute' (0-59).
    Maximum 5 notification times allowed.

    Args:
        notification_times: List of dicts with 'hour' and 'minute' keys.

    Returns:
        Error message string if validation fails, None if valid.
    """
    if not isinstance(notification_times, list):
        return "notification_times must be a list"

    if len(notification_times) == 0:
        return "At least one notification time is required"

    if len(notification_times) > MAX_NOTIFICATION_TIMES:
        return f"Maximum {MAX_NOTIFICATION_TIMES} notification times allowed"

    for i, entry in enumerate(notification_times):
        if not isinstance(entry, dict):
            return f"notification_times[{i}] must be an object with 'hour' and 'minute'"

        if "hour" not in entry or "minute" not in entry:
            return f"notification_times[{i}] must contain 'hour' and 'minute' fields"

        hour = entry["hour"]
        minute = entry["minute"]

        if not isinstance(hour, int) or not isinstance(minute, int):
            return f"notification_times[{i}]: hour and minute must be integers"

        if hour < MIN_HOUR or hour > MAX_HOUR:
            return f"notification_times[{i}]: hour must be between {MIN_HOUR} and {MAX_HOUR}"

        if minute < MIN_MINUTE or minute > MAX_MINUTE:
            return f"notification_times[{i}]: minute must be between {MIN_MINUTE} and {MAX_MINUTE}"

    return None


def validate_keywords(keywords: List[str]) -> Optional[str]:
    """
    Validate a list of keywords.

    At least 1 keyword required, each between 1-100 characters.
    Maximum 20 keywords allowed.

    Args:
        keywords: List of keyword strings.

    Returns:
        Error message string if validation fails, None if valid.
    """
    if not isinstance(keywords, list):
        return "keywords must be a list"

    if len(keywords) < MIN_KEYWORDS:
        return "At least one keyword is required"

    if len(keywords) > MAX_KEYWORDS:
        return f"Maximum {MAX_KEYWORDS} keywords allowed"

    for i, keyword in enumerate(keywords):
        if not isinstance(keyword, str):
            return f"keywords[{i}] must be a string"

        if len(keyword) < MIN_KEYWORD_LENGTH:
            return f"keywords[{i}]: keyword must be at least {MIN_KEYWORD_LENGTH} character(s)"

        if len(keyword) > MAX_KEYWORD_LENGTH:
            return f"keywords[{i}]: keyword must be at most {MAX_KEYWORD_LENGTH} characters"

    return None


def update_notification_times(
    user_id: str,
    notification_times: List[Dict[str, Any]],
    db: DatabaseInterface,
) -> ApiResult:
    """
    Handle PUT /users/{user_id}/notification-times.

    Validates the notification times and updates the user's MongoDB record.

    Args:
        user_id: The user's unique identifier.
        notification_times: List of dicts with 'hour' and 'minute'.
        db: Database interface for MongoDB operations.

    Returns:
        ApiResult with success/failure information.
    """
    # Validate input
    error = validate_notification_times(notification_times)
    if error:
        logger.warning(
            "Invalid notification times for user %s: %s", user_id, error
        )
        return ApiResult(
            success=False,
            error_message=error,
            status_code=400,
        )

    # Build NotificationTime objects to ensure model validation passes
    try:
        nt_objects = [
            NotificationTime(hour=entry["hour"], minute=entry["minute"])
            for entry in notification_times
        ]
    except ValueError as e:
        return ApiResult(
            success=False,
            error_message=f"Invalid time format: {str(e)}",
            status_code=400,
        )

    # Check user exists
    user_doc = db.find_one("users", {"user_id": user_id})
    if user_doc is None:
        logger.warning("User not found: %s", user_id)
        return ApiResult(
            success=False,
            error_message="User not found",
            status_code=404,
        )

    # Update MongoDB
    try:
        serialized_times = [nt.to_dict() for nt in nt_objects]
        updated = db.update_one(
            "users",
            {"user_id": user_id},
            {"$set": {"notification_times": serialized_times}},
        )

        if not updated:
            logger.error(
                "Failed to update notification times for user %s: no document modified",
                user_id,
            )
            return ApiResult(
                success=False,
                error_message="Failed to update notification times",
                status_code=500,
            )

    except Exception as e:
        logger.error(
            "MongoDB error updating notification times for user %s: %s",
            user_id,
            str(e),
        )
        return ApiResult(
            success=False,
            error_message="Database error: failed to update notification times",
            status_code=500,
        )

    logger.info(
        "Updated notification times for user %s: %d time(s) configured",
        user_id,
        len(nt_objects),
    )
    return ApiResult(
        success=True,
        data={
            "user_id": user_id,
            "notification_times": serialized_times,
            "message": f"Successfully configured {len(nt_objects)} notification time(s)",
        },
        status_code=200,
    )


def update_keywords(
    user_id: str,
    keywords: List[str],
    db: DatabaseInterface,
) -> ApiResult:
    """
    Handle PUT /users/{user_id}/keywords.

    Validates the keywords and updates the user's MongoDB record.

    Args:
        user_id: The user's unique identifier.
        keywords: List of keyword strings.
        db: Database interface for MongoDB operations.

    Returns:
        ApiResult with success/failure information.
    """
    # Validate input
    error = validate_keywords(keywords)
    if error:
        logger.warning("Invalid keywords for user %s: %s", user_id, error)
        return ApiResult(
            success=False,
            error_message=error,
            status_code=400,
        )

    # Check user exists
    user_doc = db.find_one("users", {"user_id": user_id})
    if user_doc is None:
        logger.warning("User not found: %s", user_id)
        return ApiResult(
            success=False,
            error_message="User not found",
            status_code=404,
        )

    # Update MongoDB
    try:
        updated = db.update_one(
            "users",
            {"user_id": user_id},
            {"$set": {"keywords": keywords}},
        )

        if not updated:
            logger.error(
                "Failed to update keywords for user %s: no document modified",
                user_id,
            )
            return ApiResult(
                success=False,
                error_message="Failed to update keywords",
                status_code=500,
            )

    except Exception as e:
        logger.error(
            "MongoDB error updating keywords for user %s: %s",
            user_id,
            str(e),
        )
        return ApiResult(
            success=False,
            error_message="Database error: failed to update keywords",
            status_code=500,
        )

    logger.info(
        "Updated keywords for user %s: %d keyword(s) configured",
        user_id,
        len(keywords),
    )
    return ApiResult(
        success=True,
        data={
            "user_id": user_id,
            "keywords": keywords,
            "message": f"Successfully configured {len(keywords)} keyword(s)",
        },
        status_code=200,
    )
