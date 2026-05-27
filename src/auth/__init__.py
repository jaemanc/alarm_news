# Authentication Service Module
"""
This module handles user registration, authentication, subscription management,
and notification/keyword configuration API endpoints.
"""
from src.auth.registration import (
    RegistrationInterface,
    RegistrationResult,
    UserRegistrationHandler,
)
from src.auth.authentication import (
    AuthenticationInterface,
    AuthenticationHandler,
    AuthResult,
)
from src.auth.subscription import (
    SubscriptionManagerInterface,
    SubscriptionManager,
    RenewalResult,
    CancellationResult,
)
from src.auth.api import (
    ApiResult,
    update_notification_times,
    update_keywords,
    validate_notification_times,
    validate_keywords,
)

__all__ = [
    "RegistrationInterface",
    "RegistrationResult",
    "UserRegistrationHandler",
    "AuthenticationInterface",
    "AuthenticationHandler",
    "AuthResult",
    "SubscriptionManagerInterface",
    "SubscriptionManager",
    "RenewalResult",
    "CancellationResult",
    "ApiResult",
    "update_notification_times",
    "update_keywords",
    "validate_notification_times",
    "validate_keywords",
]
