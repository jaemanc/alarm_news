# Authentication Service Module
"""
This module handles user registration, authentication, and subscription management.
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
]
