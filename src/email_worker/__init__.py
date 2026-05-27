# Email Delivery Worker Module
"""
This module handles email delivery via SMTP.
"""
from src.email_worker.smtp_client import (
    SMTPClient,
    SMTPClientError,
    SMTPConnectionError,
    SMTPAuthenticationError,
    SMTPDeliveryError,
)
