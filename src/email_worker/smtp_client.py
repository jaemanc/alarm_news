"""
SMTP client for the Email Delivery Worker.

This module provides an SMTP client that connects to an SMTP server with TLS
encryption, authenticates with configured credentials, and sends HTML-formatted
emails with retry logic for connection and delivery failures.

Retry Policy:
    - Connection: up to 3 attempts with 30-second intervals
    - Delivery: up to 3 attempts with 30-second intervals
    - Retries on: network timeouts, 5xx SMTP errors, 429 (rate limit)
    - No retry on: 4xx errors (except 429)

Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.10
"""
import logging
import smtplib
import time
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from socket import timeout as SocketTimeout
from typing import Optional

from src.shared.models import EmailNotification

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONNECTION_TIMEOUT = 10  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL = 30  # seconds


class SMTPClientError(Exception):
    """Base exception for SMTP client errors."""

    def __init__(self, message: str, correlation_id: Optional[str] = None):
        self.correlation_id = correlation_id
        super().__init__(message)


class SMTPConnectionError(SMTPClientError):
    """Raised when SMTP connection cannot be established after retries."""
    pass


class SMTPAuthenticationError(SMTPClientError):
    """Raised when SMTP authentication fails."""
    pass


class SMTPDeliveryError(SMTPClientError):
    """Raised when email delivery fails after retries."""
    pass


def _is_retryable_smtp_error(error: smtplib.SMTPResponseException) -> bool:
    """
    Determine if an SMTP error is retryable.

    Retryable errors:
        - 5xx server errors
        - 429 rate limit exceeded

    Non-retryable errors:
        - 4xx client errors (except 429)

    Args:
        error: The SMTP response exception.

    Returns:
        True if the error is retryable, False otherwise.
    """
    code = error.smtp_code
    if 500 <= code < 600:
        return True
    if code == 429:
        return True
    return False


class SMTPClient:
    """
    SMTP client for sending HTML-formatted emails with retry logic.

    Connects to an SMTP server with TLS encryption, authenticates with
    configured credentials, and sends emails with proper MIME encoding.

    Args:
        host: SMTP server hostname.
        port: SMTP server port.
        username: SMTP authentication username.
        password: SMTP authentication password.
        from_email: Sender email address.
        from_name: Sender display name.
        use_ssl: If True, use SMTP_SSL (port 465). If False, use STARTTLS.
        connection_timeout: Timeout in seconds for SMTP connection.
        max_retries: Maximum number of retry attempts for connection and delivery.
        retry_interval: Seconds to wait between retry attempts.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        from_email: str,
        from_name: str = "Alarm News",
        use_ssl: bool = False,
        connection_timeout: int = DEFAULT_CONNECTION_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval: int = DEFAULT_RETRY_INTERVAL,
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._from_email = from_email
        self._from_name = from_name
        self._use_ssl = use_ssl
        self._connection_timeout = connection_timeout
        self._max_retries = max_retries
        self._retry_interval = retry_interval
        self._connection: Optional[smtplib.SMTP] = None

    def connect(self) -> bool:
        """
        Connect to the SMTP server with TLS encryption.

        Retries connection up to max_retries times with retry_interval seconds
        between attempts.

        Returns:
            True if connection was established successfully.

        Raises:
            SMTPConnectionError: If connection fails after all retry attempts.
        """
        correlation_id = str(uuid.uuid4())

        for attempt in range(1, self._max_retries + 1):
            try:
                if self._use_ssl:
                    self._connection = smtplib.SMTP_SSL(
                        self._host,
                        self._port,
                        timeout=self._connection_timeout,
                    )
                else:
                    self._connection = smtplib.SMTP(
                        self._host,
                        self._port,
                        timeout=self._connection_timeout,
                    )
                    self._connection.starttls()

                logger.info(
                    "SMTP connection established",
                    extra={
                        "correlation_id": correlation_id,
                        "host": self._host,
                        "port": self._port,
                        "attempt": attempt,
                    },
                )
                return True

            except (
                OSError,
                SocketTimeout,
                smtplib.SMTPException,
            ) as e:
                logger.warning(
                    "SMTP connection attempt failed",
                    extra={
                        "correlation_id": correlation_id,
                        "host": self._host,
                        "port": self._port,
                        "attempt": attempt,
                        "max_retries": self._max_retries,
                        "error": str(e),
                    },
                )
                self._connection = None

                if attempt < self._max_retries:
                    time.sleep(self._retry_interval)

        raise SMTPConnectionError(
            f"Failed to connect to SMTP server {self._host}:{self._port} "
            f"after {self._max_retries} attempts",
            correlation_id=correlation_id,
        )

    def authenticate(self) -> bool:
        """
        Authenticate with the SMTP server using configured credentials.

        Returns:
            True if authentication was successful.

        Raises:
            SMTPConnectionError: If not connected to the server.
            SMTPAuthenticationError: If authentication fails.
        """
        correlation_id = str(uuid.uuid4())

        if self._connection is None:
            raise SMTPConnectionError(
                "Not connected to SMTP server. Call connect() first.",
                correlation_id=correlation_id,
            )

        try:
            self._connection.login(self._username, self._password)
            logger.info(
                "SMTP authentication successful",
                extra={
                    "correlation_id": correlation_id,
                    "username": self._username,
                },
            )
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(
                "SMTP authentication failed",
                extra={
                    "correlation_id": correlation_id,
                    "username": self._username,
                    "error": str(e),
                },
            )
            raise SMTPAuthenticationError(
                f"SMTP authentication failed for user {self._username}",
                correlation_id=correlation_id,
            ) from e

    def send_email(self, email: EmailNotification) -> bool:
        """
        Send an HTML-formatted email with retry logic.

        Retries delivery up to max_retries times with retry_interval seconds
        between attempts. Retries on network timeouts and 5xx SMTP errors.
        Does not retry on 4xx errors (except 429).

        Args:
            email: The email notification to send.

        Returns:
            True if the email was sent successfully.

        Raises:
            SMTPConnectionError: If not connected to the server.
            SMTPDeliveryError: If delivery fails after all retry attempts.
        """
        correlation_id = str(uuid.uuid4())

        if self._connection is None:
            raise SMTPConnectionError(
                "Not connected to SMTP server. Call connect() first.",
                correlation_id=correlation_id,
            )

        message = self._build_mime_message(email)

        last_error: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            try:
                self._connection.sendmail(
                    self._from_email,
                    email.to_email,
                    message.as_string(),
                )
                logger.info(
                    "Email sent successfully",
                    extra={
                        "correlation_id": correlation_id,
                        "to_email": email.to_email,
                        "subject": email.subject,
                        "attempt": attempt,
                    },
                )
                return True

            except smtplib.SMTPResponseException as e:
                last_error = e
                if not _is_retryable_smtp_error(e):
                    # Non-retryable 4xx error (except 429)
                    logger.error(
                        "Non-retryable SMTP error during delivery",
                        extra={
                            "correlation_id": correlation_id,
                            "to_email": email.to_email,
                            "smtp_code": e.smtp_code,
                            "smtp_error": str(e.smtp_error),
                            "attempt": attempt,
                        },
                    )
                    raise SMTPDeliveryError(
                        f"Non-retryable SMTP error {e.smtp_code}: {e.smtp_error}",
                        correlation_id=correlation_id,
                    ) from e

                logger.warning(
                    "Retryable SMTP error during delivery",
                    extra={
                        "correlation_id": correlation_id,
                        "to_email": email.to_email,
                        "smtp_code": e.smtp_code,
                        "attempt": attempt,
                        "max_retries": self._max_retries,
                    },
                )

                if attempt < self._max_retries:
                    time.sleep(self._retry_interval)

            except (OSError, SocketTimeout, smtplib.SMTPException) as e:
                last_error = e
                logger.warning(
                    "Network error during email delivery",
                    extra={
                        "correlation_id": correlation_id,
                        "to_email": email.to_email,
                        "error": str(e),
                        "attempt": attempt,
                        "max_retries": self._max_retries,
                    },
                )

                if attempt < self._max_retries:
                    time.sleep(self._retry_interval)

        raise SMTPDeliveryError(
            f"Failed to deliver email to {email.to_email} "
            f"after {self._max_retries} attempts: {last_error}",
            correlation_id=correlation_id,
        )

    def disconnect(self) -> None:
        """Close the SMTP connection gracefully."""
        if self._connection is not None:
            try:
                self._connection.quit()
            except smtplib.SMTPException:
                pass
            finally:
                self._connection = None

    def _build_mime_message(self, email: EmailNotification) -> MIMEMultipart:
        """
        Build a MIME message with HTML content.

        Args:
            email: The email notification to format.

        Returns:
            A MIMEMultipart message ready for sending.
        """
        message = MIMEMultipart("alternative")
        message["From"] = f"{self._from_name} <{self._from_email}>"
        message["To"] = email.to_email
        message["Subject"] = email.subject

        html_part = MIMEText(email.body_html, "html", "utf-8")
        message.attach(html_part)

        return message

    @property
    def is_connected(self) -> bool:
        """Check if the client has an active connection."""
        return self._connection is not None
