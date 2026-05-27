"""
Unit tests for the SMTP client module.

Tests cover:
- TLS connection establishment (SMTP_SSL and STARTTLS)
- Connection retry logic (3 attempts, 30-second intervals)
- SMTP authentication
- HTML email formatting with MIME encoding
- Delivery retry logic for network errors and 5xx SMTP errors
- No retry on 4xx errors (except 429)
- Correlation ID logging on errors
- Disconnect behavior

Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.10
"""
import smtplib
from datetime import datetime
from socket import timeout as SocketTimeout
from unittest.mock import MagicMock, patch, call

import pytest

from src.email_worker.smtp_client import (
    SMTPClient,
    SMTPClientError,
    SMTPConnectionError,
    SMTPAuthenticationError,
    SMTPDeliveryError,
    _is_retryable_smtp_error,
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_INTERVAL,
)
from src.shared.models import EmailNotification


@pytest.fixture
def smtp_config():
    """Default SMTP client configuration for tests."""
    return {
        "host": "smtp.example.com",
        "port": 587,
        "username": "user@example.com",
        "password": "secret123",
        "from_email": "noreply@alarmnews.com",
        "from_name": "Alarm News",
    }


@pytest.fixture
def sample_email():
    """Sample email notification for tests."""
    return EmailNotification(
        to_email="recipient@example.com",
        subject="Alarm News - 2024-01-15 - technology, AI",
        body_html="<html><body><h1>Your News</h1><p>Hello!</p></body></html>",
        timestamp=datetime(2024, 1, 15, 9, 0, 0),
    )


# --- Retryable Error Classification Tests ---


class TestIsRetryableSmtpError:
    """Tests for the _is_retryable_smtp_error helper."""

    def test_5xx_errors_are_retryable(self):
        """5xx server errors should be retried."""
        for code in [500, 501, 502, 503, 550, 599]:
            error = smtplib.SMTPResponseException(code, b"Server error")
            assert _is_retryable_smtp_error(error) is True

    def test_429_is_retryable(self):
        """429 rate limit error should be retried."""
        error = smtplib.SMTPResponseException(429, b"Too many requests")
        assert _is_retryable_smtp_error(error) is True

    def test_4xx_errors_not_retryable(self):
        """4xx client errors (except 429) should not be retried."""
        for code in [400, 401, 403, 450, 451, 452]:
            error = smtplib.SMTPResponseException(code, b"Client error")
            assert _is_retryable_smtp_error(error) is False


# --- Connection Tests ---


class TestSMTPClientConnection:
    """Tests for SMTP connection establishment."""

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_with_starttls(self, mock_smtp_class, smtp_config):
        """Verify connection uses STARTTLS when use_ssl=False."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config, use_ssl=False)
        result = client.connect()

        assert result is True
        mock_smtp_class.assert_called_once_with(
            "smtp.example.com",
            587,
            timeout=DEFAULT_CONNECTION_TIMEOUT,
        )
        mock_smtp.starttls.assert_called_once()

    @patch("src.email_worker.smtp_client.smtplib.SMTP_SSL")
    def test_connect_with_ssl(self, mock_smtp_ssl_class, smtp_config):
        """Verify connection uses SMTP_SSL when use_ssl=True."""
        mock_smtp = MagicMock()
        mock_smtp_ssl_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config, use_ssl=True)
        result = client.connect()

        assert result is True
        mock_smtp_ssl_class.assert_called_once_with(
            "smtp.example.com",
            587,
            timeout=DEFAULT_CONNECTION_TIMEOUT,
        )

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_uses_configured_timeout(self, mock_smtp_class, smtp_config):
        """Verify connection timeout is set to 10 seconds by default."""
        mock_smtp_class.return_value = MagicMock()

        client = SMTPClient(**smtp_config, connection_timeout=10)
        client.connect()

        mock_smtp_class.assert_called_once_with(
            "smtp.example.com",
            587,
            timeout=10,
        )

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_retries_on_failure(self, mock_smtp_class, mock_sleep, smtp_config):
        """Verify connection retries 3 times with 30-second intervals."""
        mock_smtp_class.side_effect = OSError("Connection refused")

        client = SMTPClient(**smtp_config, retry_interval=30)

        with pytest.raises(SMTPConnectionError) as exc_info:
            client.connect()

        assert mock_smtp_class.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(30)
        assert exc_info.value.correlation_id is not None

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_retries_on_timeout(self, mock_smtp_class, mock_sleep, smtp_config):
        """Verify connection retries on socket timeout."""
        mock_smtp_class.side_effect = SocketTimeout("Connection timed out")

        client = SMTPClient(**smtp_config)

        with pytest.raises(SMTPConnectionError):
            client.connect()

        assert mock_smtp_class.call_count == 3

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_succeeds_on_second_attempt(
        self, mock_smtp_class, mock_sleep, smtp_config
    ):
        """Verify connection succeeds if retry works."""
        mock_smtp = MagicMock()
        mock_smtp_class.side_effect = [OSError("Temporary failure"), mock_smtp]

        client = SMTPClient(**smtp_config)
        result = client.connect()

        assert result is True
        assert mock_smtp_class.call_count == 2
        assert mock_sleep.call_count == 1
        mock_smtp.starttls.assert_called_once()

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connect_retries_on_smtp_exception(
        self, mock_smtp_class, mock_sleep, smtp_config
    ):
        """Verify connection retries on SMTPException."""
        mock_smtp_class.side_effect = smtplib.SMTPException("SMTP error")

        client = SMTPClient(**smtp_config)

        with pytest.raises(SMTPConnectionError):
            client.connect()

        assert mock_smtp_class.call_count == 3


# --- Authentication Tests ---


class TestSMTPClientAuthentication:
    """Tests for SMTP authentication."""

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_authenticate_success(self, mock_smtp_class, smtp_config):
        """Verify successful authentication with credentials."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        result = client.authenticate()

        assert result is True
        mock_smtp.login.assert_called_once_with("user@example.com", "secret123")

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_authenticate_fails_with_wrong_credentials(
        self, mock_smtp_class, smtp_config
    ):
        """Verify authentication failure raises SMTPAuthenticationError."""
        mock_smtp = MagicMock()
        mock_smtp.login.side_effect = smtplib.SMTPAuthenticationError(
            535, b"Authentication failed"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPAuthenticationError) as exc_info:
            client.authenticate()

        assert exc_info.value.correlation_id is not None

    def test_authenticate_raises_when_not_connected(self, smtp_config):
        """Verify authentication raises error when not connected."""
        client = SMTPClient(**smtp_config)

        with pytest.raises(SMTPConnectionError):
            client.authenticate()


# --- Email Sending Tests ---


class TestSMTPClientSendEmail:
    """Tests for email sending with retry logic."""

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_success(self, mock_smtp_class, smtp_config, sample_email):
        """Verify successful email delivery."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        result = client.send_email(sample_email)

        assert result is True
        mock_smtp.sendmail.assert_called_once()
        call_args = mock_smtp.sendmail.call_args
        assert call_args[0][0] == "noreply@alarmnews.com"
        assert call_args[0][1] == "recipient@example.com"

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_html_mime_encoding(
        self, mock_smtp_class, smtp_config, sample_email
    ):
        """Verify email is sent with proper HTML MIME encoding."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        client.send_email(sample_email)

        call_args = mock_smtp.sendmail.call_args
        raw_message = call_args[0][2]

        # Verify MIME headers
        assert "Content-Type: multipart/alternative" in raw_message
        assert "Content-Type: text/html" in raw_message
        assert sample_email.subject in raw_message
        assert sample_email.to_email in raw_message
        assert "Alarm News <noreply@alarmnews.com>" in raw_message

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_retries_on_network_timeout(
        self, mock_smtp_class, mock_sleep, smtp_config, sample_email
    ):
        """Verify delivery retries on network timeout."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = SocketTimeout("Send timed out")
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config, retry_interval=30)
        client.connect()

        with pytest.raises(SMTPDeliveryError) as exc_info:
            client.send_email(sample_email)

        assert mock_smtp.sendmail.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(30)
        assert exc_info.value.correlation_id is not None

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_retries_on_5xx_error(
        self, mock_smtp_class, mock_sleep, smtp_config, sample_email
    ):
        """Verify delivery retries on 5xx SMTP errors."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPResponseException(
            550, b"Mailbox unavailable"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPDeliveryError):
            client.send_email(sample_email)

        assert mock_smtp.sendmail.call_count == 3

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_retries_on_429_error(
        self, mock_smtp_class, mock_sleep, smtp_config, sample_email
    ):
        """Verify delivery retries on 429 rate limit error."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPResponseException(
            429, b"Too many requests"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPDeliveryError):
            client.send_email(sample_email)

        assert mock_smtp.sendmail.call_count == 3

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_no_retry_on_4xx_error(
        self, mock_smtp_class, smtp_config, sample_email
    ):
        """Verify delivery does NOT retry on 4xx errors (except 429)."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPResponseException(
            450, b"Requested action not taken"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPDeliveryError):
            client.send_email(sample_email)

        # Should fail immediately without retrying
        assert mock_smtp.sendmail.call_count == 1

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_succeeds_on_retry(
        self, mock_smtp_class, mock_sleep, smtp_config, sample_email
    ):
        """Verify email is sent if retry succeeds."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = [
            SocketTimeout("Temporary timeout"),
            None,  # Success on second attempt
        ]
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        result = client.send_email(sample_email)

        assert result is True
        assert mock_smtp.sendmail.call_count == 2
        assert mock_sleep.call_count == 1

    def test_send_email_raises_when_not_connected(self, smtp_config, sample_email):
        """Verify send raises error when not connected."""
        client = SMTPClient(**smtp_config)

        with pytest.raises(SMTPConnectionError):
            client.send_email(sample_email)

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_send_email_retries_on_os_error(
        self, mock_smtp_class, mock_sleep, smtp_config, sample_email
    ):
        """Verify delivery retries on generic OS/network errors."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = OSError("Connection reset by peer")
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPDeliveryError):
            client.send_email(sample_email)

        assert mock_smtp.sendmail.call_count == 3


# --- Disconnect Tests ---


class TestSMTPClientDisconnect:
    """Tests for SMTP disconnection."""

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_disconnect_calls_quit(self, mock_smtp_class, smtp_config):
        """Verify disconnect calls quit on the SMTP connection."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        client.disconnect()

        mock_smtp.quit.assert_called_once()
        assert client.is_connected is False

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_disconnect_handles_exception_gracefully(
        self, mock_smtp_class, smtp_config
    ):
        """Verify disconnect handles quit() exceptions gracefully."""
        mock_smtp = MagicMock()
        mock_smtp.quit.side_effect = smtplib.SMTPException("Already disconnected")
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()
        client.disconnect()  # Should not raise

        assert client.is_connected is False

    def test_disconnect_when_not_connected(self, smtp_config):
        """Verify disconnect is safe when not connected."""
        client = SMTPClient(**smtp_config)
        client.disconnect()  # Should not raise
        assert client.is_connected is False


# --- Property Tests ---


class TestSMTPClientProperties:
    """Tests for SMTP client properties."""

    def test_is_connected_false_initially(self, smtp_config):
        """Verify is_connected is False before connect()."""
        client = SMTPClient(**smtp_config)
        assert client.is_connected is False

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_is_connected_true_after_connect(self, mock_smtp_class, smtp_config):
        """Verify is_connected is True after successful connect()."""
        mock_smtp_class.return_value = MagicMock()

        client = SMTPClient(**smtp_config)
        client.connect()
        assert client.is_connected is True


# --- Correlation ID Tests ---


class TestCorrelationIds:
    """Tests for correlation ID logging on errors."""

    @patch("src.email_worker.smtp_client.time.sleep")
    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_connection_error_has_correlation_id(
        self, mock_smtp_class, mock_sleep, smtp_config
    ):
        """Verify connection errors include correlation IDs."""
        mock_smtp_class.side_effect = OSError("Connection refused")

        client = SMTPClient(**smtp_config)

        with pytest.raises(SMTPConnectionError) as exc_info:
            client.connect()

        assert exc_info.value.correlation_id is not None
        # UUID4 format check
        assert len(exc_info.value.correlation_id) == 36

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_auth_error_has_correlation_id(self, mock_smtp_class, smtp_config):
        """Verify authentication errors include correlation IDs."""
        mock_smtp = MagicMock()
        mock_smtp.login.side_effect = smtplib.SMTPAuthenticationError(
            535, b"Auth failed"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPAuthenticationError) as exc_info:
            client.authenticate()

        assert exc_info.value.correlation_id is not None
        assert len(exc_info.value.correlation_id) == 36

    @patch("src.email_worker.smtp_client.smtplib.SMTP")
    def test_delivery_error_has_correlation_id(
        self, mock_smtp_class, smtp_config, sample_email
    ):
        """Verify delivery errors include correlation IDs."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPResponseException(
            450, b"Mailbox busy"
        )
        mock_smtp_class.return_value = mock_smtp

        client = SMTPClient(**smtp_config)
        client.connect()

        with pytest.raises(SMTPDeliveryError) as exc_info:
            client.send_email(sample_email)

        assert exc_info.value.correlation_id is not None
        assert len(exc_info.value.correlation_id) == 36
