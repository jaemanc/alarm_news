"""
Email Delivery Worker Main Loop for the Alarm News System.

Orchestrates the email delivery worker lifecycle:
1. Initializes EmailConsumer (Kafka consumer for email-delivery topic)
2. Initializes SMTPClient with config from environment
3. Initializes ProducerInterface for DLQ publishing
4. Creates DeliveryHandler with smtp_client and dlq_producer
5. Starts consuming emails: email_consumer.consume_emails(handler.handle)
6. Handles SIGTERM for graceful shutdown

Requirements: 10.1, 10.9
"""
import logging
import signal
import sys
from typing import Optional

from src.email_worker.delivery_handler import DeliveryHandler
from src.email_worker.email_consumer import EmailConsumer, create_email_consumer
from src.email_worker.smtp_client import SMTPClient
from src.shared.config import get_config
from src.shared.kafka_producer import (
    AlarmNewsKafkaProducer,
    ProducerInterface,
    create_kafka_producer,
)

logger = logging.getLogger(__name__)


class EmailWorkerMain:
    """
    Main email delivery worker that orchestrates Kafka consumption,
    SMTP delivery, and DLQ publishing.

    Lifecycle:
    - start(): Initialize dependencies, connect SMTP, begin consuming
    - stop(): Signal shutdown, wait for in-flight delivery, clean up resources
    """

    def __init__(
        self,
        email_consumer: Optional[EmailConsumer] = None,
        smtp_client: Optional[SMTPClient] = None,
        dlq_producer: Optional[ProducerInterface] = None,
        delivery_handler: Optional[DeliveryHandler] = None,
    ) -> None:
        """
        Initialize the email delivery worker.

        All dependencies can be injected for testing. If not provided,
        they are created from configuration.

        Args:
            email_consumer: EmailConsumer instance (created from config if None).
            smtp_client: SMTPClient instance (created from config if None).
            dlq_producer: ProducerInterface for DLQ publishing (created from config if None).
            delivery_handler: DeliveryHandler instance (created with smtp/dlq if None).
        """
        self._email_consumer = email_consumer
        self._smtp_client = smtp_client
        self._dlq_producer = dlq_producer
        self._delivery_handler = delivery_handler
        self._running = False

    @property
    def is_running(self) -> bool:
        """Whether the worker is currently running."""
        return self._running

    def _initialize_dependencies(self) -> bool:
        """
        Initialize Kafka consumer, SMTP client, DLQ producer, and delivery handler.

        Returns:
            True if all dependencies initialized successfully, False otherwise.
        """
        config = get_config()

        # Initialize EmailConsumer (Kafka consumer for email-delivery topic)
        if self._email_consumer is None:
            try:
                self._email_consumer = create_email_consumer()
                logger.info("EmailConsumer initialized.")
            except Exception as e:
                logger.error("Failed to initialize EmailConsumer: %s", e)
                return False

        # Initialize SMTPClient with config from environment
        if self._smtp_client is None:
            try:
                self._smtp_client = SMTPClient(
                    host=config.smtp.host,
                    port=config.smtp.port,
                    username=config.smtp.username,
                    password=config.smtp.password,
                    from_email=config.smtp.from_email,
                    from_name=config.smtp.from_name,
                )
                logger.info("SMTPClient initialized.")
            except Exception as e:
                logger.error("Failed to initialize SMTPClient: %s", e)
                return False

        # Initialize ProducerInterface for DLQ publishing
        if self._dlq_producer is None:
            try:
                self._dlq_producer = create_kafka_producer()
                logger.info("DLQ producer initialized.")
            except Exception as e:
                logger.error("Failed to initialize DLQ producer: %s", e)
                return False

        # Create DeliveryHandler with smtp_client and dlq_producer
        if self._delivery_handler is None:
            self._delivery_handler = DeliveryHandler(
                smtp_client=self._smtp_client,
                dlq_producer=self._dlq_producer,
                dlq_topic=config.kafka.dlq_topic,
            )
            logger.info("DeliveryHandler initialized.")

        return True

    def start(self) -> bool:
        """
        Start the email delivery worker: initialize dependencies and begin consuming.

        Returns:
            True if the worker started successfully, False otherwise.
        """
        if self._running:
            logger.warning("Email worker is already running.")
            return True

        logger.info("Starting email delivery worker...")

        if not self._initialize_dependencies():
            logger.error("Failed to initialize email worker dependencies.")
            return False

        self._running = True

        try:
            # Start consuming emails with delivery handler
            self._email_consumer.consume_emails(self._delivery_handler.handle)
        except Exception as e:
            logger.error("Email worker stopped with error: %s", e)
            raise
        finally:
            self._running = False
            self._cleanup()

        return True

    def stop(self) -> None:
        """
        Stop the email delivery worker gracefully.

        Signals the consumer to stop, waits for in-flight delivery to complete,
        and cleans up resources.
        """
        if not self._running:
            return

        logger.info("Stopping email delivery worker...")
        self._running = False

        # Signal the email consumer to shut down gracefully
        if self._email_consumer is not None:
            self._email_consumer.shutdown()

    def _cleanup(self) -> None:
        """Clean up resources: close SMTP connection, producer, and consumer."""
        if self._smtp_client is not None:
            try:
                self._smtp_client.disconnect()
                logger.info("SMTP client disconnected.")
            except Exception as e:
                logger.warning("Error disconnecting SMTP client: %s", e)

        if self._dlq_producer is not None:
            try:
                self._dlq_producer.close()
                logger.info("DLQ producer closed.")
            except Exception as e:
                logger.warning("Error closing DLQ producer: %s", e)

        if self._email_consumer is not None:
            try:
                self._email_consumer.close()
                logger.info("EmailConsumer closed.")
            except Exception as e:
                logger.warning("Error closing EmailConsumer: %s", e)

        logger.info("Email delivery worker stopped.")

    def register_signal_handlers(self) -> None:
        """
        Register SIGTERM and SIGINT handlers for graceful shutdown.

        Should only be called from the main thread.
        """

        def _signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.info(
                "Received %s signal. Initiating graceful shutdown.", sig_name
            )
            self.stop()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        logger.info("Signal handlers registered (SIGTERM, SIGINT).")

    def run_forever(self) -> None:
        """
        Run the email delivery worker until a shutdown signal is received.

        Registers signal handlers and blocks on consume_emails until shutdown.
        This is the main entry point for production use.
        """
        self.register_signal_handlers()

        try:
            if not self.start():
                logger.error("Email delivery worker failed to start. Exiting.")
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received.")
            self.stop()

        logger.info("Email delivery worker exited.")


def main() -> None:
    """Entry point for the email delivery worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = EmailWorkerMain()
    worker.run_forever()


if __name__ == "__main__":
    main()
