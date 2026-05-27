"""
Worker Main Loop for the Alarm News System.

Orchestrates the notification worker lifecycle:
1. Initializes MongoDB (DatabaseInterface), Redis (for distributed locking), Kafka consumer and producer
2. Creates DataRetriever, EmailFormatter, EmailPublisher, EventProcessor
3. Creates EventConsumer and starts consuming with processor.process_event as handler
4. Registers SIGTERM/SIGINT handlers for graceful shutdown

Requirements: 9.4, 12.9, 12.10
"""
import logging
import signal
import sys
from typing import Optional

from src.shared.cache import RedisCache
from src.shared.config import get_config
from src.shared.data_store import DataStore
from src.shared.database import DatabaseInterface, MongoDBConnectionManager
from src.shared.kafka_producer import (
    AlarmNewsKafkaProducer,
    ProducerInterface,
    create_kafka_producer,
)
from src.shared.locking import LockInterface, RedisLock, create_lock_manager
from src.shared.redis_client import RedisConnectionManager
from src.worker.data_retriever import DataRetriever
from src.worker.email_formatter import EmailFormatter
from src.worker.email_publisher import EmailPublisher
from src.worker.event_consumer import EventConsumer, create_event_consumer
from src.worker.event_processor import EventProcessor

logger = logging.getLogger(__name__)


class WorkerMain:
    """
    Main notification worker that orchestrates Kafka consumption,
    distributed locking, data retrieval, email formatting, and publishing.

    Lifecycle:
    - start(): Initialize dependencies, begin consuming events
    - stop(): Signal shutdown, wait for in-flight processing, clean up resources
    """

    def __init__(
        self,
        database: Optional[DatabaseInterface] = None,
        redis_manager: Optional[RedisConnectionManager] = None,
        lock_manager: Optional[LockInterface] = None,
        producer: Optional[ProducerInterface] = None,
        event_consumer: Optional[EventConsumer] = None,
        data_retriever: Optional[DataRetriever] = None,
        email_formatter: Optional[EmailFormatter] = None,
        email_publisher: Optional[EmailPublisher] = None,
        event_processor: Optional[EventProcessor] = None,
    ) -> None:
        """
        Initialize the notification worker.

        All dependencies can be injected for testing. If not provided,
        they are created from configuration.

        Args:
            database: MongoDB connection manager (created from config if None).
            redis_manager: Redis connection manager (created from config if None).
            lock_manager: Distributed lock manager (created with Redis if None).
            producer: Kafka producer for email publishing (created from config if None).
            event_consumer: EventConsumer instance (created from config if None).
            data_retriever: DataRetriever instance (created with database if None).
            email_formatter: EmailFormatter instance (created if None).
            email_publisher: EmailPublisher instance (created with producer if None).
            event_processor: EventProcessor instance (created with all deps if None).
        """
        self._database = database
        self._redis_manager = redis_manager
        self._lock_manager = lock_manager
        self._producer = producer
        self._event_consumer = event_consumer
        self._data_retriever = data_retriever
        self._email_formatter = email_formatter
        self._email_publisher = email_publisher
        self._event_processor = event_processor
        self._running = False

    @property
    def is_running(self) -> bool:
        """Whether the worker is currently running."""
        return self._running

    def _initialize_dependencies(self) -> bool:
        """
        Initialize MongoDB, Redis, Kafka, and all worker components from config.

        Returns:
            True if all dependencies initialized successfully, False otherwise.
        """
        config = get_config()

        # Initialize MongoDB connection
        if self._database is None:
            try:
                self._database = MongoDBConnectionManager(config.mongodb)
                self._database.connect()
                logger.info("MongoDB connection established.")
            except Exception as e:
                logger.error("Failed to initialize MongoDB connection: %s", e)
                return False

        # Initialize Redis connection
        if self._redis_manager is None:
            try:
                self._redis_manager = RedisConnectionManager(config.redis)
                self._redis_manager.connect()
                logger.info("Redis connection established.")
            except Exception as e:
                logger.error("Failed to initialize Redis connection: %s", e)
                return False

        # Initialize distributed lock manager (Redis-backed)
        if self._lock_manager is None:
            try:
                redis_client = self._redis_manager.get_client()
                self._lock_manager = create_lock_manager(
                    "redis", redis_client=redis_client
                )
                logger.info("Distributed lock manager initialized (Redis).")
            except Exception as e:
                logger.error("Failed to initialize lock manager: %s", e)
                return False

        # Initialize Kafka producer
        if self._producer is None:
            try:
                self._producer = AlarmNewsKafkaProducer(
                    bootstrap_servers=config.kafka.bootstrap_servers,
                )
                logger.info("Kafka producer initialized.")
            except Exception as e:
                logger.error("Failed to initialize Kafka producer: %s", e)
                return False

        # Initialize DataRetriever
        if self._data_retriever is None:
            redis_client = self._redis_manager.get_client()
            cache = RedisCache(redis_client)
            data_store = DataStore(database=self._database, cache=cache)
            self._data_retriever = DataRetriever(
                database=self._database, data_store=data_store
            )
            logger.info("DataRetriever initialized.")

        # Initialize EmailFormatter
        if self._email_formatter is None:
            self._email_formatter = EmailFormatter()
            logger.info("EmailFormatter initialized.")

        # Initialize EmailPublisher
        if self._email_publisher is None:
            self._email_publisher = EmailPublisher(
                producer=self._producer,
                topic=config.kafka.email_topic,
                dlq_topic=config.kafka.dlq_topic,
            )
            logger.info("EmailPublisher initialized.")

        # Initialize EventProcessor
        if self._event_processor is None:
            self._event_processor = EventProcessor(
                lock_manager=self._lock_manager,
                data_retriever=self._data_retriever,
                email_formatter=self._email_formatter,
                email_publisher=self._email_publisher,
                worker_id=config.worker.worker_id,
                lock_ttl_seconds=config.worker.lock_ttl_seconds,
                lock_timeout_seconds=config.worker.lock_timeout_seconds,
            )
            logger.info(
                "EventProcessor initialized (worker_id=%s).",
                config.worker.worker_id,
            )

        # Initialize EventConsumer
        if self._event_consumer is None:
            try:
                self._event_consumer = create_event_consumer(
                    shutdown_grace_period_seconds=config.worker.shutdown_grace_period_seconds,
                )
                logger.info("EventConsumer initialized.")
            except Exception as e:
                logger.error("Failed to initialize EventConsumer: %s", e)
                return False

        return True

    def start(self) -> bool:
        """
        Start the notification worker: initialize dependencies and begin consuming.

        Returns:
            True if the worker started successfully, False otherwise.
        """
        if self._running:
            logger.warning("Worker is already running.")
            return True

        logger.info("Starting notification worker...")

        if not self._initialize_dependencies():
            logger.error("Failed to initialize worker dependencies.")
            return False

        self._running = True

        try:
            # Start consuming events with the event processor handler
            self._event_consumer.consume_events(
                self._event_processor.process_event
            )
        except Exception as e:
            logger.error("Worker stopped with error: %s", e)
            raise
        finally:
            self._running = False
            self._cleanup()

        return True

    def stop(self) -> None:
        """
        Stop the notification worker gracefully.

        Signals the consumer to stop, waits for in-flight processing to complete,
        and cleans up resources.
        """
        if not self._running:
            return

        logger.info("Stopping notification worker...")
        self._running = False

        # Signal the event consumer to shut down gracefully
        if self._event_consumer is not None:
            self._event_consumer.shutdown()

    def _cleanup(self) -> None:
        """Clean up resources: close producer, Redis, MongoDB, and consumer."""
        if self._email_publisher is not None:
            try:
                self._email_publisher.close()
                logger.info("EmailPublisher closed.")
            except Exception as e:
                logger.warning("Error closing EmailPublisher: %s", e)

        if self._producer is not None:
            try:
                self._producer.close()
                logger.info("Kafka producer closed.")
            except Exception as e:
                logger.warning("Error closing Kafka producer: %s", e)

        if self._redis_manager is not None:
            try:
                self._redis_manager.close()
                logger.info("Redis connection closed.")
            except Exception as e:
                logger.warning("Error closing Redis connection: %s", e)

        if self._database is not None:
            try:
                self._database.disconnect()
                logger.info("MongoDB connection closed.")
            except Exception as e:
                logger.warning("Error closing MongoDB connection: %s", e)

        if self._event_consumer is not None:
            try:
                self._event_consumer.close()
                logger.info("EventConsumer closed.")
            except Exception as e:
                logger.warning("Error closing EventConsumer: %s", e)

        logger.info("Notification worker stopped.")

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
        Run the notification worker until a shutdown signal is received.

        Registers signal handlers and blocks on consume_events until shutdown.
        This is the main entry point for production use.
        """
        self.register_signal_handlers()

        try:
            if not self.start():
                logger.error("Notification worker failed to start. Exiting.")
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received.")
            self.stop()

        logger.info("Notification worker exited.")


def main() -> None:
    """Entry point for the notification worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = WorkerMain()
    worker.run_forever()


if __name__ == "__main__":
    main()
