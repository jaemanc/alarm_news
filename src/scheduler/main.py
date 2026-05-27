"""
Scheduler Main Loop for the Alarm News System.

Orchestrates the scheduler lifecycle:
1. Initializes MongoDB connection and Kafka producer
2. Starts the UserLoader (initial load with retries + periodic reloads)
3. Evaluates notification times every minute against cached users
4. Publishes matched events to Kafka via EventPublisher
5. Handles graceful shutdown on SIGTERM

Requirements: 8.1, 8.3, 8.4, 8.6
"""
import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from src.scheduler.event_publisher import EventPublisher
from src.scheduler.notification_evaluator import NotificationTimeEvaluator
from src.scheduler.user_loader import UserLoader
from src.shared.config import get_config
from src.shared.database import MongoDBConnectionManager
from src.shared.kafka_producer import AlarmNewsKafkaProducer, ProducerInterface

logger = logging.getLogger(__name__)

# Default evaluation interval: 60 seconds (1 minute)
DEFAULT_EVALUATION_INTERVAL_SECONDS = 60


class SchedulerMain:
    """
    Main scheduler loop that orchestrates user loading, notification
    time evaluation, and event publishing.

    Lifecycle:
    - start(): Initialize dependencies, start user loader, begin evaluation loop
    - stop(): Signal shutdown, wait for current cycle to finish, clean up resources
    """

    def __init__(
        self,
        database: Optional[MongoDBConnectionManager] = None,
        producer: Optional[ProducerInterface] = None,
        user_loader: Optional[UserLoader] = None,
        evaluator: Optional[NotificationTimeEvaluator] = None,
        event_publisher: Optional[EventPublisher] = None,
        evaluation_interval_seconds: int = DEFAULT_EVALUATION_INTERVAL_SECONDS,
        instance_id: int = 0,
        total_instances: int = 1,
    ) -> None:
        """
        Initialize the scheduler main loop.

        All dependencies can be injected for testing. If not provided,
        they are created from configuration.

        Args:
            database: MongoDB connection manager (created from config if None).
            producer: Kafka producer (created from config if None).
            user_loader: UserLoader instance (created with database if None).
            evaluator: NotificationTimeEvaluator (created with instance params if None).
            event_publisher: EventPublisher (created with producer if None).
            evaluation_interval_seconds: Seconds between evaluation cycles (default: 60).
            instance_id: This scheduler instance's ID for consistent hashing.
            total_instances: Total number of scheduler instances.
        """
        self._database = database
        self._producer = producer
        self._user_loader = user_loader
        self._evaluator = evaluator
        self._event_publisher = event_publisher
        self._evaluation_interval_seconds = evaluation_interval_seconds
        self._instance_id = instance_id
        self._total_instances = total_instances

        self._running = False
        self._shutdown_event = threading.Event()
        self._evaluation_thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        """Whether the scheduler is currently running."""
        return self._running

    def _initialize_dependencies(self) -> bool:
        """
        Initialize MongoDB connection and Kafka producer from config.

        If all dependencies are already injected, skips config loading.

        Returns:
            True if all dependencies initialized successfully, False otherwise.
        """
        # Check if we need config (any dependency not yet provided)
        needs_config = (
            self._database is None
            or self._producer is None
            or self._user_loader is None
            or self._evaluator is None
            or self._event_publisher is None
        )

        config = None
        if needs_config:
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

        # Initialize UserLoader
        if self._user_loader is None:
            self._user_loader = UserLoader(
                database=self._database,
                reload_interval_seconds=config.scheduler.user_reload_minutes * 60,
            )

        # Initialize NotificationTimeEvaluator
        if self._evaluator is None:
            self._evaluator = NotificationTimeEvaluator(
                instance_id=self._instance_id,
                total_instances=self._total_instances,
            )

        # Initialize EventPublisher
        if self._event_publisher is None:
            self._event_publisher = EventPublisher(
                producer=self._producer,
                topic=config.kafka.notification_topic,
            )

        return True

    def start(self) -> bool:
        """
        Start the scheduler: initialize dependencies, load users, begin evaluation.

        Returns:
            True if the scheduler started successfully, False otherwise.
        """
        if self._running:
            logger.warning("Scheduler is already running.")
            return True

        logger.info(
            "Starting scheduler (instance %d/%d, evaluation interval: %ds)",
            self._instance_id,
            self._total_instances,
            self._evaluation_interval_seconds,
        )

        # Initialize dependencies
        if not self._initialize_dependencies():
            logger.error("Failed to initialize scheduler dependencies.")
            return False

        # Start user loader (performs initial load with retries)
        if not self._user_loader.start():
            logger.error("Failed to start user loader. Scheduler cannot start.")
            return False

        # Mark as running and start evaluation loop
        self._running = True
        self._shutdown_event.clear()

        self._evaluation_thread = threading.Thread(
            target=self._evaluation_loop,
            name="scheduler-evaluation-loop",
            daemon=True,
        )
        self._evaluation_thread.start()

        logger.info("Scheduler started successfully.")
        return True

    def stop(self) -> None:
        """
        Stop the scheduler gracefully.

        Signals the evaluation loop to stop, waits for the current cycle
        to complete, stops the user loader, and closes resources.
        """
        if not self._running:
            return

        logger.info("Stopping scheduler...")
        self._running = False
        self._shutdown_event.set()

        # Wait for evaluation thread to finish
        if self._evaluation_thread is not None and self._evaluation_thread.is_alive():
            self._evaluation_thread.join(timeout=self._evaluation_interval_seconds + 5)
            if self._evaluation_thread.is_alive():
                logger.warning("Evaluation thread did not stop within timeout.")

        # Stop user loader
        if self._user_loader is not None:
            self._user_loader.stop()

        # Close event publisher and producer
        if self._event_publisher is not None:
            self._event_publisher.close()

        # Disconnect from MongoDB
        if self._database is not None:
            self._database.disconnect()

        logger.info("Scheduler stopped.")

    def _evaluation_loop(self) -> None:
        """
        Main evaluation loop that runs every evaluation_interval_seconds.

        Evaluates notification times against cached users and publishes
        matched events to Kafka.
        """
        logger.info("Evaluation loop started.")

        while self._running and not self._shutdown_event.is_set():
            try:
                self._evaluate_and_publish()
            except Exception as e:
                logger.error("Error in evaluation cycle: %s", e, exc_info=True)

            # Wait for the next cycle or shutdown signal
            self._shutdown_event.wait(timeout=self._evaluation_interval_seconds)

        logger.info("Evaluation loop stopped.")

    def _evaluate_and_publish(self) -> None:
        """
        Run one evaluation cycle: get users, evaluate times, publish events.
        """
        current_time = datetime.utcnow()
        users = self._user_loader.users

        if not users:
            logger.debug("No cached users to evaluate.")
            return

        # Evaluate notification times
        events = self._evaluator.evaluate_notification_times(current_time, users)

        if not events:
            logger.debug(
                "No notification times matched at %s for %d users.",
                current_time.strftime("%H:%M"),
                len(users),
            )
            return

        # Publish matched events
        published_count = 0
        failed_count = 0

        for event in events:
            success = self._event_publisher.publish_event(event)
            if success:
                published_count += 1
            else:
                failed_count += 1

        logger.info(
            "Evaluation cycle complete: %d events published, %d failed "
            "(time=%s, users=%d)",
            published_count,
            failed_count,
            current_time.strftime("%H:%M"),
            len(users),
        )

    def register_signal_handlers(self) -> None:
        """
        Register SIGTERM and SIGINT handlers for graceful shutdown.

        Should only be called from the main thread.
        """

        def _signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.info("Received %s signal. Initiating graceful shutdown.", sig_name)
            self.stop()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        logger.info("Signal handlers registered (SIGTERM, SIGINT).")

    def run_forever(self) -> None:
        """
        Run the scheduler until a shutdown signal is received.

        Registers signal handlers and blocks until shutdown.
        This is the main entry point for production use.
        """
        self.register_signal_handlers()

        if not self.start():
            logger.error("Scheduler failed to start. Exiting.")
            sys.exit(1)

        logger.info("Scheduler running. Waiting for shutdown signal...")

        # Block until shutdown
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received.")
            self.stop()

        logger.info("Scheduler exited.")


def main() -> None:
    """Entry point for the scheduler process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    scheduler = SchedulerMain()
    scheduler.run_forever()


if __name__ == "__main__":
    main()
