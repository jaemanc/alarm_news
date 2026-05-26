"""
Kafka topic initialization script for the Alarm News System.

Creates the required Kafka topics with appropriate configurations:
- notification-events: 12 partitions, replication factor 3, retention 24h
- email-delivery: 12 partitions, replication factor 3, retention 24h
- notification-dlq: 3 partitions, replication factor 3, retention 7 days

This script is idempotent — it skips topics that already exist.

Usage:
    python scripts/init_kafka_topics.py

Environment Variables:
    KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS: Comma-separated broker addresses
        (default: localhost:9092)
"""
import logging
import os
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Topic definitions
TOPICS = [
    {
        "name": "notification-events",
        "num_partitions": 12,
        "replication_factor": 3,
        "topic_configs": {
            "min.insync.replicas": "2",
            "retention.ms": str(24 * 60 * 60 * 1000),  # 24 hours
        },
    },
    {
        "name": "email-delivery",
        "num_partitions": 12,
        "replication_factor": 3,
        "topic_configs": {
            "min.insync.replicas": "2",
            "retention.ms": str(24 * 60 * 60 * 1000),  # 24 hours
        },
    },
    {
        "name": "notification-dlq",
        "num_partitions": 3,
        "replication_factor": 3,
        "topic_configs": {
            "min.insync.replicas": "2",
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
        },
    },
]


def get_broker_addresses() -> str:
    """
    Load Kafka broker addresses from environment variables.

    Checks KAFKA_BROKERS first, then falls back to KAFKA_BOOTSTRAP_SERVERS.
    Defaults to localhost:9092 if neither is set.

    Returns:
        Comma-separated broker addresses.
    """
    brokers = os.environ.get(
        "KAFKA_BROKERS",
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    return brokers


def create_admin_client(bootstrap_servers: str) -> KafkaAdminClient:
    """
    Create a KafkaAdminClient connected to the specified brokers.

    Args:
        bootstrap_servers: Comma-separated broker addresses.

    Returns:
        A connected KafkaAdminClient instance.

    Raises:
        NoBrokersAvailable: If connection to brokers fails.
    """
    logger.info("Connecting to Kafka brokers: %s", bootstrap_servers)
    client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="alarm-news-topic-init",
    )
    logger.info("Connected to Kafka brokers successfully")
    return client


def get_existing_topics(admin_client: KafkaAdminClient) -> set:
    """
    Retrieve the set of existing topic names from the Kafka cluster.

    Args:
        admin_client: A connected KafkaAdminClient.

    Returns:
        A set of existing topic names.
    """
    existing = set(admin_client.list_topics())
    return existing


def create_topics(admin_client: KafkaAdminClient) -> None:
    """
    Create all required Kafka topics, skipping any that already exist.

    Args:
        admin_client: A connected KafkaAdminClient.
    """
    existing_topics = get_existing_topics(admin_client)
    logger.info("Existing topics: %s", existing_topics)

    topics_to_create = []
    for topic_def in TOPICS:
        topic_name = topic_def["name"]
        if topic_name in existing_topics:
            logger.info(
                "Topic '%s' already exists — skipping creation", topic_name
            )
            continue

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=topic_def["num_partitions"],
            replication_factor=topic_def["replication_factor"],
            topic_configs=topic_def["topic_configs"],
        )
        topics_to_create.append(new_topic)
        logger.info(
            "Preparing topic '%s': partitions=%d, replication_factor=%d, "
            "retention=%s ms",
            topic_name,
            topic_def["num_partitions"],
            topic_def["replication_factor"],
            topic_def["topic_configs"]["retention.ms"],
        )

    if not topics_to_create:
        logger.info("All topics already exist. Nothing to create.")
        return

    try:
        admin_client.create_topics(
            new_topics=topics_to_create, validate_only=False
        )
        for topic in topics_to_create:
            logger.info("Successfully created topic '%s'", topic.name)
    except TopicAlreadyExistsError as e:
        # Handle race condition where topic was created between check and create
        logger.warning(
            "Some topics already existed (race condition): %s", e
        )
    except Exception as e:
        logger.error("Failed to create topics: %s", e)
        raise


def main() -> None:
    """Main entry point for the Kafka topic initialization script."""
    logger.info("Starting Kafka topic initialization for Alarm News System")

    bootstrap_servers = get_broker_addresses()
    if not bootstrap_servers:
        logger.error(
            "No Kafka broker addresses configured. "
            "Set KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS environment variable."
        )
        sys.exit(1)

    try:
        admin_client = create_admin_client(bootstrap_servers)
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka brokers at %s. "
            "Ensure Kafka is running and accessible.",
            bootstrap_servers,
        )
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error connecting to Kafka: %s", e)
        sys.exit(1)

    try:
        create_topics(admin_client)
    finally:
        admin_client.close()
        logger.info("Kafka admin client closed")

    logger.info("Kafka topic initialization complete")


if __name__ == "__main__":
    main()
