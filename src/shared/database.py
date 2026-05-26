"""
MongoDB connection manager for Alarm News System.

Provides an abstract database interface for swappability and a concrete
MongoDB implementation with connection pooling, retry logic, and index management.
"""
import logging
import time
import functools
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, IndexModel, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    PyMongoError,
    AutoReconnect,
    NetworkTimeout,
)
from pymongo.read_preferences import Primary
from pymongo.write_concern import WriteConcern

from src.shared.config import get_config, MongoDBConfig

logger = logging.getLogger(__name__)


# Retry configuration constants
WRITE_RETRY_ATTEMPTS = 3
WRITE_RETRY_INTERVAL_SECONDS = 5


def retry_on_write_failure(attempts: int = WRITE_RETRY_ATTEMPTS, interval: float = WRITE_RETRY_INTERVAL_SECONDS):
    """
    Decorator that retries write operations on connection failures.

    Args:
        attempts: Maximum number of retry attempts (default: 3).
        interval: Seconds to wait between retries (default: 5).

    Returns:
        Decorated function with retry logic.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except (ConnectionFailure, AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError) as e:
                    last_exception = e
                    if attempt < attempts:
                        logger.warning(
                            "Write operation failed (attempt %d/%d): %s. Retrying in %s seconds...",
                            attempt,
                            attempts,
                            str(e),
                            interval,
                        )
                        time.sleep(interval)
                    else:
                        logger.error(
                            "Write operation failed after %d attempts: %s",
                            attempts,
                            str(e),
                        )
            raise last_exception
        return wrapper
    return decorator


class DatabaseInterface(ABC):
    """
    Abstract database interface for swappability.

    Allows other database backends (e.g., PostgreSQL, DynamoDB) to be
    substituted by implementing this interface.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check database connectivity for readiness probes.

        Returns:
            True if the database is reachable and responsive, False otherwise.
        """
        ...

    @abstractmethod
    def get_collection(self, name: str) -> Any:
        """
        Get a reference to a collection/table.

        Args:
            name: Collection or table name.

        Returns:
            Collection reference.
        """
        ...

    @abstractmethod
    def insert_one(self, collection: str, document: Dict[str, Any]) -> Optional[str]:
        """
        Insert a single document.

        Args:
            collection: Collection name.
            document: Document to insert.

        Returns:
            Inserted document ID or None on failure.
        """
        ...

    @abstractmethod
    def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find a single document matching the query.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            Matching document or None.
        """
        ...

    @abstractmethod
    def update_one(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """
        Update a single document matching the query.

        Args:
            collection: Collection name.
            query: Query filter.
            update: Update operations.

        Returns:
            True if a document was modified, False otherwise.
        """
        ...

    @abstractmethod
    def delete_one(self, collection: str, query: Dict[str, Any]) -> bool:
        """
        Delete a single document matching the query.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            True if a document was deleted, False otherwise.
        """
        ...

    @abstractmethod
    def find_many(self, collection: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Find all documents matching the query.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            List of matching documents.
        """
        ...


class MongoDBConnectionManager(DatabaseInterface):
    """
    MongoDB connection manager with connection pooling, write concern,
    read preference, retry logic, and index management.

    Configuration is loaded from the shared config module (environment variables).
    """

    def __init__(self, config: Optional[MongoDBConfig] = None):
        """
        Initialize the MongoDB connection manager.

        Args:
            config: Optional MongoDBConfig. If not provided, loads from environment.
        """
        self._config = config or get_config().mongodb
        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None

    @property
    def client(self) -> MongoClient:
        """Get the MongoClient instance, connecting if necessary."""
        if self._client is None:
            self.connect()
        return self._client

    @property
    def db(self) -> Database:
        """Get the database instance, connecting if necessary."""
        if self._db is None:
            self.connect()
        return self._db

    def connect(self) -> None:
        """
        Establish connection to MongoDB with connection pooling and configuration.

        Configures:
        - Connection pooling: min 10, max 100 (from config)
        - Write concern: "majority"
        - Read preference: "primary"
        """
        logger.info(
            "Connecting to MongoDB at %s (database: %s, pool: %d-%d)",
            self._config.uri,
            self._config.database,
            self._config.min_pool_size,
            self._config.max_pool_size,
        )

        self._client = MongoClient(
            self._config.uri,
            minPoolSize=self._config.min_pool_size,
            maxPoolSize=self._config.max_pool_size,
            w="majority",
            readPreference="primary",
            serverSelectionTimeoutMS=5000,
        )

        self._db = self._client[self._config.database]

        # Verify connectivity
        self._client.admin.command("ping")
        logger.info("Successfully connected to MongoDB.")

        # Initialize collections and indexes
        self._initialize_collections()

    def disconnect(self) -> None:
        """Close the MongoDB connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("Disconnected from MongoDB.")

    def health_check(self) -> bool:
        """
        Check MongoDB connectivity for readiness probes.

        Sends a ping command to verify the server is responsive.

        Returns:
            True if MongoDB responds to ping within timeout, False otherwise.
        """
        try:
            if self._client is None:
                return False
            self._client.admin.command("ping")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError, PyMongoError) as e:
            logger.warning("MongoDB health check failed: %s", str(e))
            return False

    def get_collection(self, name: str) -> Collection:
        """
        Get a reference to a MongoDB collection.

        Args:
            name: Collection name.

        Returns:
            pymongo Collection instance.
        """
        return self.db[name]

    @retry_on_write_failure()
    def insert_one(self, collection: str, document: Dict[str, Any]) -> Optional[str]:
        """
        Insert a single document with retry logic.

        Args:
            collection: Collection name.
            document: Document to insert.

        Returns:
            Inserted document ID as string, or None on failure.
        """
        result = self.db[collection].insert_one(document)
        return str(result.inserted_id)

    def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find a single document matching the query.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            Matching document or None.
        """
        return self.db[collection].find_one(query)

    @retry_on_write_failure()
    def update_one(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """
        Update a single document with retry logic.

        Args:
            collection: Collection name.
            query: Query filter.
            update: Update operations (e.g., {"$set": {...}}).

        Returns:
            True if a document was modified, False otherwise.
        """
        result = self.db[collection].update_one(query, update)
        return result.modified_count > 0

    @retry_on_write_failure()
    def delete_one(self, collection: str, query: Dict[str, Any]) -> bool:
        """
        Delete a single document with retry logic.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            True if a document was deleted, False otherwise.
        """
        result = self.db[collection].delete_one(query)
        return result.deleted_count > 0

    def find_many(self, collection: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Find all documents matching the query.

        Args:
            collection: Collection name.
            query: Query filter.

        Returns:
            List of matching documents.
        """
        return list(self.db[collection].find(query))

    def _initialize_collections(self) -> None:
        """
        Create the 'users' collection and required indexes.

        Indexes:
        - Unique index on 'user_id' field
        - Index on 'subscription_expiry' field for efficient expiry queries
        """
        users_collection = self.db["users"]

        # Create unique index on user_id
        users_collection.create_index(
            [("user_id", ASCENDING)],
            unique=True,
            name="idx_user_id_unique",
        )
        logger.info("Created unique index on 'user_id' field.")

        # Create index on subscription_expiry for efficient expiry queries
        users_collection.create_index(
            [("subscription_expiry", ASCENDING)],
            name="idx_subscription_expiry",
        )
        logger.info("Created index on 'subscription_expiry' field.")


# Module-level singleton instance
_db_manager: Optional[MongoDBConnectionManager] = None


def get_database() -> MongoDBConnectionManager:
    """
    Get the global MongoDB connection manager instance.

    Returns:
        MongoDBConnectionManager singleton instance.
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = MongoDBConnectionManager()
    return _db_manager


def create_database(config: Optional[MongoDBConfig] = None) -> MongoDBConnectionManager:
    """
    Create a new MongoDB connection manager instance.

    Useful for testing or when multiple connections are needed.

    Args:
        config: Optional MongoDBConfig override.

    Returns:
        New MongoDBConnectionManager instance.
    """
    return MongoDBConnectionManager(config=config)
