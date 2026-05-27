# Shared Module
"""
This module contains shared utilities, data models, and database connections.

Key abstractions for extensibility:
- CacheInterface: Abstract caching (in-memory or Redis)
- LockInterface: Abstract distributed locking (in-memory or Redis)
- SessionInterface: Abstract session management (JWT or Redis)
- DatabaseInterface: Abstract database access (MongoDB or other backends)
"""
from src.shared.cache import CacheInterface, InMemoryCache, RedisCache, create_cache
from src.shared.locking import LockInterface, InMemoryLock, RedisLock, create_lock_manager
from src.shared.session import SessionInterface, JWTSessionManager, RedisSessionManager, create_session_manager
from src.shared.config import load_config, get_config, Config
from src.shared.models import (
    NotificationTime,
    User,
    NotificationEvent,
    NewsArticle,
    StockData,
    EmailNotification,
)
from src.shared.database import (
    DatabaseInterface,
    MongoDBConnectionManager,
    get_database,
    create_database,
    retry_on_write_failure,
)
from src.shared.kafka_producer import (
    ProducerInterface,
    AlarmNewsKafkaProducer,
    InMemoryProducer,
    create_kafka_producer,
)
from src.shared.health import (
    HealthChecker,
    HealthStatus,
    DependencyStatus,
)
from src.shared.metrics import (
    MetricsCollector,
    MetricsSnapshot,
    get_metrics_collector,
    reset_metrics_collector,
)
from src.shared.logging_config import (
    setup_logging,
    get_correlation_id,
    set_correlation_id,
    clear_correlation_id,
    log_notification_event,
)

__all__ = [
    # Cache
    "CacheInterface",
    "InMemoryCache",
    "RedisCache",
    "create_cache",
    # Locking
    "LockInterface",
    "InMemoryLock",
    "RedisLock",
    "create_lock_manager",
    # Session
    "SessionInterface",
    "JWTSessionManager",
    "RedisSessionManager",
    "create_session_manager",
    # Config
    "load_config",
    "get_config",
    "Config",
    # Models
    "NotificationTime",
    "User",
    "NotificationEvent",
    "NewsArticle",
    "StockData",
    "EmailNotification",
    # Database
    "DatabaseInterface",
    "MongoDBConnectionManager",
    "get_database",
    "create_database",
    "retry_on_write_failure",
    # Kafka Producer
    "ProducerInterface",
    "AlarmNewsKafkaProducer",
    "InMemoryProducer",
    "create_kafka_producer",
    # Health Check
    "HealthChecker",
    "HealthStatus",
    "DependencyStatus",
    # Metrics
    "MetricsCollector",
    "MetricsSnapshot",
    "get_metrics_collector",
    "reset_metrics_collector",
    # Logging
    "setup_logging",
    "get_correlation_id",
    "set_correlation_id",
    "clear_correlation_id",
    "log_notification_event",
]
