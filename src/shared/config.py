"""
Configuration management for Alarm News System.

This module loads and validates configuration from environment variables.
"""
import os
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class MongoDBConfig:
    """MongoDB configuration."""
    uri: str
    database: str
    min_pool_size: int
    max_pool_size: int


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str
    notification_topic: str
    email_topic: str
    dlq_topic: str
    consumer_group_worker: str
    consumer_group_email: str


@dataclass
class RedisConfig:
    """Redis configuration."""
    host: str
    port: int
    password: Optional[str]
    db: int


@dataclass
class SMTPConfig:
    """SMTP configuration."""
    host: str
    port: int
    username: str
    password: str
    from_email: str
    from_name: str


@dataclass
class AuthConfig:
    """Authentication configuration."""
    jwt_secret: str
    jwt_expiry_hours: int
    bcrypt_cost_factor: int


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    max_attempts: int
    window_minutes: int


@dataclass
class CrawlerConfig:
    """Crawler configuration."""
    interval_minutes: int
    request_delay_seconds: int
    request_timeout_seconds: int
    user_agents: List[str]


@dataclass
class SchedulerConfig:
    """Scheduler configuration."""
    user_reload_minutes: int
    evaluation_interval_seconds: int


@dataclass
class WorkerConfig:
    """Worker configuration."""
    worker_id: str
    lock_ttl_seconds: int
    lock_timeout_seconds: int
    shutdown_grace_period_seconds: int


@dataclass
class HealthCheckConfig:
    """Health check configuration."""
    timeout_seconds: int
    inactivity_threshold_minutes: int


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str
    format: str


@dataclass
class Config:
    """Main configuration container."""
    mongodb: MongoDBConfig
    kafka: KafkaConfig
    redis: RedisConfig
    smtp: SMTPConfig
    auth: AuthConfig
    rate_limit: RateLimitConfig
    crawler: CrawlerConfig
    scheduler: SchedulerConfig
    worker: WorkerConfig
    health_check: HealthCheckConfig
    logging: LoggingConfig


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Get environment variable with validation.
    
    Args:
        key: Environment variable name
        default: Default value if not set
        required: Whether the variable is required
        
    Returns:
        Environment variable value
        
    Raises:
        ValueError: If required variable is not set
    """
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Required environment variable {key} is not set")
    return value


def load_config() -> Config:
    """
    Load configuration from environment variables.
    
    Returns:
        Configuration object
        
    Raises:
        ValueError: If required configuration is missing
    """
    mongodb = MongoDBConfig(
        uri=get_env("MONGODB_URI", "mongodb://localhost:27017", required=True),
        database=get_env("MONGODB_DATABASE", "alarm_news", required=True),
        min_pool_size=int(get_env("MONGODB_MIN_POOL_SIZE", "10")),
        max_pool_size=int(get_env("MONGODB_MAX_POOL_SIZE", "100")),
    )
    
    kafka = KafkaConfig(
        bootstrap_servers=get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092", required=True),
        notification_topic=get_env("KAFKA_NOTIFICATION_TOPIC", "notification-events"),
        email_topic=get_env("KAFKA_EMAIL_TOPIC", "email-delivery"),
        dlq_topic=get_env("KAFKA_DLQ_TOPIC", "notification-dlq"),
        consumer_group_worker=get_env("KAFKA_CONSUMER_GROUP_WORKER", "alarm-news-workers"),
        consumer_group_email=get_env("KAFKA_CONSUMER_GROUP_EMAIL", "alarm-news-email-workers"),
    )
    
    redis = RedisConfig(
        host=get_env("REDIS_HOST", "localhost"),
        port=int(get_env("REDIS_PORT", "6379")),
        password=get_env("REDIS_PASSWORD") or None,
        db=int(get_env("REDIS_DB", "0")),
    )
    
    smtp = SMTPConfig(
        host=get_env("SMTP_HOST", required=True),
        port=int(get_env("SMTP_PORT", "587")),
        username=get_env("SMTP_USERNAME", required=True),
        password=get_env("SMTP_PASSWORD", required=True),
        from_email=get_env("SMTP_FROM_EMAIL", required=True),
        from_name=get_env("SMTP_FROM_NAME", "Alarm News"),
    )
    
    auth = AuthConfig(
        jwt_secret=get_env("JWT_SECRET", required=True),
        jwt_expiry_hours=int(get_env("JWT_EXPIRY_HOURS", "24")),
        bcrypt_cost_factor=int(get_env("BCRYPT_COST_FACTOR", "12")),
    )
    
    rate_limit = RateLimitConfig(
        max_attempts=int(get_env("RATE_LIMIT_MAX_ATTEMPTS", "5")),
        window_minutes=int(get_env("RATE_LIMIT_WINDOW_MINUTES", "15")),
    )
    
    user_agents_str = get_env(
        "CRAWLER_USER_AGENTS",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    user_agents = [ua.strip() for ua in user_agents_str.split(",")]
    
    crawler = CrawlerConfig(
        interval_minutes=int(get_env("CRAWLER_INTERVAL_MINUTES", "30")),
        request_delay_seconds=int(get_env("CRAWLER_REQUEST_DELAY_SECONDS", "2")),
        request_timeout_seconds=int(get_env("CRAWLER_REQUEST_TIMEOUT_SECONDS", "30")),
        user_agents=user_agents,
    )
    
    scheduler = SchedulerConfig(
        user_reload_minutes=int(get_env("SCHEDULER_USER_RELOAD_MINUTES", "5")),
        evaluation_interval_seconds=int(get_env("SCHEDULER_EVALUATION_INTERVAL_SECONDS", "60")),
    )
    
    worker = WorkerConfig(
        worker_id=get_env("WORKER_ID", "worker-1"),
        lock_ttl_seconds=int(get_env("WORKER_LOCK_TTL_SECONDS", "300")),
        lock_timeout_seconds=int(get_env("WORKER_LOCK_TIMEOUT_SECONDS", "10")),
        shutdown_grace_period_seconds=int(get_env("WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS", "60")),
    )
    
    health_check = HealthCheckConfig(
        timeout_seconds=int(get_env("HEALTH_CHECK_TIMEOUT_SECONDS", "5")),
        inactivity_threshold_minutes=int(get_env("HEALTH_CHECK_INACTIVITY_THRESHOLD_MINUTES", "5")),
    )
    
    logging_config = LoggingConfig(
        level=get_env("LOG_LEVEL", "INFO"),
        format=get_env("LOG_FORMAT", "json"),
    )
    
    return Config(
        mongodb=mongodb,
        kafka=kafka,
        redis=redis,
        smtp=smtp,
        auth=auth,
        rate_limit=rate_limit,
        crawler=crawler,
        scheduler=scheduler,
        worker=worker,
        health_check=health_check,
        logging=logging_config,
    )


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        Configuration object
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config
