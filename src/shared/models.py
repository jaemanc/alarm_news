"""
Core domain models for the Alarm News System.

This module defines Python dataclasses for all domain entities used across
the system components. Each model includes serialization helpers (to_dict,
from_dict) for MongoDB compatibility and future Redis serialization.

Models:
    - NotificationTime: User-specified notification time (hour, minute)
    - User: Registered user with credentials, keywords, and subscription info
    - NotificationEvent: Scheduler-generated event for notification processing
    - NewsArticle: Crawled news article data
    - StockData: Crawled stock information
    - EmailNotification: Formatted email ready for delivery
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class NotificationTime:
    """
    Represents a user-configured notification time.

    Attributes:
        hour: Hour of day (0-23)
        minute: Minute of hour (0-59)
    """
    hour: int
    minute: int

    def __post_init__(self) -> None:
        if not (0 <= self.hour <= 23):
            raise ValueError(f"hour must be between 0 and 23, got {self.hour}")
        if not (0 <= self.minute <= 59):
            raise ValueError(f"minute must be between 0 and 59, got {self.minute}")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for MongoDB storage."""
        return {"hour": self.hour, "minute": self.minute}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotificationTime":
        """Deserialize from a MongoDB document."""
        return cls(hour=data["hour"], minute=data["minute"])


@dataclass
class User:
    """
    Represents a registered user in the system.

    Attributes:
        user_id: Unique identifier (UUID4 string)
        hashed_password: Bcrypt-hashed password
        email: User's email address
        keywords: List of keyword strings for news/stock matching
        notification_times: List of configured notification times
        subscription_expiry: Timestamp when subscription expires
    """
    user_id: str
    hashed_password: str
    email: str
    keywords: List[str] = field(default_factory=list)
    notification_times: List[NotificationTime] = field(default_factory=list)
    subscription_expiry: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for MongoDB storage."""
        return {
            "user_id": self.user_id,
            "hashed_password": self.hashed_password,
            "email": self.email,
            "keywords": self.keywords,
            "notification_times": [nt.to_dict() for nt in self.notification_times],
            "subscription_expiry": self.subscription_expiry,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "User":
        """Deserialize from a MongoDB document."""
        notification_times = [
            NotificationTime.from_dict(nt)
            for nt in data.get("notification_times", [])
        ]
        return cls(
            user_id=data["user_id"],
            hashed_password=data["hashed_password"],
            email=data["email"],
            keywords=data.get("keywords", []),
            notification_times=notification_times,
            subscription_expiry=data.get("subscription_expiry"),
        )


@dataclass
class NotificationEvent:
    """
    Represents a scheduler-generated notification event.

    Published to Kafka when a user's notification time is reached.

    Attributes:
        event_id: Unique event identifier (UUID4 string) for idempotency
        user_id: The user to notify
        notification_timestamp: When the notification was triggered
    """
    event_id: str
    user_id: str
    notification_timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for Kafka/MongoDB storage."""
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "notification_timestamp": self.notification_timestamp.isoformat()
            if isinstance(self.notification_timestamp, datetime)
            else self.notification_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotificationEvent":
        """Deserialize from a Kafka message or MongoDB document."""
        timestamp = data["notification_timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return cls(
            event_id=data["event_id"],
            user_id=data["user_id"],
            notification_timestamp=timestamp,
        )


@dataclass
class NewsArticle:
    """
    Represents a crawled news article.

    Attributes:
        article_id: Unique article identifier (UUID4 string)
        title: Article headline
        summary: Content summary (up to 500 characters)
        url: Source URL of the article
        published_date: When the article was published
        source: Name of the news source/website
        matched_keyword: The keyword that matched this article
        crawl_timestamp: When the article was crawled
    """
    article_id: str
    title: str
    summary: str
    url: str
    published_date: Optional[datetime] = None
    source: str = ""
    matched_keyword: str = ""
    crawl_timestamp: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for MongoDB/Data Store storage."""
        return {
            "article_id": self.article_id,
            "title": self.title,
            "summary": self.summary,
            "url": self.url,
            "published_date": self.published_date.isoformat()
            if isinstance(self.published_date, datetime)
            else self.published_date,
            "source": self.source,
            "matched_keyword": self.matched_keyword,
            "crawl_timestamp": self.crawl_timestamp.isoformat()
            if isinstance(self.crawl_timestamp, datetime)
            else self.crawl_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsArticle":
        """Deserialize from a MongoDB/Data Store document."""
        published_date = data.get("published_date")
        if isinstance(published_date, str):
            published_date = datetime.fromisoformat(published_date)

        crawl_timestamp = data.get("crawl_timestamp")
        if isinstance(crawl_timestamp, str):
            crawl_timestamp = datetime.fromisoformat(crawl_timestamp)

        return cls(
            article_id=data["article_id"],
            title=data["title"],
            summary=data["summary"],
            url=data["url"],
            published_date=published_date,
            source=data.get("source", ""),
            matched_keyword=data.get("matched_keyword", ""),
            crawl_timestamp=crawl_timestamp,
        )


@dataclass
class StockData:
    """
    Represents crawled stock information.

    Attributes:
        stock_id: Unique stock data identifier (UUID4 string)
        symbol: Stock ticker symbol
        company_name: Full company name
        current_price: Current stock price (positive number)
        price_change: Absolute price change from previous
        percentage_change: Percentage change, rounded to 2 decimals
        last_update: When the stock data was last updated
        matched_keyword: The keyword that matched this stock
        crawl_timestamp: When the stock data was crawled
    """
    stock_id: str
    symbol: str
    company_name: str
    current_price: float
    price_change: float
    percentage_change: float
    last_update: Optional[datetime] = None
    matched_keyword: str = ""
    crawl_timestamp: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for MongoDB/Data Store storage."""
        return {
            "stock_id": self.stock_id,
            "symbol": self.symbol,
            "company_name": self.company_name,
            "current_price": self.current_price,
            "price_change": self.price_change,
            "percentage_change": round(self.percentage_change, 2),
            "last_update": self.last_update.isoformat()
            if isinstance(self.last_update, datetime)
            else self.last_update,
            "matched_keyword": self.matched_keyword,
            "crawl_timestamp": self.crawl_timestamp.isoformat()
            if isinstance(self.crawl_timestamp, datetime)
            else self.crawl_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StockData":
        """Deserialize from a MongoDB/Data Store document."""
        last_update = data.get("last_update")
        if isinstance(last_update, str):
            last_update = datetime.fromisoformat(last_update)

        crawl_timestamp = data.get("crawl_timestamp")
        if isinstance(crawl_timestamp, str):
            crawl_timestamp = datetime.fromisoformat(crawl_timestamp)

        return cls(
            stock_id=data["stock_id"],
            symbol=data["symbol"],
            company_name=data["company_name"],
            current_price=data["current_price"],
            price_change=data["price_change"],
            percentage_change=data["percentage_change"],
            last_update=last_update,
            matched_keyword=data.get("matched_keyword", ""),
            crawl_timestamp=crawl_timestamp,
        )


@dataclass
class EmailNotification:
    """
    Represents a formatted email notification ready for delivery.

    Attributes:
        to_email: Recipient email address
        subject: Email subject line
        body_html: HTML-formatted email body
        timestamp: When the notification was generated (ISO 8601)
    """
    to_email: str
    subject: str
    body_html: str
    timestamp: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dictionary for Kafka/MongoDB storage."""
        return {
            "to_email": self.to_email,
            "subject": self.subject,
            "body_html": self.body_html,
            "timestamp": self.timestamp.isoformat()
            if isinstance(self.timestamp, datetime)
            else self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmailNotification":
        """Deserialize from a Kafka message or MongoDB document."""
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return cls(
            to_email=data["to_email"],
            subject=data["subject"],
            body_html=data["body_html"],
            timestamp=timestamp,
        )
