"""
Data retriever for the Worker component.

Retrieves user information from MongoDB and crawled data from the Data Store
to prepare content for email notifications.

Responsibilities:
- Look up user by user_id in MongoDB
- Skip processing if user not found or subscription expired
- Query Data Store for crawled data matching user keywords (past 24 hours)
- Return grouped results (news_articles and stock_data) sorted by crawl_timestamp descending
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from src.shared.database import DatabaseInterface
from src.shared.data_store import DataStoreInterface
from src.shared.models import NewsArticle, StockData

logger = logging.getLogger(__name__)

# Constants
USERS_COLLECTION = "users"
DEFAULT_DATA_HOURS = 24


@dataclass
class UserInfo:
    """User information needed for notification processing."""

    user_id: str
    email: str
    keywords: List[str]
    subscription_expiry: Optional[datetime] = None


@dataclass
class CrawledData:
    """Grouped crawled data for email notification content."""

    news_articles: List[NewsArticle]
    stock_data: List[StockData]


class DataRetriever:
    """
    Retrieves user information and crawled data for notification processing.

    Uses DatabaseInterface to look up user info from MongoDB and
    DataStoreInterface to query crawled news/stock data by keywords.
    """

    def __init__(self, database: DatabaseInterface, data_store: DataStoreInterface):
        """
        Initialize the DataRetriever.

        Args:
            database: Database interface for user lookups in MongoDB.
            data_store: Data store interface for querying crawled data.
        """
        self._database = database
        self._data_store = data_store

    def get_user_info(self, user_id: str) -> Optional[UserInfo]:
        """
        Retrieve user email and keywords from MongoDB by user_id.

        Returns None if user not found or subscription has expired.

        Args:
            user_id: The unique user identifier.

        Returns:
            UserInfo if user exists and subscription is active, None otherwise.
        """
        user_doc = self._database.find_one(USERS_COLLECTION, {"user_id": user_id})

        if user_doc is None:
            logger.info("User not found: %s", user_id)
            return None

        # Parse subscription_expiry
        subscription_expiry = user_doc.get("subscription_expiry")
        if isinstance(subscription_expiry, str):
            subscription_expiry = datetime.fromisoformat(subscription_expiry)

        # Check if subscription has expired
        if subscription_expiry is None or subscription_expiry <= datetime.utcnow():
            logger.info(
                "User %s subscription expired (expiry: %s)",
                user_id,
                subscription_expiry,
            )
            return None

        return UserInfo(
            user_id=user_doc["user_id"],
            email=user_doc["email"],
            keywords=user_doc.get("keywords", []),
            subscription_expiry=subscription_expiry,
        )

    def get_crawled_data(self, keywords: List[str], hours: int = DEFAULT_DATA_HOURS) -> CrawledData:
        """
        Query Data Store for crawled data matching keywords within a time range.

        Args:
            keywords: List of keywords to match against crawled data.
            hours: Number of hours to look back (default: 24).

        Returns:
            CrawledData with news_articles and stock_data sorted by
            crawl_timestamp descending.
        """
        now = datetime.utcnow()
        start_time = now - timedelta(hours=hours)
        end_time = now

        result = self._data_store.query_by_keywords(
            keywords=keywords,
            start_time=start_time,
            end_time=end_time,
        )

        news_articles = result.get("news_articles", [])
        stock_data = result.get("stock_data", [])

        logger.debug(
            "Retrieved %d news articles and %d stock items for keywords: %s",
            len(news_articles),
            len(stock_data),
            keywords,
        )

        return CrawledData(
            news_articles=news_articles,
            stock_data=stock_data,
        )

    def retrieve_notification_data(self, user_id: str) -> Optional[tuple]:
        """
        Retrieve all data needed for a notification email.

        Combines user lookup and crawled data retrieval into a single
        convenience method. Returns None if user not found or subscription expired.

        Args:
            user_id: The unique user identifier.

        Returns:
            Tuple of (UserInfo, CrawledData) if user is active, None otherwise.
        """
        user_info = self.get_user_info(user_id)
        if user_info is None:
            return None

        if not user_info.keywords:
            logger.info("User %s has no keywords configured", user_id)
            return user_info, CrawledData(news_articles=[], stock_data=[])

        crawled_data = self.get_crawled_data(user_info.keywords)
        return user_info, crawled_data
