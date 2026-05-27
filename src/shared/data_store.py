"""
Data Store interface for the Alarm News System.

Provides methods for storing and querying crawled data (news articles and
stock data). Used by the web crawlers to persist crawled data and by the
worker to retrieve data for email notifications.

Collections:
    - crawled_news: Stores crawled news articles
    - crawled_stocks: Stores crawled stock data
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Optional

from src.shared.cache import CacheInterface
from src.shared.database import DatabaseInterface
from src.shared.models import NewsArticle, StockData

logger = logging.getLogger(__name__)

# Constants
CRAWLED_NEWS_COLLECTION = "crawled_news"
CRAWLED_STOCKS_COLLECTION = "crawled_stocks"
CRAWLED_URL_CACHE_PREFIX = "crawled_url:"
CRAWLED_URL_TTL_DAYS = 7


class DataStoreInterface(ABC):
    """
    Abstract interface for the crawled data store.

    Provides methods for storing news articles and stock data,
    querying by keywords with time range filtering, and checking
    for duplicate URLs.
    """

    @abstractmethod
    def store_news_article(self, article: NewsArticle) -> None:
        """
        Store a crawled news article in the data store.

        Args:
            article: NewsArticle instance to store.

        Raises:
            Exception: If storage fails after retries.
        """
        ...

    @abstractmethod
    def store_stock_data(self, stock_data: StockData) -> None:
        """
        Store crawled stock data in the data store.

        Args:
            stock_data: StockData instance to store.

        Raises:
            Exception: If storage fails after retries.
        """
        ...

    @abstractmethod
    def query_by_keywords(
        self,
        keywords: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> dict:
        """
        Query the data store for crawled data matching keywords within a time range.

        Args:
            keywords: List of keywords to match against.
            start_time: Start of time range filter (inclusive). Defaults to 24 hours ago.
            end_time: End of time range filter (inclusive). Defaults to now.

        Returns:
            Dictionary with keys 'news_articles' and 'stock_data', each containing
            a list of matching items sorted by crawl_timestamp descending.
        """
        ...

    @abstractmethod
    def is_url_crawled(self, url: str) -> bool:
        """
        Check if a URL has already been crawled (duplicate check).

        Uses cache with a 7-day TTL to track crawled URLs.

        Args:
            url: The article URL to check.

        Returns:
            True if the URL has been crawled before, False otherwise.
        """
        ...


class DataStore(DataStoreInterface):
    """
    Concrete implementation of the data store using MongoDB and cache.

    Stores crawled news articles and stock data in MongoDB collections,
    and uses the cache layer for fast duplicate URL checking.
    """

    def __init__(self, database: DatabaseInterface, cache: CacheInterface):
        """
        Initialize the DataStore.

        Args:
            database: Database interface for persistent storage.
            cache: Cache interface for duplicate URL tracking.
        """
        self._database = database
        self._cache = cache

    def store_news_article(self, article: NewsArticle) -> None:
        """
        Store a crawled news article in the data store.

        Also marks the article URL as crawled in the cache with a 7-day TTL.

        Args:
            article: NewsArticle instance to store.
        """
        try:
            self._database.insert_one(CRAWLED_NEWS_COLLECTION, article.to_dict())
            self._mark_url_crawled(article.url)
            logger.debug(
                "Stored news article '%s' (id: %s, keyword: %s)",
                article.title,
                article.article_id,
                article.matched_keyword,
            )
        except Exception as e:
            logger.error(
                "Failed to store news article %s: %s",
                article.article_id,
                str(e),
            )
            raise

    def store_stock_data(self, stock_data: StockData) -> None:
        """
        Store crawled stock data in the data store.

        Args:
            stock_data: StockData instance to store.
        """
        try:
            self._database.insert_one(CRAWLED_STOCKS_COLLECTION, stock_data.to_dict())
            logger.debug(
                "Stored stock data %s (%s) price=%.2f (keyword: %s)",
                stock_data.symbol,
                stock_data.company_name,
                stock_data.current_price,
                stock_data.matched_keyword,
            )
        except Exception as e:
            logger.error(
                "Failed to store stock data %s: %s",
                stock_data.stock_id,
                str(e),
            )
            raise

    def query_by_keywords(
        self,
        keywords: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> dict:
        """
        Query the data store for crawled data matching keywords within a time range.

        Args:
            keywords: List of keywords to match against.
            start_time: Start of time range filter (inclusive). Defaults to 24 hours ago.
            end_time: End of time range filter (inclusive). Defaults to now.

        Returns:
            Dictionary with keys 'news_articles' (List[NewsArticle]) and
            'stock_data' (List[StockData]), sorted by crawl_timestamp descending.
        """
        now = datetime.utcnow()
        if end_time is None:
            end_time = now
        if start_time is None:
            start_time = now - timedelta(hours=24)

        # Build query filter for keywords and time range
        query = {
            "matched_keyword": {"$in": keywords},
            "crawl_timestamp": {
                "$gte": start_time.isoformat(),
                "$lte": end_time.isoformat(),
            },
        }

        # Query news articles
        news_docs = self._database.find_many(CRAWLED_NEWS_COLLECTION, query)
        news_articles = [NewsArticle.from_dict(doc) for doc in news_docs]
        # Sort by crawl_timestamp descending
        news_articles.sort(
            key=lambda a: a.crawl_timestamp or datetime.min,
            reverse=True,
        )

        # Query stock data
        stock_docs = self._database.find_many(CRAWLED_STOCKS_COLLECTION, query)
        stock_data = [StockData.from_dict(doc) for doc in stock_docs]
        # Sort by crawl_timestamp descending
        stock_data.sort(
            key=lambda s: s.crawl_timestamp or datetime.min,
            reverse=True,
        )

        return {
            "news_articles": news_articles,
            "stock_data": stock_data,
        }

    def is_url_crawled(self, url: str) -> bool:
        """
        Check if a URL has already been crawled.

        Uses the cache with a 7-day TTL for fast lookups.

        Args:
            url: The article URL to check.

        Returns:
            True if the URL was already crawled, False otherwise.
        """
        cache_key = f"{CRAWLED_URL_CACHE_PREFIX}{url}"
        return self._cache.exists(cache_key)

    def _mark_url_crawled(self, url: str) -> None:
        """
        Mark a URL as crawled in the cache with 7-day TTL.

        Args:
            url: The URL to mark as crawled.
        """
        cache_key = f"{CRAWLED_URL_CACHE_PREFIX}{url}"
        ttl = timedelta(days=CRAWLED_URL_TTL_DAYS)
        self._cache.set(cache_key, True, ttl=ttl)
