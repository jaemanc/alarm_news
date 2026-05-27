"""
Unit tests for the Worker Data Retriever.

Tests cover:
- Retrieving user info from MongoDB by user_id
- Skipping processing when user not found
- Skipping processing when subscription expired
- Querying Data Store for crawled data matching keywords
- Filtering data by timestamp range (past 24 hours)
- Grouping data into news articles and stock information
- Sorting by crawl_timestamp descending
- The combined retrieve_notification_data convenience method
"""
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.shared.models import NewsArticle, StockData
from src.worker.data_retriever import (
    DataRetriever,
    UserInfo,
    CrawledData,
    USERS_COLLECTION,
    DEFAULT_DATA_HOURS,
)


@pytest.fixture
def mock_database():
    """Create a mock database interface."""
    db = MagicMock()
    db.find_one = MagicMock(return_value=None)
    return db


@pytest.fixture
def mock_data_store():
    """Create a mock data store interface."""
    store = MagicMock()
    store.query_by_keywords = MagicMock(
        return_value={"news_articles": [], "stock_data": []}
    )
    return store


@pytest.fixture
def data_retriever(mock_database, mock_data_store):
    """Create a DataRetriever with mock dependencies."""
    return DataRetriever(database=mock_database, data_store=mock_data_store)


@pytest.fixture
def active_user_doc():
    """Create a sample active user document from MongoDB."""
    return {
        "user_id": "user-123",
        "email": "user@example.com",
        "hashed_password": "$2b$12$hashedpassword",
        "keywords": ["python", "AI", "AAPL"],
        "notification_times": [{"hour": 9, "minute": 0}],
        "subscription_expiry": datetime.utcnow() + timedelta(days=15),
    }


@pytest.fixture
def expired_user_doc():
    """Create a sample user document with expired subscription."""
    return {
        "user_id": "user-expired",
        "email": "expired@example.com",
        "hashed_password": "$2b$12$hashedpassword",
        "keywords": ["tech"],
        "notification_times": [{"hour": 8, "minute": 30}],
        "subscription_expiry": datetime.utcnow() - timedelta(days=5),
    }


@pytest.fixture
def sample_news_articles():
    """Create sample NewsArticle objects."""
    return [
        NewsArticle(
            article_id="art-1",
            title="Python 3.12 Released",
            summary="Major improvements in Python 3.12.",
            url="https://news.example.com/python-3-12",
            published_date=datetime(2025, 1, 15, 10, 0, 0),
            source="news.example.com",
            matched_keyword="python",
            crawl_timestamp=datetime(2025, 1, 15, 14, 0, 0),
        ),
        NewsArticle(
            article_id="art-2",
            title="AI Breakthrough",
            summary="New AI model achieves state-of-the-art results.",
            url="https://news.example.com/ai-breakthrough",
            published_date=datetime(2025, 1, 15, 12, 0, 0),
            source="news.example.com",
            matched_keyword="AI",
            crawl_timestamp=datetime(2025, 1, 15, 15, 0, 0),
        ),
    ]


@pytest.fixture
def sample_stock_data():
    """Create sample StockData objects."""
    return [
        StockData(
            stock_id="stock-1",
            symbol="AAPL",
            company_name="Apple Inc.",
            current_price=150.25,
            price_change=0.75,
            percentage_change=0.50,
            last_update=datetime(2025, 1, 15, 14, 0, 0),
            matched_keyword="AAPL",
            crawl_timestamp=datetime(2025, 1, 15, 14, 30, 0),
        ),
    ]


class TestGetUserInfo:
    """Tests for get_user_info method."""

    def test_returns_user_info_for_active_user(
        self, data_retriever, mock_database, active_user_doc
    ):
        """Should return UserInfo when user exists and subscription is active."""
        mock_database.find_one.return_value = active_user_doc

        result = data_retriever.get_user_info("user-123")

        assert result is not None
        assert isinstance(result, UserInfo)
        assert result.user_id == "user-123"
        assert result.email == "user@example.com"
        assert result.keywords == ["python", "AI", "AAPL"]

    def test_queries_users_collection_by_user_id(
        self, data_retriever, mock_database, active_user_doc
    ):
        """Should query the users collection with user_id filter."""
        mock_database.find_one.return_value = active_user_doc

        data_retriever.get_user_info("user-123")

        mock_database.find_one.assert_called_once_with(
            USERS_COLLECTION, {"user_id": "user-123"}
        )

    def test_returns_none_when_user_not_found(self, data_retriever, mock_database):
        """Should return None when no user document is found."""
        mock_database.find_one.return_value = None

        result = data_retriever.get_user_info("nonexistent-user")

        assert result is None

    def test_returns_none_when_subscription_expired(
        self, data_retriever, mock_database, expired_user_doc
    ):
        """Should return None when user subscription has expired."""
        mock_database.find_one.return_value = expired_user_doc

        result = data_retriever.get_user_info("user-expired")

        assert result is None

    def test_returns_none_when_subscription_expiry_is_none(
        self, data_retriever, mock_database
    ):
        """Should return None when subscription_expiry is None."""
        user_doc = {
            "user_id": "user-no-expiry",
            "email": "noexpiry@example.com",
            "hashed_password": "$2b$12$hash",
            "keywords": ["tech"],
            "subscription_expiry": None,
        }
        mock_database.find_one.return_value = user_doc

        result = data_retriever.get_user_info("user-no-expiry")

        assert result is None

    def test_handles_iso_string_subscription_expiry(
        self, data_retriever, mock_database
    ):
        """Should parse ISO format string for subscription_expiry."""
        future_date = datetime.utcnow() + timedelta(days=10)
        user_doc = {
            "user_id": "user-iso",
            "email": "iso@example.com",
            "hashed_password": "$2b$12$hash",
            "keywords": ["news"],
            "subscription_expiry": future_date.isoformat(),
        }
        mock_database.find_one.return_value = user_doc

        result = data_retriever.get_user_info("user-iso")

        assert result is not None
        assert result.user_id == "user-iso"

    def test_returns_empty_keywords_list_when_missing(
        self, data_retriever, mock_database
    ):
        """Should return empty keywords list when field is missing from doc."""
        user_doc = {
            "user_id": "user-no-kw",
            "email": "nokw@example.com",
            "hashed_password": "$2b$12$hash",
            "subscription_expiry": datetime.utcnow() + timedelta(days=10),
        }
        mock_database.find_one.return_value = user_doc

        result = data_retriever.get_user_info("user-no-kw")

        assert result is not None
        assert result.keywords == []


class TestGetCrawledData:
    """Tests for get_crawled_data method."""

    def test_queries_data_store_with_keywords(
        self, data_retriever, mock_data_store
    ):
        """Should query data store with provided keywords."""
        data_retriever.get_crawled_data(["python", "AI"])

        mock_data_store.query_by_keywords.assert_called_once()
        call_kwargs = mock_data_store.query_by_keywords.call_args
        assert call_kwargs[1]["keywords"] == ["python", "AI"]

    def test_uses_24_hour_default_time_range(
        self, data_retriever, mock_data_store
    ):
        """Should default to 24-hour lookback window."""
        with patch("src.worker.data_retriever.datetime") as mock_dt:
            now = datetime(2025, 1, 15, 14, 0, 0)
            mock_dt.utcnow.return_value = now
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            data_retriever.get_crawled_data(["python"])

            call_kwargs = mock_data_store.query_by_keywords.call_args[1]
            expected_start = now - timedelta(hours=24)
            assert call_kwargs["start_time"] == expected_start
            assert call_kwargs["end_time"] == now

    def test_uses_custom_hours_parameter(
        self, data_retriever, mock_data_store
    ):
        """Should use custom hours parameter for time range."""
        with patch("src.worker.data_retriever.datetime") as mock_dt:
            now = datetime(2025, 1, 15, 14, 0, 0)
            mock_dt.utcnow.return_value = now
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            data_retriever.get_crawled_data(["python"], hours=12)

            call_kwargs = mock_data_store.query_by_keywords.call_args[1]
            expected_start = now - timedelta(hours=12)
            assert call_kwargs["start_time"] == expected_start

    def test_returns_crawled_data_with_news_articles(
        self, data_retriever, mock_data_store, sample_news_articles
    ):
        """Should return CrawledData with news articles from data store."""
        mock_data_store.query_by_keywords.return_value = {
            "news_articles": sample_news_articles,
            "stock_data": [],
        }

        result = data_retriever.get_crawled_data(["python", "AI"])

        assert isinstance(result, CrawledData)
        assert len(result.news_articles) == 2
        assert result.news_articles[0].title == "Python 3.12 Released"

    def test_returns_crawled_data_with_stock_data(
        self, data_retriever, mock_data_store, sample_stock_data
    ):
        """Should return CrawledData with stock data from data store."""
        mock_data_store.query_by_keywords.return_value = {
            "news_articles": [],
            "stock_data": sample_stock_data,
        }

        result = data_retriever.get_crawled_data(["AAPL"])

        assert isinstance(result, CrawledData)
        assert len(result.stock_data) == 1
        assert result.stock_data[0].symbol == "AAPL"

    def test_returns_empty_crawled_data_when_no_matches(
        self, data_retriever, mock_data_store
    ):
        """Should return empty CrawledData when no matching data found."""
        mock_data_store.query_by_keywords.return_value = {
            "news_articles": [],
            "stock_data": [],
        }

        result = data_retriever.get_crawled_data(["nonexistent"])

        assert result.news_articles == []
        assert result.stock_data == []


class TestRetrieveNotificationData:
    """Tests for retrieve_notification_data convenience method."""

    def test_returns_user_info_and_crawled_data(
        self,
        data_retriever,
        mock_database,
        mock_data_store,
        active_user_doc,
        sample_news_articles,
        sample_stock_data,
    ):
        """Should return tuple of (UserInfo, CrawledData) for active user."""
        mock_database.find_one.return_value = active_user_doc
        mock_data_store.query_by_keywords.return_value = {
            "news_articles": sample_news_articles,
            "stock_data": sample_stock_data,
        }

        result = data_retriever.retrieve_notification_data("user-123")

        assert result is not None
        user_info, crawled_data = result
        assert isinstance(user_info, UserInfo)
        assert isinstance(crawled_data, CrawledData)
        assert user_info.user_id == "user-123"
        assert len(crawled_data.news_articles) == 2
        assert len(crawled_data.stock_data) == 1

    def test_returns_none_when_user_not_found(
        self, data_retriever, mock_database
    ):
        """Should return None when user is not found."""
        mock_database.find_one.return_value = None

        result = data_retriever.retrieve_notification_data("nonexistent")

        assert result is None

    def test_returns_none_when_subscription_expired(
        self, data_retriever, mock_database, expired_user_doc
    ):
        """Should return None when user subscription has expired."""
        mock_database.find_one.return_value = expired_user_doc

        result = data_retriever.retrieve_notification_data("user-expired")

        assert result is None

    def test_does_not_query_data_store_when_user_not_found(
        self, data_retriever, mock_database, mock_data_store
    ):
        """Should not query data store if user lookup fails."""
        mock_database.find_one.return_value = None

        data_retriever.retrieve_notification_data("nonexistent")

        mock_data_store.query_by_keywords.assert_not_called()

    def test_returns_empty_crawled_data_when_no_keywords(
        self, data_retriever, mock_database, mock_data_store
    ):
        """Should return empty CrawledData when user has no keywords."""
        user_doc = {
            "user_id": "user-no-kw",
            "email": "nokw@example.com",
            "hashed_password": "$2b$12$hash",
            "keywords": [],
            "subscription_expiry": datetime.utcnow() + timedelta(days=10),
        }
        mock_database.find_one.return_value = user_doc

        result = data_retriever.retrieve_notification_data("user-no-kw")

        assert result is not None
        user_info, crawled_data = result
        assert user_info.keywords == []
        assert crawled_data.news_articles == []
        assert crawled_data.stock_data == []
        mock_data_store.query_by_keywords.assert_not_called()

    def test_queries_data_store_with_user_keywords(
        self, data_retriever, mock_database, mock_data_store, active_user_doc
    ):
        """Should query data store using the user's keywords."""
        mock_database.find_one.return_value = active_user_doc

        data_retriever.retrieve_notification_data("user-123")

        mock_data_store.query_by_keywords.assert_called_once()
        call_kwargs = mock_data_store.query_by_keywords.call_args[1]
        assert call_kwargs["keywords"] == ["python", "AI", "AAPL"]
