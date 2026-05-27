"""
Unit tests for the Data Store interface.

Tests cover:
- Storing news articles in the data store
- Storing stock data in the data store
- Querying by keywords with time range filter
- Duplicate URL checking via cache
- Error handling for storage failures
- Sorting results by crawl_timestamp descending
"""
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from src.shared.data_store import (
    DataStore,
    DataStoreInterface,
    CRAWLED_NEWS_COLLECTION,
    CRAWLED_STOCKS_COLLECTION,
    CRAWLED_URL_CACHE_PREFIX,
    CRAWLED_URL_TTL_DAYS,
)
from src.shared.models import NewsArticle, StockData


@pytest.fixture
def mock_database():
    """Create a mock database interface."""
    db = MagicMock()
    db.insert_one = MagicMock(return_value="inserted_id")
    db.find_many = MagicMock(return_value=[])
    return db


@pytest.fixture
def mock_cache():
    """Create a mock cache interface."""
    cache = MagicMock()
    cache.exists = MagicMock(return_value=False)
    cache.set = MagicMock(return_value=True)
    return cache


@pytest.fixture
def data_store(mock_database, mock_cache):
    """Create a DataStore with mock dependencies."""
    return DataStore(database=mock_database, cache=mock_cache)


@pytest.fixture
def sample_news_article():
    """Create a sample NewsArticle for testing."""
    return NewsArticle(
        article_id=str(uuid.uuid4()),
        title="Python 3.12 Released",
        summary="The Python Software Foundation has released Python 3.12 with major improvements.",
        url="https://news.example.com/python-3-12",
        published_date=datetime(2025, 1, 15, 10, 0, 0),
        source="news.example.com",
        matched_keyword="python",
        crawl_timestamp=datetime(2025, 1, 15, 14, 30, 0),
    )


@pytest.fixture
def sample_stock_data():
    """Create a sample StockData for testing."""
    return StockData(
        stock_id=str(uuid.uuid4()),
        symbol="AAPL",
        company_name="Apple Inc.",
        current_price=150.25,
        price_change=0.75,
        percentage_change=0.50,
        last_update=datetime(2025, 1, 15, 14, 30, 0),
        matched_keyword="AAPL",
        crawl_timestamp=datetime(2025, 1, 15, 14, 30, 0),
    )


class TestStoreNewsArticle:
    """Tests for store_news_article method."""

    def test_stores_article_in_database(self, data_store, mock_database, sample_news_article):
        """Should insert article dict into crawled_news collection."""
        data_store.store_news_article(sample_news_article)

        mock_database.insert_one.assert_called_once_with(
            CRAWLED_NEWS_COLLECTION,
            sample_news_article.to_dict(),
        )

    def test_marks_url_as_crawled_in_cache(self, data_store, mock_cache, sample_news_article):
        """Should mark the article URL in cache with 7-day TTL."""
        data_store.store_news_article(sample_news_article)

        expected_key = f"{CRAWLED_URL_CACHE_PREFIX}{sample_news_article.url}"
        mock_cache.set.assert_called_once_with(
            expected_key,
            True,
            ttl=timedelta(days=CRAWLED_URL_TTL_DAYS),
        )

    def test_raises_on_database_failure(self, data_store, mock_database, sample_news_article):
        """Should raise exception when database insert fails."""
        mock_database.insert_one.side_effect = Exception("DB connection lost")

        with pytest.raises(Exception, match="DB connection lost"):
            data_store.store_news_article(sample_news_article)

    def test_stores_all_article_fields(self, data_store, mock_database):
        """Should store all fields from the NewsArticle model."""
        article = NewsArticle(
            article_id="art-123",
            title="Test Title",
            summary="Test summary content",
            url="https://example.com/test",
            published_date=datetime(2025, 1, 10, 8, 0, 0),
            source="example.com",
            matched_keyword="test",
            crawl_timestamp=datetime(2025, 1, 10, 12, 0, 0),
        )

        data_store.store_news_article(article)

        stored_doc = mock_database.insert_one.call_args[0][1]
        assert stored_doc["article_id"] == "art-123"
        assert stored_doc["title"] == "Test Title"
        assert stored_doc["summary"] == "Test summary content"
        assert stored_doc["url"] == "https://example.com/test"
        assert stored_doc["source"] == "example.com"
        assert stored_doc["matched_keyword"] == "test"


class TestStoreStockData:
    """Tests for store_stock_data method."""

    def test_stores_stock_in_database(self, data_store, mock_database, sample_stock_data):
        """Should insert stock data dict into crawled_stocks collection."""
        data_store.store_stock_data(sample_stock_data)

        mock_database.insert_one.assert_called_once_with(
            CRAWLED_STOCKS_COLLECTION,
            sample_stock_data.to_dict(),
        )

    def test_does_not_mark_url_in_cache(self, data_store, mock_cache, sample_stock_data):
        """Should not mark any URL in cache for stock data (no URL dedup needed)."""
        data_store.store_stock_data(sample_stock_data)

        mock_cache.set.assert_not_called()

    def test_raises_on_database_failure(self, data_store, mock_database, sample_stock_data):
        """Should raise exception when database insert fails."""
        mock_database.insert_one.side_effect = Exception("DB write failed")

        with pytest.raises(Exception, match="DB write failed"):
            data_store.store_stock_data(sample_stock_data)

    def test_stores_all_stock_fields(self, data_store, mock_database):
        """Should store all fields from the StockData model."""
        stock = StockData(
            stock_id="stock-456",
            symbol="GOOG",
            company_name="Alphabet Inc.",
            current_price=2800.50,
            price_change=15.25,
            percentage_change=0.55,
            last_update=datetime(2025, 1, 15, 14, 0, 0),
            matched_keyword="google",
            crawl_timestamp=datetime(2025, 1, 15, 14, 0, 0),
        )

        data_store.store_stock_data(stock)

        stored_doc = mock_database.insert_one.call_args[0][1]
        assert stored_doc["stock_id"] == "stock-456"
        assert stored_doc["symbol"] == "GOOG"
        assert stored_doc["company_name"] == "Alphabet Inc."
        assert stored_doc["current_price"] == 2800.50
        assert stored_doc["price_change"] == 15.25
        assert stored_doc["percentage_change"] == 0.55
        assert stored_doc["matched_keyword"] == "google"


class TestQueryByKeywords:
    """Tests for query_by_keywords method."""

    def test_queries_news_and_stocks_collections(self, data_store, mock_database):
        """Should query both crawled_news and crawled_stocks collections."""
        data_store.query_by_keywords(["python", "AI"])

        assert mock_database.find_many.call_count == 2
        collection_names = [call[0][0] for call in mock_database.find_many.call_args_list]
        assert CRAWLED_NEWS_COLLECTION in collection_names
        assert CRAWLED_STOCKS_COLLECTION in collection_names

    def test_filters_by_keywords(self, data_store, mock_database):
        """Should include keyword filter in query."""
        keywords = ["python", "AI"]
        data_store.query_by_keywords(keywords)

        query = mock_database.find_many.call_args_list[0][0][1]
        assert query["matched_keyword"] == {"$in": ["python", "AI"]}

    def test_filters_by_time_range(self, data_store, mock_database):
        """Should include time range filter in query."""
        start = datetime(2025, 1, 14, 0, 0, 0)
        end = datetime(2025, 1, 15, 0, 0, 0)

        data_store.query_by_keywords(["python"], start_time=start, end_time=end)

        query = mock_database.find_many.call_args_list[0][0][1]
        assert query["crawl_timestamp"]["$gte"] == start.isoformat()
        assert query["crawl_timestamp"]["$lte"] == end.isoformat()

    def test_defaults_to_24_hours_when_no_time_range(self, data_store, mock_database):
        """Should default to past 24 hours when no time range specified."""
        with patch("src.shared.data_store.datetime") as mock_dt:
            now = datetime(2025, 1, 15, 14, 0, 0)
            mock_dt.utcnow.return_value = now
            mock_dt.min = datetime.min
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            data_store.query_by_keywords(["python"])

            query = mock_database.find_many.call_args_list[0][0][1]
            expected_start = (now - timedelta(hours=24)).isoformat()
            assert query["crawl_timestamp"]["$gte"] == expected_start
            assert query["crawl_timestamp"]["$lte"] == now.isoformat()

    def test_returns_news_articles_as_model_objects(self, data_store, mock_database):
        """Should deserialize news documents into NewsArticle objects."""
        news_doc = {
            "article_id": "art-1",
            "title": "Python News",
            "summary": "Summary text",
            "url": "https://example.com/article",
            "published_date": "2025-01-15T10:00:00",
            "source": "example.com",
            "matched_keyword": "python",
            "crawl_timestamp": "2025-01-15T14:00:00",
        }
        mock_database.find_many.side_effect = [[news_doc], []]

        result = data_store.query_by_keywords(["python"])

        assert len(result["news_articles"]) == 1
        article = result["news_articles"][0]
        assert isinstance(article, NewsArticle)
        assert article.title == "Python News"
        assert article.article_id == "art-1"

    def test_returns_stock_data_as_model_objects(self, data_store, mock_database):
        """Should deserialize stock documents into StockData objects."""
        stock_doc = {
            "stock_id": "stock-1",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "current_price": 150.25,
            "price_change": 0.75,
            "percentage_change": 0.50,
            "last_update": "2025-01-15T14:00:00",
            "matched_keyword": "AAPL",
            "crawl_timestamp": "2025-01-15T14:00:00",
        }
        mock_database.find_many.side_effect = [[], [stock_doc]]

        result = data_store.query_by_keywords(["AAPL"])

        assert len(result["stock_data"]) == 1
        stock = result["stock_data"][0]
        assert isinstance(stock, StockData)
        assert stock.symbol == "AAPL"
        assert stock.current_price == 150.25

    def test_sorts_news_by_crawl_timestamp_descending(self, data_store, mock_database):
        """Should sort news articles by crawl_timestamp in descending order."""
        news_docs = [
            {
                "article_id": "art-old",
                "title": "Old Article",
                "summary": "Old",
                "url": "https://example.com/old",
                "source": "example.com",
                "matched_keyword": "python",
                "crawl_timestamp": "2025-01-14T10:00:00",
            },
            {
                "article_id": "art-new",
                "title": "New Article",
                "summary": "New",
                "url": "https://example.com/new",
                "source": "example.com",
                "matched_keyword": "python",
                "crawl_timestamp": "2025-01-15T10:00:00",
            },
        ]
        mock_database.find_many.side_effect = [news_docs, []]

        result = data_store.query_by_keywords(["python"])

        articles = result["news_articles"]
        assert articles[0].article_id == "art-new"
        assert articles[1].article_id == "art-old"

    def test_sorts_stocks_by_crawl_timestamp_descending(self, data_store, mock_database):
        """Should sort stock data by crawl_timestamp in descending order."""
        stock_docs = [
            {
                "stock_id": "stock-old",
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "current_price": 149.00,
                "price_change": -1.0,
                "percentage_change": -0.67,
                "matched_keyword": "AAPL",
                "crawl_timestamp": "2025-01-14T10:00:00",
            },
            {
                "stock_id": "stock-new",
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "current_price": 150.25,
                "price_change": 0.75,
                "percentage_change": 0.50,
                "matched_keyword": "AAPL",
                "crawl_timestamp": "2025-01-15T10:00:00",
            },
        ]
        mock_database.find_many.side_effect = [[], stock_docs]

        result = data_store.query_by_keywords(["AAPL"])

        stocks = result["stock_data"]
        assert stocks[0].stock_id == "stock-new"
        assert stocks[1].stock_id == "stock-old"

    def test_returns_empty_results_when_no_data(self, data_store, mock_database):
        """Should return empty lists when no matching data found."""
        mock_database.find_many.return_value = []

        result = data_store.query_by_keywords(["nonexistent"])

        assert result["news_articles"] == []
        assert result["stock_data"] == []

    def test_handles_multiple_keywords(self, data_store, mock_database):
        """Should query with multiple keywords using $in operator."""
        keywords = ["python", "AI", "AAPL"]
        data_store.query_by_keywords(keywords)

        query = mock_database.find_many.call_args_list[0][0][1]
        assert query["matched_keyword"] == {"$in": ["python", "AI", "AAPL"]}


class TestIsUrlCrawled:
    """Tests for is_url_crawled method."""

    def test_returns_true_for_cached_url(self, data_store, mock_cache):
        """Should return True when URL exists in cache."""
        mock_cache.exists.return_value = True

        result = data_store.is_url_crawled("https://example.com/article-1")

        assert result is True
        mock_cache.exists.assert_called_once_with(
            f"{CRAWLED_URL_CACHE_PREFIX}https://example.com/article-1"
        )

    def test_returns_false_for_new_url(self, data_store, mock_cache):
        """Should return False when URL is not in cache."""
        mock_cache.exists.return_value = False

        result = data_store.is_url_crawled("https://example.com/new-article")

        assert result is False

    def test_uses_correct_cache_key_prefix(self, data_store, mock_cache):
        """Should use the correct cache key prefix for URL lookups."""
        url = "https://news.example.com/breaking-news"
        data_store.is_url_crawled(url)

        expected_key = f"crawled_url:{url}"
        mock_cache.exists.assert_called_once_with(expected_key)

    def test_handles_urls_with_special_characters(self, data_store, mock_cache):
        """Should handle URLs with query params and special characters."""
        url = "https://example.com/article?id=123&lang=en"
        mock_cache.exists.return_value = False

        result = data_store.is_url_crawled(url)

        assert result is False
        mock_cache.exists.assert_called_once_with(f"{CRAWLED_URL_CACHE_PREFIX}{url}")


class TestMarkUrlCrawled:
    """Tests for the internal _mark_url_crawled method."""

    def test_sets_cache_with_7_day_ttl(self, data_store, mock_cache):
        """Should set URL in cache with 7-day TTL."""
        url = "https://example.com/article"
        data_store._mark_url_crawled(url)

        mock_cache.set.assert_called_once_with(
            f"{CRAWLED_URL_CACHE_PREFIX}{url}",
            True,
            ttl=timedelta(days=7),
        )


class TestDataStoreInterface:
    """Tests for the abstract DataStoreInterface."""

    def test_data_store_implements_interface(self, data_store):
        """DataStore should implement DataStoreInterface."""
        assert isinstance(data_store, DataStoreInterface)

    def test_interface_has_required_methods(self):
        """DataStoreInterface should define all required abstract methods."""
        required_methods = [
            "store_news_article",
            "store_stock_data",
            "query_by_keywords",
            "is_url_crawled",
        ]
        for method_name in required_methods:
            assert hasattr(DataStoreInterface, method_name)
