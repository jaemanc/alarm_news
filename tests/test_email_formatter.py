"""
Unit tests for the Worker Email Formatter.

Tests cover:
- Subject line formatting with date and keywords
- HTML body structure (greeting, news section, stock section, footer)
- Item limiting (10 max for news and stocks)
- Price formatting (2 decimal places)
- Percentage change formatting (+/- sign)
- Unsubscribe instructions in footer
- ISO 8601 timestamp in footer
- Empty data handling
"""
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.shared.models import EmailNotification, NewsArticle, StockData
from src.worker.data_retriever import CrawledData, UserInfo
from src.worker.email_formatter import (
    EmailFormatter,
    MAX_NEWS_ARTICLES,
    MAX_STOCK_ITEMS,
)


@pytest.fixture
def formatter():
    """Create an EmailFormatter instance."""
    return EmailFormatter()


@pytest.fixture
def sample_user_info():
    """Create sample user info for testing."""
    return UserInfo(
        user_id="user-123",
        email="test@example.com",
        keywords=["technology", "AI"],
        subscription_expiry=datetime.utcnow() + timedelta(days=30),
    )


@pytest.fixture
def sample_news_article():
    """Create a sample news article."""
    return NewsArticle(
        article_id=str(uuid.uuid4()),
        title="AI Breakthrough in 2025",
        summary="Researchers have achieved a major breakthrough in artificial intelligence.",
        url="https://news.example.com/ai-breakthrough",
        published_date=datetime(2025, 1, 15, 10, 0, 0),
        source="TechNews",
        matched_keyword="AI",
        crawl_timestamp=datetime(2025, 1, 15, 12, 0, 0),
    )


@pytest.fixture
def sample_stock_data():
    """Create a sample stock data entry."""
    return StockData(
        stock_id=str(uuid.uuid4()),
        symbol="AAPL",
        company_name="Apple Inc.",
        current_price=185.50,
        price_change=2.30,
        percentage_change=1.25,
        last_update=datetime(2025, 1, 15, 16, 0, 0),
        matched_keyword="technology",
        crawl_timestamp=datetime(2025, 1, 15, 16, 5, 0),
    )


class TestCreateSubject:
    """Tests for subject line formatting."""

    def test_subject_contains_date(self, formatter):
        """Subject line should contain the date in YYYY-MM-DD format."""
        date = datetime(2025, 1, 15, 9, 0, 0)
        keywords = ["technology"]
        subject = formatter.create_subject(date, keywords)
        assert "2025-01-15" in subject

    def test_subject_contains_keywords(self, formatter):
        """Subject line should contain the user's keywords."""
        date = datetime(2025, 1, 15, 9, 0, 0)
        keywords = ["technology", "AI"]
        subject = formatter.create_subject(date, keywords)
        assert "technology" in subject
        assert "AI" in subject

    def test_subject_format(self, formatter):
        """Subject line should follow the format: Alarm News - {date} - {keywords}."""
        date = datetime(2025, 3, 20, 9, 0, 0)
        keywords = ["stocks", "crypto"]
        subject = formatter.create_subject(date, keywords)
        assert subject == "Alarm News - 2025-03-20 - stocks, crypto"

    def test_subject_single_keyword(self, formatter):
        """Subject line should work with a single keyword."""
        date = datetime(2025, 6, 1, 9, 0, 0)
        keywords = ["finance"]
        subject = formatter.create_subject(date, keywords)
        assert subject == "Alarm News - 2025-06-01 - finance"


class TestCreateBody:
    """Tests for HTML body structure."""

    def test_body_contains_greeting(self, formatter, sample_news_article, sample_stock_data):
        """Body should contain a greeting section."""
        body = formatter.create_body([sample_news_article], [sample_stock_data])
        assert "Alarm News Digest" in body

    def test_body_contains_news_section(self, formatter, sample_news_article):
        """Body should contain a news section header."""
        body = formatter.create_body([sample_news_article], [])
        assert "<h2>News</h2>" in body

    def test_body_contains_stock_section(self, formatter, sample_stock_data):
        """Body should contain a stock section header."""
        body = formatter.create_body([], [sample_stock_data])
        assert "<h2>Stocks</h2>" in body

    def test_body_contains_footer(self, formatter):
        """Body should contain a footer section."""
        body = formatter.create_body([], [])
        assert "<footer>" in body

    def test_body_contains_unsubscribe(self, formatter):
        """Body should contain unsubscribe instructions in footer."""
        body = formatter.create_body([], [])
        assert "unsubscribe" in body.lower()

    def test_body_contains_iso_timestamp(self, formatter):
        """Body should contain an ISO 8601 timestamp."""
        body = formatter.create_body([], [])
        # ISO 8601 format includes T separator and Z suffix
        assert "Generated at:" in body
        # Check for ISO-like pattern (contains T separator)
        assert "T" in body

    def test_body_news_article_title(self, formatter, sample_news_article):
        """Body should include article title."""
        body = formatter.create_body([sample_news_article], [])
        assert sample_news_article.title in body

    def test_body_news_article_summary(self, formatter, sample_news_article):
        """Body should include article summary."""
        body = formatter.create_body([sample_news_article], [])
        assert sample_news_article.summary in body

    def test_body_news_article_url(self, formatter, sample_news_article):
        """Body should include article URL as a link."""
        body = formatter.create_body([sample_news_article], [])
        assert sample_news_article.url in body

    def test_body_stock_symbol(self, formatter, sample_stock_data):
        """Body should include stock symbol."""
        body = formatter.create_body([], [sample_stock_data])
        assert sample_stock_data.symbol in body

    def test_body_stock_company_name(self, formatter, sample_stock_data):
        """Body should include company name."""
        body = formatter.create_body([], [sample_stock_data])
        assert sample_stock_data.company_name in body

    def test_body_empty_news(self, formatter):
        """Body should handle empty news articles gracefully."""
        body = formatter.create_body([], [])
        assert "No news articles found" in body

    def test_body_empty_stocks(self, formatter):
        """Body should handle empty stock data gracefully."""
        body = formatter.create_body([], [])
        assert "No stock data found" in body


class TestPriceFormatting:
    """Tests for price formatting (2 decimal places)."""

    def test_price_two_decimals(self, formatter):
        """Stock price should be formatted with exactly 2 decimal places."""
        stock = StockData(
            stock_id="s1",
            symbol="TSLA",
            company_name="Tesla Inc.",
            current_price=250.1,
            price_change=1.0,
            percentage_change=0.4,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "250.10" in body

    def test_price_whole_number(self, formatter):
        """Whole number price should show .00."""
        stock = StockData(
            stock_id="s2",
            symbol="GOOG",
            company_name="Alphabet Inc.",
            current_price=100.0,
            price_change=0.0,
            percentage_change=0.0,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "100.00" in body

    def test_price_many_decimals_truncated(self, formatter):
        """Price with many decimals should be formatted to 2 places."""
        stock = StockData(
            stock_id="s3",
            symbol="MSFT",
            company_name="Microsoft Corp.",
            current_price=399.999,
            price_change=0.5,
            percentage_change=0.13,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "400.00" in body


class TestPercentageChangeFormatting:
    """Tests for percentage change formatting (+/- sign)."""

    def test_positive_change_has_plus_sign(self, formatter):
        """Positive percentage change should have + sign."""
        stock = StockData(
            stock_id="s1",
            symbol="AAPL",
            company_name="Apple Inc.",
            current_price=185.50,
            price_change=2.30,
            percentage_change=1.25,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "+1.25%" in body

    def test_negative_change_has_minus_sign(self, formatter):
        """Negative percentage change should have - sign."""
        stock = StockData(
            stock_id="s2",
            symbol="META",
            company_name="Meta Platforms",
            current_price=350.00,
            price_change=-5.00,
            percentage_change=-1.41,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "-1.41%" in body

    def test_zero_change_has_plus_sign(self, formatter):
        """Zero percentage change should have + sign."""
        stock = StockData(
            stock_id="s3",
            symbol="IBM",
            company_name="IBM Corp.",
            current_price=150.00,
            price_change=0.0,
            percentage_change=0.0,
            crawl_timestamp=datetime.utcnow(),
        )
        body = formatter.create_body([], [stock])
        assert "+0.00%" in body


class TestItemLimiting:
    """Tests for item limiting (10 max)."""

    def test_limit_news_to_10(self, formatter):
        """Should limit news articles to 10 items."""
        articles = []
        for i in range(15):
            articles.append(
                NewsArticle(
                    article_id=f"a{i}",
                    title=f"Article {i}",
                    summary=f"Summary {i}",
                    url=f"https://example.com/{i}",
                    crawl_timestamp=datetime(2025, 1, 15, 12, i, 0),
                )
            )
        limited = formatter.limit_items(articles, MAX_NEWS_ARTICLES)
        assert len(limited) == 10

    def test_limit_stocks_to_10(self, formatter):
        """Should limit stock items to 10."""
        stocks = []
        for i in range(15):
            stocks.append(
                StockData(
                    stock_id=f"s{i}",
                    symbol=f"SYM{i}",
                    company_name=f"Company {i}",
                    current_price=100.0 + i,
                    price_change=float(i),
                    percentage_change=float(i) * 0.5,
                    crawl_timestamp=datetime(2025, 1, 15, 12, i, 0),
                )
            )
        limited = formatter.limit_items(stocks, MAX_STOCK_ITEMS)
        assert len(limited) == 10

    def test_selects_most_recent_items(self, formatter):
        """Should select the most recent items by crawl_timestamp."""
        articles = []
        for i in range(15):
            articles.append(
                NewsArticle(
                    article_id=f"a{i}",
                    title=f"Article {i}",
                    summary=f"Summary {i}",
                    url=f"https://example.com/{i}",
                    crawl_timestamp=datetime(2025, 1, 15, 12, i, 0),
                )
            )
        limited = formatter.limit_items(articles, MAX_NEWS_ARTICLES)
        # Most recent should be articles 5-14 (minutes 5-14)
        timestamps = [a.crawl_timestamp for a in limited]
        # All should be from the most recent 10
        oldest_allowed = datetime(2025, 1, 15, 12, 5, 0)
        for ts in timestamps:
            assert ts >= oldest_allowed

    def test_no_limit_when_under_max(self, formatter):
        """Should return all items when count is under the max."""
        articles = []
        for i in range(5):
            articles.append(
                NewsArticle(
                    article_id=f"a{i}",
                    title=f"Article {i}",
                    summary=f"Summary {i}",
                    url=f"https://example.com/{i}",
                    crawl_timestamp=datetime(2025, 1, 15, 12, i, 0),
                )
            )
        limited = formatter.limit_items(articles, MAX_NEWS_ARTICLES)
        assert len(limited) == 5

    def test_limit_with_none_timestamps(self, formatter):
        """Should handle items with None crawl_timestamp."""
        articles = []
        for i in range(12):
            articles.append(
                NewsArticle(
                    article_id=f"a{i}",
                    title=f"Article {i}",
                    summary=f"Summary {i}",
                    url=f"https://example.com/{i}",
                    crawl_timestamp=None if i < 2 else datetime(2025, 1, 15, 12, i, 0),
                )
            )
        limited = formatter.limit_items(articles, MAX_NEWS_ARTICLES)
        assert len(limited) == 10


class TestFormatEmail:
    """Tests for the full format_email method."""

    def test_returns_email_notification(self, formatter, sample_user_info):
        """Should return an EmailNotification instance."""
        data = CrawledData(news_articles=[], stock_data=[])
        result = formatter.format_email(sample_user_info, data)
        assert isinstance(result, EmailNotification)

    def test_email_to_address(self, formatter, sample_user_info):
        """Should set the to_email to the user's email."""
        data = CrawledData(news_articles=[], stock_data=[])
        result = formatter.format_email(sample_user_info, data)
        assert result.to_email == "test@example.com"

    def test_email_has_subject(self, formatter, sample_user_info):
        """Should set a subject line."""
        data = CrawledData(news_articles=[], stock_data=[])
        result = formatter.format_email(sample_user_info, data)
        assert "Alarm News" in result.subject
        assert "technology" in result.subject
        assert "AI" in result.subject

    def test_email_has_html_body(self, formatter, sample_user_info):
        """Should set an HTML body."""
        data = CrawledData(news_articles=[], stock_data=[])
        result = formatter.format_email(sample_user_info, data)
        assert "<html>" in result.body_html
        assert "</html>" in result.body_html

    def test_email_has_timestamp(self, formatter, sample_user_info):
        """Should set a timestamp."""
        data = CrawledData(news_articles=[], stock_data=[])
        result = formatter.format_email(sample_user_info, data)
        assert result.timestamp is not None
        assert isinstance(result.timestamp, datetime)

    def test_email_with_news_and_stocks(
        self, formatter, sample_user_info, sample_news_article, sample_stock_data
    ):
        """Should include both news and stock data in the body."""
        data = CrawledData(
            news_articles=[sample_news_article],
            stock_data=[sample_stock_data],
        )
        result = formatter.format_email(sample_user_info, data)
        assert sample_news_article.title in result.body_html
        assert sample_stock_data.symbol in result.body_html
