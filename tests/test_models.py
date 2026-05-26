"""
Unit tests for core domain models.

Tests serialization (to_dict/from_dict), validation, and edge cases
for all dataclasses in src.shared.models.
"""
import pytest
from datetime import datetime

from src.shared.models import (
    NotificationTime,
    User,
    NotificationEvent,
    NewsArticle,
    StockData,
    EmailNotification,
)


class TestNotificationTime:
    """Tests for NotificationTime dataclass."""

    def test_valid_creation(self):
        nt = NotificationTime(hour=8, minute=30)
        assert nt.hour == 8
        assert nt.minute == 30

    def test_boundary_values(self):
        nt_min = NotificationTime(hour=0, minute=0)
        assert nt_min.hour == 0
        assert nt_min.minute == 0

        nt_max = NotificationTime(hour=23, minute=59)
        assert nt_max.hour == 23
        assert nt_max.minute == 59

    def test_invalid_hour_raises(self):
        with pytest.raises(ValueError, match="hour must be between 0 and 23"):
            NotificationTime(hour=24, minute=0)
        with pytest.raises(ValueError, match="hour must be between 0 and 23"):
            NotificationTime(hour=-1, minute=0)

    def test_invalid_minute_raises(self):
        with pytest.raises(ValueError, match="minute must be between 0 and 59"):
            NotificationTime(hour=0, minute=60)
        with pytest.raises(ValueError, match="minute must be between 0 and 59"):
            NotificationTime(hour=0, minute=-1)

    def test_to_dict(self):
        nt = NotificationTime(hour=14, minute=45)
        result = nt.to_dict()
        assert result == {"hour": 14, "minute": 45}

    def test_from_dict(self):
        data = {"hour": 9, "minute": 15}
        nt = NotificationTime.from_dict(data)
        assert nt.hour == 9
        assert nt.minute == 15

    def test_roundtrip(self):
        original = NotificationTime(hour=22, minute=0)
        restored = NotificationTime.from_dict(original.to_dict())
        assert restored.hour == original.hour
        assert restored.minute == original.minute


class TestUser:
    """Tests for User dataclass."""

    def test_creation_with_all_fields(self):
        expiry = datetime(2025, 7, 15, 12, 0, 0)
        user = User(
            user_id="abc-123",
            hashed_password="$2b$12$hashedvalue",
            email="test@example.com",
            keywords=["python", "AI"],
            notification_times=[NotificationTime(hour=8, minute=0)],
            subscription_expiry=expiry,
        )
        assert user.user_id == "abc-123"
        assert user.email == "test@example.com"
        assert len(user.keywords) == 2
        assert len(user.notification_times) == 1
        assert user.subscription_expiry == expiry

    def test_default_fields(self):
        user = User(
            user_id="id-1",
            hashed_password="hash",
            email="a@b.com",
        )
        assert user.keywords == []
        assert user.notification_times == []
        assert user.subscription_expiry is None

    def test_to_dict(self):
        expiry = datetime(2025, 8, 1, 0, 0, 0)
        user = User(
            user_id="u-1",
            hashed_password="hashed",
            email="user@test.com",
            keywords=["news"],
            notification_times=[NotificationTime(hour=7, minute=30)],
            subscription_expiry=expiry,
        )
        result = user.to_dict()
        assert result["user_id"] == "u-1"
        assert result["hashed_password"] == "hashed"
        assert result["email"] == "user@test.com"
        assert result["keywords"] == ["news"]
        assert result["notification_times"] == [{"hour": 7, "minute": 30}]
        assert result["subscription_expiry"] == expiry

    def test_from_dict(self):
        expiry = datetime(2025, 8, 1, 0, 0, 0)
        data = {
            "user_id": "u-2",
            "hashed_password": "hash2",
            "email": "u2@test.com",
            "keywords": ["stocks", "crypto"],
            "notification_times": [{"hour": 18, "minute": 0}],
            "subscription_expiry": expiry,
        }
        user = User.from_dict(data)
        assert user.user_id == "u-2"
        assert user.keywords == ["stocks", "crypto"]
        assert user.notification_times[0].hour == 18
        assert user.subscription_expiry == expiry

    def test_from_dict_missing_optional_fields(self):
        data = {
            "user_id": "u-3",
            "hashed_password": "hash3",
            "email": "u3@test.com",
        }
        user = User.from_dict(data)
        assert user.keywords == []
        assert user.notification_times == []
        assert user.subscription_expiry is None

    def test_roundtrip(self):
        expiry = datetime(2025, 9, 1, 10, 30, 0)
        original = User(
            user_id="roundtrip-id",
            hashed_password="$2b$12$abc",
            email="round@trip.com",
            keywords=["tech", "finance"],
            notification_times=[
                NotificationTime(hour=8, minute=0),
                NotificationTime(hour=18, minute=30),
            ],
            subscription_expiry=expiry,
        )
        restored = User.from_dict(original.to_dict())
        assert restored.user_id == original.user_id
        assert restored.email == original.email
        assert restored.keywords == original.keywords
        assert len(restored.notification_times) == 2
        assert restored.subscription_expiry == original.subscription_expiry


class TestNotificationEvent:
    """Tests for NotificationEvent dataclass."""

    def test_creation(self):
        ts = datetime(2025, 6, 15, 8, 0, 0)
        event = NotificationEvent(
            event_id="evt-001",
            user_id="user-123",
            notification_timestamp=ts,
        )
        assert event.event_id == "evt-001"
        assert event.user_id == "user-123"
        assert event.notification_timestamp == ts

    def test_to_dict_serializes_timestamp(self):
        ts = datetime(2025, 6, 15, 8, 0, 0)
        event = NotificationEvent(event_id="e1", user_id="u1", notification_timestamp=ts)
        result = event.to_dict()
        assert result["notification_timestamp"] == "2025-06-15T08:00:00"

    def test_from_dict_with_iso_string(self):
        data = {
            "event_id": "e2",
            "user_id": "u2",
            "notification_timestamp": "2025-06-15T09:30:00",
        }
        event = NotificationEvent.from_dict(data)
        assert event.notification_timestamp == datetime(2025, 6, 15, 9, 30, 0)

    def test_from_dict_with_datetime_object(self):
        ts = datetime(2025, 6, 15, 10, 0, 0)
        data = {"event_id": "e3", "user_id": "u3", "notification_timestamp": ts}
        event = NotificationEvent.from_dict(data)
        assert event.notification_timestamp == ts

    def test_roundtrip(self):
        ts = datetime(2025, 6, 15, 12, 45, 0)
        original = NotificationEvent(event_id="rt-1", user_id="rt-u", notification_timestamp=ts)
        restored = NotificationEvent.from_dict(original.to_dict())
        assert restored.event_id == original.event_id
        assert restored.user_id == original.user_id
        assert restored.notification_timestamp == original.notification_timestamp


class TestNewsArticle:
    """Tests for NewsArticle dataclass."""

    def test_creation_with_all_fields(self):
        pub_date = datetime(2025, 6, 14, 10, 0, 0)
        crawl_ts = datetime(2025, 6, 14, 10, 30, 0)
        article = NewsArticle(
            article_id="art-001",
            title="Breaking News",
            summary="Summary of the article",
            url="https://example.com/article",
            published_date=pub_date,
            source="Example News",
            matched_keyword="tech",
            crawl_timestamp=crawl_ts,
        )
        assert article.article_id == "art-001"
        assert article.title == "Breaking News"
        assert article.url == "https://example.com/article"
        assert article.source == "Example News"

    def test_default_fields(self):
        article = NewsArticle(
            article_id="art-002",
            title="Title",
            summary="Summary",
            url="https://example.com",
        )
        assert article.published_date is None
        assert article.source == ""
        assert article.matched_keyword == ""
        assert article.crawl_timestamp is None

    def test_to_dict(self):
        pub_date = datetime(2025, 6, 14, 10, 0, 0)
        crawl_ts = datetime(2025, 6, 14, 10, 30, 0)
        article = NewsArticle(
            article_id="art-003",
            title="Test",
            summary="Test summary",
            url="https://test.com",
            published_date=pub_date,
            source="Test Source",
            matched_keyword="keyword",
            crawl_timestamp=crawl_ts,
        )
        result = article.to_dict()
        assert result["article_id"] == "art-003"
        assert result["published_date"] == "2025-06-14T10:00:00"
        assert result["crawl_timestamp"] == "2025-06-14T10:30:00"

    def test_to_dict_with_none_timestamps(self):
        article = NewsArticle(
            article_id="art-004",
            title="No dates",
            summary="No dates article",
            url="https://nodate.com",
        )
        result = article.to_dict()
        assert result["published_date"] is None
        assert result["crawl_timestamp"] is None

    def test_from_dict_with_iso_strings(self):
        data = {
            "article_id": "art-005",
            "title": "From Dict",
            "summary": "From dict summary",
            "url": "https://fromdict.com",
            "published_date": "2025-06-14T11:00:00",
            "source": "Dict Source",
            "matched_keyword": "dict",
            "crawl_timestamp": "2025-06-14T11:30:00",
        }
        article = NewsArticle.from_dict(data)
        assert article.published_date == datetime(2025, 6, 14, 11, 0, 0)
        assert article.crawl_timestamp == datetime(2025, 6, 14, 11, 30, 0)

    def test_roundtrip(self):
        pub_date = datetime(2025, 6, 14, 12, 0, 0)
        crawl_ts = datetime(2025, 6, 14, 12, 15, 0)
        original = NewsArticle(
            article_id="rt-art",
            title="Roundtrip Article",
            summary="Roundtrip summary text",
            url="https://roundtrip.com/article",
            published_date=pub_date,
            source="RT Source",
            matched_keyword="roundtrip",
            crawl_timestamp=crawl_ts,
        )
        restored = NewsArticle.from_dict(original.to_dict())
        assert restored.article_id == original.article_id
        assert restored.title == original.title
        assert restored.published_date == original.published_date
        assert restored.crawl_timestamp == original.crawl_timestamp


class TestStockData:
    """Tests for StockData dataclass."""

    def test_creation_with_all_fields(self):
        last_update = datetime(2025, 6, 14, 16, 0, 0)
        crawl_ts = datetime(2025, 6, 14, 16, 5, 0)
        stock = StockData(
            stock_id="stk-001",
            symbol="AAPL",
            company_name="Apple Inc.",
            current_price=195.50,
            price_change=2.30,
            percentage_change=1.19,
            last_update=last_update,
            matched_keyword="apple",
            crawl_timestamp=crawl_ts,
        )
        assert stock.symbol == "AAPL"
        assert stock.current_price == 195.50
        assert stock.percentage_change == 1.19

    def test_default_fields(self):
        stock = StockData(
            stock_id="stk-002",
            symbol="GOOG",
            company_name="Alphabet Inc.",
            current_price=175.00,
            price_change=-1.50,
            percentage_change=-0.85,
        )
        assert stock.last_update is None
        assert stock.matched_keyword == ""
        assert stock.crawl_timestamp is None

    def test_to_dict_rounds_percentage(self):
        stock = StockData(
            stock_id="stk-003",
            symbol="MSFT",
            company_name="Microsoft",
            current_price=420.123,
            price_change=3.456,
            percentage_change=0.82857142,
            last_update=datetime(2025, 6, 14, 16, 0, 0),
            matched_keyword="microsoft",
            crawl_timestamp=datetime(2025, 6, 14, 16, 5, 0),
        )
        result = stock.to_dict()
        assert result["percentage_change"] == 0.83

    def test_to_dict_with_none_timestamps(self):
        stock = StockData(
            stock_id="stk-004",
            symbol="TSLA",
            company_name="Tesla",
            current_price=250.00,
            price_change=5.00,
            percentage_change=2.04,
        )
        result = stock.to_dict()
        assert result["last_update"] is None
        assert result["crawl_timestamp"] is None

    def test_from_dict_with_iso_strings(self):
        data = {
            "stock_id": "stk-005",
            "symbol": "AMZN",
            "company_name": "Amazon",
            "current_price": 185.75,
            "price_change": -2.25,
            "percentage_change": -1.20,
            "last_update": "2025-06-14T15:00:00",
            "matched_keyword": "amazon",
            "crawl_timestamp": "2025-06-14T15:05:00",
        }
        stock = StockData.from_dict(data)
        assert stock.last_update == datetime(2025, 6, 14, 15, 0, 0)
        assert stock.crawl_timestamp == datetime(2025, 6, 14, 15, 5, 0)
        assert stock.percentage_change == -1.20

    def test_roundtrip(self):
        last_update = datetime(2025, 6, 14, 16, 0, 0)
        crawl_ts = datetime(2025, 6, 14, 16, 5, 0)
        original = StockData(
            stock_id="rt-stk",
            symbol="NVDA",
            company_name="NVIDIA",
            current_price=130.50,
            price_change=4.20,
            percentage_change=3.33,
            last_update=last_update,
            matched_keyword="nvidia",
            crawl_timestamp=crawl_ts,
        )
        restored = StockData.from_dict(original.to_dict())
        assert restored.stock_id == original.stock_id
        assert restored.symbol == original.symbol
        assert restored.current_price == original.current_price
        assert restored.percentage_change == original.percentage_change
        assert restored.last_update == original.last_update


class TestEmailNotification:
    """Tests for EmailNotification dataclass."""

    def test_creation(self):
        ts = datetime(2025, 6, 15, 8, 0, 0)
        email = EmailNotification(
            to_email="user@example.com",
            subject="Alarm News - 2025-06-15 - tech, AI",
            body_html="<html><body>Hello</body></html>",
            timestamp=ts,
        )
        assert email.to_email == "user@example.com"
        assert email.subject == "Alarm News - 2025-06-15 - tech, AI"
        assert email.timestamp == ts

    def test_default_timestamp(self):
        email = EmailNotification(
            to_email="a@b.com",
            subject="Test",
            body_html="<p>Test</p>",
        )
        assert email.timestamp is None

    def test_to_dict(self):
        ts = datetime(2025, 6, 15, 8, 0, 0)
        email = EmailNotification(
            to_email="user@test.com",
            subject="Subject",
            body_html="<p>Body</p>",
            timestamp=ts,
        )
        result = email.to_dict()
        assert result["to_email"] == "user@test.com"
        assert result["timestamp"] == "2025-06-15T08:00:00"

    def test_to_dict_with_none_timestamp(self):
        email = EmailNotification(
            to_email="a@b.com",
            subject="No TS",
            body_html="<p>No timestamp</p>",
        )
        result = email.to_dict()
        assert result["timestamp"] is None

    def test_from_dict_with_iso_string(self):
        data = {
            "to_email": "from@dict.com",
            "subject": "From Dict",
            "body_html": "<p>From dict</p>",
            "timestamp": "2025-06-15T09:00:00",
        }
        email = EmailNotification.from_dict(data)
        assert email.timestamp == datetime(2025, 6, 15, 9, 0, 0)

    def test_from_dict_with_none_timestamp(self):
        data = {
            "to_email": "no@ts.com",
            "subject": "No TS",
            "body_html": "<p>No ts</p>",
            "timestamp": None,
        }
        email = EmailNotification.from_dict(data)
        assert email.timestamp is None

    def test_roundtrip(self):
        ts = datetime(2025, 6, 15, 10, 30, 0)
        original = EmailNotification(
            to_email="roundtrip@test.com",
            subject="Roundtrip Subject",
            body_html="<html><body><h1>Roundtrip</h1></body></html>",
            timestamp=ts,
        )
        restored = EmailNotification.from_dict(original.to_dict())
        assert restored.to_email == original.to_email
        assert restored.subject == original.subject
        assert restored.body_html == original.body_html
        assert restored.timestamp == original.timestamp
