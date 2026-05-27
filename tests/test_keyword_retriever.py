"""
Unit tests for the Keyword Retriever component.

Tests cover:
- Querying active users from MongoDB
- Keyword extraction and deduplication
- Crawler job creation with UUID4 job_ids
- Scheduling mechanism (start/stop)
"""
import uuid
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.crawler.keyword_retriever import CrawlerJob, KeywordRetriever


@pytest.fixture
def mock_database():
    """Create a mock database interface."""
    db = MagicMock()
    db.find_many = MagicMock(return_value=[])
    return db


@pytest.fixture
def retriever(mock_database):
    """Create a KeywordRetriever with mock database."""
    return KeywordRetriever(
        database=mock_database,
        target_sites=["https://news.example.com", "https://stocks.example.com"],
        interval_seconds=1,  # Short interval for testing
    )


class TestGetUniqueKeywords:
    """Tests for get_unique_keywords method."""

    def test_returns_empty_list_when_no_active_users(self, retriever, mock_database):
        """No active users means no keywords."""
        mock_database.find_many.return_value = []

        keywords = retriever.get_unique_keywords()

        assert keywords == []
        mock_database.find_many.assert_called_once()

    def test_queries_users_with_valid_subscription(self, retriever, mock_database):
        """Should query for users with subscription_expiry > now."""
        mock_database.find_many.return_value = []

        retriever.get_unique_keywords()

        call_args = mock_database.find_many.call_args
        assert call_args[0][0] == "users"
        query = call_args[0][1]
        assert "$gt" in query["subscription_expiry"]

    def test_extracts_keywords_from_single_user(self, retriever, mock_database):
        """Should extract keywords from a single active user."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["python", "AI", "machine learning"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        keywords = retriever.get_unique_keywords()

        assert "python" in keywords
        assert "ai" in keywords
        assert "machine learning" in keywords

    def test_deduplicates_keywords_across_users(self, retriever, mock_database):
        """Should deduplicate keywords from multiple users."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["python", "AI"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
            {
                "user_id": "user-2",
                "keywords": ["Python", "blockchain"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=5),
            },
            {
                "user_id": "user-3",
                "keywords": ["ai", "blockchain", "python"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=20),
            },
        ]

        keywords = retriever.get_unique_keywords()

        # "python", "Python" should be deduplicated (case-insensitive)
        assert keywords.count("python") == 1
        assert keywords.count("ai") == 1
        assert keywords.count("blockchain") == 1
        assert len(keywords) == 3

    def test_skips_empty_keywords(self, retriever, mock_database):
        """Should skip empty string keywords."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["python", "", "  ", "AI"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        keywords = retriever.get_unique_keywords()

        assert "" not in keywords
        assert len(keywords) == 2

    def test_handles_user_without_keywords_field(self, retriever, mock_database):
        """Should handle users that have no keywords field gracefully."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        keywords = retriever.get_unique_keywords()

        assert keywords == []

    def test_returns_sorted_keywords(self, retriever, mock_database):
        """Should return keywords in sorted order."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["zebra", "apple", "mango"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        keywords = retriever.get_unique_keywords()

        assert keywords == ["apple", "mango", "zebra"]

    def test_strips_whitespace_from_keywords(self, retriever, mock_database):
        """Should strip leading/trailing whitespace from keywords."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["  python  ", "AI  ", "  blockchain"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        keywords = retriever.get_unique_keywords()

        assert "python" in keywords
        assert "ai" in keywords
        assert "blockchain" in keywords


class TestCreateCrawlerJobs:
    """Tests for create_crawler_jobs method."""

    def test_creates_job_for_each_keyword(self, retriever):
        """Should create one CrawlerJob per keyword."""
        keywords = ["python", "ai", "blockchain"]

        jobs = retriever.create_crawler_jobs(keywords)

        assert len(jobs) == 3
        job_keywords = [job.keyword for job in jobs]
        assert "python" in job_keywords
        assert "ai" in job_keywords
        assert "blockchain" in job_keywords

    def test_job_has_uuid4_job_id(self, retriever):
        """Each job should have a valid UUID4 job_id."""
        keywords = ["python"]

        jobs = retriever.create_crawler_jobs(keywords)

        job = jobs[0]
        # Validate it's a valid UUID4
        parsed_uuid = uuid.UUID(job.job_id, version=4)
        assert str(parsed_uuid) == job.job_id

    def test_each_job_has_unique_id(self, retriever):
        """Each job should have a unique job_id."""
        keywords = ["python", "ai", "blockchain", "stocks", "crypto"]

        jobs = retriever.create_crawler_jobs(keywords)

        job_ids = [job.job_id for job in jobs]
        assert len(set(job_ids)) == len(job_ids)

    def test_jobs_include_target_sites(self, retriever):
        """Jobs should include the configured target sites."""
        keywords = ["python"]

        jobs = retriever.create_crawler_jobs(keywords)

        assert jobs[0].target_sites == [
            "https://news.example.com",
            "https://stocks.example.com",
        ]

    def test_jobs_have_created_at_timestamp(self, retriever):
        """Jobs should have a created_at timestamp."""
        keywords = ["python"]

        jobs = retriever.create_crawler_jobs(keywords)

        assert jobs[0].created_at is not None
        assert isinstance(jobs[0].created_at, datetime)

    def test_empty_keywords_returns_empty_list(self, retriever):
        """Empty keyword list should return empty job list."""
        jobs = retriever.create_crawler_jobs([])

        assert jobs == []


class TestRetrieveAndCreateJobs:
    """Tests for the combined retrieve_and_create_jobs method."""

    def test_full_cycle_with_active_users(self, retriever, mock_database):
        """Should retrieve keywords and create jobs in one call."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["python", "AI"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            },
            {
                "user_id": "user-2",
                "keywords": ["AI", "blockchain"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=5),
            },
        ]

        jobs = retriever.retrieve_and_create_jobs()

        # 3 unique keywords: python, ai, blockchain
        assert len(jobs) == 3

    def test_returns_empty_when_no_keywords(self, retriever, mock_database):
        """Should return empty list when no active users have keywords."""
        mock_database.find_many.return_value = []

        jobs = retriever.retrieve_and_create_jobs()

        assert jobs == []


class TestScheduling:
    """Tests for the start/stop scheduling mechanism."""

    def test_start_sets_running_flag(self, mock_database):
        """Starting the retriever should set is_running to True."""
        retriever = KeywordRetriever(
            database=mock_database,
            interval_seconds=60,
        )

        retriever.start()
        assert retriever.is_running is True
        retriever.stop()

    def test_stop_clears_running_flag(self, mock_database):
        """Stopping the retriever should set is_running to False."""
        retriever = KeywordRetriever(
            database=mock_database,
            interval_seconds=60,
        )

        retriever.start()
        retriever.stop()

        assert retriever.is_running is False

    def test_start_when_already_running_does_nothing(self, mock_database):
        """Starting when already running should not create duplicate timers."""
        retriever = KeywordRetriever(
            database=mock_database,
            interval_seconds=60,
        )

        retriever.start()
        retriever.start()  # Should not raise or create duplicate

        assert retriever.is_running is True
        retriever.stop()

    def test_periodic_execution(self, mock_database):
        """Should execute retrieval periodically."""
        mock_database.find_many.return_value = [
            {
                "user_id": "user-1",
                "keywords": ["test"],
                "subscription_expiry": datetime.utcnow() + timedelta(days=10),
            }
        ]

        retriever = KeywordRetriever(
            database=mock_database,
            interval_seconds=0.1,  # Very short for testing
        )

        retriever.start()
        time.sleep(0.35)  # Wait for a few cycles
        retriever.stop()

        # Should have been called multiple times (initial + periodic)
        assert mock_database.find_many.call_count >= 2

    def test_handles_database_error_gracefully(self, mock_database):
        """Should not crash if database raises an error during cycle."""
        mock_database.find_many.side_effect = Exception("Connection lost")

        retriever = KeywordRetriever(
            database=mock_database,
            interval_seconds=0.1,
        )

        # Should not raise
        retriever.start()
        time.sleep(0.15)
        retriever.stop()

        assert retriever.is_running is False


class TestCrawlerJob:
    """Tests for the CrawlerJob dataclass."""

    def test_to_dict(self):
        """Should serialize to dictionary correctly."""
        now = datetime(2025, 1, 15, 10, 30, 0)
        job = CrawlerJob(
            job_id="test-uuid",
            keyword="python",
            target_sites=["https://example.com"],
            created_at=now,
        )

        result = job.to_dict()

        assert result == {
            "job_id": "test-uuid",
            "keyword": "python",
            "target_sites": ["https://example.com"],
            "created_at": "2025-01-15T10:30:00",
        }

    def test_to_dict_with_none_created_at(self):
        """Should handle None created_at in serialization."""
        job = CrawlerJob(
            job_id="test-uuid",
            keyword="python",
        )

        result = job.to_dict()

        assert result["created_at"] is None
