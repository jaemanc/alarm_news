"""
Unit tests for the News Crawler component.

Tests cover:
- Article extraction from HTML using BeautifulSoup
- Case-insensitive keyword matching in title/content
- Duplicate URL detection via cache
- robots.txt compliance
- Polite crawling with delays
- Rotating user agents
- Request timeout handling
- HTTP error logging (4xx/5xx)
- Storage of articles in database
- Summary truncation to 500 characters
"""
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from src.crawler.news_crawler import (
    NewsCrawler,
    USER_AGENTS,
    REQUEST_TIMEOUT_SECONDS,
    CRAWL_DELAY_SECONDS,
    CRAWLED_URL_TTL_DAYS,
    MAX_SUMMARY_LENGTH,
    CACHE_KEY_PREFIX,
)
from src.crawler.keyword_retriever import CrawlerJob
from src.shared.models import NewsArticle


@pytest.fixture
def mock_database():
    """Create a mock database interface."""
    db = MagicMock()
    db.insert_one = MagicMock(return_value="inserted_id")
    return db


@pytest.fixture
def mock_cache():
    """Create a mock cache interface."""
    cache = MagicMock()
    cache.exists = MagicMock(return_value=False)
    cache.set = MagicMock(return_value=True)
    return cache


@pytest.fixture
def crawler(mock_database, mock_cache):
    """Create a NewsCrawler with mock dependencies."""
    return NewsCrawler(
        database=mock_database,
        cache=mock_cache,
        request_timeout=30,
        crawl_delay=0,  # No delay for tests
    )


@pytest.fixture
def sample_html_with_articles():
    """Sample HTML containing article elements with keyword matches."""
    return """
    <html>
    <body>
        <article>
            <h2><a href="https://news.example.com/article-1">Python 3.12 Released with Major Performance Improvements</a></h2>
            <time datetime="2025-01-15T10:00:00">January 15, 2025</time>
            <p>The Python Software Foundation has announced the release of Python 3.12, featuring significant performance improvements and new syntax features.</p>
        </article>
        <article>
            <h2><a href="https://news.example.com/article-2">JavaScript Frameworks in 2025</a></h2>
            <time datetime="2025-01-14T08:00:00">January 14, 2025</time>
            <p>A comprehensive overview of the JavaScript framework landscape in 2025.</p>
        </article>
        <article>
            <h3><a href="https://news.example.com/article-3">New AI Model Uses Python for Training</a></h3>
            <time datetime="2025-01-13T12:00:00">January 13, 2025</time>
            <p>Researchers have developed a new AI model that leverages Python-based tools for efficient training.</p>
        </article>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_no_articles():
    """Sample HTML with no article elements."""
    return """
    <html>
    <body>
        <div class="header">
            <h1>News Website</h1>
        </div>
        <div class="footer">
            <p>Copyright 2025</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_crawler_job():
    """Create a sample CrawlerJob."""
    return CrawlerJob(
        job_id=str(uuid.uuid4()),
        keyword="python",
        target_sites=[
            "https://news.example.com",
            "https://tech.example.com",
        ],
        created_at=datetime.utcnow(),
    )


class TestExtractArticles:
    """Tests for article extraction from HTML."""

    def test_extracts_matching_articles_from_article_tags(self, crawler, sample_html_with_articles):
        """Should extract articles that match the keyword from <article> tags."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        # "Python 3.12 Released..." and "New AI Model Uses Python..." should match
        assert len(articles) == 2
        titles = [a.title for a in articles]
        assert "Python 3.12 Released with Major Performance Improvements" in titles
        assert "New AI Model Uses Python for Training" in titles

    def test_does_not_extract_non_matching_articles(self, crawler, sample_html_with_articles):
        """Should not extract articles that don't match the keyword."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "blockchain",
        )

        assert len(articles) == 0

    def test_extracts_from_div_with_article_class(self, crawler):
        """Should extract articles from divs with article-related class names."""
        html = """
        <html><body>
            <div class="article-item">
                <h2><a href="/story-1">Python News Today</a></h2>
                <p>Latest updates about Python programming language.</p>
            </div>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")

        assert len(articles) == 1
        assert articles[0].title == "Python News Today"

    def test_returns_empty_list_for_no_articles(self, crawler, sample_html_no_articles):
        """Should return empty list when no article elements found."""
        articles = crawler.extract_articles(
            sample_html_no_articles,
            "https://news.example.com",
            "python",
        )

        assert articles == []

    def test_extracts_article_url(self, crawler, sample_html_with_articles):
        """Should extract the article URL from link tags."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        urls = [a.url for a in articles]
        assert "https://news.example.com/article-1" in urls

    def test_resolves_relative_urls(self, crawler):
        """Should resolve relative URLs to absolute URLs."""
        html = """
        <html><body>
            <article>
                <h2><a href="/articles/python-news">Python Update</a></h2>
                <p>Python content here.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://news.example.com/page", "python")

        assert len(articles) == 1
        assert articles[0].url == "https://news.example.com/articles/python-news"

    def test_extracts_publication_date_from_time_tag(self, crawler, sample_html_with_articles):
        """Should extract publication date from <time> datetime attribute."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        # First article has datetime="2025-01-15T10:00:00"
        article_with_date = next(
            a for a in articles
            if a.title == "Python 3.12 Released with Major Performance Improvements"
        )
        assert article_with_date.published_date is not None

    def test_extracts_source_from_url_domain(self, crawler, sample_html_with_articles):
        """Should set source to the domain name of the crawled URL."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com/tech",
            "python",
        )

        for article in articles:
            assert article.source == "news.example.com"

    def test_sets_matched_keyword(self, crawler, sample_html_with_articles):
        """Should set matched_keyword on extracted articles."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        for article in articles:
            assert article.matched_keyword == "python"

    def test_sets_crawl_timestamp(self, crawler, sample_html_with_articles):
        """Should set crawl_timestamp on extracted articles."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        for article in articles:
            assert article.crawl_timestamp is not None
            assert isinstance(article.crawl_timestamp, datetime)

    def test_generates_unique_article_ids(self, crawler, sample_html_with_articles):
        """Should generate unique UUID4 article_ids for each article."""
        articles = crawler.extract_articles(
            sample_html_with_articles,
            "https://news.example.com",
            "python",
        )

        article_ids = [a.article_id for a in articles]
        assert len(set(article_ids)) == len(article_ids)
        # Validate UUID4 format
        for aid in article_ids:
            parsed = uuid.UUID(aid, version=4)
            assert str(parsed) == aid

    def test_skips_articles_without_title(self, crawler):
        """Should skip article elements that have no heading tag."""
        html = """
        <html><body>
            <article>
                <p>Some content about python without a title.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")

        assert len(articles) == 0


class TestKeywordMatching:
    """Tests for case-insensitive keyword matching."""

    def test_matches_keyword_in_title_case_insensitive(self, crawler):
        """Should match keyword regardless of case in title."""
        html = """
        <html><body>
            <article>
                <h2><a href="/a">PYTHON Is Great</a></h2>
                <p>Some unrelated content here.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")
        assert len(articles) == 1

    def test_matches_keyword_in_content_case_insensitive(self, crawler):
        """Should match keyword in content text regardless of case."""
        html = """
        <html><body>
            <article>
                <h2><a href="/a">Programming News</a></h2>
                <p>The latest PYTHON release brings many improvements.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")
        assert len(articles) == 1

    def test_no_match_when_keyword_absent(self, crawler):
        """Should not match when keyword is not in title or content."""
        html = """
        <html><body>
            <article>
                <h2><a href="/a">Java Programming Guide</a></h2>
                <p>Learn about Java development best practices.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")
        assert len(articles) == 0

    def test_matches_substring_in_title(self, crawler):
        """Should match keyword as substring within words."""
        html = """
        <html><body>
            <article>
                <h2><a href="/a">MicroPython for IoT Devices</a></h2>
                <p>Using embedded systems with microcontrollers.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")
        assert len(articles) == 1


class TestSummaryExtraction:
    """Tests for summary extraction and truncation."""

    def test_summary_truncated_to_500_chars(self, crawler):
        """Should truncate summary to 500 characters maximum."""
        long_content = "Python " * 200  # Much longer than 500 chars
        html = f"""
        <html><body>
            <article>
                <h2><a href="/a">Python Article</a></h2>
                <p>{long_content}</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")

        assert len(articles) == 1
        assert len(articles[0].summary) <= MAX_SUMMARY_LENGTH

    def test_summary_from_paragraph_tag(self, crawler):
        """Should extract summary from <p> tag when available."""
        html = """
        <html><body>
            <article>
                <h2><a href="/a">Python News</a></h2>
                <p>This is the article summary about Python developments.</p>
            </article>
        </body></html>
        """

        articles = crawler.extract_articles(html, "https://example.com", "python")

        assert len(articles) == 1
        assert "article summary about Python" in articles[0].summary


class TestDuplicateDetection:
    """Tests for duplicate URL tracking."""

    def test_is_duplicate_returns_true_for_cached_url(self, crawler, mock_cache):
        """Should return True when URL exists in cache."""
        mock_cache.exists.return_value = True

        assert crawler.is_duplicate("https://example.com/article-1") is True
        mock_cache.exists.assert_called_once_with(
            f"{CACHE_KEY_PREFIX}https://example.com/article-1"
        )

    def test_is_duplicate_returns_false_for_new_url(self, crawler, mock_cache):
        """Should return False when URL is not in cache."""
        mock_cache.exists.return_value = False

        assert crawler.is_duplicate("https://example.com/new-article") is False

    def test_mark_url_crawled_sets_cache_with_ttl(self, crawler, mock_cache):
        """Should mark URL in cache with 7-day TTL."""
        crawler._mark_url_crawled("https://example.com/article-1")

        mock_cache.set.assert_called_once_with(
            f"{CACHE_KEY_PREFIX}https://example.com/article-1",
            True,
            ttl=timedelta(days=CRAWLED_URL_TTL_DAYS),
        )

    def test_crawl_site_skips_duplicate_articles(self, crawler, mock_cache, mock_database):
        """Should skip articles whose URLs are already in cache."""
        # is_duplicate checks cache.exists with the URL key
        # First article URL is duplicate, second is new
        def exists_side_effect(key):
            if "old-article" in key:
                return True
            return False

        mock_cache.exists.side_effect = exists_side_effect

        html = """
        <html><body>
            <article>
                <h2><a href="https://example.com/old-article">Old Python Article</a></h2>
                <p>Python content.</p>
            </article>
            <article>
                <h2><a href="https://example.com/new-article">New Python Article</a></h2>
                <p>More Python content.</p>
            </article>
        </body></html>
        """

        with patch.object(crawler, '_fetch_page', return_value=html):
            with patch.object(crawler, 'is_allowed_by_robots', return_value=True):
                articles = crawler.crawl_site("https://example.com", "python")

        # Only the non-duplicate article should be returned
        assert len(articles) == 1
        assert articles[0].title == "New Python Article"


class TestRobotsTxtCompliance:
    """Tests for robots.txt compliance."""

    def test_is_allowed_returns_true_when_no_robots_txt(self, crawler):
        """Should allow crawling when robots.txt cannot be fetched."""
        with patch.object(crawler, '_get_robots_parser', return_value=None):
            assert crawler.is_allowed_by_robots("https://example.com/page") is True

    def test_is_allowed_returns_false_when_disallowed(self, crawler):
        """Should return False when robots.txt disallows the URL."""
        mock_rp = MagicMock()
        mock_rp.can_fetch.return_value = False

        with patch.object(crawler, '_get_robots_parser', return_value=mock_rp):
            assert crawler.is_allowed_by_robots("https://example.com/private") is False

    def test_is_allowed_returns_true_when_allowed(self, crawler):
        """Should return True when robots.txt allows the URL."""
        mock_rp = MagicMock()
        mock_rp.can_fetch.return_value = True

        with patch.object(crawler, '_get_robots_parser', return_value=mock_rp):
            assert crawler.is_allowed_by_robots("https://example.com/public") is True

    def test_crawl_site_skips_when_robots_disallows(self, crawler):
        """Should skip crawling when robots.txt disallows."""
        with patch.object(crawler, 'is_allowed_by_robots', return_value=False):
            articles = crawler.crawl_site("https://example.com", "python")

        assert articles == []

    def test_robots_parser_is_cached(self, crawler):
        """Should cache robots.txt parser for the same domain."""
        with patch('src.crawler.news_crawler.RobotFileParser') as MockRP:
            mock_instance = MagicMock()
            MockRP.return_value = mock_instance

            crawler._get_robots_parser("https://example.com/page1")
            crawler._get_robots_parser("https://example.com/page2")

            # Should only create one parser (cached for same domain)
            assert MockRP.call_count == 1


class TestRotatingUserAgents:
    """Tests for rotating user agents."""

    def test_rotates_through_user_agents(self, crawler):
        """Should cycle through user agents on each call."""
        agents = []
        for _ in range(len(USER_AGENTS) + 1):
            agents.append(crawler._get_next_user_agent())

        # Should have cycled through all agents and started over
        assert agents[0] == USER_AGENTS[0]
        assert agents[len(USER_AGENTS)] == USER_AGENTS[0]

    def test_different_agents_for_consecutive_requests(self, crawler):
        """Should use different user agents for consecutive requests."""
        agent1 = crawler._get_next_user_agent()
        agent2 = crawler._get_next_user_agent()

        assert agent1 != agent2

    @patch('src.crawler.news_crawler.requests.get')
    def test_fetch_page_uses_user_agent_header(self, mock_get, crawler):
        """Should include User-Agent header in requests."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html></html>"
        mock_get.return_value = mock_response

        crawler._fetch_page("https://example.com")

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert "User-Agent" in call_kwargs["headers"]
        assert call_kwargs["headers"]["User-Agent"] in USER_AGENTS


class TestRequestTimeout:
    """Tests for request timeout handling."""

    @patch('src.crawler.news_crawler.requests.get')
    def test_request_uses_30_second_timeout(self, mock_get, mock_database, mock_cache):
        """Should set timeout to 30 seconds on requests."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html></html>"
        mock_get.return_value = mock_response

        crawler = NewsCrawler(
            database=mock_database,
            cache=mock_cache,
            request_timeout=REQUEST_TIMEOUT_SECONDS,
        )
        crawler._fetch_page("https://example.com")

        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["timeout"] == 30

    @patch('src.crawler.news_crawler.requests.get')
    def test_timeout_returns_none(self, mock_get, crawler):
        """Should return None when request times out."""
        import requests
        mock_get.side_effect = requests.Timeout("Connection timed out")

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.news_crawler.requests.get')
    def test_connection_error_returns_none(self, mock_get, crawler):
        """Should return None on connection errors."""
        import requests
        mock_get.side_effect = requests.ConnectionError("Connection refused")

        result = crawler._fetch_page("https://example.com")

        assert result is None


class TestHttpErrorHandling:
    """Tests for HTTP error response handling."""

    @patch('src.crawler.news_crawler.requests.get')
    def test_4xx_error_returns_none(self, mock_get, crawler):
        """Should return None and log error for 4xx responses."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.news_crawler.requests.get')
    def test_5xx_error_returns_none(self, mock_get, crawler):
        """Should return None and log error for 5xx responses."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.news_crawler.requests.get')
    def test_404_error_returns_none(self, mock_get, crawler):
        """Should return None for 404 Not Found."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com/missing")

        assert result is None

    @patch('src.crawler.news_crawler.requests.get')
    def test_200_returns_html_content(self, mock_get, crawler):
        """Should return HTML content for successful 200 response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Content</body></html>"
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result == "<html><body>Content</body></html>"


class TestPoliteCrawling:
    """Tests for polite crawling with delays."""

    @patch('src.crawler.news_crawler.time.sleep')
    def test_crawl_news_delays_between_sites(self, mock_sleep, mock_database, mock_cache):
        """Should sleep 2 seconds between crawling different sites."""
        crawler = NewsCrawler(
            database=mock_database,
            cache=mock_cache,
            crawl_delay=CRAWL_DELAY_SECONDS,
        )

        job = CrawlerJob(
            job_id="test-job",
            keyword="python",
            target_sites=["https://site1.com", "https://site2.com", "https://site3.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, 'crawl_site', return_value=[]):
            crawler.crawl_news(job)

        # Should sleep between sites (not before first)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(CRAWL_DELAY_SECONDS)

    @patch('src.crawler.news_crawler.time.sleep')
    def test_no_delay_before_first_site(self, mock_sleep, mock_database, mock_cache):
        """Should not delay before crawling the first site."""
        crawler = NewsCrawler(
            database=mock_database,
            cache=mock_cache,
            crawl_delay=CRAWL_DELAY_SECONDS,
        )

        job = CrawlerJob(
            job_id="test-job",
            keyword="python",
            target_sites=["https://site1.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, 'crawl_site', return_value=[]):
            crawler.crawl_news(job)

        mock_sleep.assert_not_called()


class TestCrawlNews:
    """Tests for the main crawl_news method."""

    def test_crawl_news_returns_articles_from_all_sites(self, crawler):
        """Should aggregate articles from all target sites."""
        job = CrawlerJob(
            job_id="test-job",
            keyword="python",
            target_sites=["https://site1.com", "https://site2.com"],
            created_at=datetime.utcnow(),
        )

        article1 = NewsArticle(
            article_id="id1", title="Article 1", summary="Summary 1",
            url="https://site1.com/a1", source="site1.com", matched_keyword="python",
        )
        article2 = NewsArticle(
            article_id="id2", title="Article 2", summary="Summary 2",
            url="https://site2.com/a2", source="site2.com", matched_keyword="python",
        )

        with patch.object(crawler, 'crawl_site', side_effect=[[article1], [article2]]):
            articles = crawler.crawl_news(job)

        assert len(articles) == 2

    def test_crawl_news_handles_site_errors_gracefully(self, crawler):
        """Should continue crawling other sites if one fails."""
        job = CrawlerJob(
            job_id="test-job",
            keyword="python",
            target_sites=["https://site1.com", "https://site2.com"],
            created_at=datetime.utcnow(),
        )

        article = NewsArticle(
            article_id="id1", title="Article 1", summary="Summary 1",
            url="https://site2.com/a1", source="site2.com", matched_keyword="python",
        )

        def side_effect(url, keyword):
            if url == "https://site1.com":
                raise Exception("Connection failed")
            return [article]

        with patch.object(crawler, 'crawl_site', side_effect=side_effect):
            articles = crawler.crawl_news(job)

        # Should still get articles from site2
        assert len(articles) == 1

    def test_crawl_news_empty_target_sites(self, crawler):
        """Should return empty list when no target sites configured."""
        job = CrawlerJob(
            job_id="test-job",
            keyword="python",
            target_sites=[],
            created_at=datetime.utcnow(),
        )

        articles = crawler.crawl_news(job)

        assert articles == []


class TestStoreArticle:
    """Tests for article storage in database."""

    def test_stores_article_in_database(self, crawler, mock_database):
        """Should insert article dict into news_articles collection."""
        article = NewsArticle(
            article_id="test-id",
            title="Test Article",
            summary="Test summary",
            url="https://example.com/article",
            source="example.com",
            matched_keyword="python",
            crawl_timestamp=datetime.utcnow(),
        )

        crawler._store_article(article)

        mock_database.insert_one.assert_called_once_with(
            "news_articles",
            article.to_dict(),
        )

    def test_store_article_handles_database_error(self, crawler, mock_database):
        """Should log error and not raise when database insert fails."""
        mock_database.insert_one.side_effect = Exception("DB connection lost")

        article = NewsArticle(
            article_id="test-id",
            title="Test Article",
            summary="Test summary",
            url="https://example.com/article",
            source="example.com",
            matched_keyword="python",
        )

        # Should not raise
        crawler._store_article(article)


class TestCrawlSite:
    """Tests for crawl_site method integration."""

    def test_crawl_site_full_flow(self, crawler, mock_cache, mock_database):
        """Should fetch page, extract articles, filter duplicates, and store."""
        html = """
        <html><body>
            <article>
                <h2><a href="https://example.com/new">Python Update</a></h2>
                <p>New Python features released today.</p>
            </article>
        </body></html>
        """

        mock_cache.exists.return_value = False

        with patch.object(crawler, '_fetch_page', return_value=html):
            with patch.object(crawler, 'is_allowed_by_robots', return_value=True):
                articles = crawler.crawl_site("https://example.com", "python")

        assert len(articles) == 1
        assert articles[0].title == "Python Update"
        # Should have stored the article
        mock_database.insert_one.assert_called_once()
        # Should have marked URL as crawled
        mock_cache.set.assert_called_once()

    def test_crawl_site_returns_empty_when_fetch_fails(self, crawler):
        """Should return empty list when page fetch fails."""
        with patch.object(crawler, '_fetch_page', return_value=None):
            with patch.object(crawler, 'is_allowed_by_robots', return_value=True):
                articles = crawler.crawl_site("https://example.com", "python")

        assert articles == []


class TestConstants:
    """Tests for module constants."""

    def test_request_timeout_is_30_seconds(self):
        """Request timeout should be 30 seconds."""
        assert REQUEST_TIMEOUT_SECONDS == 30

    def test_crawl_delay_is_2_seconds(self):
        """Crawl delay should be 2 seconds."""
        assert CRAWL_DELAY_SECONDS == 2

    def test_crawled_url_ttl_is_7_days(self):
        """Crawled URL TTL should be 7 days."""
        assert CRAWLED_URL_TTL_DAYS == 7

    def test_max_summary_length_is_500(self):
        """Max summary length should be 500 characters."""
        assert MAX_SUMMARY_LENGTH == 500

    def test_user_agents_list_not_empty(self):
        """Should have at least one user agent configured."""
        assert len(USER_AGENTS) > 0
