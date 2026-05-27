"""
News Crawler for the Alarm News System.

Crawls configured news websites using BeautifulSoup, matches articles
by keyword, and stores results in the data store. Implements polite
crawling with delays, robots.txt compliance, and rotating user agents.
"""
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from src.crawler.keyword_retriever import CrawlerJob
from src.shared.cache import CacheInterface
from src.shared.database import DatabaseInterface
from src.shared.models import NewsArticle

logger = logging.getLogger(__name__)

# Rotating user agents to avoid bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Constants
REQUEST_TIMEOUT_SECONDS = 30
CRAWL_DELAY_SECONDS = 2
CRAWLED_URL_TTL_DAYS = 7
MAX_SUMMARY_LENGTH = 500
CACHE_KEY_PREFIX = "crawled_url:"


class NewsCrawler:
    """
    Crawls news websites for articles matching a given keyword.

    Features:
    - Case-insensitive keyword matching in title/content
    - Polite crawling with 2-second delays between requests
    - robots.txt compliance
    - Rotating user agents
    - Duplicate URL tracking via cache (7-day TTL)
    - 30-second request timeout
    - Error logging for 4xx/5xx responses
    """

    def __init__(
        self,
        database: DatabaseInterface,
        cache: CacheInterface,
        request_timeout: int = REQUEST_TIMEOUT_SECONDS,
        crawl_delay: float = CRAWL_DELAY_SECONDS,
    ):
        """
        Initialize the NewsCrawler.

        Args:
            database: Database interface for storing articles.
            cache: Cache interface for tracking crawled URLs.
            request_timeout: Timeout for HTTP requests in seconds.
            crawl_delay: Delay between requests in seconds.
        """
        self._database = database
        self._cache = cache
        self._request_timeout = request_timeout
        self._crawl_delay = crawl_delay
        self._user_agent_index = 0
        self._robots_cache: dict = {}

    def _get_next_user_agent(self) -> str:
        """Get the next user agent from the rotation."""
        agent = USER_AGENTS[self._user_agent_index % len(USER_AGENTS)]
        self._user_agent_index += 1
        return agent

    def _get_robots_parser(self, site_url: str) -> Optional[RobotFileParser]:
        """
        Get or create a RobotFileParser for the given site.

        Args:
            site_url: Base URL of the site.

        Returns:
            RobotFileParser instance or None if robots.txt cannot be fetched.
        """
        parsed = urlparse(site_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        if base_url in self._robots_cache:
            return self._robots_cache[base_url]

        robots_url = f"{base_url}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)

        try:
            rp.read()
            self._robots_cache[base_url] = rp
            return rp
        except Exception as e:
            logger.warning("Failed to fetch robots.txt from %s: %s", robots_url, str(e))
            # If we can't read robots.txt, allow crawling (permissive default)
            self._robots_cache[base_url] = None
            return None

    def is_allowed_by_robots(self, url: str) -> bool:
        """
        Check if crawling the URL is allowed by robots.txt.

        Args:
            url: The URL to check.

        Returns:
            True if crawling is allowed, False otherwise.
        """
        rp = self._get_robots_parser(url)
        if rp is None:
            # If robots.txt is unavailable, allow crawling
            return True

        user_agent = USER_AGENTS[0]
        return rp.can_fetch(user_agent, url)

    def is_duplicate(self, url: str) -> bool:
        """
        Check if the URL has already been crawled.

        Args:
            url: The article URL to check.

        Returns:
            True if the URL was already crawled, False otherwise.
        """
        cache_key = f"{CACHE_KEY_PREFIX}{url}"
        return self._cache.exists(cache_key)

    def _mark_url_crawled(self, url: str) -> None:
        """
        Mark a URL as crawled in the cache with 7-day TTL.

        Args:
            url: The URL to mark as crawled.
        """
        cache_key = f"{CACHE_KEY_PREFIX}{url}"
        ttl = timedelta(days=CRAWLED_URL_TTL_DAYS)
        self._cache.set(cache_key, True, ttl=ttl)

    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a web page with timeout and error handling.

        Args:
            url: The URL to fetch.

        Returns:
            HTML content as string, or None on failure.
        """
        user_agent = self._get_next_user_agent()
        headers = {"User-Agent": user_agent}

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=self._request_timeout,
            )

            if response.status_code >= 400:
                logger.error(
                    "HTTP %d error fetching %s",
                    response.status_code,
                    url,
                )
                return None

            return response.text

        except requests.Timeout:
            logger.error("Request timed out after %ds for %s", self._request_timeout, url)
            return None
        except requests.RequestException as e:
            logger.error("Request failed for %s: %s", url, str(e))
            return None

    def extract_articles(self, html: str, url: str, keyword: str) -> List[NewsArticle]:
        """
        Extract articles from HTML that match the keyword.

        Searches for article elements and matches keyword case-insensitively
        in the title or content text.

        Args:
            html: Raw HTML content of the page.
            url: The source URL (used for source name).
            keyword: The keyword to match against.

        Returns:
            List of NewsArticle instances that match the keyword.
        """
        soup = BeautifulSoup(html, "html.parser")
        articles: List[NewsArticle] = []
        parsed_url = urlparse(url)
        source_name = parsed_url.netloc

        # Look for article elements in common HTML structures
        article_elements = soup.find_all("article")

        # If no <article> tags, try common news site patterns
        if not article_elements:
            article_elements = soup.find_all(
                ["div", "section"],
                class_=lambda c: c and any(
                    term in (c if isinstance(c, str) else " ".join(c)).lower()
                    for term in ["article", "post", "story", "news-item", "entry"]
                ),
            )

        for element in article_elements:
            article = self._parse_article_element(element, source_name, keyword, url)
            if article is not None:
                articles.append(article)

        return articles

    def _parse_article_element(
        self,
        element,
        source_name: str,
        keyword: str,
        page_url: str,
    ) -> Optional[NewsArticle]:
        """
        Parse a single article element and check for keyword match.

        Args:
            element: BeautifulSoup element representing an article.
            source_name: Name of the news source.
            keyword: Keyword to match (case-insensitive).
            page_url: The page URL for resolving relative links.

        Returns:
            NewsArticle if keyword matches, None otherwise.
        """
        # Extract title
        title_tag = element.find(["h1", "h2", "h3", "h4", "h5", "h6"])
        if title_tag is None:
            return None

        title = title_tag.get_text(strip=True)
        if not title:
            return None

        # Extract content/summary text
        content_text = element.get_text(separator=" ", strip=True)

        # Case-insensitive keyword matching in title or content
        keyword_lower = keyword.lower()
        if keyword_lower not in title.lower() and keyword_lower not in content_text.lower():
            return None

        # Extract URL from link
        link_tag = element.find("a", href=True)
        article_url = ""
        if link_tag:
            href = link_tag["href"]
            if href.startswith("http"):
                article_url = href
            elif href.startswith("/"):
                parsed = urlparse(page_url)
                article_url = f"{parsed.scheme}://{parsed.netloc}{href}"
            else:
                article_url = href
        else:
            article_url = page_url

        # Extract summary (up to 500 chars)
        # Try to find a paragraph or description
        summary_tag = element.find("p")
        if summary_tag:
            summary = summary_tag.get_text(strip=True)
        else:
            # Use content text minus the title
            summary = content_text.replace(title, "", 1).strip()

        summary = summary[:MAX_SUMMARY_LENGTH]

        # Extract publication date
        published_date = self._extract_date(element)

        now = datetime.utcnow()

        return NewsArticle(
            article_id=str(uuid.uuid4()),
            title=title,
            summary=summary,
            url=article_url,
            published_date=published_date,
            source=source_name,
            matched_keyword=keyword,
            crawl_timestamp=now,
        )

    def _extract_date(self, element) -> Optional[datetime]:
        """
        Attempt to extract a publication date from an article element.

        Args:
            element: BeautifulSoup element.

        Returns:
            datetime if found, None otherwise.
        """
        # Try <time> tag with datetime attribute
        time_tag = element.find("time")
        if time_tag and time_tag.get("datetime"):
            try:
                return datetime.fromisoformat(
                    time_tag["datetime"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        # Try common date meta attributes
        date_meta = element.find(attrs={"class": lambda c: c and "date" in str(c).lower()})
        if date_meta:
            date_text = date_meta.get_text(strip=True)
            try:
                return datetime.fromisoformat(date_text)
            except (ValueError, TypeError):
                pass

        return None

    def crawl_site(self, site_url: str, keyword: str) -> List[NewsArticle]:
        """
        Crawl a single site for articles matching the keyword.

        Checks robots.txt, fetches the page, extracts matching articles,
        filters duplicates, and stores new articles.

        Args:
            site_url: URL of the news site to crawl.
            keyword: Keyword to match articles against.

        Returns:
            List of new (non-duplicate) articles found.
        """
        # Check robots.txt
        if not self.is_allowed_by_robots(site_url):
            logger.info("Crawling disallowed by robots.txt: %s", site_url)
            return []

        # Fetch page
        html = self._fetch_page(site_url)
        if html is None:
            return []

        # Extract matching articles
        articles = self.extract_articles(html, site_url, keyword)

        # Filter duplicates and store new articles
        new_articles: List[NewsArticle] = []
        for article in articles:
            if not self.is_duplicate(article.url):
                self._mark_url_crawled(article.url)
                self._store_article(article)
                new_articles.append(article)
            else:
                logger.debug("Skipping duplicate URL: %s", article.url)

        return new_articles

    def _store_article(self, article: NewsArticle) -> None:
        """
        Store an article in the database.

        Args:
            article: The NewsArticle to store.
        """
        try:
            self._database.insert_one("news_articles", article.to_dict())
        except Exception as e:
            logger.error("Failed to store article %s: %s", article.article_id, str(e))

    def crawl_news(self, job: CrawlerJob) -> List[NewsArticle]:
        """
        Execute a crawl job: crawl all target sites for the job's keyword.

        Implements polite crawling with 2-second delays between requests.

        Args:
            job: CrawlerJob containing keyword and target sites.

        Returns:
            List of all new articles found across all sites.
        """
        logger.info(
            "Starting news crawl for keyword '%s' across %d sites (job: %s)",
            job.keyword,
            len(job.target_sites),
            job.job_id,
        )

        all_articles: List[NewsArticle] = []

        for i, site_url in enumerate(job.target_sites):
            # Polite crawling: 2-second delay between requests (skip first)
            if i > 0:
                time.sleep(self._crawl_delay)

            try:
                articles = self.crawl_site(site_url, job.keyword)
                all_articles.extend(articles)
                logger.info(
                    "Found %d new articles on %s for keyword '%s'",
                    len(articles),
                    site_url,
                    job.keyword,
                )
            except Exception as e:
                logger.error(
                    "Error crawling %s for keyword '%s': %s",
                    site_url,
                    job.keyword,
                    str(e),
                )

        logger.info(
            "Completed crawl job %s: found %d total articles for keyword '%s'",
            job.job_id,
            len(all_articles),
            job.keyword,
        )

        return all_articles
