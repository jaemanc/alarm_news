"""
Keyword Retriever for the Alarm News Web Crawler.

Retrieves unique keywords from active users in MongoDB and creates
crawler jobs for each keyword. Designed to run every 30 minutes.
"""
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from src.shared.database import DatabaseInterface

logger = logging.getLogger(__name__)


@dataclass
class CrawlerJob:
    """
    Represents a scheduled crawl task for a specific keyword.

    Attributes:
        job_id: Unique identifier for the job (UUID4 string).
        keyword: The keyword to crawl for.
        target_sites: List of website URLs to crawl.
        created_at: Timestamp when the job was created.
    """
    job_id: str
    keyword: str
    target_sites: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Serialize to a dictionary."""
        return {
            "job_id": self.job_id,
            "keyword": self.keyword,
            "target_sites": self.target_sites,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# Default interval: 30 minutes in seconds
DEFAULT_INTERVAL_SECONDS = 30 * 60


class KeywordRetriever:
    """
    Retrieves unique keywords from active MongoDB users and creates crawler jobs.

    Queries the users collection for users with valid subscriptions
    (subscription_expiry > current timestamp), extracts all keywords,
    deduplicates them, and creates a CrawlerJob for each unique keyword.

    Can be scheduled to run every 30 minutes using the start/stop methods.
    """

    def __init__(
        self,
        database: DatabaseInterface,
        target_sites: Optional[List[str]] = None,
        interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    ):
        """
        Initialize the KeywordRetriever.

        Args:
            database: Database interface for querying users.
            target_sites: Default list of target sites for crawler jobs.
            interval_seconds: Interval between retrieval runs (default: 1800s / 30 min).
        """
        self._database = database
        self._target_sites = target_sites or []
        self._interval_seconds = interval_seconds
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        """Whether the periodic retrieval is currently running."""
        return self._running

    def get_unique_keywords(self) -> List[str]:
        """
        Retrieve all unique keywords from active users in MongoDB.

        Queries for users where subscription_expiry > current timestamp,
        then extracts and deduplicates all keywords.

        Returns:
            Sorted list of unique keywords from all active users.
        """
        now = datetime.utcnow()
        query = {"subscription_expiry": {"$gt": now}}

        logger.info("Querying MongoDB for active users (subscription_expiry > %s)", now.isoformat())

        users = self._database.find_many("users", query)

        all_keywords: set = set()
        for user in users:
            keywords = user.get("keywords", [])
            for keyword in keywords:
                stripped = keyword.strip().lower() if keyword else ""
                if stripped:  # Skip empty/whitespace-only strings
                    all_keywords.add(stripped)

        unique_keywords = sorted(all_keywords)
        logger.info(
            "Retrieved %d unique keywords from %d active users",
            len(unique_keywords),
            len(users),
        )
        return unique_keywords

    def create_crawler_jobs(self, keywords: List[str]) -> List[CrawlerJob]:
        """
        Create crawler jobs for each unique keyword.

        Each job gets a unique job_id generated using UUID4.

        Args:
            keywords: List of unique keywords to create jobs for.

        Returns:
            List of CrawlerJob instances, one per keyword.
        """
        jobs = []
        now = datetime.utcnow()

        for keyword in keywords:
            job = CrawlerJob(
                job_id=str(uuid.uuid4()),
                keyword=keyword,
                target_sites=list(self._target_sites),
                created_at=now,
            )
            jobs.append(job)

        logger.info("Created %d crawler jobs", len(jobs))
        return jobs

    def retrieve_and_create_jobs(self) -> List[CrawlerJob]:
        """
        Perform a full retrieval cycle: get keywords and create jobs.

        This is the main entry point for a single retrieval run.

        Returns:
            List of CrawlerJob instances for all unique active keywords.
        """
        keywords = self.get_unique_keywords()
        if not keywords:
            logger.info("No keywords found from active users. No jobs created.")
            return []
        return self.create_crawler_jobs(keywords)

    def start(self) -> None:
        """
        Start the periodic keyword retrieval (every 30 minutes by default).

        Uses a threading.Timer for scheduling. The first run executes immediately.
        """
        with self._lock:
            if self._running:
                logger.warning("KeywordRetriever is already running.")
                return
            self._running = True

        logger.info(
            "Starting KeywordRetriever with interval of %d seconds",
            self._interval_seconds,
        )
        self._run_cycle()

    def stop(self) -> None:
        """Stop the periodic keyword retrieval."""
        with self._lock:
            self._running = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

        logger.info("KeywordRetriever stopped.")

    def _run_cycle(self) -> None:
        """Execute one retrieval cycle and schedule the next."""
        if not self._running:
            return

        try:
            self.retrieve_and_create_jobs()
        except Exception as e:
            logger.error("Error during keyword retrieval cycle: %s", str(e))

        # Schedule next run
        if self._running:
            self._timer = threading.Timer(self._interval_seconds, self._run_cycle)
            self._timer.daemon = True
            self._timer.start()
