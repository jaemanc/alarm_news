"""
Stock Crawler for the Alarm News Web Crawler.

Crawls configured stock information websites for stock symbols and company
names matching user keywords. Extracts price data, calculates changes,
and stores results in the Data Store.

Designed to run every 15 minutes during market hours (9:00-16:00 weekdays).
"""
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import List, Optional, Protocol

import requests
from bs4 import BeautifulSoup

from src.crawler.keyword_retriever import CrawlerJob
from src.shared.cache import CacheInterface
from src.shared.models import StockData

logger = logging.getLogger(__name__)

# Default crawl interval: 15 minutes in seconds
DEFAULT_CRAWL_INTERVAL_SECONDS = 15 * 60

# Market hours: 9:00 - 16:00 (weekdays only)
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 16


class DataStoreInterface(Protocol):
    """Protocol for storing crawled stock data."""

    def store_stock_data(self, stock_data: StockData) -> None:
        """Store stock data in the data store."""
        ...


class StockCrawler:
    """
    Crawls stock information websites for stock data matching user keywords.

    Uses BeautifulSoup to parse HTML and extract stock symbols, company names,
    prices, and price changes. Tracks previous prices in cache to calculate
    percentage changes.

    Can be scheduled to run every 15 minutes during market hours.
    """

    def __init__(
        self,
        cache: CacheInterface,
        data_store: DataStoreInterface,
        interval_seconds: int = DEFAULT_CRAWL_INTERVAL_SECONDS,
        request_timeout: int = 30,
    ):
        """
        Initialize the StockCrawler.

        Args:
            cache: Cache interface for tracking previous prices.
            data_store: Data store interface for persisting stock data.
            interval_seconds: Interval between crawl cycles (default: 900s / 15 min).
            request_timeout: HTTP request timeout in seconds (default: 30).
        """
        self._cache = cache
        self._data_store = data_store
        self._interval_seconds = interval_seconds
        self._request_timeout = request_timeout
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        """Whether the periodic crawling is currently running."""
        return self._running

    def is_market_hours(self, now: Optional[datetime] = None) -> bool:
        """
        Check if current time is within market hours.

        Market hours are 9:00-16:00 on weekdays (Monday-Friday).

        Args:
            now: Optional datetime to check. Defaults to current UTC time.

        Returns:
            True if within market hours, False otherwise.
        """
        if now is None:
            now = datetime.utcnow()

        # Weekday: Monday=0, Sunday=6
        if now.weekday() >= 5:
            return False

        return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR

    def validate_price(self, price: float) -> bool:
        """
        Validate that a price is a positive number.

        Args:
            price: The price value to validate.

        Returns:
            True if price is a positive number, False otherwise.
        """
        try:
            return isinstance(price, (int, float)) and price > 0
        except (TypeError, ValueError):
            return False

    def calculate_price_change(self, current: float, previous: float) -> float:
        """
        Calculate the absolute price change.

        Args:
            current: Current stock price.
            previous: Previous stock price.

        Returns:
            Absolute price change (current - previous).
        """
        return round(current - previous, 2)

    def calculate_percentage_change(self, current: float, previous: float) -> float:
        """
        Calculate percentage change between current and previous price.

        Formula: ((current - previous) / previous) * 100, rounded to 2 decimals.

        Args:
            current: Current stock price.
            previous: Previous stock price.

        Returns:
            Percentage change rounded to 2 decimal places.
        """
        if previous == 0:
            return 0.0
        return round(((current - previous) / previous) * 100, 2)

    def matches_keyword(self, keyword: str, symbol: str, company_name: str) -> bool:
        """
        Check if keyword matches stock symbol or company name.

        Uses case-insensitive substring matching.

        Args:
            keyword: The keyword to match against.
            symbol: Stock ticker symbol.
            company_name: Full company name.

        Returns:
            True if keyword is a substring of symbol or company name.
        """
        keyword_lower = keyword.lower()
        return (
            keyword_lower in symbol.lower()
            or keyword_lower in company_name.lower()
        )

    def extract_stocks_from_html(self, html: str) -> List[dict]:
        """
        Extract stock data from HTML using BeautifulSoup.

        Parses HTML looking for stock data in table rows with class 'stock-row'
        or similar structures. Each stock entry should contain symbol, company
        name, and current price.

        Args:
            html: Raw HTML content from a stock website.

        Returns:
            List of dictionaries with keys: symbol, company_name, current_price.
        """
        stocks = []
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: Look for table rows with stock data
        stock_rows = soup.find_all("tr", class_="stock-row")
        if stock_rows:
            for row in stock_rows:
                stock = self._extract_from_table_row(row)
                if stock:
                    stocks.append(stock)
            return stocks

        # Strategy 2: Look for div-based stock cards
        stock_cards = soup.find_all("div", class_="stock-card")
        if stock_cards:
            for card in stock_cards:
                stock = self._extract_from_card(card)
                if stock:
                    stocks.append(stock)
            return stocks

        # Strategy 3: Look for generic table with stock data
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header row
                stock = self._extract_from_generic_row(row)
                if stock:
                    stocks.append(stock)

        return stocks

    def _extract_from_table_row(self, row) -> Optional[dict]:
        """Extract stock data from a table row with class 'stock-row'."""
        try:
            cells = row.find_all("td")
            if len(cells) >= 3:
                symbol = cells[0].get_text(strip=True)
                company_name = cells[1].get_text(strip=True)
                price_text = cells[2].get_text(strip=True)
                price = self._parse_price(price_text)
                if price is not None and symbol and company_name:
                    return {
                        "symbol": symbol,
                        "company_name": company_name,
                        "current_price": price,
                    }
        except (IndexError, ValueError) as e:
            logger.debug("Failed to extract from table row: %s", str(e))
        return None

    def _extract_from_card(self, card) -> Optional[dict]:
        """Extract stock data from a div-based stock card."""
        try:
            symbol_elem = card.find(class_="stock-symbol")
            name_elem = card.find(class_="stock-name")
            price_elem = card.find(class_="stock-price")

            if symbol_elem and name_elem and price_elem:
                symbol = symbol_elem.get_text(strip=True)
                company_name = name_elem.get_text(strip=True)
                price = self._parse_price(price_elem.get_text(strip=True))
                if price is not None and symbol and company_name:
                    return {
                        "symbol": symbol,
                        "company_name": company_name,
                        "current_price": price,
                    }
        except (AttributeError, ValueError) as e:
            logger.debug("Failed to extract from card: %s", str(e))
        return None

    def _extract_from_generic_row(self, row) -> Optional[dict]:
        """Extract stock data from a generic table row."""
        try:
            cells = row.find_all("td")
            if len(cells) >= 3:
                symbol = cells[0].get_text(strip=True)
                company_name = cells[1].get_text(strip=True)
                price_text = cells[2].get_text(strip=True)
                price = self._parse_price(price_text)
                if price is not None and symbol and company_name:
                    return {
                        "symbol": symbol,
                        "company_name": company_name,
                        "current_price": price,
                    }
        except (IndexError, ValueError) as e:
            logger.debug("Failed to extract from generic row: %s", str(e))
        return None

    def _parse_price(self, price_text: str) -> Optional[float]:
        """
        Parse a price string into a float.

        Handles common formats like "$123.45", "123.45", "1,234.56".

        Args:
            price_text: Raw price text from HTML.

        Returns:
            Parsed price as float, or None if parsing fails.
        """
        try:
            # Remove currency symbols and commas
            cleaned = price_text.replace("$", "").replace(",", "").replace("€", "").replace("£", "").strip()
            price = float(cleaned)
            if self.validate_price(price):
                return price
            logger.error("Invalid price data: price is not positive: %s", price_text)
            return None
        except (ValueError, TypeError):
            logger.error("Invalid price data: cannot parse '%s' as number", price_text)
            return None

    def _get_previous_price(self, symbol: str) -> Optional[float]:
        """
        Get the previous price for a stock symbol from cache.

        Args:
            symbol: Stock ticker symbol.

        Returns:
            Previous price or None if not cached.
        """
        cache_key = f"stock_price:{symbol}"
        return self._cache.get(cache_key)

    def _store_current_price(self, symbol: str, price: float) -> None:
        """
        Store the current price in cache for future change calculations.

        Args:
            symbol: Stock ticker symbol.
            price: Current stock price.
        """
        cache_key = f"stock_price:{symbol}"
        self._cache.set(cache_key, price)

    def crawl_stocks(self, job: CrawlerJob) -> List[StockData]:
        """
        Crawl stock websites for a given crawler job.

        Fetches HTML from each target site, extracts stock data, matches
        against the job keyword, calculates price changes, and stores results.

        Args:
            job: CrawlerJob containing keyword and target sites.

        Returns:
            List of StockData objects that matched the keyword.
        """
        matched_stocks: List[StockData] = []
        crawl_timestamp = datetime.utcnow()

        for site_url in job.target_sites:
            try:
                html = self._fetch_page(site_url)
                if html is None:
                    continue

                raw_stocks = self.extract_stocks_from_html(html)

                for raw_stock in raw_stocks:
                    symbol = raw_stock["symbol"]
                    company_name = raw_stock["company_name"]
                    current_price = raw_stock["current_price"]

                    # Check keyword match
                    if not self.matches_keyword(job.keyword, symbol, company_name):
                        continue

                    # Get previous price and calculate changes
                    previous_price = self._get_previous_price(symbol)
                    if previous_price is not None:
                        price_change = self.calculate_price_change(current_price, previous_price)
                        percentage_change = self.calculate_percentage_change(current_price, previous_price)
                    else:
                        price_change = 0.0
                        percentage_change = 0.0

                    # Store current price for next cycle
                    self._store_current_price(symbol, current_price)

                    # Create StockData object
                    stock_data = StockData(
                        stock_id=str(uuid.uuid4()),
                        symbol=symbol,
                        company_name=company_name,
                        current_price=current_price,
                        price_change=price_change,
                        percentage_change=percentage_change,
                        last_update=crawl_timestamp,
                        matched_keyword=job.keyword,
                        crawl_timestamp=crawl_timestamp,
                    )

                    # Store in data store
                    self._data_store.store_stock_data(stock_data)
                    matched_stocks.append(stock_data)

                    logger.info(
                        "Matched stock %s (%s) for keyword '%s': price=%.2f, change=%.2f%%",
                        symbol,
                        company_name,
                        job.keyword,
                        current_price,
                        percentage_change,
                    )

            except Exception as e:
                logger.error(
                    "Error crawling stock site %s for keyword '%s': %s",
                    site_url,
                    job.keyword,
                    str(e),
                )

        logger.info(
            "Crawl complete for keyword '%s': found %d matching stocks",
            job.keyword,
            len(matched_stocks),
        )
        return matched_stocks

    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a web page with timeout and error handling.

        Args:
            url: URL to fetch.

        Returns:
            HTML content as string, or None on failure.
        """
        try:
            response = requests.get(url, timeout=self._request_timeout)
            if response.status_code >= 400:
                logger.error(
                    "HTTP %d error fetching %s",
                    response.status_code,
                    url,
                )
                return None
            return response.text
        except requests.Timeout:
            logger.error("Timeout fetching %s (timeout=%ds)", url, self._request_timeout)
            return None
        except requests.RequestException as e:
            logger.error("Request error fetching %s: %s", url, str(e))
            return None

    def start(self) -> None:
        """
        Start the periodic stock crawling (every 15 minutes during market hours).

        Uses a threading.Timer for scheduling.
        """
        with self._lock:
            if self._running:
                logger.warning("StockCrawler is already running.")
                return
            self._running = True

        logger.info(
            "Starting StockCrawler with interval of %d seconds",
            self._interval_seconds,
        )
        self._schedule_next()

    def stop(self) -> None:
        """Stop the periodic stock crawling."""
        with self._lock:
            self._running = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

        logger.info("StockCrawler stopped.")

    def _schedule_next(self) -> None:
        """Schedule the next crawl cycle."""
        if not self._running:
            return

        self._timer = threading.Timer(self._interval_seconds, self._run_cycle)
        self._timer.daemon = True
        self._timer.start()

    def _run_cycle(self) -> None:
        """Execute one crawl cycle if within market hours, then schedule next."""
        if not self._running:
            return

        if self.is_market_hours():
            logger.info("Market hours active, running stock crawl cycle.")
        else:
            logger.info("Outside market hours, skipping stock crawl cycle.")

        # Schedule next run regardless
        self._schedule_next()
