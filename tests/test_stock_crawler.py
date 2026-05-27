"""
Unit tests for the Stock Crawler component.

Tests cover:
- Stock data extraction from HTML (table rows, cards, generic tables)
- Case-insensitive keyword matching in symbol/company name
- Price validation (positive numbers)
- Price change calculation (absolute)
- Percentage change calculation with rounding to 2 decimals
- Previous price tracking via cache
- Market hours detection
- HTTP error handling
- Data store integration
- Periodic crawling start/stop
"""
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.crawler.stock_crawler import (
    StockCrawler,
    DEFAULT_CRAWL_INTERVAL_SECONDS,
    MARKET_OPEN_HOUR,
    MARKET_CLOSE_HOUR,
)
from src.crawler.keyword_retriever import CrawlerJob
from src.shared.models import StockData


@pytest.fixture
def mock_cache():
    """Create a mock cache interface."""
    cache = MagicMock()
    cache.get = MagicMock(return_value=None)
    cache.set = MagicMock(return_value=True)
    cache.exists = MagicMock(return_value=False)
    return cache


@pytest.fixture
def mock_data_store():
    """Create a mock data store interface."""
    store = MagicMock()
    store.store_stock_data = MagicMock()
    return store


@pytest.fixture
def crawler(mock_cache, mock_data_store):
    """Create a StockCrawler with mock dependencies."""
    return StockCrawler(
        cache=mock_cache,
        data_store=mock_data_store,
        interval_seconds=60,
        request_timeout=30,
    )


@pytest.fixture
def sample_html_table_rows():
    """Sample HTML with stock data in table rows with class 'stock-row'."""
    return """
    <html>
    <body>
        <table>
            <tr class="stock-row">
                <td>AAPL</td>
                <td>Apple Inc.</td>
                <td>$185.50</td>
            </tr>
            <tr class="stock-row">
                <td>GOOGL</td>
                <td>Alphabet Inc.</td>
                <td>$142.75</td>
            </tr>
            <tr class="stock-row">
                <td>MSFT</td>
                <td>Microsoft Corporation</td>
                <td>$378.20</td>
            </tr>
        </table>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_stock_cards():
    """Sample HTML with stock data in div-based cards."""
    return """
    <html>
    <body>
        <div class="stock-card">
            <span class="stock-symbol">TSLA</span>
            <span class="stock-name">Tesla Inc.</span>
            <span class="stock-price">$245.30</span>
        </div>
        <div class="stock-card">
            <span class="stock-symbol">AMZN</span>
            <span class="stock-name">Amazon.com Inc.</span>
            <span class="stock-price">$178.90</span>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_generic_table():
    """Sample HTML with stock data in a generic table (no stock-row class)."""
    return """
    <html>
    <body>
        <table>
            <tr><th>Symbol</th><th>Company</th><th>Price</th></tr>
            <tr>
                <td>NVDA</td>
                <td>NVIDIA Corporation</td>
                <td>$875.50</td>
            </tr>
            <tr>
                <td>META</td>
                <td>Meta Platforms Inc.</td>
                <td>$485.20</td>
            </tr>
        </table>
    </body>
    </html>
    """


@pytest.fixture
def sample_crawler_job():
    """Create a sample CrawlerJob for stock crawling."""
    return CrawlerJob(
        job_id=str(uuid.uuid4()),
        keyword="apple",
        target_sites=[
            "https://stocks.example.com",
            "https://finance.example.com",
        ],
        created_at=datetime.utcnow(),
    )


class TestExtractStocksFromHtml:
    """Tests for stock data extraction from HTML."""

    def test_extracts_from_table_rows(self, crawler, sample_html_table_rows):
        """Should extract stock data from table rows with class 'stock-row'."""
        stocks = crawler.extract_stocks_from_html(sample_html_table_rows)

        assert len(stocks) == 3
        assert stocks[0]["symbol"] == "AAPL"
        assert stocks[0]["company_name"] == "Apple Inc."
        assert stocks[0]["current_price"] == 185.50

    def test_extracts_from_stock_cards(self, crawler, sample_html_stock_cards):
        """Should extract stock data from div-based stock cards."""
        stocks = crawler.extract_stocks_from_html(sample_html_stock_cards)

        assert len(stocks) == 2
        assert stocks[0]["symbol"] == "TSLA"
        assert stocks[0]["company_name"] == "Tesla Inc."
        assert stocks[0]["current_price"] == 245.30

    def test_extracts_from_generic_table(self, crawler, sample_html_generic_table):
        """Should extract stock data from generic table rows (skipping header)."""
        stocks = crawler.extract_stocks_from_html(sample_html_generic_table)

        assert len(stocks) == 2
        assert stocks[0]["symbol"] == "NVDA"
        assert stocks[0]["company_name"] == "NVIDIA Corporation"
        assert stocks[0]["current_price"] == 875.50

    def test_returns_empty_for_no_stock_data(self, crawler):
        """Should return empty list when no stock data found in HTML."""
        html = "<html><body><p>No stock data here.</p></body></html>"
        stocks = crawler.extract_stocks_from_html(html)
        assert stocks == []

    def test_handles_malformed_html(self, crawler):
        """Should handle malformed HTML gracefully."""
        html = "<html><body><table><tr class='stock-row'><td>AAPL</td></tr></table></body></html>"
        stocks = crawler.extract_stocks_from_html(html)
        # Not enough cells, should return empty
        assert stocks == []


class TestPriceValidation:
    """Tests for price validation (positive numbers)."""

    def test_positive_price_is_valid(self, crawler):
        """Should return True for positive prices."""
        assert crawler.validate_price(100.0) is True
        assert crawler.validate_price(0.01) is True
        assert crawler.validate_price(99999.99) is True

    def test_zero_price_is_invalid(self, crawler):
        """Should return False for zero price."""
        assert crawler.validate_price(0.0) is False

    def test_negative_price_is_invalid(self, crawler):
        """Should return False for negative prices."""
        assert crawler.validate_price(-1.0) is False
        assert crawler.validate_price(-100.50) is False

    def test_integer_price_is_valid(self, crawler):
        """Should accept integer prices as valid."""
        assert crawler.validate_price(100) is True
        assert crawler.validate_price(1) is True


class TestPriceChangeCalculation:
    """Tests for absolute price change calculation."""

    def test_positive_change(self, crawler):
        """Should calculate positive change when price increases."""
        change = crawler.calculate_price_change(150.0, 100.0)
        assert change == 50.0

    def test_negative_change(self, crawler):
        """Should calculate negative change when price decreases."""
        change = crawler.calculate_price_change(80.0, 100.0)
        assert change == -20.0

    def test_no_change(self, crawler):
        """Should return 0 when price is unchanged."""
        change = crawler.calculate_price_change(100.0, 100.0)
        assert change == 0.0

    def test_rounds_to_2_decimals(self, crawler):
        """Should round price change to 2 decimal places."""
        change = crawler.calculate_price_change(100.333, 100.0)
        assert change == 0.33


class TestPercentageChangeCalculation:
    """Tests for percentage change calculation."""

    def test_positive_percentage(self, crawler):
        """Should calculate positive percentage for price increase."""
        pct = crawler.calculate_percentage_change(110.0, 100.0)
        assert pct == 10.0

    def test_negative_percentage(self, crawler):
        """Should calculate negative percentage for price decrease."""
        pct = crawler.calculate_percentage_change(90.0, 100.0)
        assert pct == -10.0

    def test_no_change_percentage(self, crawler):
        """Should return 0.0 when price is unchanged."""
        pct = crawler.calculate_percentage_change(100.0, 100.0)
        assert pct == 0.0

    def test_rounds_to_2_decimals(self, crawler):
        """Should round percentage to 2 decimal places."""
        # ((105.5 - 100.0) / 100.0) * 100 = 5.5
        pct = crawler.calculate_percentage_change(105.5, 100.0)
        assert pct == 5.5

        # ((1.0 - 3.0) / 3.0) * 100 = -66.666...
        pct = crawler.calculate_percentage_change(1.0, 3.0)
        assert pct == -66.67

    def test_formula_correctness(self, crawler):
        """Should use formula: ((current - previous) / previous) * 100."""
        current = 150.0
        previous = 120.0
        expected = round(((current - previous) / previous) * 100, 2)
        assert crawler.calculate_percentage_change(current, previous) == expected

    def test_handles_zero_previous_price(self, crawler):
        """Should return 0.0 when previous price is zero (avoid division by zero)."""
        pct = crawler.calculate_percentage_change(100.0, 0.0)
        assert pct == 0.0


class TestKeywordMatching:
    """Tests for case-insensitive keyword matching in symbol/company name."""

    def test_matches_keyword_in_symbol(self, crawler):
        """Should match keyword as substring in stock symbol."""
        assert crawler.matches_keyword("aapl", "AAPL", "Apple Inc.") is True

    def test_matches_keyword_in_company_name(self, crawler):
        """Should match keyword as substring in company name."""
        assert crawler.matches_keyword("apple", "AAPL", "Apple Inc.") is True

    def test_case_insensitive_matching(self, crawler):
        """Should match regardless of case."""
        assert crawler.matches_keyword("APPLE", "AAPL", "Apple Inc.") is True
        assert crawler.matches_keyword("Apple", "aapl", "apple inc.") is True

    def test_substring_matching(self, crawler):
        """Should match keyword as substring within symbol or name."""
        assert crawler.matches_keyword("micro", "MSFT", "Microsoft Corporation") is True
        assert crawler.matches_keyword("vid", "NVDA", "NVIDIA Corporation") is True

    def test_no_match_when_keyword_absent(self, crawler):
        """Should return False when keyword is not in symbol or name."""
        assert crawler.matches_keyword("tesla", "AAPL", "Apple Inc.") is False


class TestPriceParsing:
    """Tests for price string parsing."""

    def test_parses_dollar_sign_price(self, crawler):
        """Should parse prices with dollar sign."""
        assert crawler._parse_price("$185.50") == 185.50

    def test_parses_plain_number(self, crawler):
        """Should parse plain numeric prices."""
        assert crawler._parse_price("142.75") == 142.75

    def test_parses_comma_separated_price(self, crawler):
        """Should parse prices with comma separators."""
        assert crawler._parse_price("$1,234.56") == 1234.56

    def test_parses_euro_price(self, crawler):
        """Should parse prices with euro symbol."""
        assert crawler._parse_price("€100.50") == 100.50

    def test_returns_none_for_invalid_price(self, crawler):
        """Should return None for non-numeric price text."""
        assert crawler._parse_price("N/A") is None
        assert crawler._parse_price("") is None

    def test_returns_none_for_negative_price(self, crawler):
        """Should return None for negative price values."""
        assert crawler._parse_price("-5.00") is None

    def test_returns_none_for_zero_price(self, crawler):
        """Should return None for zero price."""
        assert crawler._parse_price("$0.00") is None


class TestMarketHours:
    """Tests for market hours detection."""

    def test_within_market_hours_weekday(self, crawler):
        """Should return True during market hours on weekday."""
        # Wednesday at 10:00
        dt = datetime(2025, 1, 15, 10, 0, 0)
        assert crawler.is_market_hours(dt) is True

    def test_before_market_open(self, crawler):
        """Should return False before market opens."""
        # Monday at 8:59
        dt = datetime(2025, 1, 13, 8, 59, 0)
        assert crawler.is_market_hours(dt) is False

    def test_at_market_open(self, crawler):
        """Should return True at market open time."""
        # Tuesday at 9:00
        dt = datetime(2025, 1, 14, 9, 0, 0)
        assert crawler.is_market_hours(dt) is True

    def test_at_market_close(self, crawler):
        """Should return False at market close time (16:00 is not included)."""
        # Thursday at 16:00
        dt = datetime(2025, 1, 16, 16, 0, 0)
        assert crawler.is_market_hours(dt) is False

    def test_weekend_saturday(self, crawler):
        """Should return False on Saturday."""
        # Saturday at 10:00
        dt = datetime(2025, 1, 18, 10, 0, 0)
        assert crawler.is_market_hours(dt) is False

    def test_weekend_sunday(self, crawler):
        """Should return False on Sunday."""
        # Sunday at 12:00
        dt = datetime(2025, 1, 19, 12, 0, 0)
        assert crawler.is_market_hours(dt) is False


class TestPreviousPriceTracking:
    """Tests for previous price tracking via cache."""

    def test_get_previous_price_from_cache(self, crawler, mock_cache):
        """Should retrieve previous price from cache."""
        mock_cache.get.return_value = 150.0

        price = crawler._get_previous_price("AAPL")

        assert price == 150.0
        mock_cache.get.assert_called_once_with("stock_price:AAPL")

    def test_get_previous_price_returns_none_when_not_cached(self, crawler, mock_cache):
        """Should return None when no previous price in cache."""
        mock_cache.get.return_value = None

        price = crawler._get_previous_price("NEWSTOCK")

        assert price is None

    def test_store_current_price_in_cache(self, crawler, mock_cache):
        """Should store current price in cache for future lookups."""
        crawler._store_current_price("AAPL", 185.50)

        mock_cache.set.assert_called_once_with("stock_price:AAPL", 185.50)


class TestCrawlStocks:
    """Tests for the main crawl_stocks method."""

    def test_crawl_stocks_matches_and_stores(self, crawler, mock_cache, mock_data_store):
        """Should crawl, match keyword, and store matching stocks."""
        html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
                <tr class="stock-row">
                    <td>GOOGL</td>
                    <td>Alphabet Inc.</td>
                    <td>$142.75</td>
                </tr>
            </table>
        </body></html>
        """

        mock_cache.get.return_value = None  # No previous price

        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', return_value=html):
            results = crawler.crawl_stocks(job)

        # Only AAPL / Apple Inc. should match "apple"
        assert len(results) == 1
        assert results[0].symbol == "AAPL"
        assert results[0].company_name == "Apple Inc."
        assert results[0].current_price == 185.50
        assert results[0].matched_keyword == "apple"
        mock_data_store.store_stock_data.assert_called_once()

    def test_crawl_stocks_calculates_change_with_previous_price(self, crawler, mock_cache, mock_data_store):
        """Should calculate price change when previous price exists."""
        html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
            </table>
        </body></html>
        """

        # Previous price was 180.00
        mock_cache.get.return_value = 180.0

        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', return_value=html):
            results = crawler.crawl_stocks(job)

        assert len(results) == 1
        assert results[0].price_change == 5.50
        # ((185.50 - 180.0) / 180.0) * 100 = 3.055... -> 3.06
        assert results[0].percentage_change == 3.06

    def test_crawl_stocks_zero_change_without_previous_price(self, crawler, mock_cache, mock_data_store):
        """Should set price_change and percentage_change to 0 when no previous price."""
        html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
            </table>
        </body></html>
        """

        mock_cache.get.return_value = None

        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', return_value=html):
            results = crawler.crawl_stocks(job)

        assert len(results) == 1
        assert results[0].price_change == 0.0
        assert results[0].percentage_change == 0.0

    def test_crawl_stocks_handles_fetch_failure(self, crawler, mock_data_store):
        """Should continue to next site when fetch fails."""
        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://bad-site.com", "https://good-site.com"],
            created_at=datetime.utcnow(),
        )

        good_html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
            </table>
        </body></html>
        """

        def fetch_side_effect(url):
            if "bad-site" in url:
                return None
            return good_html

        with patch.object(crawler, '_fetch_page', side_effect=fetch_side_effect):
            results = crawler.crawl_stocks(job)

        assert len(results) == 1

    def test_crawl_stocks_stores_current_price_in_cache(self, crawler, mock_cache, mock_data_store):
        """Should store current price in cache after processing."""
        html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
            </table>
        </body></html>
        """

        mock_cache.get.return_value = None

        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', return_value=html):
            crawler.crawl_stocks(job)

        mock_cache.set.assert_called_once_with("stock_price:AAPL", 185.50)

    def test_crawl_stocks_sets_crawl_timestamp(self, crawler, mock_cache, mock_data_store):
        """Should set crawl_timestamp on all matched stocks."""
        html = """
        <html><body>
            <table>
                <tr class="stock-row">
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>$185.50</td>
                </tr>
            </table>
        </body></html>
        """

        mock_cache.get.return_value = None

        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', return_value=html):
            results = crawler.crawl_stocks(job)

        assert results[0].crawl_timestamp is not None
        assert isinstance(results[0].crawl_timestamp, datetime)

    def test_crawl_stocks_handles_exception_gracefully(self, crawler, mock_data_store):
        """Should handle exceptions from individual sites and continue."""
        job = CrawlerJob(
            job_id="test-job",
            keyword="apple",
            target_sites=["https://stocks.example.com"],
            created_at=datetime.utcnow(),
        )

        with patch.object(crawler, '_fetch_page', side_effect=Exception("Network error")):
            results = crawler.crawl_stocks(job)

        assert results == []


class TestFetchPage:
    """Tests for HTTP page fetching."""

    @patch('src.crawler.stock_crawler.requests.get')
    def test_successful_fetch(self, mock_get, crawler):
        """Should return HTML content on successful response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html>content</html>"
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result == "<html>content</html>"

    @patch('src.crawler.stock_crawler.requests.get')
    def test_4xx_error_returns_none(self, mock_get, crawler):
        """Should return None for 4xx HTTP errors."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.stock_crawler.requests.get')
    def test_5xx_error_returns_none(self, mock_get, crawler):
        """Should return None for 5xx HTTP errors."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.stock_crawler.requests.get')
    def test_timeout_returns_none(self, mock_get, crawler):
        """Should return None on request timeout."""
        import requests
        mock_get.side_effect = requests.Timeout("Timed out")

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.stock_crawler.requests.get')
    def test_connection_error_returns_none(self, mock_get, crawler):
        """Should return None on connection error."""
        import requests
        mock_get.side_effect = requests.ConnectionError("Connection refused")

        result = crawler._fetch_page("https://example.com")

        assert result is None

    @patch('src.crawler.stock_crawler.requests.get')
    def test_uses_configured_timeout(self, mock_get, crawler):
        """Should use the configured request timeout."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html></html>"
        mock_get.return_value = mock_response

        crawler._fetch_page("https://example.com")

        mock_get.assert_called_once_with("https://example.com", timeout=30)


class TestPeriodicCrawling:
    """Tests for start/stop periodic crawling."""

    def test_start_sets_running_flag(self, crawler):
        """Should set is_running to True when started."""
        crawler.start()
        assert crawler.is_running is True
        crawler.stop()

    def test_stop_clears_running_flag(self, crawler):
        """Should set is_running to False when stopped."""
        crawler.start()
        crawler.stop()
        assert crawler.is_running is False

    def test_start_when_already_running(self, crawler):
        """Should not start again if already running."""
        crawler.start()
        crawler.start()  # Should not raise
        assert crawler.is_running is True
        crawler.stop()

    def test_stop_when_not_running(self, crawler):
        """Should handle stop gracefully when not running."""
        crawler.stop()  # Should not raise
        assert crawler.is_running is False


class TestConstants:
    """Tests for module constants."""

    def test_default_crawl_interval_is_15_minutes(self):
        """Default crawl interval should be 15 minutes (900 seconds)."""
        assert DEFAULT_CRAWL_INTERVAL_SECONDS == 900

    def test_market_open_hour(self):
        """Market open hour should be 9."""
        assert MARKET_OPEN_HOUR == 9

    def test_market_close_hour(self):
        """Market close hour should be 16."""
        assert MARKET_CLOSE_HOUR == 16
