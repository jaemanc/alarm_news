"""
Email formatter for the Worker component.

Formats email notifications with news articles and stock information
based on crawled data matching user keywords.

Responsibilities:
- Format subject line with date and keywords
- Create HTML body with greeting, news section, stock section, footer
- Limit to 10 news articles and 10 stock items (most recent by crawl_timestamp)
- Include unsubscribe instructions in footer
- Add timestamp in ISO 8601 format
"""
import logging
from datetime import datetime
from typing import List

from src.shared.models import EmailNotification, NewsArticle, StockData
from src.worker.data_retriever import CrawledData, UserInfo

logger = logging.getLogger(__name__)

# Constants
MAX_NEWS_ARTICLES = 10
MAX_STOCK_ITEMS = 10


class EmailFormatter:
    """
    Formats email notifications with news and stock data.

    Takes user information and crawled data, produces a formatted
    EmailNotification ready for delivery via the email publisher.
    """

    def format_email(self, user_info: UserInfo, data: CrawledData) -> EmailNotification:
        """
        Format a complete email notification with news and stock data.

        Args:
            user_info: User information (email, keywords).
            data: Crawled data containing news articles and stock data.

        Returns:
            EmailNotification ready for publishing to Kafka.
        """
        now = datetime.utcnow()
        subject = self.create_subject(now, user_info.keywords)
        body_html = self.create_body(data.news_articles, data.stock_data)

        return EmailNotification(
            to_email=user_info.email,
            subject=subject,
            body_html=body_html,
            timestamp=now,
        )

    def create_subject(self, date: datetime, keywords: List[str]) -> str:
        """
        Create subject line with date and keywords.

        Format: "Alarm News - {YYYY-MM-DD} - {keyword1, keyword2, ...}"

        Args:
            date: The notification date.
            keywords: User's keyword list.

        Returns:
            Formatted subject line string.
        """
        date_str = date.strftime("%Y-%m-%d")
        keywords_str = ", ".join(keywords)
        return f"Alarm News - {date_str} - {keywords_str}"

    def create_body(self, news_articles: List[NewsArticle], stock_data: List[StockData]) -> str:
        """
        Create HTML email body with greeting, news section, stock section, and footer.

        Limits to 10 news articles and 10 stock items (most recent by crawl_timestamp).

        Args:
            news_articles: List of crawled news articles.
            stock_data: List of crawled stock data.

        Returns:
            HTML-formatted email body string.
        """
        limited_news = self.limit_items(news_articles, MAX_NEWS_ARTICLES)
        limited_stocks = self.limit_items(stock_data, MAX_STOCK_ITEMS)

        now = datetime.utcnow()
        timestamp_iso = now.isoformat() + "Z"

        greeting = self._build_greeting()
        news_section = self._build_news_section(limited_news)
        stock_section = self._build_stock_section(limited_stocks)
        footer = self._build_footer(timestamp_iso)

        html = f"""<html>
<head><meta charset="utf-8"></head>
<body>
{greeting}
{news_section}
{stock_section}
{footer}
</body>
</html>"""
        return html

    def limit_items(self, items: List, max_count: int = 10) -> List:
        """
        Limit items to max_count, selecting the most recent by crawl_timestamp.

        Args:
            items: List of NewsArticle or StockData objects.
            max_count: Maximum number of items to return.

        Returns:
            List limited to max_count most recent items.
        """
        if len(items) <= max_count:
            return items

        # Sort by crawl_timestamp descending (most recent first)
        sorted_items = sorted(
            items,
            key=lambda x: x.crawl_timestamp if x.crawl_timestamp else datetime.min,
            reverse=True,
        )
        return sorted_items[:max_count]

    def _build_greeting(self) -> str:
        """Build the greeting section of the email."""
        return "<h1>Alarm News Digest</h1>\n<p>Here is your latest news and stock update.</p>"

    def _build_news_section(self, articles: List[NewsArticle]) -> str:
        """
        Build the news articles section of the email.

        Each article includes title, summary, and URL.
        """
        if not articles:
            return "<h2>News</h2>\n<p>No news articles found for your keywords.</p>"

        items_html = ""
        for article in articles:
            items_html += (
                f'<li><strong><a href="{article.url}">{article.title}</a></strong>'
                f"<br>{article.summary}</li>\n"
            )

        return f"<h2>News</h2>\n<ul>\n{items_html}</ul>"

    def _build_stock_section(self, stocks: List[StockData]) -> str:
        """
        Build the stock information section of the email.

        Each stock includes symbol, company name, price (2 decimals),
        and percentage change with +/- sign.
        """
        if not stocks:
            return "<h2>Stocks</h2>\n<p>No stock data found for your keywords.</p>"

        rows_html = ""
        for stock in stocks:
            price_str = f"{stock.current_price:.2f}"
            pct_change = stock.percentage_change
            if pct_change >= 0:
                pct_str = f"+{pct_change:.2f}%"
            else:
                pct_str = f"{pct_change:.2f}%"

            rows_html += (
                f"<tr>"
                f"<td>{stock.symbol}</td>"
                f"<td>{stock.company_name}</td>"
                f"<td>{price_str}</td>"
                f"<td>{pct_str}</td>"
                f"</tr>\n"
            )

        return (
            "<h2>Stocks</h2>\n"
            "<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"
            "<tr><th>Symbol</th><th>Company</th><th>Price</th><th>Change</th></tr>\n"
            f"{rows_html}"
            "</table>"
        )

    def _build_footer(self, timestamp_iso: str) -> str:
        """
        Build the footer section with unsubscribe instructions and timestamp.

        Args:
            timestamp_iso: ISO 8601 formatted timestamp string.
        """
        return (
            "<hr>\n"
            "<footer>\n"
            f"<p><small>Generated at: {timestamp_iso}</small></p>\n"
            "<p><small>To unsubscribe, log in to your account and cancel your subscription, "
            "or contact support to remove your account.</small></p>\n"
            "</footer>"
        )
