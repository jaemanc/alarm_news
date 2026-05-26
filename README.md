# Alarm News (알람 뉴스)

A Korean-language alarm news service that crawls news and stock data based on user-defined keywords and sends personalized email notifications at scheduled times.

## Overview

Alarm News is a distributed, event-driven email notification system built with Python. It collects news articles and stock information through web crawling, stores user preferences in MongoDB, and delivers personalized alerts via email through a Kafka message queue.

## Architecture

The system consists of four primary components:

- **Authentication Service** - User registration, login, subscription management
- **Web Crawler** - Crawls news and stock websites based on user keywords
- **Scheduler** - Evaluates notification times and publishes events to Kafka
- **Worker** - Processes notification events, formats emails, publishes to delivery queue
- **Email Delivery Worker** - Sends emails via SMTP with retry logic

## Tech Stack

- **Language**: Python 3.9+
- **Database**: MongoDB (user data, subscriptions)
- **Message Queue**: Apache Kafka (event streaming)
- **Cache/Locking**: Redis (distributed locks, rate limiting, caching)
- **Web Crawling**: BeautifulSoup, Requests
- **Email**: SMTP with TLS (smtplib)
- **Auth**: bcrypt (password hashing), PyJWT (tokens)
- **Deployment**: Docker + Kubernetes (blue/green strategy)

## Project Structure

```
alarm_news/
├── src/
│   ├── auth/           # Authentication and subscription management
│   ├── crawler/        # Web crawling for news and stock data
│   ├── scheduler/      # Notification time evaluation and event publishing
│   ├── worker/         # Notification event processing and email formatting
│   ├── email_worker/   # Email delivery via SMTP
│   └── shared/         # Shared utilities, config, abstractions
│       ├── cache.py    # Cache interface (in-memory / Redis)
│       ├── locking.py  # Distributed lock interface (in-memory / Redis)
│       ├── session.py  # Session management (JWT / Redis)
│       └── config.py   # Configuration from environment variables
├── tests/              # Test suite (pytest)
├── requirements.txt    # Python dependencies
├── pytest.ini          # Pytest configuration
├── setup.py            # Package setup
└── .env.example        # Environment variable template
```

## Getting Started

### Prerequisites

- Python 3.9+ (3.13.5 recommended)
- MongoDB
- Apache Kafka
- Redis

### Installation

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and fill in your configuration values:

```bash
cp .env.example .env
```

### Running Tests

```bash
pytest
```

## Design Principles

- **Extensibility**: Abstract interfaces (Protocol/ABC) for caching, locking, and sessions allow swapping implementations (e.g., in-memory for dev, Redis for production)
- **Exactly-once delivery**: Distributed locks + manual Kafka offset commits prevent duplicate notifications
- **Graceful degradation**: Components continue with cached data when dependencies are temporarily unavailable
- **Zero-downtime deployments**: Blue/green strategy with graceful shutdown handling

## License

MIT
