# Alarm News System - Source Code Structure

## Directory Overview

This directory contains the source code for the Alarm News System, organized into modular components:

### `/auth` - Authentication Service
Handles user registration, authentication, and subscription management.
- User registration with email validation
- Password generation and bcrypt hashing
- JWT token-based authentication
- Subscription renewal and cancellation
- Rate limiting for authentication attempts

### `/crawler` - Web Crawler
Collects news articles and stock information from websites based on user keywords.
- Keyword retrieval from MongoDB
- News article crawling with BeautifulSoup
- Stock data crawling
- Polite crawling with robots.txt compliance
- Duplicate URL prevention

### `/scheduler` - Notification Scheduler
Evaluates user notification times and publishes events to Kafka.
- User data loading from MongoDB
- Notification time evaluation (1-minute precision)
- Event publishing to Kafka
- Consistent hashing for workload distribution

### `/worker` - Notification Worker
Processes notification events and formats email notifications.
- Event consumption from Kafka
- Distributed locking with Redis
- Data retrieval from MongoDB and Data Store
- Email formatting with HTML templates
- Email publishing to Kafka

### `/email_worker` - Email Delivery Worker
Delivers email notifications via SMTP.
- Email consumption from Kafka
- SMTP connection with TLS
- Retry logic for failed deliveries
- Dead letter queue for failed emails

### `/shared` - Shared Utilities
Common code used across multiple components.
- Data models (dataclasses)
- MongoDB connection manager
- Redis client
- Kafka producer/consumer utilities
- Configuration management
- Logging utilities

## Design Principles

### Abstraction Layers
The system is designed with abstraction layers to support future extensibility:

1. **Caching Layer**: Abstract interface for caching operations
   - Currently implemented with in-memory caching
   - Designed for easy Redis integration later
   - Used for: user data caching, crawled URL tracking

2. **Session Management**: Abstract interface for session storage
   - Currently implemented with JWT tokens
   - Designed for Redis-backed sessions in the future
   - Used for: authentication tokens, rate limiting

3. **Data Store Interface**: Abstract interface for crawled data storage
   - Allows switching between different storage backends
   - Currently can use MongoDB or file-based storage
   - Designed for easy migration to specialized time-series databases

### Redis Extensibility
While Redis is currently used only for distributed locking, the architecture supports easy integration for:
- Session storage (replacing JWT-only approach)
- Caching layer (replacing in-memory caching)
- Rate limiting (currently in-memory, can move to Redis)
- Pub/sub for real-time updates

To integrate Redis for these features:
1. Implement the abstract interfaces in `/shared/cache.py` and `/shared/session.py`
2. Update configuration to enable Redis-backed implementations
3. No changes required to business logic in other modules

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. Run tests:
   ```bash
   pytest
   ```

4. Run individual components:
   ```bash
   python -m src.auth.main
   python -m src.crawler.main
   python -m src.scheduler.main
   python -m src.worker.main
   python -m src.email_worker.main
   ```

## Development Guidelines

- Follow PEP 8 style guidelines
- Write unit tests for all new functionality
- Use type hints for function signatures
- Document public APIs with docstrings
- Keep modules loosely coupled through interfaces
- Use dependency injection for testability
