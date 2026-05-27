# Implementation Plan: Alarm News System

## Overview

This implementation plan breaks down the Alarm News system into discrete coding tasks. The system is a distributed, event-driven email notification service built with Python that uses web crawling to collect news and stock information, stores user data in MongoDB, and delivers personalized alerts through email. The implementation follows a bottom-up approach: infrastructure setup, authentication, web crawler, scheduler, workers, and deployment configuration.

## Tasks

- [x] 1. Set up project structure and dependencies
  - Create Python project with virtual environment (Python 3.9+)
  - Install dependencies: pymongo, kafka-python, redis, bcrypt, beautifulsoup4, requests, pytest, python-dotenv, pyjwt, smtplib
  - Configure project structure: src/auth, src/crawler, src/scheduler, src/worker, src/email_worker, src/shared, tests/
  - Set up pytest for testing framework
  - Create requirements.txt with all dependencies
  - _Requirements: 15.2, 15.9_

- [x] 2. Implement shared data models and MongoDB schemas
  - [x] 2.1 Create Python dataclasses for core domain models
    - Define User dataclass with user_id, hashed_password, email, keywords, notification_times, subscription_expiry
    - Define NotificationTime dataclass with hour and minute
    - Define NotificationEvent dataclass with event_id, user_id, notification_timestamp
    - Define NewsArticle dataclass with article_id, title, summary, url, published_date, source, matched_keyword, crawl_timestamp
    - Define StockData dataclass with stock_id, symbol, company_name, current_price, price_change, percentage_change, last_update, matched_keyword, crawl_timestamp
    - Define EmailNotification dataclass with to_email, subject, body_html, timestamp
    - _Requirements: 1.9, 1.11, 5.5, 6.5, 7.3_
  
  - [x] 2.2 Create MongoDB connection manager
    - Implement connection with connection pooling (min 10, max 100)
    - Configure write concern "majority" and read preference "primary"
    - Add retry logic: 3 attempts with 5-second intervals for write operations
    - Create database "alarm_news" and collection "users"
    - Create unique index on user_id field
    - Create index on subscription_expiry field
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.6, 16.7, 16.8, 16.9_

- [x] 3. Implement authentication service
  - [x] 3.1 Create user registration handler
    - Implement validate_email using regex pattern ^[^@]+@[^@]+$
    - Implement validate_keywords: at least 1, each 1-100 chars, max 20 keywords
    - Implement generate_password: 12+ chars with uppercase, lowercase, numbers, special chars using secrets module
    - Implement hash_password using bcrypt with cost factor 12
    - Generate unique user_id using UUID4
    - Set subscription_expiry to 30 days from registration
    - Store user document in MongoDB
    - Return user_id, password, subscription_expiry on success
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.11_
  
  - [x]* 3.2 Write unit tests for user registration
    - Test email validation with valid and invalid formats
    - Test keyword validation with edge cases
    - Test password generation meets requirements
    - Test bcrypt hashing
    - _Requirements: 1.1, 1.2, 1.3, 1.6, 1.7_
  
  - [x] 3.3 Create authentication handler
    - Implement authenticate: retrieve user from MongoDB by user_id
    - Implement verify_password using bcrypt.checkpw()
    - Check subscription_expiry > current timestamp
    - Generate JWT token with HS256 algorithm, 24-hour expiry
    - Implement rate_limit_check using Redis: 5 attempts per 15 minutes
    - Block user_id for 15 minutes after limit exceeded
    - Return generic "invalid credentials" error for security
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10_
  
  - [x]* 3.4 Write unit tests for authentication
    - Test password verification with correct and incorrect passwords
    - Test subscription expiry checking
    - Test JWT token generation and expiry
    - Test rate limiting logic
    - _Requirements: 2.3, 2.4, 2.5, 2.7, 2.9_
  
  - [x] 3.5 Create subscription manager
    - Implement renew_subscription: calculate new expiry as max(current_expiry, now) + 30 days
    - Allow renewal up to 7 days before expiry
    - Update subscription_expiry in MongoDB
    - Implement cancel_subscription: delete user document from MongoDB
    - Invalidate tokens by storing in Redis with TTL
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7_
  
  - [x]* 3.6 Write unit tests for subscription management
    - Test renewal calculation with expired and active subscriptions
    - Test early renewal (7 days before expiry)
    - Test cancellation deletes user data
    - _Requirements: 3.2, 3.3, 3.8, 4.2_

- [x] 4. Implement Redis client and distributed locking
  - [x] 4.1 Create Redis connection manager
    - Implement connection with configurable host, port, password from environment variables
    - Add retry logic: 10 attempts with 10-second intervals on startup
    - Add connection health check method
    - _Requirements: 13.1, 13.2_
  
  - [x] 4.2 Implement distributed lock manager
    - Implement acquire_lock using Redis SET NX EX with 5-minute TTL
    - Lock key format: lock:event:{event_id}
    - Lock value: worker_id for ownership tracking
    - Implement release_lock using Lua script for atomic check-and-delete
    - Add 10-second timeout for lock acquisition attempts
    - Return False if lock already held
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.9_
  
  - [x]* 4.3 Write unit tests for distributed lock
    - Test lock acquisition with SET NX EX
    - Test lock release with Lua script
    - Test lock timeout behavior
    - Test lock already held scenario
    - _Requirements: 11.1, 11.2, 11.3, 11.7_

- [x] 5. Implement Kafka client and event streaming
  - [x] 5.1 Create Kafka producer for scheduler
    - Configure producer with acks='all', enable_idempotence=True
    - Implement publish_event method with user_id partitioning
    - Add retry logic: 3 attempts with 5-second intervals
    - Set request timeout to 30 seconds
    - _Requirements: 8.4, 8.5, 8.6_
  
  - [x] 5.2 Create Kafka consumer for worker
    - Configure consumer group with enable_auto_commit=False (manual mode)
    - Set session_timeout_ms=30000, max_poll_interval_ms=300000 (5 minutes)
    - Implement consume_events method with event handler callback
    - Implement graceful shutdown: stop consuming on SIGTERM, complete in-flight messages within 60 seconds
    - Commit offsets only after successful email publication to Kafka
    - _Requirements: 9.3, 9.4, 12.4, 12.9, 12.10_
  
  - [x] 5.3 Create Kafka consumer for email delivery worker
    - Configure consumer group with enable_auto_commit=False
    - Implement consume_emails method with email handler callback
    - Commit offsets only after successful email delivery or DLQ storage
    - _Requirements: 10.1, 10.9_
  
  - [x] 5.4 Create Kafka topic initialization script
    - Create notification-events topic: 12 partitions, replication factor 3, min.insync.replicas 2, retention 24 hours
    - Create email-delivery topic: 12 partitions, replication factor 3, min.insync.replicas 2, retention 24 hours
    - Create notification-dlq topic: 3 partitions, replication factor 3, retention 7 days
    - _Requirements: 12.1, 12.2_

- [x] 6. Implement web crawler component
  - [x] 6.1 Create keyword retriever
    - Query MongoDB for all users where subscription_expiry > current_timestamp
    - Extract and deduplicate keywords using aggregation
    - Create crawler jobs for each unique keyword
    - Run every 30 minutes
    - Generate job_id using UUID4
    - _Requirements: 6.2, 6.3, 17.1_
  
  - [x] 6.2 Implement news crawler
    - Crawl configured news websites using BeautifulSoup or Scrapy
    - Match articles using case-insensitive substring in title/content
    - Extract title, summary (500 chars), URL, publication date, source
    - Store in Data Store with matched keyword and crawl timestamp
    - Track crawled URLs to prevent duplicates (TTL: 7 days)
    - Implement polite crawling: 2-second delay using time.sleep(2)
    - Respect robots.txt using urllib.robotparser
    - Use rotating user agents
    - Timeout requests after 30 seconds
    - Log errors for 4xx/5xx responses
    - _Requirements: 6.1, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10, 6.11, 6.12_
  
  - [x]* 6.3 Write unit tests for news crawler
    - Test article extraction from HTML
    - Test keyword matching (case-insensitive)
    - Test duplicate URL detection
    - Test robots.txt compliance
    - _Requirements: 6.4, 6.5, 6.7, 6.10_
  
  - [x] 6.4 Implement stock crawler
    - Crawl stock websites using BeautifulSoup
    - Match stocks using case-insensitive substring in symbol/company name
    - Extract symbol, company name, current price, price change, percentage change
    - Store in Data Store with matched keyword and crawl timestamp
    - Crawl every 15 minutes during market hours
    - Validate prices are positive numbers
    - Track previous prices to calculate changes
    - Calculate percentage: ((current - previous) / previous) * 100
    - Round percentage to 2 decimals
    - Log errors for invalid price data
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8_
  
  - [x]* 6.5 Write unit tests for stock crawler
    - Test stock data extraction from HTML
    - Test price validation (positive numbers)
    - Test percentage change calculation
    - Test rounding to 2 decimals
    - _Requirements: 7.2, 7.5, 7.7, 7.8_
  
  - [x] 6.6 Create data store interface
    - Implement store_news_article method
    - Implement store_stock_data method
    - Implement query_by_keywords method with time range filter
    - Implement is_url_crawled method for duplicate checking
    - _Requirements: 6.6, 7.3, 9.3_

- [x] 7. Implement scheduler component
  - [x] 7.1 Create user loader
    - Load all users from MongoDB where subscription_expiry > current_timestamp on startup
    - Retry MongoDB connection every 10 seconds (max 10 attempts) on startup failure
    - Reload user data every 5 minutes
    - Cache users in memory between reloads
    - Continue with cached data if MongoDB unavailable during reload
    - Log warnings on MongoDB reload failures
    - _Requirements: 8.1, 8.2, 8.6, 8.7, 8.8, 17.1, 17.2_
  
  - [x] 7.2 Implement notification time evaluator
    - Evaluate notification times with 1-minute precision
    - Match current time (hour, minute) against user notification times
    - Distribute users across scheduler instances using consistent hashing based on user_id
    - Skip users with expired subscriptions
    - Generate event_id using UUID4 for idempotency
    - _Requirements: 8.3, 8.4, 8.8, 8.9_
  
  - [x] 7.3 Create event publisher
    - Publish notification events to Kafka with acks='all'
    - Partition by user_id for ordered processing
    - Retry failed publishes up to 3 times with 5-second intervals
    - Log and discard events after retry exhaustion
    - Publish within 10 seconds of time match
    - _Requirements: 8.4, 8.5, 8.6_
  
  - [x]* 7.4 Write unit tests for scheduler
    - Test notification time matching (1-minute precision)
    - Test consistent hashing distribution
    - Test subscription expiry filtering
    - _Requirements: 8.3, 8.8, 8.9_
  
  - [x] 7.5 Create scheduler main loop
    - Initialize MongoDB connection and Kafka producer
    - Load user data on startup
    - Evaluate notification times every minute
    - Reload user data every 5 minutes
    - Publish events to Kafka when times match
    - _Requirements: 8.1, 8.3, 8.4, 8.6_

- [ ] 8. Implement worker component
  - [x] 8.1 Create event consumer
    - Consume events from Kafka consumer group
    - Use manual offset commit mode
    - Handle graceful shutdown on SIGTERM
    - Stop consuming new messages on shutdown signal
    - Complete in-flight notifications within 60 seconds
    - _Requirements: 9.3, 9.4, 12.9, 12.10_
  
  - [x] 8.2 Implement data retriever
    - Retrieve user email and keywords from MongoDB by user_id
    - Skip processing if user not found or subscription expired
    - Query Data Store for crawled data matching user keywords (past 24 hours)
    - Filter data by timestamp range
    - Group data into news articles and stock information
    - Sort by crawl_timestamp descending
    - _Requirements: 9.1, 9.2, 9.3, 9.4_
  
  - [x] 8.3 Create email formatter
    - Format subject line: "Alarm News - {date} - {keywords}"
    - Create HTML body with greeting, news section, stock section, footer
    - Include article title, summary, URL for each news item
    - Include symbol, company name, price (2 decimals), percentage change (+/- sign) for each stock
    - Limit to 10 news articles and 10 stock items (most recent)
    - Include unsubscribe instructions in footer
    - Add timestamp in ISO 8601 format
    - _Requirements: 9.5, 9.6, 9.7, 9.8, 9.12_
  
  - [ ]* 8.4 Write unit tests for email formatter
    - Test subject line formatting
    - Test HTML body structure
    - Test item limiting (10 max)
    - Test price formatting (2 decimals)
    - Test percentage change formatting (+/- sign)
    - _Requirements: 9.5, 9.6, 9.7, 9.8_
  
  - [x] 8.5 Implement email publisher
    - Publish formatted email to Kafka email delivery topic
    - Retry failed publishes up to 3 times with 5-second intervals
    - Store failed emails in dead letter queue after retry exhaustion
    - Include failure reason and attempt count in DLQ messages
    - Publish within 10 seconds
    - Log failures with correlation IDs
    - _Requirements: 9.9, 9.10, 9.11_
  
  - [x] 8.6 Create worker event processor
    - Consume events from Kafka consumer group
    - Acquire distributed lock using event_id
    - Skip processing if lock already held (acknowledge message)
    - Do not acknowledge if lock acquisition times out
    - Retrieve user info and crawled data
    - Format email notification
    - Publish email to Kafka
    - Release lock and commit Kafka offset on success
    - Release lock without commit on failure (allow redelivery)
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 9.1, 9.2, 9.9, 12.3_
  
  - [x] 8.7 Create worker main loop
    - Initialize MongoDB, Redis, and Kafka connections
    - Start consuming events with event processor handler
    - Handle SIGTERM for graceful shutdown
    - _Requirements: 9.4, 12.9, 12.10_

- [ ] 9. Implement email delivery worker component
  - [x] 9.1 Create email consumer
    - Consume email notifications from Kafka email delivery topic
    - Use manual offset commit mode
    - Commit offsets only after successful delivery or DLQ storage
    - _Requirements: 10.1, 10.9_
  
  - [x] 9.2 Implement SMTP client
    - Connect to SMTP server with TLS encryption using smtplib.SMTP_SSL or SMTP with starttls()
    - Set connection timeout to 10 seconds
    - Authenticate with configured SMTP credentials
    - Send HTML-formatted emails with proper MIME encoding using email.mime
    - Retry connection up to 3 times with 30-second intervals on failure
    - Retry email delivery up to 3 times with 30-second intervals
    - Retry on network timeouts and 5xx SMTP errors
    - Do not retry on 4xx errors (except 429)
    - Log SMTP errors with correlation IDs
    - _Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.10_
  
  - [ ]* 9.3 Write unit tests for SMTP client
    - Test TLS connection establishment
    - Test authentication
    - Test HTML email formatting
    - Test retry logic for network errors
    - _Requirements: 10.2, 10.3, 10.6, 10.7_
  
  - [x] 9.4 Implement email delivery handler
    - Consume email from Kafka
    - Connect to SMTP server
    - Send email to user address
    - Store failed emails in dead letter queue after retry exhaustion
    - Acknowledge Kafka message on success
    - _Requirements: 10.1, 10.2, 10.6, 10.8, 10.9_
  
  - [x] 9.5 Create email delivery worker main loop
    - Initialize Kafka consumer and SMTP connection
    - Start consuming emails with delivery handler
    - Handle graceful shutdown
    - _Requirements: 10.1, 10.9_

- [ ] 10. Implement notification time and keyword management API
  - [x] 10.1 Create REST API endpoint for configuring notification times
    - Validate hour (0-23) and minute (0-59)
    - Update notification_times in MongoDB user record
    - Allow up to 5 notification times per user
    - Return error for invalid time format or MongoDB failure
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_
  
  - [x] 10.2 Create REST API endpoint for updating keywords
    - Validate at least 1 keyword, each 1-100 chars
    - Allow up to 20 keywords per user
    - Update keywords in MongoDB user record
    - Return error for invalid keywords or MongoDB failure
    - _Requirements: 5.7, 5.8, 5.9_
  
  - [ ]* 10.3 Write unit tests for API endpoints
    - Test notification time validation
    - Test keyword validation
    - Test MongoDB update operations
    - _Requirements: 5.1, 5.2, 5.7, 5.8_

- [ ] 11. Implement subscription expiry cleanup job
  - [x] 11.1 Create cleanup job
    - Run daily at midnight UTC
    - Query MongoDB for users where subscription_expiry is more than 90 days in the past
    - Delete user records that have been expired for more than 90 days
    - Log the number of deleted user records
    - _Requirements: 17.3, 17.4, 17.5, 17.6_
  
  - [ ]* 11.2 Write unit tests for cleanup job
    - Test query for expired users (90+ days)
    - Test deletion of expired records
    - Test logging of deletion count
    - _Requirements: 17.4, 17.5, 17.6_

- [ ] 12. Implement health checks and monitoring
  - [x] 12.1 Create health check endpoint
    - Check Kafka connectivity within 5 seconds
    - Check MongoDB connectivity within 5 seconds
    - Check Redis connectivity within 5 seconds
    - Return HTTP 200 with status "healthy" and dependency states if all respond
    - Return HTTP 503 with status "unhealthy" and failed dependency names if any fail
    - Mark unhealthy if worker has not processed any notification for 5+ minutes
    - _Requirements: 13.1, 13.2, 13.4_
  
  - [x] 12.2 Implement metrics collection
    - Collect notification processing latency in milliseconds
    - Collect crawl success rate as percentage (0-100)
    - Collect email delivery success rate as percentage (0-100)
    - Emit metrics to monitoring system every 1 minute
    - _Requirements: 13.3, 13.6, 13.7_
  
  - [x] 12.3 Implement structured logging with correlation IDs
    - Log notification events: received, processed, delivered, failed
    - Include correlation ID for tracing across scheduler, worker, and email worker
    - _Requirements: 13.5_

- [x] 13. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 14. Create Kubernetes deployment manifests
  - [x] 14.1 Create web crawler deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check MongoDB connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.8_
  
  - [x] 14.2 Create scheduler deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check Kafka, MongoDB, Redis connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.8_
  
  - [x] 14.3 Create worker deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check Kafka, MongoDB, Redis connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - Handle SIGTERM: stop consuming, complete in-flight notifications within 60 seconds
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.8, 14.9, 14.10, 14.11_
  
  - [x] 14.4 Create email delivery worker deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check Kafka connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.8_
  
  - [x] 14.5 Create service manifests for all components
    - Define ClusterIP services for internal communication
    - Expose health check endpoints
    - _Requirements: 14.1_
  
  - [x] 14.6 Create ConfigMap for application configuration
    - Define Kafka broker addresses, MongoDB connection string, Redis host/port, SMTP server settings, crawl target websites
    - _Requirements: 14.1_
  
  - [x] 14.7 Create Secret for sensitive credentials
    - Define MongoDB password, Redis password, SMTP credentials, JWT secret
    - _Requirements: 15.5_

- [ ] 15. Create Docker images
  - [x] 15.1 Create Dockerfile for authentication service
    - Use Python 3.9+ base image
    - Copy requirements.txt and install dependencies
    - Copy source code
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9_
  
  - [x] 15.2 Create Dockerfile for web crawler
    - Use Python 3.9+ base image
    - Copy requirements.txt and install dependencies
    - Copy source code
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9_
  
  - [x] 15.3 Create Dockerfile for scheduler
    - Use Python 3.9+ base image
    - Copy requirements.txt and install dependencies
    - Copy source code
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9_
  
  - [x] 15.4 Create Dockerfile for worker
    - Use Python 3.9+ base image
    - Copy requirements.txt and install dependencies
    - Copy source code
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9_
  
  - [x] 15.5 Create Dockerfile for email delivery worker
    - Use Python 3.9+ base image
    - Copy requirements.txt and install dependencies
    - Copy source code
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9_
  
  - [x] 15.6 Create docker-compose.yml for local development
    - Define services: auth, crawler, scheduler, worker, email_worker, Kafka, Zookeeper, MongoDB, Redis
    - Configure service dependencies and networking
    - Mount source code for hot reload during development
    - _Requirements: 15.1_

- [ ] 16. Write integration tests
  - [ ]* 16.1 Write integration test for end-to-end notification flow
    - Start Kafka, MongoDB, and Redis using Testcontainers
    - Register user, configure notification time, crawl data, verify email delivered
    - _Requirements: 1.11, 5.5, 6.6, 9.9, 10.9_
  
  - [ ]* 16.2 Write integration test for duplicate prevention
    - Start two worker instances
    - Publish same event to both workers
    - Verify only one email delivered using distributed locks
    - _Requirements: 11.1, 11.2, 11.3_
  
  - [ ]* 16.3 Write integration test for graceful shutdown
    - Start worker, send SIGTERM during notification processing
    - Verify worker completes in-flight notifications within 60 seconds
    - Verify Kafka offset committed after completion
    - _Requirements: 12.9, 12.10, 12.11_
  
  - [ ]* 16.4 Write integration test for subscription expiry
    - Create user with expired subscription
    - Verify scheduler excludes user from notification scheduling
    - Verify authentication fails for expired user
    - _Requirements: 2.6, 8.8, 17.1, 17.7_
  
  - [ ]* 16.5 Write integration test for web crawler
    - Mock target websites with sample HTML
    - Run crawler with test keywords
    - Verify articles and stocks extracted and stored
    - Verify duplicate URLs not re-crawled
    - _Requirements: 6.4, 6.5, 6.6, 6.7, 7.2, 7.3_

- [x] 17. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- The system uses Python for all components (authentication, crawler, scheduler, worker, email worker)
- MongoDB stores user data with 1-month subscription management
- Web crawling replaces external APIs for data collection
- Email-only notifications (no Slack/Discord/KakaoTalk)
- Property-based testing is not applicable to this infrastructure orchestration system
- Integration tests use Testcontainers for Kafka, MongoDB, and Redis
- Checkpoints ensure incremental validation before proceeding to next phase
- Blue/green deployment testing should be performed in staging environment

## Testing & Development Guidelines

### 테스트 최소화 원칙
- 각 컴포넌트(모듈) 구현 시 개별 단위 테스트를 작성하지 않는다
- 테스트는 작업 단위(wave)의 마지막에 한 번만 통합적으로 검증한다
- 검증 방식: 로직 테스트 대신 **가벼운 스키마/인터페이스 체크**만 수행
  - 함수가 올바른 타입을 반환하는지
  - 필수 필드가 존재하는지
  - 에러 시 올바른 예외 타입이 발생하는지

### API/외부 서비스 테스트 시 더미 데이터 사용
- 개발/테스트 단계에서 외부 API나 서비스 호출 시 **초미니 더미 데이터**를 사용한다
- 더미 데이터 예시: 1~2개의 최소 필드만 포함한 fixture
- 실제 네트워크 호출 없이 mock/stub으로 대체
- 대량 데이터 fixture 금지 (최대 2~3개 항목)

### 적용 범위
- 이 지침은 앞으로 남은 모든 태스크(9.4 이후)에 적용
- 기존에 작성된 테스트는 그대로 유지하되, 새로 작성하는 테스트는 이 원칙을 따른다


## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "4.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "4.2", "5.1"] },
    { "id": 4, "tasks": ["3.4", "3.5", "4.3", "5.2", "5.3", "5.4"] },
    { "id": 5, "tasks": ["3.6", "6.1"] },
    { "id": 6, "tasks": ["6.2", "6.4"] },
    { "id": 7, "tasks": ["6.3", "6.5", "6.6", "7.1"] },
    { "id": 8, "tasks": ["7.2", "7.3"] },
    { "id": 9, "tasks": ["7.4", "7.5", "8.1", "8.2"] },
    { "id": 10, "tasks": ["8.3", "9.1", "9.2"] },
    { "id": 11, "tasks": ["8.4", "8.5", "9.3", "9.4"] },
    { "id": 12, "tasks": ["8.6", "9.5", "10.1", "10.2", "11.1"] },
    { "id": 13, "tasks": ["8.7", "10.3", "11.2", "12.1", "12.2", "12.3"] },
    { "id": 14, "tasks": ["14.1", "14.2", "14.3", "14.4", "14.5", "14.6", "14.7"] },
    { "id": 15, "tasks": ["15.1", "15.2", "15.3", "15.4", "15.5", "15.6"] },
    { "id": 16, "tasks": ["16.1", "16.2", "16.3", "16.4", "16.5"] }
  ]
}
```
