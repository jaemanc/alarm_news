# Implementation Plan: Alarm News System

## Overview

This implementation plan breaks down the Alarm News system into discrete coding tasks. The system is a distributed, event-driven notification service built with TypeScript that monitors external APIs and delivers personalized alerts through multiple channels. The implementation follows a bottom-up approach: infrastructure setup, core components, integration, and deployment configuration.

## Tasks

- [ ] 1. Set up project structure and dependencies
  - Create TypeScript project with Node.js runtime
  - Install dependencies: Kafka client (kafkajs), Redis client (ioredis), HTTP client (axios), testing framework (jest), Docker SDK
  - Configure TypeScript compiler options for strict mode
  - Set up ESLint and Prettier for code quality
  - Create directory structure: src/scheduler, src/worker, src/shared, src/config, tests/
  - _Requirements: 13.1, 13.2_

- [ ] 2. Implement shared data models and interfaces
  - [ ] 2.1 Create TypeScript interfaces for user conditions, notification events, and external API responses
    - Define UserCondition, TimeBasedParams, ThresholdBasedParams interfaces
    - Define NotificationEvent, NewsArticle, StockPrice interfaces
    - Define NotificationChannel, KakaoCredentials interfaces
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 3.1, 4.1_
  
  - [ ] 2.2 Create Redis data model utilities
    - Implement key pattern generators for user settings, processed articles, previous prices, threshold triggers, API cache, rate limits, distributed locks
    - Implement serialization/deserialization helpers for Redis data structures
    - _Requirements: 5.7, 8.1_

- [ ] 3. Implement Redis client and connection management
  - [ ] 3.1 Create Redis connection manager with retry logic
    - Implement connection with configurable host, port, password from environment variables
    - Add retry logic: 10 attempts with 10-second intervals on startup
    - Add connection health check method
    - _Requirements: 10.2, 11.2_
  
  - [ ] 3.2 Implement distributed lock manager
    - Implement acquireLock using Redis SET NX EX with 5-minute TTL
    - Implement releaseLock using Lua script for atomic check-and-delete
    - Add 10-second timeout for lock acquisition attempts
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_
  
  - [ ] 3.3 Implement API cache manager
    - Implement cache key generation from endpoint URL and query parameters
    - Implement get method: return cached data if less than 10 minutes old
    - Implement set method: store data with 10-minute TTL
    - Implement stale cache logic: return data 10-60 minutes old if API fails
    - Add dynamic TTL adjustment: increase to 15 minutes at 80% rate limit, restore to 10 minutes at 50%
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.8, 6.8, 6.9_

- [ ] 4. Implement Kafka client and event streaming
  - [ ] 4.1 Create Kafka producer for scheduler
    - Configure producer with acks=all, idempotent writes enabled
    - Implement publishEvent method with userId partitioning
    - Add retry logic: 3 attempts with 5-second intervals
    - Set request timeout to 30 seconds
    - _Requirements: 9.1, 10.6, 10.7_
  
  - [ ] 4.2 Create Kafka consumer for worker
    - Configure consumer group with manual offset commit mode
    - Set session.timeout.ms to 30 seconds, max.poll.interval.ms to 5 minutes
    - Implement consumeEvents method with event handler callback
    - Implement graceful shutdown: stop consuming on SIGTERM, complete in-flight messages
    - Commit offsets only after successful notification delivery
    - _Requirements: 8.8, 9.3, 9.4, 12.9, 12.10_
  
  - [ ] 4.3 Create Kafka topic initialization script
    - Create notification-events topic: 12 partitions, replication factor 3, min.insync.replicas 2, retention 24 hours
    - Create notification-dlq topic: 3 partitions, replication factor 3, retention 7 days
    - _Requirements: 9.2, 7.9_

- [ ] 5. Implement rate limit handler
  - [ ] 5.1 Create rate limit tracker using Redis sliding window
    - Implement trackAPICall using Redis ZSET with Lua script
    - Remove entries older than 1 minute using ZREMRANGEBYSCORE
    - Return current call count within 1-minute window
    - _Requirements: 6.5_
  
  - [ ] 5.2 Implement rate limit error handler
    - Detect HTTP 429 responses and extract Retry-After or X-RateLimit-Reset headers
    - Implement exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s max, up to 5 retries
    - Track rate limit errors in Redis ZSET with 5-minute window
    - Send operator alert when 3+ errors occur within 5 minutes
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.6_
  
  - [ ] 5.3 Implement cache TTL adjustment logic
    - Check if API call count reaches 80% of configured rate limit
    - Increase cache TTL to 15 minutes when threshold reached
    - Restore cache TTL to 10 minutes when usage drops to 50%
    - _Requirements: 6.8, 6.9_

- [ ] 6. Implement external API clients
  - [ ] 6.1 Create base HTTP client with retry and timeout logic
    - Set default timeout to 30 seconds
    - Implement retry logic: 3 attempts with 10-second intervals for 5xx and network errors
    - Skip retry for 4xx errors (except 429)
    - Integrate rate limit handler for 429 responses
    - _Requirements: 2.10, 3.8, 6.1, 6.2, 6.3, 6.4_
  
  - [ ] 6.2 Implement news API client
    - Create pollNews method to fetch articles from external news API
    - Parse response into NewsArticle objects
    - Integrate with API cache manager
    - _Requirements: 2.4, 2.7_
  
  - [ ] 6.3 Implement stock API client
    - Create pollStockPrices method to fetch prices from external stock API
    - Parse response into StockPrice objects with current price and previous close
    - Integrate with API cache manager
    - _Requirements: 3.4, 3.10_
  
  - [ ] 6.4 Implement cryptocurrency API client
    - Create pollCryptoPrices method to fetch prices from external crypto API
    - Parse response into StockPrice objects (reuse interface)
    - Integrate with API cache manager
    - _Requirements: 3.4, 3.10_

- [ ] 7. Implement scheduler component
  - [ ] 7.1 Create user condition loader
    - Implement loadConditions: use Redis SCAN to iterate user settings keys
    - Implement reloadConditions: reload every 1 minute
    - Cache conditions in memory between reload intervals
    - Handle Redis connection failures by continuing with cached conditions
    - _Requirements: 10.1, 10.8, 10.9_
  
  - [ ] 7.2 Implement condition validator
    - Validate time-based conditions: hour 0-23, minute 0-59
    - Validate threshold-based conditions: non-empty assetId, positive threshold value, operator "above" or "below"
    - Validate keyword length: 1-100 characters
    - Validate price threshold range: 0.01 to 999,999,999.99
    - Return specific error messages for each validation failure
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 3.1, 3.2_
  
  - [ ] 7.3 Implement consistent hashing for workload distribution
    - Hash user IDs to distribute conditions across scheduler instances
    - Ensure each user's conditions evaluated by same scheduler instance
    - _Requirements: 10.10_
  
  - [ ] 7.4 Implement condition evaluator for news keywords
    - Match article title and body against keyword subscriptions using case-insensitive substring matching
    - Check Redis SET for processed article IDs (TTL: 7 days)
    - Generate NotificationEvent for matched articles
    - Store article ID in Redis to prevent duplicates
    - _Requirements: 2.5, 2.6, 2.9_
  
  - [ ] 7.5 Implement condition evaluator for stock price thresholds
    - Fetch previous price from Redis HASH
    - Compare current price to threshold with operator (above/below)
    - Trigger only on threshold crossing (previous below/at threshold, current above for "above" operator)
    - Store trigger timestamp in Redis with 10-minute TTL to prevent re-triggering
    - Generate NotificationEvent for satisfied thresholds
    - _Requirements: 3.5, 3.6, 3.7, 3.13_
  
  - [ ] 7.6 Implement condition evaluator for combined alerts
    - Match news articles against keywords for combined alert conditions
    - Include both article ID and stock symbol in NotificationEvent payload
    - Capture stock price within 1 minute of detecting matching article
    - _Requirements: 4.4, 4.9_
  
  - [ ] 7.7 Create scheduler main loop
    - Initialize Redis connection and Kafka producer
    - Load user conditions on startup
    - Run separate polling loops: 1 minute for stock, 5 minutes for news
    - Evaluate conditions and publish events to Kafka
    - Reload user conditions every 1 minute
    - _Requirements: 10.3, 10.4, 10.5, 10.6_

- [ ] 8. Implement worker component
  - [ ] 8.1 Create notification formatter
    - Implement formatNewsNotification: include timestamp (ISO 8601), article title, summary, URL, matched keywords
    - Implement formatStockNotification: include timestamp, asset name, current price (2 decimals), threshold value, percentage change (2 decimals)
    - Implement formatCombinedNotification: include article title, summary (max 500 chars), URL, stock name, current price, price change percentage
    - Implement truncateForChannel: preserve timestamp and alert type, truncate content to channel limits
    - Calculate price change percentage: ((current - previousClose) / previousClose) * 100, rounded to 2 decimals
    - _Requirements: 2.8, 3.12, 4.6, 4.7, 4.8, 7.7_
  
  - [ ] 8.2 Create notification sender for Slack
    - Implement sendToSlack using webhook URL
    - Set HTTP timeout to 10 seconds
    - Retry 3 times with 30-second intervals on network errors and 5xx
    - Do not retry on 4xx (except 429)
    - _Requirements: 7.1, 7.6, 7.8_
  
  - [ ] 8.3 Create notification sender for Discord
    - Implement sendToDiscord using webhook URL
    - Set HTTP timeout to 10 seconds
    - Retry 3 times with 30-second intervals on network errors and 5xx
    - Do not retry on 4xx (except 429)
    - _Requirements: 7.1, 7.6, 7.8_
  
  - [ ] 8.4 Create notification sender for KakaoTalk
    - Implement sendToKakaoTalk using access token and chat ID
    - Set HTTP timeout to 10 seconds
    - Retry 3 times with 30-second intervals on network errors and 5xx
    - Do not retry on 4xx (except 429)
    - _Requirements: 7.1, 7.6, 7.8_
  
  - [ ] 8.5 Implement dead letter queue handler
    - Store failed notifications in notification-dlq Kafka topic
    - Include original event, failure reason, attempt count, timestamps
    - _Requirements: 4.10, 7.9_
  
  - [ ] 8.6 Create worker event processor
    - Consume events from Kafka consumer group
    - Acquire distributed lock using event ID
    - Skip processing if lock already held (acknowledge message)
    - Fetch enriched data from API cache or external APIs
    - Format notification based on event type
    - Send notification to user's configured channel
    - Release lock and commit Kafka offset on success
    - Send to DLQ after retry exhaustion
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 9.3, 2.7, 3.10, 4.5, 7.6_
  
  - [ ] 8.7 Create worker main loop
    - Initialize Redis connection and Kafka consumer
    - Start consuming events with event processor handler
    - Handle SIGTERM for graceful shutdown: stop consuming, complete in-flight notifications within 60 seconds
    - _Requirements: 9.4, 9.6, 12.9, 12.10, 12.11_

- [ ] 9. Implement user condition management API
  - [ ] 9.1 Create REST API endpoint for registering conditions
    - Validate condition parameters using condition validator
    - Store condition in Redis user settings
    - Enforce maximum 100 conditions per user
    - Return error messages for validation failures or storage failures
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.9_
  
  - [ ] 9.2 Create REST API endpoint for updating conditions
    - Validate updated condition parameters
    - Update condition in Redis user settings
    - Scheduler will detect change within 1 minute via reload
    - _Requirements: 1.7_
  
  - [ ] 9.3 Create REST API endpoint for deleting conditions
    - Remove condition from Redis user settings
    - Scheduler will detect deletion within 1 minute via reload
    - _Requirements: 1.8_
  
  - [ ] 9.4 Create REST API endpoint for registering notification channels
    - Validate channel type: Slack, Discord, or KakaoTalk
    - Validate authentication credentials by testing connectivity
    - Store channel configuration in Redis user settings
    - Return error for unsupported channel or authentication failure
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  
  - [ ] 9.5 Create REST API endpoint for registering keyword subscriptions
    - Validate keyword length: 1-100 characters
    - Store keyword in user settings
    - _Requirements: 2.1, 2.2, 2.3_
  
  - [ ] 9.6 Create REST API endpoint for registering price thresholds
    - Validate threshold value: 0.01 to 999,999,999.99
    - Store price threshold with asset ID, value, and operator
    - _Requirements: 3.1, 3.2, 3.3_
  
  - [ ] 9.7 Create REST API endpoint for registering combined alerts
    - Validate stock identifier against external API
    - Store both keyword subscription and associated stock identifier
    - _Requirements: 4.1, 4.2, 4.3_

- [ ] 10. Implement health checks and monitoring
  - [ ] 10.1 Create health check endpoint
    - Check Kafka connectivity within 5 seconds
    - Check Redis connectivity within 5 seconds
    - Return HTTP 200 with status "healthy" if all dependencies respond
    - Return HTTP 503 with status "unhealthy" and failed dependency names if any fail
    - Mark unhealthy if worker has not processed any notification for 5+ minutes
    - _Requirements: 11.1, 11.2, 11.4_
  
  - [ ] 10.2 Implement metrics collection
    - Collect notification processing latency in milliseconds
    - Collect API cache hit rate as percentage (0-100)
    - Collect notification delivery success rate as percentage (0-100)
    - Emit metrics to monitoring system every 1 minute
    - _Requirements: 11.3, 11.6_
  
  - [ ] 10.3 Implement structured logging with correlation IDs
    - Log notification events: received, processed, delivered, failed
    - Include correlation ID for tracing across scheduler and worker
    - _Requirements: 11.5_

- [ ] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 12. Create Kubernetes deployment manifests
  - [ ] 12.1 Create scheduler deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check Kafka and Redis connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.8_
  
  - [ ] 12.2 Create worker deployment manifest
    - Define deployment with replicas, resource limits, environment variables
    - Configure readiness probe: initial delay 10s, timeout 5s, failure threshold 3, period 10s, check Kafka and Redis connectivity
    - Configure liveness probe: initial delay 30s, timeout 5s, failure threshold 3, period 30s, check health endpoint
    - Set termination grace period to 60 seconds
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.8_
  
  - [ ] 12.3 Create service manifests for scheduler and worker
    - Define ClusterIP services for internal communication
    - Expose health check endpoints
    - _Requirements: 12.1_
  
  - [ ] 12.4 Create ConfigMap for application configuration
    - Define Kafka broker addresses, Redis host/port, API endpoints, rate limits
    - _Requirements: 6.7_
  
  - [ ] 12.5 Create Secret for sensitive credentials
    - Define Redis password, external API keys, notification channel webhooks
    - _Requirements: 13.5_

- [ ] 13. Create Docker images
  - [ ] 13.1 Create Dockerfile for scheduler
    - Use Node.js base image
    - Copy package.json and install dependencies
    - Copy source code and compile TypeScript
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.8_
  
  - [ ] 13.2 Create Dockerfile for worker
    - Use Node.js base image
    - Copy package.json and install dependencies
    - Copy source code and compile TypeScript
    - Create non-root user with UID 1000
    - Set environment variables with defaults
    - Document required environment variables in comments
    - Ensure image size under 500MB compressed
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.8_
  
  - [ ] 13.3 Create docker-compose.yml for local development
    - Define services: scheduler, worker, Kafka, Zookeeper, Redis
    - Configure service dependencies and networking
    - Mount source code for hot reload during development
    - _Requirements: 13.1_

- [ ] 14. Write integration tests
  - [ ]* 14.1 Write integration test for end-to-end notification flow
    - Start Kafka and Redis using Testcontainers
    - Register user condition, publish matching data, verify notification delivered
    - _Requirements: 2.6, 3.6, 4.4, 7.6_
  
  - [ ]* 14.2 Write integration test for duplicate prevention
    - Start two worker instances
    - Publish same event to both workers
    - Verify only one notification delivered using distributed locks
    - _Requirements: 8.1, 8.2, 8.3_
  
  - [ ]* 14.3 Write integration test for API cache behavior
    - Mock external API with WireMock
    - Verify cache hit reduces API calls
    - Verify stale cache used when API fails
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_
  
  - [ ]* 14.4 Write integration test for rate limit handling
    - Mock external API returning HTTP 429
    - Verify exponential backoff and Retry-After header handling
    - Verify cache TTL adjustment at 80% rate limit
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.8_
  
  - [ ]* 14.5 Write integration test for dead letter queue
    - Mock notification channel returning errors
    - Verify failed notification sent to DLQ after retry exhaustion
    - _Requirements: 7.8, 7.9_
  
  - [ ]* 14.6 Write integration test for graceful shutdown
    - Start worker, send SIGTERM during notification processing
    - Verify worker completes in-flight notifications within 60 seconds
    - Verify Kafka offset committed after completion
    - _Requirements: 9.4, 9.6, 12.9, 12.10, 12.11_

- [ ] 15. Write unit tests
  - [ ]* 15.1 Write unit tests for condition validator
    - Test valid and invalid time-based parameters
    - Test valid and invalid threshold-based parameters
    - Test keyword length validation
    - Test price threshold range validation
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 3.1_
  
  - [ ]* 15.2 Write unit tests for notification formatter
    - Test news notification formatting with timestamp and keywords
    - Test stock notification formatting with price change calculation
    - Test combined notification formatting with 500-char summary truncation
    - Test channel-specific message truncation
    - _Requirements: 2.8, 3.12, 4.6, 4.7, 4.8, 7.7_
  
  - [ ]* 15.3 Write unit tests for keyword matching
    - Test case-insensitive substring matching
    - Test matching in article title and body
    - _Requirements: 2.5_
  
  - [ ]* 15.4 Write unit tests for threshold comparison
    - Test "above" operator with threshold crossing
    - Test "below" operator with threshold crossing
    - Test no trigger when threshold not crossed
    - _Requirements: 3.5, 3.6, 3.7_
  
  - [ ]* 15.5 Write unit tests for cache key generation
    - Test key generation from endpoint URL and query parameters
    - Test consistent keys for same endpoint and parameters
    - _Requirements: 5.7_
  
  - [ ]* 15.6 Write unit tests for distributed lock logic
    - Test lock acquisition with SET NX EX
    - Test lock release with Lua script
    - Test lock timeout behavior
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [ ] 16. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- The design uses TypeScript, so all implementation will be in TypeScript
- Property-based testing is not applicable to this infrastructure orchestration system (see design document)
- Integration tests use Testcontainers for Kafka and Redis, WireMock for external APIs
- Unit tests focus on business logic: validation, formatting, matching, comparison
- Checkpoints ensure incremental validation before proceeding to next phase
- Blue/green deployment testing should be performed in staging environment

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "4.1", "4.3"] },
    { "id": 4, "tasks": ["4.2", "5.1", "6.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "6.2", "6.3", "6.4", "7.1", "7.2", "7.3"] },
    { "id": 6, "tasks": ["7.4", "7.5", "7.6", "8.1"] },
    { "id": 7, "tasks": ["7.7", "8.2", "8.3", "8.4", "8.5"] },
    { "id": 8, "tasks": ["8.6"] },
    { "id": 9, "tasks": ["8.7", "9.1", "9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "10.1", "10.2", "10.3"] },
    { "id": 10, "tasks": ["12.1", "12.2", "12.3", "12.4", "12.5", "13.1", "13.2", "13.3"] },
    { "id": 11, "tasks": ["14.1", "14.2", "14.3", "14.4", "14.5", "14.6", "15.1", "15.2", "15.3", "15.4", "15.5", "15.6"] }
  ]
}
```
