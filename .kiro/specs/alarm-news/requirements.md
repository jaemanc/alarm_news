# Requirements Document

## Introduction

This document specifies the requirements for the Alarm News system, a personalized email notification service that delivers news and stock information based on user-defined keywords. The system uses Python-based web crawling to collect data, stores user information in MongoDB, and sends email notifications at user-specified times through a Kafka message queue. The system is deployed on Kubernetes with Docker containerization using a blue/green deployment strategy for zero-downtime updates.

## Glossary

- **Alarm_News_System**: The complete notification service including user management, web crawler, scheduler, workers, and email delivery
- **User**: A registered subscriber with credentials, email address, keywords, and subscription expiry
- **User_ID**: A unique identifier generated for each user upon registration
- **Hashed_Password**: A cryptographically hashed password stored in MongoDB
- **Subscription**: A time-limited registration (maximum 1 month) that grants access to notification services
- **Subscription_Expiry**: The timestamp when a user's subscription ends
- **Web_Crawler**: A Python-based component that crawls websites to collect news and stock information
- **Crawler_Job**: A scheduled task that crawls specific websites based on user keywords
- **Scheduler**: The component that evaluates user notification times and triggers notification events
- **Worker**: The component that subscribes to Kafka topics, processes crawled data, and sends email notifications
- **Notification_Event**: An event published to Kafka when a user's notification time is reached
- **Notification_Time**: A user-specified time of day when they want to receive email notifications
- **Keyword**: A user-registered search term for filtering news and stock information
- **Email_Notification**: An email message containing news articles and stock information matching user keywords
- **Distributed_Lock**: A mechanism to prevent duplicate notification processing during deployments
- **Consumer_Group**: A Kafka consumer group that ensures each notification event is processed exactly once
- **MongoDB**: A NoSQL database storing user credentials, email addresses, keywords, and subscription data
- **Kafka**: A message queue system for email notification events
- **Blue_Green_Deployment**: A zero-downtime deployment strategy running two production environments
- **Crawled_Data**: News articles and stock information collected by the Web_Crawler
- **Data_Store**: A temporary storage for crawled data before processing

## Requirements

### Requirement 1: User Condition Management

**User Story:** As a user, I want to register and manage notification conditions, so that I receive alerts based on my preferences.

#### Acceptance Criteria

1. WHEN a user registers a new condition, THE Alarm_News_System SHALL check that the condition type is either time-based or threshold-based
2. IF the condition type is time-based, THEN THE Alarm_News_System SHALL verify that the hour is between 0 and 23 and the minute is between 0 and 59
3. IF the condition type is threshold-based, THEN THE Alarm_News_System SHALL verify that the asset identifier is non-empty, the threshold value is a positive number, and the comparison operator is either "above" or "below"
4. IF any validation fails, THEN THE Alarm_News_System SHALL reject the registration and return an error message indicating which parameter is invalid
5. WHEN all validations pass, THE Alarm_News_System SHALL store the condition in User_Settings in Redis
6. IF the Redis write operation fails, THEN THE Alarm_News_System SHALL return an error message indicating storage failure
7. WHEN a user updates an existing condition, THE Scheduler SHALL detect the change within 1 minute by reloading User_Settings from Redis
8. WHEN a user deletes a condition, THE Scheduler SHALL detect the deletion within 1 minute by reloading User_Settings from Redis
9. THE Alarm_News_System SHALL limit each user to a maximum of 100 active conditions

### Requirement 2: News Keyword Monitoring

**User Story:** As a user, I want to receive notifications when news articles containing my registered keywords are published, so that I stay informed about topics I care about.

#### Acceptance Criteria

1. WHEN a user registers a Keyword_Subscription, THE Alarm_News_System SHALL verify that the keyword is between 1 and 100 characters in length
2. IF the keyword length is invalid, THEN THE Alarm_News_System SHALL reject the registration and return an error message
3. WHEN the keyword is valid, THE Alarm_News_System SHALL store it in User_Settings
4. THE Scheduler SHALL poll the External_API for news articles at intervals not exceeding 5 minutes
5. WHEN a new article is retrieved, THE Scheduler SHALL check if the article title or body contains any registered Keyword_Subscription using case-insensitive substring matching
6. WHEN an article matches a Keyword_Subscription, THE Scheduler SHALL publish a Notification_Event to Kafka within 10 seconds
7. THE Worker SHALL fetch the full article details from the External_API or API_Cache
8. THE Worker SHALL format the notification with article title, summary, URL, and matched keywords
9. THE Alarm_News_System SHALL track article identifiers in Redis to prevent duplicate notifications for the same article
10. IF the External_API for news articles fails to respond within 30 seconds or returns an error, THEN THE Scheduler SHALL log the error and retry on the next polling interval

### Requirement 3: Stock Price Monitoring

**User Story:** As a user, I want to receive notifications when stock or cryptocurrency prices cross my defined thresholds, so that I can react to market changes.

#### Acceptance Criteria

1. WHEN a user registers a Price_Threshold, THE Alarm_News_System SHALL verify that the threshold value is between 0.01 and 999,999,999.99
2. IF the threshold value is outside this range, THEN THE Alarm_News_System SHALL reject the registration and return an error message
3. WHEN the threshold value is valid, THE Alarm_News_System SHALL store the Price_Threshold with the asset identifier, threshold value, and comparison operator (above/below)
4. THE Scheduler SHALL poll the External_API for price data at intervals not exceeding 1 minute
5. WHEN the Scheduler retrieves a new price, THE Scheduler SHALL compare it to the previous price retrieved for the same asset
6. IF the comparison operator is "above" and the previous price was at or below the threshold and the new price is above the threshold, THEN THE Scheduler SHALL publish a Notification_Event to Kafka within 10 seconds
7. IF the comparison operator is "below" and the previous price was at or above the threshold and the new price is below the threshold, THEN THE Scheduler SHALL publish a Notification_Event to Kafka within 10 seconds
8. IF the External_API for price data fails to respond within 30 seconds or returns an error, THEN THE Scheduler SHALL retry up to 3 times with 10-second intervals
9. IF all retries fail, THEN THE Scheduler SHALL log the error and continue with the next polling interval
10. WHEN the Worker receives a Notification_Event for a Price_Threshold, THE Worker SHALL fetch current price details from the API_Cache if the cached data is less than 30 seconds old, otherwise from the External_API
11. IF the Worker fails to publish the Notification_Event to Kafka after 3 retry attempts with 5-second intervals, THEN THE Scheduler SHALL log the error and discard the event
12. THE Worker SHALL format the notification with asset name, current price rounded to 2 decimal places, threshold value, and percentage change rounded to 2 decimal places
13. WHEN a Price_Threshold is triggered, THE Alarm_News_System SHALL record the trigger timestamp in Redis and prevent re-triggering the same threshold for the same user and asset for 10 minutes

### Requirement 4: Combined News and Stock Alerts

**User Story:** As a user, I want to receive combined notifications when news about a specific stock is published along with the current stock price, so that I can make informed decisions.

#### Acceptance Criteria

1. WHEN a user registers a combined alert condition, THE Alarm_News_System SHALL verify that the stock identifier is non-empty and matches a supported asset in the External_API
2. IF the stock identifier is invalid, THEN THE Alarm_News_System SHALL reject the registration and return an error message
3. WHEN the stock identifier is valid, THE Alarm_News_System SHALL store both the Keyword_Subscription and associated stock identifier
4. WHEN a news article matches a Keyword_Subscription in a combined alert, THE Scheduler SHALL publish a Notification_Event to Kafka with both news article identifier and stock identifier within 10 seconds
5. WHEN the Worker receives a Combined_Alert Notification_Event, THE Worker SHALL fetch both news article details and current stock price from External_API or API_Cache
6. THE Worker SHALL truncate the article summary to a maximum of 500 characters if it exceeds this length
7. THE Worker SHALL calculate the price change percentage as ((current price - previous trading day's closing price) / previous trading day's closing price) * 100, rounded to 2 decimal places
8. THE Worker SHALL format the Combined_Alert with article title, summary (up to 500 characters), URL, stock name, current price, and price change percentage
9. THE Alarm_News_System SHALL capture the stock price within 1 minute of detecting the matching news article
10. IF both the External_API and API_Cache fail to provide either the news article or stock price, THEN THE Worker SHALL log the error and store the notification in a dead letter queue

### Requirement 5: External API Caching

**User Story:** As a system operator, I want to cache external API responses, so that I reduce API calls and respect rate limits.

#### Acceptance Criteria

1. WHEN the Worker requests data from an External_API, THE Alarm_News_System SHALL check the API_Cache in Redis first using the External_API endpoint and parameters as the cache key
2. IF the requested data exists in API_Cache and is less than 10 minutes old, THEN THE Alarm_News_System SHALL return the cached data without calling the External_API
3. IF the requested data does not exist in API_Cache or is 10 minutes or older, THEN THE Alarm_News_System SHALL call the External_API
4. WHEN the Worker successfully fetches data from an External_API, THE Alarm_News_System SHALL store the response in API_Cache with a 10-minute TTL
5. WHEN an External_API call fails and stale cache data exists (older than 10 minutes but less than 60 minutes), THE Alarm_News_System SHALL return the stale cache data and log a warning
6. WHEN an External_API call fails and no cache data exists or cache data is 60 minutes or older, THE Alarm_News_System SHALL return an error to the caller
7. THE Alarm_News_System SHALL use the External_API endpoint URL and query parameters as the cache key
8. THE Alarm_News_System SHALL NOT cache External_API responses that return error status codes (4xx or 5xx)

### Requirement 6: External API Rate Limit Handling

**User Story:** As a system operator, I want the system to handle external API rate limits gracefully, so that the service remains stable and compliant with API provider terms.

#### Acceptance Criteria

1. WHEN an External_API returns a rate limit error (HTTP 429), THE Alarm_News_System SHALL log the error with the API endpoint and timestamp
2. THE Alarm_News_System SHALL retry the request with exponential backoff starting at 1 second, doubling each retry, up to a maximum of 60 seconds, for up to 5 retry attempts
3. IF the External_API response includes a Retry-After header, THEN THE Alarm_News_System SHALL wait for the number of seconds specified in the header before retrying
4. IF the External_API response includes an X-RateLimit-Reset header, THEN THE Alarm_News_System SHALL wait until the Unix timestamp specified in the header before retrying
5. THE Alarm_News_System SHALL track API call counts per External_API per minute in Redis using a sliding window
6. WHEN 3 or more rate limit errors (HTTP 429) occur for the same External_API within a 5-minute window, THE Alarm_News_System SHALL send an alert to system operators
7. THE Alarm_News_System SHALL store the Rate_Limit value for each External_API in configuration
8. WHEN API call counts reach or exceed 80% of the configured Rate_Limit for an External_API, THE Alarm_News_System SHALL increase the API_Cache TTL to 15 minutes for that External_API
9. WHEN API call counts drop to 50% or below of the configured Rate_Limit, THE Alarm_News_System SHALL restore the API_Cache TTL to 10 minutes

### Requirement 7: Notification Delivery

**User Story:** As a user, I want to receive notifications through my preferred channel, so that I can see alerts where I'm most active.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL accept configuration for Slack, Discord, and KakaoTalk as Notification_Channels
2. WHEN a user registers a notification preference, THE Alarm_News_System SHALL validate the Notification_Channel type and authentication credentials
3. IF the Notification_Channel type is not Slack, Discord, or KakaoTalk, THEN THE Alarm_News_System SHALL reject the registration with an error message indicating unsupported channel
4. IF the authentication credentials are invalid or unreachable during registration, THEN THE Alarm_News_System SHALL reject the registration with an error message indicating authentication failure
5. WHEN the authentication credentials are valid, THE Alarm_News_System SHALL store the Notification_Channel and authentication credentials in User_Settings
6. WHEN the Worker processes a Notification_Event, THE Worker SHALL send the formatted message to the user's configured Notification_Channel within 10 seconds
7. IF the formatted message exceeds the Notification_Channel's size limit, THEN THE Worker SHALL truncate the message content while preserving the timestamp and essential alert information
8. IF notification delivery fails due to network timeout, HTTP error status (4xx or 5xx), or authentication rejection, THEN THE Worker SHALL retry up to 3 times with 30-second intervals
9. WHEN all retry attempts fail, THE Worker SHALL log the failure and store the notification in a dead letter queue
10. THE Worker SHALL include a timestamp in ISO 8601 format in each notification showing when the condition was triggered

### Requirement 8: Duplicate Notification Prevention During Deployment

**User Story:** As a system operator, I want to prevent duplicate notifications during blue/green deployments, so that users don't receive the same alert multiple times.

#### Acceptance Criteria

1. WHEN a Worker receives a Notification_Event from Kafka, THE Worker SHALL attempt to acquire a Distributed_Lock in Redis using the event identifier as the lock key with a timeout of 10 seconds
2. IF the Distributed_Lock acquisition times out or fails, THEN THE Worker SHALL NOT acknowledge the Kafka message to allow redelivery
3. IF the Distributed_Lock is already held by another Worker, THEN THE Worker SHALL skip processing the event and acknowledge the Kafka message
4. WHEN the Distributed_Lock is successfully acquired, THE Worker SHALL hold the lock for the duration of notification processing
5. WHEN notification processing completes successfully, THE Worker SHALL release the Distributed_Lock
6. IF notification processing fails, THEN THE Worker SHALL release the Distributed_Lock and NOT acknowledge the Kafka message to allow redelivery
7. THE Distributed_Lock SHALL have a TTL of 5 minutes to prevent deadlocks if a Worker crashes
8. THE Alarm_News_System SHALL configure Kafka Consumer_Groups to ensure each Notification_Event is delivered to only one Worker instance within the same Consumer_Group

### Requirement 9: Notification Loss Prevention During Deployment

**User Story:** As a system operator, I want to ensure no notifications are lost during blue/green deployments, so that users receive all triggered alerts.

#### Acceptance Criteria

1. THE Scheduler SHALL publish Notification_Events to Kafka with persistence enabled (acks=all)
2. THE Alarm_News_System SHALL configure Kafka topics with a replication factor of at least 3 and min.insync.replicas=2
3. THE Worker SHALL use Kafka manual commit mode and commit offsets only after the notification is successfully delivered to the Notification_Channel or all retry attempts are exhausted
4. WHEN a Worker instance is terminated during deployment, THE Kafka Consumer_Group SHALL reassign unprocessed messages to active Workers within 30 seconds
5. WHEN the blue environment is drained during deployment, THE Alarm_News_System SHALL stop accepting new Notification_Events for Workers in the blue environment
6. THE Alarm_News_System SHALL wait up to 60 seconds for all in-flight notifications in the blue environment to complete before terminating Workers
7. IF in-flight notifications do not complete within 60 seconds, THE Alarm_News_System SHALL force terminate the Workers and log the incomplete notifications for manual review

### Requirement 10: Scheduler Condition Evaluation

**User Story:** As a system operator, I want the scheduler to efficiently evaluate user conditions, so that notifications are triggered promptly.

#### Acceptance Criteria

1. WHEN the Scheduler starts, THE Scheduler SHALL load all User_Conditions from Redis
2. IF Redis is unreachable on startup, THEN THE Scheduler SHALL retry the connection every 10 seconds for up to 10 attempts before failing
3. THE Scheduler SHALL evaluate time-based User_Conditions with a precision of 1 minute
4. THE Scheduler SHALL evaluate threshold-based User_Conditions for stock data at intervals of 1 minute
5. THE Scheduler SHALL evaluate threshold-based User_Conditions for news data at intervals of 5 minutes
6. WHEN a User_Condition is satisfied, THE Scheduler SHALL publish a Notification_Event to Kafka within 5 seconds
7. IF the Kafka publish operation fails, THEN THE Scheduler SHALL retry up to 3 times with 5-second intervals before logging the error and discarding the event
8. THE Scheduler SHALL reload User_Settings from Redis every 1 minute to detect changes
9. IF Redis is unreachable during a reload attempt, THEN THE Scheduler SHALL log a warning and continue using the previously loaded User_Settings
10. THE Scheduler SHALL distribute condition evaluation across multiple instances using consistent hashing based on user identifiers

### Requirement 11: System Monitoring and Health Checks

**User Story:** As a system operator, I want to monitor system health and performance, so that I can detect and resolve issues quickly.

#### Acceptance Criteria

1. WHEN a health check request is received, THE Alarm_News_System SHALL return HTTP 200 with status "healthy" and dependency states IF all dependencies (Kafka, Redis) respond successfully to connection attempts within 5 seconds
2. IF any dependency (Kafka, Redis) fails to respond within 5 seconds or returns an error, THEN THE Alarm_News_System SHALL return HTTP 503 with status "unhealthy" and identification of which dependencies failed
3. THE Alarm_News_System SHALL expose metrics for notification processing latency (in milliseconds), API cache hit rate (as percentage 0-100), and notification delivery success rate (as percentage 0-100)
4. WHEN a Worker has not successfully completed processing any notification for more than 5 consecutive minutes, THE Alarm_News_System SHALL mark the health check as unhealthy
5. THE Alarm_News_System SHALL log notification events (received, processed, delivered, failed) with correlation IDs for tracing
6. THE Alarm_News_System SHALL emit metrics to a monitoring system at intervals not exceeding 1 minute

### Requirement 12: Kubernetes Deployment Configuration

**User Story:** As a system operator, I want to deploy the system on Kubernetes with blue/green strategy, so that I can update the service without downtime.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL provide Kubernetes deployment manifests for Scheduler and Worker components
2. THE Alarm_News_System SHALL configure readiness probes with an initial delay of 10 seconds, a timeout of 5 seconds, a failure threshold of 3, and a period of 10 seconds
3. THE readiness probe SHALL check Kafka and Redis connectivity and return success only if both dependencies are reachable
4. THE Alarm_News_System SHALL configure liveness probes with an initial delay of 30 seconds, a timeout of 5 seconds, a failure threshold of 3, and a period of 30 seconds
5. THE liveness probe SHALL verify the application is processing events by checking the health endpoint and return success only if the endpoint returns HTTP 200
6. WHEN a new deployment is initiated, THE Kubernetes cluster SHALL start the green environment and wait for readiness probes to succeed
7. WHEN the green environment is ready, THE Kubernetes cluster SHALL route traffic to the green environment and then terminate the blue environment
8. THE Alarm_News_System SHALL configure graceful shutdown with a termination grace period of at least 60 seconds
9. WHEN a Worker receives a SIGTERM signal, THE Worker SHALL stop consuming new Kafka messages from the Consumer_Group
10. THE Worker SHALL complete processing and delivery of all in-flight notifications before exiting
11. IF in-flight notifications do not complete within the termination grace period, THE Worker SHALL log the incomplete notifications and exit

### Requirement 13: Docker Containerization

**User Story:** As a system operator, I want the system components containerized, so that I can deploy consistently across environments.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL provide Dockerfiles for Scheduler and Worker components that successfully build without errors
2. THE Docker images SHALL include all runtime dependencies specified in the application's dependency manifest (e.g., package.json, requirements.txt, go.mod)
3. THE Docker images SHALL include configuration files required for application startup
4. THE Docker images SHALL create and use a non-root user with UID 1000 for running the application process
5. THE Docker images SHALL expose configuration through environment variables with default values documented in the Dockerfile or README
6. IF a required environment variable is not set at runtime, THEN THE application SHALL log an error message indicating which variable is missing and exit with a non-zero status code
7. THE Docker images SHALL be tagged with semantic version numbers in the format MAJOR.MINOR.PATCH
8. THE Docker images SHALL have a total compressed size not exceeding 500MB per component when measured with "docker images"
