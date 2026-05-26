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

### Requirement 1: User Registration

**User Story:** As a new user, I want to register for the service by providing my email and keywords, so that I can receive personalized notifications.

#### Acceptance Criteria

1. WHEN a user submits a registration request with email address and keywords, THE Alarm_News_System SHALL validate that the email address matches the pattern [local-part]@[domain] where local-part and domain are non-empty
2. IF the email address is invalid, THEN THE Alarm_News_System SHALL reject the registration and return an error message indicating invalid email format
3. THE Alarm_News_System SHALL validate that at least one keyword is provided and each keyword is between 1 and 100 characters in length
4. IF no keywords are provided or any keyword length is invalid, THEN THE Alarm_News_System SHALL reject the registration and return an error message
5. WHEN the email address and keywords are valid, THE Alarm_News_System SHALL generate a unique User_ID
6. THE Alarm_News_System SHALL generate a random password with at least 12 characters including uppercase letters, lowercase letters, numbers, and special characters
7. THE Alarm_News_System SHALL hash the password using bcrypt with a cost factor of at least 12
8. THE Alarm_News_System SHALL set the Subscription_Expiry to exactly 30 days from the registration timestamp
9. THE Alarm_News_System SHALL store the User_ID, Hashed_Password, email address, keywords, and Subscription_Expiry in MongoDB
10. IF the MongoDB write operation fails, THEN THE Alarm_News_System SHALL return an error message indicating storage failure
11. WHEN the registration is successful, THE Alarm_News_System SHALL return the User_ID and generated password to the user
12. THE Alarm_News_System SHALL send a welcome email to the user's email address containing the User_ID, password, and subscription expiry date

### Requirement 2: User Authentication

**User Story:** As a registered user, I want to authenticate with my credentials, so that I can access and manage my subscription.

#### Acceptance Criteria

1. WHEN a user submits an authentication request with User_ID and password, THE Alarm_News_System SHALL retrieve the user record from MongoDB using the User_ID
2. IF no user record is found for the User_ID, THEN THE Alarm_News_System SHALL reject the authentication and return an error message indicating invalid credentials
3. WHEN a user record is found, THE Alarm_News_System SHALL compare the provided password with the Hashed_Password using bcrypt verification
4. IF the password verification fails, THEN THE Alarm_News_System SHALL reject the authentication and return an error message indicating invalid credentials
5. WHEN the password verification succeeds, THE Alarm_News_System SHALL check if the current timestamp is before the Subscription_Expiry
6. IF the current timestamp is after the Subscription_Expiry, THEN THE Alarm_News_System SHALL reject the authentication and return an error message indicating expired subscription
7. WHEN the subscription is valid, THE Alarm_News_System SHALL generate an authentication token with an expiration time of 24 hours
8. THE Alarm_News_System SHALL return the authentication token to the user
9. THE Alarm_News_System SHALL limit authentication attempts to 5 failed attempts per User_ID within a 15-minute window
10. IF the limit is exceeded, THEN THE Alarm_News_System SHALL temporarily block authentication attempts for that User_ID for 15 minutes

### Requirement 3: Subscription Renewal

**User Story:** As a user with an expiring subscription, I want to renew my subscription, so that I can continue receiving notifications.

#### Acceptance Criteria

1. WHEN a user submits a renewal request with valid authentication, THE Alarm_News_System SHALL retrieve the user record from MongoDB
2. THE Alarm_News_System SHALL calculate the new Subscription_Expiry as 30 days from the current Subscription_Expiry if the subscription has not yet expired
3. IF the subscription has already expired, THEN THE Alarm_News_System SHALL calculate the new Subscription_Expiry as 30 days from the current timestamp
4. THE Alarm_News_System SHALL update the Subscription_Expiry in MongoDB
5. IF the MongoDB update operation fails, THEN THE Alarm_News_System SHALL return an error message indicating renewal failure
6. WHEN the renewal is successful, THE Alarm_News_System SHALL return the new Subscription_Expiry to the user
7. THE Alarm_News_System SHALL send a confirmation email to the user's email address containing the new subscription expiry date
8. THE Alarm_News_System SHALL allow renewal requests up to 7 days before the current Subscription_Expiry

### Requirement 4: Subscription Cancellation

**User Story:** As a user, I want to unsubscribe from the service, so that I stop receiving notifications and my data is removed.

#### Acceptance Criteria

1. WHEN a user submits an unsubscribe request with valid authentication, THE Alarm_News_System SHALL retrieve the user record from MongoDB
2. THE Alarm_News_System SHALL delete the user record from MongoDB including User_ID, Hashed_Password, email address, keywords, and Subscription_Expiry
3. IF the MongoDB delete operation fails, THEN THE Alarm_News_System SHALL return an error message indicating cancellation failure
4. WHEN the deletion is successful, THE Alarm_News_System SHALL return a confirmation message to the user
5. THE Alarm_News_System SHALL send a confirmation email to the user's email address confirming the subscription cancellation
6. THE Alarm_News_System SHALL invalidate all authentication tokens associated with the User_ID
7. WHEN a user attempts to authenticate after cancellation, THE Alarm_News_System SHALL reject the authentication with an error message indicating invalid credentials

### Requirement 5: Notification Time Configuration

**User Story:** As a user, I want to specify when I receive email notifications, so that I get alerts at convenient times.

#### Acceptance Criteria

1. WHEN a user submits a notification time configuration with valid authentication, THE Alarm_News_System SHALL validate that the hour is between 0 and 23 and the minute is between 0 and 59
2. IF the hour or minute is invalid, THEN THE Alarm_News_System SHALL reject the configuration and return an error message indicating invalid time format
3. WHEN the time is valid, THE Alarm_News_System SHALL update the Notification_Time in the user's MongoDB record
4. IF the MongoDB update operation fails, THEN THE Alarm_News_System SHALL return an error message indicating configuration failure
5. WHEN the update is successful, THE Alarm_News_System SHALL return a confirmation message with the configured Notification_Time
6. THE Alarm_News_System SHALL allow users to configure multiple Notification_Times up to a maximum of 5 times per day
7. WHEN a user updates their keywords with valid authentication, THE Alarm_News_System SHALL validate that at least one keyword is provided and each keyword is between 1 and 100 characters in length
8. THE Alarm_News_System SHALL allow users to register up to 20 keywords
9. WHEN the keywords are valid, THE Alarm_News_System SHALL update the keywords in the user's MongoDB record

### Requirement 6: Web Crawling for News Data

**User Story:** As a system operator, I want the system to crawl news websites based on user keywords, so that relevant news articles are collected for notifications.

#### Acceptance Criteria

1. THE Web_Crawler SHALL be implemented in Python using web scraping libraries
2. THE Web_Crawler SHALL retrieve all unique keywords from MongoDB at intervals not exceeding 30 minutes
3. WHEN keywords are retrieved, THE Web_Crawler SHALL create Crawler_Jobs for each unique keyword
4. THE Web_Crawler SHALL crawl configured news websites for articles matching the keywords using case-insensitive substring matching in article titles and content
5. WHEN an article is found matching a keyword, THE Web_Crawler SHALL extract the article title, content summary (up to 500 characters), publication date, and source URL
6. THE Web_Crawler SHALL store the Crawled_Data in the Data_Store with the matched keyword, article details, and crawl timestamp
7. THE Web_Crawler SHALL track crawled article URLs to prevent duplicate storage of the same article
8. IF a website blocks the crawler or returns an error (HTTP 4xx or 5xx), THEN THE Web_Crawler SHALL log the error and skip that website for the current crawl cycle
9. THE Web_Crawler SHALL implement polite crawling with a minimum delay of 2 seconds between requests to the same domain
10. THE Web_Crawler SHALL respect robots.txt directives for each crawled website
11. THE Web_Crawler SHALL use rotating user agents to avoid detection as a bot
12. THE Web_Crawler SHALL timeout requests after 30 seconds and move to the next URL

### Requirement 7: Web Crawling for Stock Data

**User Story:** As a system operator, I want the system to crawl stock information websites based on user keywords, so that relevant stock data is collected for notifications.

#### Acceptance Criteria

1. THE Web_Crawler SHALL crawl configured stock information websites for stock symbols and company names matching user keywords using case-insensitive substring matching
2. WHEN a stock match is found, THE Web_Crawler SHALL extract the stock symbol, company name, current price, price change, percentage change, and last update timestamp
3. THE Web_Crawler SHALL store the stock data in the Data_Store with the matched keyword, stock details, and crawl timestamp
4. THE Web_Crawler SHALL crawl stock data at intervals not exceeding 15 minutes during market hours
5. THE Web_Crawler SHALL validate that extracted price values are positive numbers before storing
6. IF price extraction fails or returns invalid data, THEN THE Web_Crawler SHALL log the error and skip that stock for the current crawl cycle
7. THE Web_Crawler SHALL track the previous price for each stock symbol to calculate price changes
8. THE Web_Crawler SHALL format percentage changes as ((current price - previous price) / previous price) * 100, rounded to 2 decimal places

### Requirement 8: Notification Scheduling

**User Story:** As a system operator, I want the scheduler to trigger notifications at user-specified times, so that users receive timely email alerts.

#### Acceptance Criteria

1. WHEN the Scheduler starts, THE Scheduler SHALL load all active users with valid subscriptions from MongoDB
2. IF MongoDB is unreachable on startup, THEN THE Scheduler SHALL retry the connection every 10 seconds for up to 10 attempts before failing
3. THE Scheduler SHALL evaluate Notification_Times with a precision of 1 minute
4. WHEN the current time matches a user's Notification_Time, THE Scheduler SHALL publish a Notification_Event to Kafka containing the User_ID and notification timestamp within 10 seconds
5. IF the Kafka publish operation fails, THEN THE Scheduler SHALL retry up to 3 times with 5-second intervals before logging the error and discarding the event
6. THE Scheduler SHALL reload user data from MongoDB every 5 minutes to detect new users, updated notification times, and expired subscriptions
7. IF MongoDB is unreachable during a reload attempt, THEN THE Scheduler SHALL log a warning and continue using the previously loaded user data
8. THE Scheduler SHALL skip users whose Subscription_Expiry is before the current timestamp
9. THE Scheduler SHALL distribute notification evaluation across multiple instances using consistent hashing based on User_ID

### Requirement 9: Email Notification Processing

**User Story:** As a user, I want to receive email notifications with relevant news and stock information, so that I stay informed about my interests.

#### Acceptance Criteria

1. WHEN a Worker receives a Notification_Event from Kafka, THE Worker SHALL retrieve the user's email address and keywords from MongoDB using the User_ID
2. IF the user record is not found or the subscription has expired, THEN THE Worker SHALL acknowledge the Kafka message and skip processing
3. THE Worker SHALL query the Data_Store for Crawled_Data matching the user's keywords from the past 24 hours
4. THE Worker SHALL group the Crawled_Data into news articles and stock information
5. THE Worker SHALL format the Email_Notification with a subject line containing the notification date and user keywords
6. THE Email_Notification body SHALL include a greeting, a section for news articles with title, summary, and URL for each article, a section for stock information with symbol, company name, current price, and percentage change for each stock, and a footer with unsubscribe instructions
7. THE Worker SHALL limit the Email_Notification to a maximum of 10 news articles and 10 stock items
8. IF more than 10 items exist in either category, THEN THE Worker SHALL select the most recent items based on crawl timestamp
9. THE Worker SHALL publish the formatted Email_Notification to a Kafka topic for email delivery within 10 seconds
10. IF the Kafka publish operation fails, THEN THE Worker SHALL retry up to 3 times with 5-second intervals
11. WHEN all retry attempts fail, THE Worker SHALL log the failure and store the notification in a dead letter queue
12. THE Worker SHALL include a timestamp in ISO 8601 format in the email showing when the notification was generated

### Requirement 10: Email Delivery

**User Story:** As a user, I want email notifications delivered reliably to my inbox, so that I receive my personalized alerts.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL implement an email delivery worker that consumes Email_Notifications from the Kafka email delivery topic
2. WHEN an Email_Notification is received, THE email delivery worker SHALL connect to the configured SMTP server with TLS encryption
3. IF the SMTP connection fails, THEN THE email delivery worker SHALL retry up to 3 times with 30-second intervals
4. WHEN the SMTP connection is established, THE email delivery worker SHALL authenticate using configured SMTP credentials
5. IF SMTP authentication fails, THEN THE email delivery worker SHALL log the error and store the notification in a dead letter queue
6. WHEN authentication succeeds, THE email delivery worker SHALL send the email to the user's email address
7. IF email delivery fails due to network timeout or SMTP error, THEN THE email delivery worker SHALL retry up to 3 times with 30-second intervals
8. WHEN all retry attempts fail, THE email delivery worker SHALL log the failure and store the notification in a dead letter queue
9. WHEN email delivery succeeds, THE email delivery worker SHALL acknowledge the Kafka message
10. THE email delivery worker SHALL support HTML-formatted email bodies with proper MIME encoding

### Requirement 11: Duplicate Notification Prevention During Deployment

**User Story:** As a system operator, I want to prevent duplicate notifications during blue/green deployments, so that users don't receive the same alert multiple times.

#### Acceptance Criteria

1. WHEN a Worker receives a Notification_Event from Kafka, THE Worker SHALL attempt to acquire a Distributed_Lock using the combination of User_ID and notification timestamp as the lock key with a timeout of 10 seconds
2. IF the Distributed_Lock acquisition times out or fails, THEN THE Worker SHALL NOT acknowledge the Kafka message to allow redelivery
3. IF the Distributed_Lock is already held by another Worker, THEN THE Worker SHALL skip processing the event and acknowledge the Kafka message
4. WHEN the Distributed_Lock is successfully acquired, THE Worker SHALL hold the lock for the duration of notification processing
5. WHEN notification processing completes successfully, THE Worker SHALL release the Distributed_Lock
6. IF notification processing fails, THEN THE Worker SHALL release the Distributed_Lock and NOT acknowledge the Kafka message to allow redelivery
7. THE Distributed_Lock SHALL have a TTL of 5 minutes to prevent deadlocks if a Worker crashes
8. THE Alarm_News_System SHALL configure Kafka Consumer_Groups to ensure each Notification_Event is delivered to only one Worker instance within the same Consumer_Group
9. THE Alarm_News_System SHALL implement the Distributed_Lock using Redis with the SET NX EX command

### Requirement 12: Notification Loss Prevention During Deployment

**User Story:** As a system operator, I want to ensure no notifications are lost during blue/green deployments, so that users receive all triggered alerts.

#### Acceptance Criteria

1. THE Scheduler SHALL publish Notification_Events to Kafka with persistence enabled (acks=all)
2. THE Alarm_News_System SHALL configure Kafka topics with a replication factor of at least 3 and min.insync.replicas=2
3. THE Worker SHALL use Kafka manual commit mode and commit offsets only after the notification is successfully published to the email delivery topic or all retry attempts are exhausted
4. WHEN a Worker instance is terminated during deployment, THE Kafka Consumer_Group SHALL reassign unprocessed messages to active Workers within 30 seconds
5. WHEN the blue environment is drained during deployment, THE Alarm_News_System SHALL stop the Scheduler from publishing new Notification_Events in the blue environment
6. THE Alarm_News_System SHALL wait up to 60 seconds for all in-flight notifications in the blue environment to complete before terminating Workers
7. IF in-flight notifications do not complete within 60 seconds, THE Alarm_News_System SHALL force terminate the Workers and log the incomplete notifications for manual review
8. THE email delivery worker SHALL use Kafka manual commit mode and commit offsets only after the email is successfully sent or stored in the dead letter queue

### Requirement 13: System Monitoring and Health Checks

**User Story:** As a system operator, I want to monitor system health and performance, so that I can detect and resolve issues quickly.

#### Acceptance Criteria

1. WHEN a health check request is received, THE Alarm_News_System SHALL return HTTP 200 with status "healthy" and dependency states IF all dependencies (Kafka, MongoDB, Redis) respond successfully to connection attempts within 5 seconds
2. IF any dependency (Kafka, MongoDB, Redis) fails to respond within 5 seconds or returns an error, THEN THE Alarm_News_System SHALL return HTTP 503 with status "unhealthy" and identification of which dependencies failed
3. THE Alarm_News_System SHALL expose metrics for notification processing latency (in milliseconds), crawl success rate (as percentage 0-100), and email delivery success rate (as percentage 0-100)
4. WHEN a Worker has not successfully completed processing any notification for more than 5 consecutive minutes, THE Alarm_News_System SHALL mark the health check as unhealthy
5. THE Alarm_News_System SHALL log notification events (received, processed, delivered, failed) with correlation IDs for tracing
6. THE Alarm_News_System SHALL emit metrics to a monitoring system at intervals not exceeding 1 minute
7. THE Web_Crawler SHALL expose metrics for crawl attempts, successful crawls, failed crawls, and articles collected per crawl cycle

### Requirement 14: Kubernetes Deployment Configuration

**User Story:** As a system operator, I want to deploy the system on Kubernetes with blue/green strategy, so that I can update the service without downtime.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL provide Kubernetes deployment manifests for Web_Crawler, Scheduler, Worker, and email delivery worker components
2. THE Alarm_News_System SHALL configure readiness probes with an initial delay of 10 seconds, a timeout of 5 seconds, a failure threshold of 3, and a period of 10 seconds
3. THE readiness probe SHALL check Kafka, MongoDB, and Redis connectivity and return success only if all dependencies are reachable
4. THE Alarm_News_System SHALL configure liveness probes with an initial delay of 30 seconds, a timeout of 5 seconds, a failure threshold of 3, and a period of 30 seconds
5. THE liveness probe SHALL verify the application is processing events by checking the health endpoint and return success only if the endpoint returns HTTP 200
6. WHEN a new deployment is initiated, THE Kubernetes cluster SHALL start the green environment and wait for readiness probes to succeed
7. WHEN the green environment is ready, THE Kubernetes cluster SHALL route traffic to the green environment and then terminate the blue environment
8. THE Alarm_News_System SHALL configure graceful shutdown with a termination grace period of at least 60 seconds
9. WHEN a Worker receives a SIGTERM signal, THE Worker SHALL stop consuming new Kafka messages from the Consumer_Group
10. THE Worker SHALL complete processing and delivery of all in-flight notifications before exiting
11. IF in-flight notifications do not complete within the termination grace period, THE Worker SHALL log the incomplete notifications and exit

### Requirement 15: Docker Containerization

**User Story:** As a system operator, I want the system components containerized, so that I can deploy consistently across environments.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL provide Dockerfiles for Web_Crawler, Scheduler, Worker, and email delivery worker components that successfully build without errors
2. THE Docker images SHALL include all Python runtime dependencies specified in requirements.txt
3. THE Docker images SHALL include configuration files required for application startup
4. THE Docker images SHALL create and use a non-root user with UID 1000 for running the application process
5. THE Docker images SHALL expose configuration through environment variables with default values documented in the Dockerfile or README
6. IF a required environment variable is not set at runtime, THEN THE application SHALL log an error message indicating which variable is missing and exit with a non-zero status code
7. THE Docker images SHALL be tagged with semantic version numbers in the format MAJOR.MINOR.PATCH
8. THE Docker images SHALL have a total compressed size not exceeding 500MB per component when measured with "docker images"
9. THE Docker images SHALL include Python 3.9 or higher as the base runtime

### Requirement 16: MongoDB Data Persistence

**User Story:** As a system operator, I want user data persisted reliably in MongoDB, so that subscriptions and preferences are not lost.

#### Acceptance Criteria

1. THE Alarm_News_System SHALL connect to MongoDB using connection pooling with a minimum pool size of 10 and maximum pool size of 100
2. THE Alarm_News_System SHALL create a database named "alarm_news" and a collection named "users"
3. THE users collection SHALL have a unique index on the User_ID field
4. THE users collection SHALL have an index on the Subscription_Expiry field for efficient expiry queries
5. THE Alarm_News_System SHALL store user documents with fields: User_ID (string), Hashed_Password (string), email (string), keywords (array of strings), Notification_Times (array of objects with hour and minute), Subscription_Expiry (timestamp)
6. WHEN a MongoDB write operation fails due to connection error, THE Alarm_News_System SHALL retry up to 3 times with 5-second intervals
7. IF all retry attempts fail, THEN THE Alarm_News_System SHALL return an error to the caller
8. THE Alarm_News_System SHALL configure MongoDB with write concern "majority" for all write operations
9. THE Alarm_News_System SHALL configure MongoDB with read preference "primary" for all read operations

### Requirement 17: Subscription Expiry Management

**User Story:** As a system operator, I want expired subscriptions automatically excluded from processing, so that inactive users don't receive notifications.

#### Acceptance Criteria

1. THE Scheduler SHALL query MongoDB for users where Subscription_Expiry is greater than the current timestamp when loading user data
2. THE Scheduler SHALL exclude users with expired subscriptions from notification scheduling
3. THE Alarm_News_System SHALL implement a cleanup job that runs daily at midnight UTC
4. WHEN the cleanup job runs, THE Alarm_News_System SHALL query MongoDB for users where Subscription_Expiry is more than 90 days in the past
5. THE Alarm_News_System SHALL delete user records that have been expired for more than 90 days
6. THE cleanup job SHALL log the number of deleted user records
7. WHEN a user with an expired subscription attempts to authenticate, THE Alarm_News_System SHALL return an error message indicating expired subscription and instructions for renewal
