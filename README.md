# Alarm News (알람 뉴스)

A Korean-language alarm news service that crawls news and stock data based on user-defined keywords and sends personalized email notifications at scheduled times.

## Overview

Alarm News is a distributed, event-driven email notification system built with Python. It collects news articles and stock information through web crawling, stores user preferences in MongoDB, and delivers personalized alerts via email through a Kafka message queue.

## Architecture

The system consists of five primary components connected via Kafka event streaming:

```
┌─────────────────────────────────────────────────────────────┐
│                    Alarm News System                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [사용자]                                                    │
│     │                                                       │
│     ▼                                                       │
│  ┌──────────────┐     ┌──────────────┐                     │
│  │  Auth API    │────▶│   MongoDB    │                     │
│  │  (등록/로그인) │     │  (사용자 DB)  │                     │
│  └──────────────┘     └──────┬───────┘                     │
│                              │                              │
│  ┌──────────────┐            │                              │
│  │  Crawler     │────────────┤                              │
│  │  (뉴스/주식)  │            │                              │
│  └──────┬───────┘     ┌──────▼───────┐                     │
│         │             │    Redis     │                     │
│         ▼             │  (캐시/락)    │                     │
│  ┌──────────────┐     └──────┬───────┘                     │
│  │  Data Store  │            │                              │
│  │  (크롤링 결과) │            │                              │
│  └──────────────┘            │                              │
│                              │                              │
│  ┌──────────────┐     ┌──────▼───────┐                     │
│  │  Scheduler   │────▶│    Kafka     │                     │
│  │  (시간 매칭)  │     │  (이벤트 큐)  │                     │
│  └──────────────┘     └──────┬───────┘                     │
│                              │                              │
│                       ┌──────▼───────┐                     │
│                       │   Worker     │                     │
│                       │ (이메일 조립) │                     │
│                       └──────┬───────┘                     │
│                              │                              │
│                       ┌──────▼───────┐                     │
│                       │ Email Worker │──▶ Resend API       │
│                       │  (이메일 발송) │   (또는 SMTP)       │
│                       └──────────────┘                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Components

| 컴포넌트 | 역할 | 소스 |
|---------|------|------|
| **Auth API** | 회원가입, 로그인, 구독관리, 키워드/시간 설정 | `src/auth/` |
| **Crawler** | 뉴스/주식 웹 크롤링 (뉴스 30분, 주식 15분 주기) | `src/crawler/` |
| **Scheduler** | 유저별 알림 시간 매칭 → Kafka 이벤트 발행 | `src/scheduler/` |
| **Worker** | 이벤트 소비 → 데이터 조회 → 이메일 포맷팅 → 발송 큐 | `src/worker/` |
| **Email Worker** | 이메일 큐 소비 → Resend API/SMTP로 실제 발송 | `src/email_worker/` |

### Infrastructure Dependencies

| 서비스 | 용도 | 비고 |
|--------|------|------|
| **SQLite** | 사용자 데이터, 크롤링 결과 저장 | 기본 (파일 기반, 설치 불필요) |
| **MongoDB** | 대규모 배포 시 대안 | `DB_BACKEND=mongodb`로 전환 |
| **Redis** | URL 중복 캐시, 분산 락, 레이트 리밋, 세션 | 필수 |
| **Kafka** | 컴포넌트 간 비동기 이벤트 전달 (3개 토픽) | 필수 |
| **Resend API** | 이메일 발송 (무료 월 3,000통) | SMTP 대안 가능 |

### Kafka Topics

| 토픽 | 용도 |
|------|------|
| `notification-events` | Scheduler → Worker (알림 이벤트) |
| `email-delivery` | Worker → Email Worker (발송 대기 이메일) |
| `notification-dlq` | 실패한 메시지 Dead Letter Queue |

### Deployment

| 파일 | 용도 |
|------|------|
| `docker-compose.yml` | 로컬 개발 (전체 스택 한번에 실행) |
| `k8s/` | Kubernetes 프로덕션 매니페스트 |
| `Dockerfile.*` | 각 컴포넌트별 Docker 이미지 (5개) |

## Tech Stack

- **Language**: Python 3.9+
- **Database**: SQLite (기본, 경량) / MongoDB (선택, 대규모 배포용)
- **Message Queue**: Apache Kafka (event streaming)
- **Cache/Locking**: Redis (distributed locks, rate limiting, caching)
- **Web Crawling**: BeautifulSoup, Requests
- **Email**: Resend API (기본) / SMTP (대안)
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
- Redis
- Apache Kafka
- (선택) MongoDB — `DB_BACKEND=mongodb`로 전환 시에만 필요

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
