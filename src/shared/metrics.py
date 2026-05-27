"""
Metrics collection module for Alarm News System.

Provides a MetricsCollector class that tracks:
- Notification processing latency (milliseconds)
- Crawl success rate (percentage 0-100)
- Email delivery success rate (percentage 0-100)

Metrics are emitted to the monitoring system every 1 minute via a
background thread. The collector uses a thread-safe design for
concurrent access from multiple workers.
"""
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

# Emit metrics every 60 seconds
DEFAULT_EMIT_INTERVAL_SECONDS = 60


@dataclass
class MetricsSnapshot:
    """A point-in-time snapshot of collected metrics."""

    timestamp: float
    notification_latency_avg_ms: float
    notification_latency_p95_ms: float
    notification_latency_p99_ms: float
    crawl_success_rate: float  # 0-100
    email_delivery_success_rate: float  # 0-100
    notification_count: int
    crawl_total: int
    crawl_success: int
    crawl_failure: int
    email_total: int
    email_success: int
    email_failure: int

    def to_dict(self) -> dict:
        """Serialize metrics snapshot to a dictionary."""
        return {
            "timestamp": self.timestamp,
            "notification_processing": {
                "latency_avg_ms": round(self.notification_latency_avg_ms, 2),
                "latency_p95_ms": round(self.notification_latency_p95_ms, 2),
                "latency_p99_ms": round(self.notification_latency_p99_ms, 2),
                "count": self.notification_count,
            },
            "crawl": {
                "success_rate": round(self.crawl_success_rate, 2),
                "total": self.crawl_total,
                "success": self.crawl_success,
                "failure": self.crawl_failure,
            },
            "email_delivery": {
                "success_rate": round(self.email_delivery_success_rate, 2),
                "total": self.email_total,
                "success": self.email_success,
                "failure": self.email_failure,
            },
        }


class MetricsCollector:
    """
    Thread-safe metrics collector for the Alarm News system.

    Tracks processing latency, crawl success rate, and email delivery
    success rate. Emits aggregated metrics every 1 minute via a
    background daemon thread.

    Usage:
        collector = MetricsCollector()
        collector.start()

        # Record metrics from various components
        collector.record_notification_latency(150.5)
        collector.record_crawl_result(success=True)
        collector.record_email_delivery_result(success=True)

        # Get current snapshot
        snapshot = collector.get_snapshot()

        # Stop when shutting down
        collector.stop()
    """

    def __init__(self, emit_interval_seconds: int = DEFAULT_EMIT_INTERVAL_SECONDS):
        """
        Initialize the MetricsCollector.

        Args:
            emit_interval_seconds: Interval between metric emissions (default: 60).
        """
        self._emit_interval_seconds = emit_interval_seconds
        self._lock = threading.Lock()

        # Notification processing latency samples (milliseconds)
        self._latency_samples: List[float] = []

        # Crawl counters
        self._crawl_success: int = 0
        self._crawl_failure: int = 0

        # Email delivery counters
        self._email_success: int = 0
        self._email_failure: int = 0

        # Background emission thread
        self._emit_thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        """Start the background metrics emission thread."""
        if self._running:
            return

        self._running = True
        self._emit_thread = threading.Thread(
            target=self._emit_loop,
            name="metrics-emitter",
            daemon=True,
        )
        self._emit_thread.start()
        logger.info(
            "Metrics collector started (emit interval: %ds)",
            self._emit_interval_seconds,
        )

    def stop(self) -> None:
        """Stop the background metrics emission thread."""
        self._running = False
        if self._emit_thread is not None:
            self._emit_thread.join(timeout=5)
            self._emit_thread = None
        logger.info("Metrics collector stopped")

    def record_notification_latency(self, latency_ms: float) -> None:
        """
        Record a notification processing latency sample.

        Args:
            latency_ms: Processing latency in milliseconds.
        """
        with self._lock:
            self._latency_samples.append(latency_ms)

    def record_crawl_result(self, success: bool) -> None:
        """
        Record a crawl attempt result.

        Args:
            success: True if crawl succeeded, False if it failed.
        """
        with self._lock:
            if success:
                self._crawl_success += 1
            else:
                self._crawl_failure += 1

    def record_email_delivery_result(self, success: bool) -> None:
        """
        Record an email delivery attempt result.

        Args:
            success: True if delivery succeeded, False if it failed.
        """
        with self._lock:
            if success:
                self._email_success += 1
            else:
                self._email_failure += 1

    def get_snapshot(self) -> MetricsSnapshot:
        """
        Get a point-in-time snapshot of all collected metrics.

        Returns:
            MetricsSnapshot with current aggregated values.
        """
        with self._lock:
            return self._compute_snapshot()

    def reset(self) -> None:
        """Reset all metrics counters and samples."""
        with self._lock:
            self._latency_samples.clear()
            self._crawl_success = 0
            self._crawl_failure = 0
            self._email_success = 0
            self._email_failure = 0

    def _compute_snapshot(self) -> MetricsSnapshot:
        """Compute metrics snapshot from current data (must hold lock)."""
        # Notification latency
        latency_count = len(self._latency_samples)
        if latency_count > 0:
            sorted_samples = sorted(self._latency_samples)
            avg_ms = sum(sorted_samples) / latency_count
            p95_idx = int(latency_count * 0.95)
            p99_idx = int(latency_count * 0.99)
            p95_ms = sorted_samples[min(p95_idx, latency_count - 1)]
            p99_ms = sorted_samples[min(p99_idx, latency_count - 1)]
        else:
            avg_ms = 0.0
            p95_ms = 0.0
            p99_ms = 0.0

        # Crawl success rate
        crawl_total = self._crawl_success + self._crawl_failure
        crawl_rate = (self._crawl_success / crawl_total * 100) if crawl_total > 0 else 100.0

        # Email delivery success rate
        email_total = self._email_success + self._email_failure
        email_rate = (self._email_success / email_total * 100) if email_total > 0 else 100.0

        return MetricsSnapshot(
            timestamp=time.time(),
            notification_latency_avg_ms=avg_ms,
            notification_latency_p95_ms=p95_ms,
            notification_latency_p99_ms=p99_ms,
            crawl_success_rate=crawl_rate,
            email_delivery_success_rate=email_rate,
            notification_count=latency_count,
            crawl_total=crawl_total,
            crawl_success=self._crawl_success,
            crawl_failure=self._crawl_failure,
            email_total=email_total,
            email_success=self._email_success,
            email_failure=self._email_failure,
        )

    def _emit_loop(self) -> None:
        """Background loop that emits metrics at the configured interval."""
        while self._running:
            time.sleep(self._emit_interval_seconds)
            if not self._running:
                break

            try:
                snapshot = self.get_snapshot()
                self._emit_metrics(snapshot)
            except Exception as e:
                logger.error("Failed to emit metrics: %s", e)

    def _emit_metrics(self, snapshot: MetricsSnapshot) -> None:
        """
        Emit metrics to the monitoring system.

        Currently logs metrics as structured data. Can be extended to
        push to Prometheus, Datadog, CloudWatch, etc.

        Args:
            snapshot: The metrics snapshot to emit.
        """
        logger.info(
            "metrics_emit",
            extra={
                "metrics": snapshot.to_dict(),
                "notification_latency_avg_ms": snapshot.notification_latency_avg_ms,
                "crawl_success_rate": snapshot.crawl_success_rate,
                "email_delivery_success_rate": snapshot.email_delivery_success_rate,
            },
        )


# Module-level singleton
_collector: Optional[MetricsCollector] = None


def get_metrics_collector(
    emit_interval_seconds: int = DEFAULT_EMIT_INTERVAL_SECONDS,
) -> MetricsCollector:
    """
    Get the global MetricsCollector singleton.

    Creates and starts the collector on first call.

    Args:
        emit_interval_seconds: Interval between metric emissions.

    Returns:
        The global MetricsCollector instance.
    """
    global _collector
    if _collector is None:
        _collector = MetricsCollector(emit_interval_seconds=emit_interval_seconds)
        _collector.start()
    return _collector


def reset_metrics_collector() -> None:
    """Reset the global MetricsCollector singleton. Useful for testing."""
    global _collector
    if _collector is not None:
        _collector.stop()
        _collector = None
