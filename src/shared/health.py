"""
Health check module for Alarm News System.

Provides a HealthChecker class that verifies connectivity to all
system dependencies (Kafka, MongoDB, Redis) with 5-second timeouts
and tracks worker activity to detect stalled processing.

Returns structured health status suitable for Kubernetes readiness
and liveness probes.
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Default timeout for dependency checks (seconds)
DEFAULT_CHECK_TIMEOUT_SECONDS = 5

# If no notification processed within this many seconds, mark unhealthy
DEFAULT_INACTIVITY_THRESHOLD_SECONDS = 300  # 5 minutes


@dataclass
class DependencyStatus:
    """Status of a single dependency."""

    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


@dataclass
class HealthStatus:
    """Overall system health status."""

    status: str  # "healthy" or "unhealthy"
    dependencies: Dict[str, DependencyStatus] = field(default_factory=dict)
    worker_active: bool = True
    http_status_code: int = 200

    def to_dict(self) -> dict:
        """Serialize health status to a dictionary for HTTP response."""
        deps = {}
        for name, dep in self.dependencies.items():
            deps[name] = {
                "healthy": dep.healthy,
                "latency_ms": dep.latency_ms,
            }
            if dep.error:
                deps[name]["error"] = dep.error

        result = {
            "status": self.status,
            "dependencies": deps,
            "worker_active": self.worker_active,
        }

        if self.status == "unhealthy":
            failed = [name for name, dep in self.dependencies.items() if not dep.healthy]
            if not self.worker_active:
                failed.append("worker_inactivity")
            result["failed"] = failed

        return result


class HealthChecker:
    """
    Checks system health by verifying connectivity to Kafka, MongoDB,
    and Redis within configured timeouts.

    Also tracks the last time a notification was processed to detect
    stalled workers (no processing for 5+ minutes).

    Usage:
        checker = HealthChecker(
            kafka_producer=producer,
            mongodb_manager=db_manager,
            redis_manager=redis_manager,
        )
        status = checker.check_health()
        # status.http_status_code -> 200 or 503
        # status.to_dict() -> JSON-serializable response
    """

    def __init__(
        self,
        kafka_producer=None,
        mongodb_manager=None,
        redis_manager=None,
        timeout_seconds: int = DEFAULT_CHECK_TIMEOUT_SECONDS,
        inactivity_threshold_seconds: int = DEFAULT_INACTIVITY_THRESHOLD_SECONDS,
    ):
        """
        Initialize the HealthChecker.

        Args:
            kafka_producer: Kafka producer instance with health_check() method.
            mongodb_manager: MongoDB connection manager with health_check() method.
            redis_manager: Redis connection manager with health_check() method.
            timeout_seconds: Maximum time to wait for each dependency check.
            inactivity_threshold_seconds: Seconds of inactivity before marking unhealthy.
        """
        self._kafka_producer = kafka_producer
        self._mongodb_manager = mongodb_manager
        self._redis_manager = redis_manager
        self._timeout_seconds = timeout_seconds
        self._inactivity_threshold_seconds = inactivity_threshold_seconds
        self._last_processing_time: Optional[float] = None

    def record_processing(self) -> None:
        """
        Record that a notification was successfully processed.

        Call this after each successful notification processing to
        keep the worker marked as active.
        """
        self._last_processing_time = time.time()

    def check_health(self) -> HealthStatus:
        """
        Perform health checks on all dependencies and worker activity.

        Returns:
            HealthStatus with overall status, dependency states, and HTTP code.
        """
        dependencies: Dict[str, DependencyStatus] = {}

        # Check Kafka
        dependencies["kafka"] = self._check_kafka()

        # Check MongoDB
        dependencies["mongodb"] = self._check_mongodb()

        # Check Redis
        dependencies["redis"] = self._check_redis()

        # Check worker activity
        worker_active = self._check_worker_activity()

        # Determine overall status
        all_deps_healthy = all(dep.healthy for dep in dependencies.values())
        overall_healthy = all_deps_healthy and worker_active

        return HealthStatus(
            status="healthy" if overall_healthy else "unhealthy",
            dependencies=dependencies,
            worker_active=worker_active,
            http_status_code=200 if overall_healthy else 503,
        )

    def _check_kafka(self) -> DependencyStatus:
        """Check Kafka connectivity within timeout."""
        if self._kafka_producer is None:
            return DependencyStatus(
                name="kafka",
                healthy=False,
                error="Kafka producer not configured",
            )

        start = time.time()
        try:
            healthy = self._kafka_producer.health_check()
            latency_ms = (time.time() - start) * 1000

            if latency_ms > self._timeout_seconds * 1000:
                return DependencyStatus(
                    name="kafka",
                    healthy=False,
                    latency_ms=latency_ms,
                    error=f"Timeout: responded in {latency_ms:.1f}ms (limit: {self._timeout_seconds * 1000}ms)",
                )

            return DependencyStatus(
                name="kafka",
                healthy=healthy,
                latency_ms=latency_ms,
                error=None if healthy else "Health check returned False",
            )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            logger.warning("Kafka health check failed: %s", e)
            return DependencyStatus(
                name="kafka",
                healthy=False,
                latency_ms=latency_ms,
                error=str(e),
            )

    def _check_mongodb(self) -> DependencyStatus:
        """Check MongoDB connectivity within timeout."""
        if self._mongodb_manager is None:
            return DependencyStatus(
                name="mongodb",
                healthy=False,
                error="MongoDB manager not configured",
            )

        start = time.time()
        try:
            healthy = self._mongodb_manager.health_check()
            latency_ms = (time.time() - start) * 1000

            if latency_ms > self._timeout_seconds * 1000:
                return DependencyStatus(
                    name="mongodb",
                    healthy=False,
                    latency_ms=latency_ms,
                    error=f"Timeout: responded in {latency_ms:.1f}ms (limit: {self._timeout_seconds * 1000}ms)",
                )

            return DependencyStatus(
                name="mongodb",
                healthy=healthy,
                latency_ms=latency_ms,
                error=None if healthy else "Health check returned False",
            )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            logger.warning("MongoDB health check failed: %s", e)
            return DependencyStatus(
                name="mongodb",
                healthy=False,
                latency_ms=latency_ms,
                error=str(e),
            )

    def _check_redis(self) -> DependencyStatus:
        """Check Redis connectivity within timeout."""
        if self._redis_manager is None:
            return DependencyStatus(
                name="redis",
                healthy=False,
                error="Redis manager not configured",
            )

        start = time.time()
        try:
            healthy = self._redis_manager.health_check()
            latency_ms = (time.time() - start) * 1000

            if latency_ms > self._timeout_seconds * 1000:
                return DependencyStatus(
                    name="redis",
                    healthy=False,
                    latency_ms=latency_ms,
                    error=f"Timeout: responded in {latency_ms:.1f}ms (limit: {self._timeout_seconds * 1000}ms)",
                )

            return DependencyStatus(
                name="redis",
                healthy=healthy,
                latency_ms=latency_ms,
                error=None if healthy else "Health check returned False",
            )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            logger.warning("Redis health check failed: %s", e)
            return DependencyStatus(
                name="redis",
                healthy=False,
                latency_ms=latency_ms,
                error=str(e),
            )

    def _check_worker_activity(self) -> bool:
        """
        Check if the worker has processed a notification recently.

        Returns True if:
        - No processing has been recorded yet (startup grace period)
        - Last processing was within the inactivity threshold

        Returns False if:
        - Last processing was more than inactivity_threshold_seconds ago
        """
        if self._last_processing_time is None:
            # No processing recorded yet — allow startup grace period
            return True

        elapsed = time.time() - self._last_processing_time
        if elapsed > self._inactivity_threshold_seconds:
            logger.warning(
                "Worker inactive for %.1f seconds (threshold: %d seconds)",
                elapsed,
                self._inactivity_threshold_seconds,
            )
            return False

        return True
