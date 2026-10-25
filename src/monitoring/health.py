from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import yaml
from confluent_kafka import Consumer, TopicPartition

from src.common.log_events import log_event
from src.database.connection import DatabaseSettings, connect, load_database_settings

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

UNHEALTHY_ISSUES = frozenset(
    {"database_unavailable", "producer_stalled", "producer_failing", "consumer_stalled"}
)
DEGRADED_ISSUES = frozenset(
    {"stale_observation", "ingestion_delay", "producer_failures", "consumer_lag"}
)


@dataclass(frozen=True)
class HealthThresholds:
    stale_after_seconds: float
    ingestion_delay_seconds: float
    producer_stall_seconds: float
    producer_failure_threshold: int
    consumer_stall_seconds: float
    startup_grace_seconds: float
    check_interval_seconds: float
    log_repeat_seconds: float


@dataclass(frozen=True)
class HealthSnapshot:
    now: datetime
    database_ok: bool
    latest_observation_time: datetime | None = None
    latest_received_at: datetime | None = None
    observation_received_at: datetime | None = None
    producer_last_success_at: datetime | None = None
    producer_consecutive_failures: int = 0
    consumer_last_write_at: datetime | None = None
    consumer_lag: int | None = None
    started_at: datetime | None = None


@dataclass(frozen=True)
class HealthReport:
    status: str
    issues: tuple[str, ...]
    data_age_seconds: int | None
    ingestion_delay_seconds: int | None
    receipt_age_seconds: int | None
    latest_observation_time: datetime | None
    latest_received_at: datetime | None
    consumer_lag: int | None
    producer_consecutive_failures: int

    @property
    def affected(self) -> tuple[str, ...]:
        components = []
        for issue in self.issues:
            if issue.startswith("producer"):
                component = "producer"
            elif issue.startswith("consumer"):
                component = "consumer"
            elif issue == "database_unavailable":
                component = "database"
            else:
                component = "freshness"
            if component not in components:
                components.append(component)
        return tuple(components)


def thresholds_from_config(raw: Mapping) -> HealthThresholds:
    health = raw.get("health") or {}
    if not isinstance(health, dict):
        raise RuntimeError("config health must be a mapping")
    values = {
        "stale_after_seconds": 3600,
        "ingestion_delay_seconds": 900,
        "producer_stall_seconds": 300,
        "producer_failure_threshold": 3,
        "consumer_stall_seconds": 300,
        "startup_grace_seconds": 300,
        "check_interval_seconds": 300,
        "log_repeat_seconds": 900,
    }
    values.update({key: health[key] for key in values if key in health})
    failure_threshold = int(values["producer_failure_threshold"])
    if failure_threshold < 1:
        raise RuntimeError("producer_failure_threshold must be at least 1")
    numbers = [values[key] for key in values if key != "producer_failure_threshold"]
    if any(float(number) < 0 for number in numbers):
        raise RuntimeError("health durations must be zero or greater")
    return HealthThresholds(
        stale_after_seconds=float(values["stale_after_seconds"]),
        ingestion_delay_seconds=float(values["ingestion_delay_seconds"]),
        producer_stall_seconds=float(values["producer_stall_seconds"]),
        producer_failure_threshold=failure_threshold,
        consumer_stall_seconds=float(values["consumer_stall_seconds"]),
        startup_grace_seconds=float(values["startup_grace_seconds"]),
        check_interval_seconds=float(values["check_interval_seconds"]),
        log_repeat_seconds=float(values["log_repeat_seconds"]),
    )


def load_health_config() -> tuple[HealthThresholds, Path, Path]:
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    health = raw.get("health") or {}
    return (
        thresholds_from_config(raw),
        _resolve(health.get("producer_status_path") or "data/health/producer.json"),
        _resolve(health.get("consumer_status_path") or "data/health/consumer.json"),
    )


def evaluate_health(snapshot: HealthSnapshot, thresholds: HealthThresholds) -> HealthReport:
    """Classify pipeline health. A quiet source is not a stopped pipeline."""
    data_age = _age_seconds(snapshot.now, snapshot.latest_observation_time)
    receipt_age = _age_seconds(snapshot.now, snapshot.latest_received_at)
    ingestion_delay = None
    if snapshot.latest_observation_time is not None and snapshot.observation_received_at is not None:
        ingestion_delay = _age_seconds(snapshot.observation_received_at, snapshot.latest_observation_time)
    within_grace = _within_grace(snapshot, thresholds)
    issues: list[str] = []

    if not snapshot.database_ok:
        issues.append("database_unavailable")

    if snapshot.producer_consecutive_failures >= thresholds.producer_failure_threshold:
        issues.append("producer_failing")
    elif snapshot.producer_consecutive_failures > 0:
        issues.append("producer_failures")

    producer_age = _age_seconds(snapshot.now, snapshot.producer_last_success_at)
    if snapshot.producer_last_success_at is None:
        if snapshot.started_at is not None and not within_grace:
            issues.append("producer_stalled")
    elif producer_age is not None and producer_age > thresholds.producer_stall_seconds:
        issues.append("producer_stalled")

    if snapshot.consumer_lag is not None and snapshot.consumer_lag > 0:
        consumer_age = _age_seconds(snapshot.now, snapshot.consumer_last_write_at)
        write_is_stale = snapshot.consumer_last_write_at is None or (
            consumer_age is not None and consumer_age > thresholds.consumer_stall_seconds
        )
        if write_is_stale and not (snapshot.consumer_last_write_at is None and within_grace):
            issues.append("consumer_stalled")
        else:
            issues.append("consumer_lag")

    if data_age is not None and data_age > thresholds.stale_after_seconds:
        issues.append("stale_observation")
    if ingestion_delay is not None and ingestion_delay > thresholds.ingestion_delay_seconds:
        issues.append("ingestion_delay")

    if any(issue in UNHEALTHY_ISSUES for issue in issues):
        status = "unhealthy"
    elif any(issue in DEGRADED_ISSUES for issue in issues):
        status = "degraded"
    else:
        status = "healthy"
    return HealthReport(
        status=status,
        issues=tuple(issues),
        data_age_seconds=data_age,
        ingestion_delay_seconds=ingestion_delay,
        receipt_age_seconds=receipt_age,
        latest_observation_time=snapshot.latest_observation_time,
        latest_received_at=snapshot.latest_received_at,
        consumer_lag=snapshot.consumer_lag,
        producer_consecutive_failures=snapshot.producer_consecutive_failures,
    )


def partition_lag(committed_offset: int | None, log_end: int) -> int:
    """Unconsumed records on one partition. A missing commit reads from the start."""
    if log_end <= 0:
        return 0
    if committed_offset is None or committed_offset < 0:
        return log_end
    return max(0, log_end - committed_offset)


class HealthLogState:
    def __init__(self) -> None:
        self._signature: tuple[str, tuple[str, ...]] | None = None
        self._last_logged_at: datetime | None = None

    def should_log(self, report: HealthReport, now: datetime, repeat_after: timedelta) -> bool:
        signature = (report.status, report.issues)
        if signature != self._signature or self._last_logged_at is None:
            return True
        return now - self._last_logged_at >= repeat_after

    def mark_logged(self, report: HealthReport, now: datetime) -> None:
        self._signature = (report.status, report.issues)
        self._last_logged_at = now


def log_health(report: HealthReport, *, reporter: str) -> None:
    level = {"healthy": logging.INFO, "degraded": logging.WARNING, "unhealthy": logging.ERROR}[report.status]
    log_event(
        logger,
        level,
        "health_check",
        reporter=reporter,
        status=report.status,
        affected=",".join(report.affected) if report.affected else "none",
        issues=",".join(report.issues) if report.issues else "none",
        latest_observation_time=report.latest_observation_time,
        data_age_seconds=report.data_age_seconds,
        latest_received_at=report.latest_received_at,
        receipt_age_seconds=report.receipt_age_seconds,
        ingestion_delay_seconds=report.ingestion_delay_seconds,
        consumer_lag=report.consumer_lag,
        producer_consecutive_failures=report.producer_consecutive_failures,
    )


class PipelineMonitor:
    """Tracks local poll or write progress and periodically emits one health check."""

    def __init__(self, reporter: str, started_at: datetime | None = None) -> None:
        thresholds, producer_path, consumer_path = load_health_config()
        self.reporter = reporter
        self.thresholds = thresholds
        self.producer_status_path = producer_path
        self.consumer_status_path = consumer_path
        self.started_at = started_at or datetime.now(timezone.utc)
        self.producer_last_success_at: datetime | None = None
        self.producer_consecutive_failures = 0
        self.producer_last_published = 0
        self.consumer_last_write_at: datetime | None = None
        self._log_state = HealthLogState()
        self._next_check = 0.0
        self._next_status_write = 0.0
        self._database_settings: DatabaseSettings | None = None
        self._kafka_target = _kafka_target()

    def record_producer_success(self, when: datetime, published: int) -> None:
        self.producer_last_success_at = when
        self.producer_consecutive_failures = 0
        self.producer_last_published = published
        self._write_producer_status(when)

    def record_producer_failure(self, when: datetime) -> None:
        self.producer_consecutive_failures += 1
        self._write_producer_status(when)

    def record_consumer_write(self, when: datetime) -> None:
        self.consumer_last_write_at = when
        now_mono = time.monotonic()
        if now_mono < self._next_status_write:
            return
        self._next_status_write = now_mono + 5
        self._write_consumer_status(when)

    def maybe_check(self, *, force: bool = False) -> HealthReport | None:
        now_mono = time.monotonic()
        if not force and now_mono < self._next_check:
            return None
        self._next_check = now_mono + self.thresholds.check_interval_seconds
        now = datetime.now(timezone.utc)
        try:
            report = evaluate_health(self.snapshot(now), self.thresholds)
        except Exception as exc:
            log_event(logger, logging.ERROR, "health_check_failed", reporter=self.reporter, error=exc)
            return None
        if self._log_state.should_log(report, now, timedelta(seconds=self.thresholds.log_repeat_seconds)):
            log_health(report, reporter=self.reporter)
            self._log_state.mark_logged(report, now)
        return report

    def snapshot(self, now: datetime) -> HealthSnapshot:
        database_ok, freshness = _read_freshness(self._database())
        if self.reporter == "producer":
            producer_success = self.producer_last_success_at
            producer_failures = self.producer_consecutive_failures
            consumer_write = _parse_time(_read_status(self.consumer_status_path).get("last_write_at"))
        elif self.reporter == "consumer":
            producer_status = _read_status(self.producer_status_path)
            producer_success = _parse_time(producer_status.get("last_success_at"))
            producer_failures = int(producer_status.get("consecutive_failures") or 0)
            consumer_write = self.consumer_last_write_at
        else:
            producer_success = self.producer_last_success_at
            producer_failures = self.producer_consecutive_failures
            consumer_write = self.consumer_last_write_at
        return HealthSnapshot(
            now=now,
            database_ok=database_ok,
            latest_observation_time=None if freshness is None else freshness[0],
            latest_received_at=None if freshness is None else freshness[1],
            observation_received_at=None if freshness is None else freshness[2],
            producer_last_success_at=producer_success,
            producer_consecutive_failures=producer_failures,
            consumer_last_write_at=consumer_write,
            consumer_lag=self._lag(),
            started_at=self.started_at,
        )

    def _database(self) -> DatabaseSettings | None:
        if self._database_settings is None:
            try:
                self._database_settings = load_database_settings()
            except Exception:
                return None
        return self._database_settings

    def _lag(self) -> int | None:
        if self._kafka_target is None:
            return None
        bootstrap, group_id, topic = self._kafka_target
        return read_consumer_lag(bootstrap, group_id, topic)

    def _write_producer_status(self, when: datetime) -> None:
        _write_status(
            self.producer_status_path,
            {
                "last_success_at": None if self.producer_last_success_at is None else self.producer_last_success_at.isoformat(),
                "consecutive_failures": self.producer_consecutive_failures,
                "last_published": self.producer_last_published,
                "updated_at": when.isoformat(),
            },
        )

    def _write_consumer_status(self, when: datetime) -> None:
        _write_status(
            self.consumer_status_path,
            {
                "last_write_at": None if self.consumer_last_write_at is None else self.consumer_last_write_at.isoformat(),
                "updated_at": when.isoformat(),
            },
        )


def read_consumer_lag(bootstrap_servers: str, group_id: str, topic: str, timeout: float = 10.0) -> int | None:
    """Read group lag without subscribing, so the running consumer keeps its assignment."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "client.id": "pipeline-health",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=timeout)
        described = metadata.topics.get(topic)
        if described is None or described.error is not None:
            return None
        partitions = [TopicPartition(topic, partition_id) for partition_id in described.partitions]
        if not partitions:
            return 0
        committed = consumer.committed(partitions, timeout=timeout)
        total = 0
        for partition in committed:
            _low, high = consumer.get_watermark_offsets(partition, timeout=timeout)
            total += partition_lag(partition.offset, high)
        return total
    except Exception:
        return None
    finally:
        consumer.close()


def _kafka_target() -> tuple[str, str, str] | None:
    try:
        with CONFIG_PATH.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        kafka = raw["kafka"]
        consumer = raw["consumer"]
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"])
        return bootstrap, consumer["group_id"], kafka["topic"]
    except Exception:
        return None


def _read_freshness(
    settings: DatabaseSettings | None,
) -> tuple[bool, tuple[datetime | None, datetime | None, datetime | None] | None]:
    if settings is None:
        return False, None
    connection = None
    try:
        connection = connect(settings)
        with connection.cursor() as cursor:
            cursor.execute("SELECT max(observation_time) FROM observations")
            latest_observation = cursor.fetchone()[0]
            cursor.execute("SELECT max(received_at) FROM observation_events")
            latest_received = cursor.fetchone()[0]
            observation_received = None
            if latest_observation is not None:
                cursor.execute(
                    """
                    SELECT max(received_at)
                    FROM observation_events
                    WHERE observation_time = %s
                    """,
                    (latest_observation,),
                )
                observation_received = cursor.fetchone()[0]
        connection.rollback()
        return True, (latest_observation, latest_received, observation_received)
    except Exception:
        return False, None
    finally:
        if connection is not None and not connection.closed:
            connection.close()


def _write_status(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        temporary.replace(path)
    except Exception as exc:
        log_event(logger, logging.ERROR, "health_status_write_failed", path=str(path), error=exc)


def _read_status(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(now: datetime, earlier: datetime | None) -> int | None:
    if earlier is None:
        return None
    return int((now - earlier).total_seconds())


def _within_grace(snapshot: HealthSnapshot, thresholds: HealthThresholds) -> bool:
    if snapshot.started_at is None:
        return False
    age = _age_seconds(snapshot.now, snapshot.started_at)
    return age is not None and age <= thresholds.startup_grace_seconds


def _resolve(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def main() -> None:
    from src.common.logging_config import configure_logging

    configure_logging()
    monitor = PipelineMonitor("check")
    monitor.producer_last_success_at = _parse_time(_read_status(monitor.producer_status_path).get("last_success_at"))
    producer_status = _read_status(monitor.producer_status_path)
    monitor.producer_consecutive_failures = int(producer_status.get("consecutive_failures") or 0)
    monitor.consumer_last_write_at = _parse_time(_read_status(monitor.consumer_status_path).get("last_write_at"))
    monitor.started_at = monitor.producer_last_success_at or monitor.consumer_last_write_at or monitor.started_at
    report = monitor.maybe_check(force=True)
    if report is not None:
        raise SystemExit(0 if report.status != "unhealthy" else 1)


if __name__ == "__main__":
    main()
