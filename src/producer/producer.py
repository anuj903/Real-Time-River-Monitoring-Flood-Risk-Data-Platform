from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from confluent_kafka import Producer
from pydantic import BaseModel

from src.common.log_events import log_event
from src.common.logging_config import configure_logging
from src.common.quality import (
    require_finite,
    require_measurement_unit,
    require_not_future,
    require_timezone,
)
from src.producer.api_client import HubeauClient, HubeauPageLimitError, HubeauResponseError
from src.producer.checkpoint import load_checkpoint, save_checkpoint
from src.producer.config import PROJECT_ROOT, Settings, load_settings
from src.monitoring.health import PipelineMonitor

logger = logging.getLogger(__name__)


class PollStopped(Exception):
    """Raised when shutdown is requested before a poll finishes."""


class WaterObservation(BaseModel):
    station_id: str
    observation_time: datetime
    measurement_type: str
    value: float
    unit: str
    source: str
    ingested_at: datetime


def to_event(row: dict, settings: Settings, ingested_at: datetime) -> WaterObservation | None:
    """Return one valid event, or None when this source row cannot be published."""
    try:
        return _event_from_row(row, settings, ingested_at)
    except (TypeError, ValueError) as exc:
        log_event(
            logger,
            logging.WARNING,
            "source_row_skipped",
            error=exc,
            station_id=row.get("code_station") if isinstance(row, dict) else None,
            observation_time=row.get("date_obs") if isinstance(row, dict) else None,
            measurement_type=row.get("grandeur_hydro") if isinstance(row, dict) else None,
        )
        return None


def _event_from_row(row: dict, settings: Settings, ingested_at: datetime) -> WaterObservation:
    if not isinstance(row, dict):
        raise ValueError("source row must be an object")
    observed_at = row.get("date_obs")
    raw_value = row.get("resultat_obs")
    station_id = row.get("code_station")
    measurement_type = row.get("grandeur_hydro")
    if not isinstance(observed_at, str) or not observed_at.strip():
        raise ValueError("date_obs must be a timestamp string")
    if not isinstance(station_id, str) or not station_id.strip():
        raise ValueError("code_station must be a non-blank string")
    if not isinstance(measurement_type, str) or not measurement_type.strip():
        raise ValueError("grandeur_hydro must be a non-blank string")
    if station_id.strip() != settings.station_code:
        raise ValueError(f"code_station '{station_id.strip()}' does not match {settings.station_code}")
    if measurement_type.strip() != settings.grandeur_hydro:
        raise ValueError(
            f"grandeur_hydro '{measurement_type.strip()}' does not match the requested {settings.grandeur_hydro}"
        )
    observation_time = datetime.fromisoformat(observed_at.strip().replace("Z", "+00:00"))
    observation_time = require_timezone(observation_time, "date_obs")
    value = require_finite(raw_value, "resultat_obs")
    measurement_type = measurement_type.strip()
    require_measurement_unit(measurement_type, settings.unit, settings.measurement_units)
    require_not_future(
        observation_time,
        "date_obs",
        now=ingested_at,
        tolerance=settings.future_timestamp_tolerance,
    )
    return WaterObservation(
        station_id=station_id.strip(),
        observation_time=observation_time,
        measurement_type=measurement_type,
        value=value,
        unit=settings.unit,
        source=settings.source,
        ingested_at=ingested_at,
    )


def window_start(
    now: datetime,
    lookback_minutes: int,
    catchup_max_age_days: int,
    checkpoint: datetime | None,
) -> datetime:
    """Start at the checkpoint when one exists, otherwise use the rolling lookback."""
    if checkpoint is None:
        return now - timedelta(minutes=lookback_minutes)
    earliest = now - timedelta(days=catchup_max_age_days)
    if checkpoint < earliest:
        return earliest
    if checkpoint > now:
        return now
    return checkpoint


def publish(producer: Producer, settings: Settings, events: list[WaterObservation]) -> int:
    failures: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            failures.append(str(err))
            log_event(logger, logging.ERROR, "message_delivery_failed", topic=settings.topic, error=err)
            return
        log_event(
            logger,
            logging.INFO,
            "message_published",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

    for event in events:
        producer.produce(
            topic=settings.topic,
            key=event.station_id.encode(),
            value=event.model_dump_json().encode(),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(timeout=30)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) were not delivered")
    if failures:
        raise RuntimeError(f"{len(failures)} message(s) failed delivery")
    return len(events)


def poll_once(
    client: HubeauClient,
    producer: Producer,
    settings: Settings,
    stop: threading.Event,
    now: datetime | None = None,
) -> int:
    ingested_at = now or datetime.now(timezone.utc)
    started = time.perf_counter()
    checkpoint_path = _checkpoint_path(settings)
    checkpoint = load_checkpoint(checkpoint_path, settings.station_code, settings.grandeur_hydro)
    start = window_start(
        ingested_at,
        settings.lookback_minutes,
        settings.catchup_max_age_days,
        checkpoint,
    )
    catch_up = checkpoint is not None and checkpoint < ingested_at - timedelta(minutes=settings.lookback_minutes)
    if checkpoint is not None and start > checkpoint:
        log_event(
            logger,
            logging.WARNING,
            "checkpoint_clamped",
            checkpoint=checkpoint,
            catchup_max_age_days=settings.catchup_max_age_days,
            window_start=start,
        )
    log_event(
        logger,
        logging.INFO,
        "poll_started",
        station_id=settings.station_code,
        measurement_type=settings.grandeur_hydro,
        window_start=start,
        checkpoint=checkpoint,
        catch_up=catch_up,
    )
    observations_received = 0
    try:
        rows, fetch_retries = _retry(
            stop,
            settings,
            lambda attempt: client.fetch_observations(
                station_code=settings.station_code,
                grandeur_hydro=settings.grandeur_hydro,
                date_debut_obs=start,
                page_size=settings.page_size,
                max_pages=settings.max_pages,
                attempt=attempt,
            ),
            "hubeau_fetch",
            _is_transient_http,
        )
        observations_received = len(rows)
        events: list[WaterObservation] = []
        skipped = 0
        for row in rows:
            event = to_event(row, settings, ingested_at)
            if event is None:
                skipped += 1
                continue
            events.append(event)
        events, duplicates = _collapse_duplicate_rows(events)
        events.sort(key=lambda event: event.observation_time)
        pages = getattr(client, "pages_fetched", 0)
        if not events:
            log_event(
                logger,
                logging.INFO,
                "poll_empty",
                **_poll_fields(started, pages, observations_received, skipped, duplicates, fetch_retries, 0, False, checkpoint),
            )
            return 0

        published, publish_retries = _retry(
            stop,
            settings,
            lambda attempt: publish(producer, settings, events),
            "kafka_publish",
            _is_transient_publish,
        )
        latest = events[-1].observation_time
        save_checkpoint(checkpoint_path, settings.station_code, settings.grandeur_hydro, latest)
        log_event(
            logger,
            logging.INFO,
            "poll_published",
            **_poll_fields(
                started,
                pages,
                observations_received,
                skipped,
                duplicates,
                fetch_retries,
                published,
                True,
                latest,
                publish_retries,
            ),
        )
        return published
    except PollStopped:
        raise
    except Exception as exc:
        log_event(
            logger,
            logging.ERROR,
            "poll_failed",
            duration_ms=_elapsed_ms(started),
            station_id=settings.station_code,
            observations_received=observations_received,
            pages=getattr(client, "pages_fetched", 0),
            messages_published=0,
            checkpoint_advanced=False,
            error=exc,
            exc_info=True,
        )
        raise


def _collapse_duplicate_rows(events: list[WaterObservation]) -> tuple[list[WaterObservation], int]:
    """Keep the last value for each observation key and count the rows that were dropped."""
    unique: dict[tuple[str, str, datetime], WaterObservation] = {}
    duplicates = 0
    for event in events:
        key = (event.station_id, event.measurement_type, event.observation_time)
        previous = unique.get(key)
        if previous is not None:
            duplicates += 1
            log_event(
                logger,
                logging.WARNING,
                "duplicate_source_row",
                station_id=event.station_id,
                measurement_type=event.measurement_type,
                observation_time=event.observation_time,
                previous_value=previous.value,
                value=event.value,
            )
        unique[key] = event
    return list(unique.values()), duplicates


def _retry(stop, settings: Settings, action, operation: str, is_retryable):
    delay = settings.retry_backoff_seconds
    retries = 0
    for attempt in range(1, settings.max_attempts + 1):
        if stop.is_set():
            raise PollStopped()
        try:
            return action(attempt), retries
        except PollStopped:
            raise
        except HubeauPageLimitError:
            raise
        except HubeauResponseError:
            raise
        except Exception as exc:
            if not is_retryable(exc) or attempt == settings.max_attempts:
                raise
            retries += 1
            log_event(
                logger,
                logging.WARNING,
                "retry",
                operation=operation,
                attempt=attempt,
                max_attempts=settings.max_attempts,
                error=exc,
            )
            _wait(stop, delay)
            delay = min(delay * 2, 30)


def _poll_fields(
    started: float,
    pages: int,
    observations_received: int,
    rows_skipped: int,
    duplicate_source_rows: int,
    fetch_retries: int,
    messages_published: int,
    checkpoint_advanced: bool,
    checkpoint: datetime | None,
    publish_retries: int = 0,
) -> dict:
    return {
        "duration_ms": _elapsed_ms(started),
        "pages": pages,
        "observations_received": observations_received,
        "rows_skipped": rows_skipped,
        "duplicate_source_rows": duplicate_source_rows,
        "fetch_retries": fetch_retries,
        "publish_retries": publish_retries,
        "messages_published": messages_published,
        "checkpoint_advanced": checkpoint_advanced,
        "checkpoint": checkpoint,
    }


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _is_transient_http(exc: Exception) -> bool:
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


def _is_transient_publish(exc: Exception) -> bool:
    return isinstance(exc, RuntimeError) and "not delivered" in str(exc)


def _checkpoint_path(settings: Settings) -> Path:
    path = Path(settings.checkpoint_path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _wait(stop: threading.Event, seconds: float) -> None:
    """Sleep in short slices so a shutdown signal is noticed during the interval."""
    deadline = time.monotonic() + seconds
    while not stop.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            time.sleep(min(0.5, remaining))
        except InterruptedError:
            continue


def run() -> None:
    configure_logging()
    settings = load_settings()
    stop = threading.Event()

    def handle_signal(signum, _frame) -> None:
        if stop.is_set():
            log_event(logger, logging.WARNING, "shutdown_forced", signal=signum)
            raise SystemExit(1)
        log_event(logger, logging.INFO, "shutdown_requested", signal=signum, after="current_poll")
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_signal)

    client = HubeauClient(settings.base_url)
    producer = Producer({"bootstrap.servers": settings.bootstrap_servers})
    monitor = PipelineMonitor("producer")
    log_event(
        logger,
        logging.INFO,
        "producer_started",
        station_id=settings.station_code,
        poll_interval_seconds=settings.poll_interval_seconds,
    )
    try:
        while not stop.is_set():
            try:
                published = poll_once(client, producer, settings, stop)
                monitor.record_producer_success(datetime.now(timezone.utc), published)
            except PollStopped:
                break
            except Exception:
                monitor.record_producer_failure(datetime.now(timezone.utc))
            if stop.is_set():
                break
            try:
                monitor.maybe_check(force=monitor.producer_consecutive_failures > 0)
            except Exception as exc:
                log_event(logger, logging.ERROR, "health_check_failed", reporter="producer", error=exc)
            log_event(logger, logging.INFO, "poll_scheduled", interval_seconds=settings.poll_interval_seconds)
            _wait(stop, settings.poll_interval_seconds)
    finally:
        producer.flush(timeout=30)
        client.close()
        log_event(logger, logging.INFO, "producer_stopped")


if __name__ == "__main__":
    run()
