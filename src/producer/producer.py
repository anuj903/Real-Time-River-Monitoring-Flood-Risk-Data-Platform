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
        logger.warning(
            "skipped malformed source row error=%s station_id=%s observation_time=%s measurement_type=%s",
            exc,
            row.get("code_station") if isinstance(row, dict) else None,
            row.get("date_obs") if isinstance(row, dict) else None,
            row.get("grandeur_hydro") if isinstance(row, dict) else None,
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
            logger.error("delivery failed: %s", err)
            return
        logger.info(
            "delivered %s [%s] offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
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
    checkpoint_path = _checkpoint_path(settings)
    checkpoint = load_checkpoint(checkpoint_path, settings.station_code, settings.grandeur_hydro)
    start = window_start(
        ingested_at,
        settings.lookback_minutes,
        settings.catchup_max_age_days,
        checkpoint,
    )
    if checkpoint is not None and start > checkpoint:
        logger.warning(
            "checkpoint %s is older than %s days; catch-up starts at %s",
            checkpoint.isoformat(),
            settings.catchup_max_age_days,
            start.isoformat(),
        )
    elif checkpoint is not None and checkpoint < ingested_at - timedelta(minutes=settings.lookback_minutes):
        logger.info("catch-up from checkpoint %s", checkpoint.isoformat())

    rows = _retry(
        stop,
        settings,
        lambda: client.fetch_observations(
            station_code=settings.station_code,
            grandeur_hydro=settings.grandeur_hydro,
            date_debut_obs=start,
            page_size=settings.page_size,
            max_pages=settings.max_pages,
        ),
        "Hub'Eau fetch",
        _is_transient_http,
    )

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

    logger.info(
        "fetched %s observation(s) for %s since %s; skipped %s incomplete row(s); ignored %s duplicate source row(s)",
        len(events),
        settings.station_code,
        start.isoformat(),
        skipped,
        duplicates,
    )
    if not events:
        return 0

    published = _retry(
        stop,
        settings,
        lambda: publish(producer, settings, events),
        "Kafka publish",
        _is_transient_publish,
    )
    latest = events[-1].observation_time
    save_checkpoint(checkpoint_path, settings.station_code, settings.grandeur_hydro, latest)
    logger.info(
        "published %s message(s) to %s; checkpoint %s",
        published,
        settings.topic,
        latest.isoformat(),
    )
    return published


def _collapse_duplicate_rows(events: list[WaterObservation]) -> tuple[list[WaterObservation], int]:
    """Keep the last value for each observation key and count the rows that were dropped."""
    unique: dict[tuple[str, str, datetime], WaterObservation] = {}
    duplicates = 0
    for event in events:
        key = (event.station_id, event.measurement_type, event.observation_time)
        previous = unique.get(key)
        if previous is not None:
            duplicates += 1
            logger.warning(
                "duplicate source row station_id=%s measurement_type=%s observation_time=%s previous_value=%s value=%s",
                event.station_id,
                event.measurement_type,
                event.observation_time.isoformat(),
                previous.value,
                event.value,
            )
        unique[key] = event
    return list(unique.values()), duplicates


def _retry(stop, settings: Settings, action, description: str, is_retryable):
    delay = settings.retry_backoff_seconds
    for attempt in range(1, settings.max_attempts + 1):
        if stop.is_set():
            raise PollStopped()
        try:
            return action()
        except PollStopped:
            raise
        except HubeauPageLimitError:
            raise
        except HubeauResponseError:
            raise
        except Exception as exc:
            if not is_retryable(exc) or attempt == settings.max_attempts:
                raise
            logger.warning(
                "%s failed attempt=%s/%s error=%s",
                description,
                attempt,
                settings.max_attempts,
                exc,
            )
            _wait(stop, delay)
            delay = min(delay * 2, 30)


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
            logger.warning("second signal %s received; exiting now", signum)
            raise SystemExit(1)
        logger.info("signal %s received; stopping after the current poll", signum)
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_signal)

    client = HubeauClient(settings.base_url)
    producer = Producer({"bootstrap.servers": settings.bootstrap_servers})
    logger.info(
        "polling %s every %s seconds",
        settings.station_code,
        settings.poll_interval_seconds,
    )
    try:
        while not stop.is_set():
            try:
                poll_once(client, producer, settings, stop)
            except PollStopped:
                break
            except Exception:
                logger.exception("poll failed")
            if stop.is_set():
                break
            logger.info("next poll in %s seconds", settings.poll_interval_seconds)
            _wait(stop, settings.poll_interval_seconds)
    finally:
        producer.flush(timeout=30)
        client.close()
        logger.info("producer stopped")


if __name__ == "__main__":
    run()
