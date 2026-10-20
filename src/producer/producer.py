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
from src.producer.api_client import HubeauClient, HubeauPageLimitError
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
    observed_at = row.get("date_obs")
    value = row.get("resultat_obs")
    if not observed_at or value is None:
        return None
    return WaterObservation(
        station_id=row["code_station"],
        observation_time=datetime.fromisoformat(observed_at.replace("Z", "+00:00")),
        measurement_type=row["grandeur_hydro"],
        value=float(value),
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
    events.sort(key=lambda event: event.observation_time)

    logger.info(
        "fetched %s observation(s) for %s since %s; skipped %s incomplete row(s)",
        len(events),
        settings.station_code,
        start.isoformat(),
        skipped,
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
