from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from pydantic import BaseModel

from src.common.logging_config import configure_logging
from src.producer.api_client import HubeauClient
from src.producer.config import Settings, load_settings

logger = logging.getLogger(__name__)


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


def poll_once(client: HubeauClient, producer: Producer, settings: Settings) -> int:
    ingested_at = datetime.now(timezone.utc)
    window_start = ingested_at - timedelta(minutes=settings.lookback_minutes)
    rows = client.fetch_observations(
        station_code=settings.station_code,
        grandeur_hydro=settings.grandeur_hydro,
        date_debut_obs=window_start,
        page_size=settings.page_size,
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
        window_start.isoformat(),
        skipped,
    )
    if not events:
        return 0

    published = publish(producer, settings, events)
    logger.info("published %s message(s) to %s", published, settings.topic)
    return published


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
                poll_once(client, producer, settings)
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
