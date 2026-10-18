from __future__ import annotations

import logging
import signal
import threading
import time

from confluent_kafka import Consumer

from src.bronze.config import ConsumerSettings, load_consumer_settings
from src.bronze.messages import parse_event
from src.common.logging_config import configure_logging
from src.database.connection import DatabaseSettings, connect, load_database_settings
from src.database.queries import ObservationRecord, WriteResult, write_observation

logger = logging.getLogger(__name__)


def kafka_consumer_config(settings: ConsumerSettings) -> dict:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": settings.auto_offset_reset,
    }


def persist_and_commit(consumer: Consumer, conn, message, record: ObservationRecord) -> WriteResult:
    result = write_observation(
        conn,
        record,
        topic=message.topic(),
        kafka_partition=message.partition(),
        kafka_offset=message.offset(),
    )
    consumer.commit(message=message, asynchronous=False)
    return result


def run() -> None:
    configure_logging()
    settings = load_consumer_settings()
    database_settings = load_database_settings()
    stop = threading.Event()
    _install_signal_handlers(stop)

    consumer = Consumer(kafka_consumer_config(settings))
    consumer.subscribe([settings.topic])
    conn = connect(database_settings)
    logger.info(
        "consuming topic=%s group_id=%s postgres_host=%s postgres_port=%s postgres_db=%s",
        settings.topic,
        settings.group_id,
        database_settings.host,
        database_settings.port,
        database_settings.dbname,
    )
    try:
        while not stop.is_set():
            message = consumer.poll(settings.poll_timeout_seconds)
            if message is None:
                continue
            if message.error():
                logger.error("kafka error: %s", message.error())
                continue
            conn = _handle_message(consumer, conn, database_settings, settings, message, stop)
    finally:
        consumer.close()
        if conn is not None and not conn.closed:
            conn.close()
        logger.info("consumer stopped")


def _handle_message(consumer, conn, database_settings, settings, message, stop: threading.Event):
    try:
        record = parse_event(message.value())
    except ValueError as exc:
        logger.error(
            "invalid message topic=%s partition=%s offset=%s error=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            exc,
        )
        consumer.commit(message=message, asynchronous=False)
        return conn

    logger.info(
        "consumed topic=%s partition=%s offset=%s station_id=%s observation_time=%s measurement_type=%s value=%s unit=%s source=%s ingested_at=%s",
        message.topic(),
        message.partition(),
        message.offset(),
        record.station_id,
        record.observation_time.isoformat(),
        record.measurement_type,
        record.value,
        record.unit,
        record.source,
        record.ingested_at.isoformat(),
    )
    while not stop.is_set():
        try:
            conn = _ensure_connection(conn, database_settings)
            result = persist_and_commit(consumer, conn, message, record)
            _log_write(message, result)
            return conn
        except Exception:
            logger.exception(
                "database write failed topic=%s partition=%s offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            conn = _reconnect(conn, database_settings)
            _wait(stop, settings.retry_seconds)
    logger.info(
        "shutdown before offset commit topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )
    return conn


def _log_write(message, result: WriteResult) -> None:
    logger.info(
        "stored topic=%s partition=%s offset=%s raw_inserted=%s canonical_written=%s",
        message.topic(),
        message.partition(),
        message.offset(),
        result.raw_inserted,
        result.canonical_written,
    )
    if not result.raw_inserted:
        logger.info(
            "duplicate raw delivery topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def _ensure_connection(conn, settings: DatabaseSettings):
    if conn is None or conn.closed:
        return connect(settings)
    return conn


def _reconnect(conn, settings: DatabaseSettings):
    if conn is not None and not conn.closed:
        try:
            conn.rollback()
        except Exception:
            logger.exception("rollback failed")
        try:
            conn.close()
        except Exception:
            logger.exception("closing database connection failed")
    try:
        return connect(settings)
    except Exception:
        logger.exception("database reconnect failed")
        return None


def _wait(stop: threading.Event, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not stop.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            time.sleep(min(0.5, remaining))
        except InterruptedError:
            continue


def _install_signal_handlers(stop: threading.Event) -> None:
    def handle_signal(signum, _frame) -> None:
        if stop.is_set():
            logger.warning("second signal %s received; exiting now", signum)
            raise SystemExit(1)
        logger.info("signal %s received; stopping after the current message", signum)
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_signal)


if __name__ == "__main__":
    run()
