from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

from src.bronze.config import ConsumerSettings, load_consumer_settings
from src.bronze.messages import parse_event
from src.common.log_events import log_event
from src.common.logging_config import configure_logging
from src.database.connection import DatabaseSettings, connect, load_database_settings
from src.database.queries import ObservationRecord, WriteResult, write_observation
from src.monitoring.health import PipelineMonitor

logger = logging.getLogger(__name__)


class PartitionRevoked(Exception):
    """The message's partition was lost before its offset could be committed."""


class Assignment:
    def __init__(self) -> None:
        self._partitions: dict[tuple[str, int], TopicPartition] = {}
        self.pending: list = []

    def on_assign(self, consumer, partitions) -> None:
        self._partitions = {(partition.topic, partition.partition): partition for partition in partitions}
        log_event(
            logger,
            logging.INFO,
            "partitions_assigned",
            partitions=",".join(sorted(f"{topic}:{partition}" for topic, partition in self._partitions)),
        )

    def on_revoke(self, consumer, partitions) -> None:
        for partition in partitions:
            self._partitions.pop((partition.topic, partition.partition), None)
        log_event(
            logger,
            logging.INFO,
            "partitions_revoked",
            partitions=",".join(sorted(f"{partition.topic}:{partition.partition}" for partition in partitions)),
        )

    def owns(self, message) -> bool:
        return (message.topic(), message.partition()) in self._partitions

    def topic_partitions(self) -> list[TopicPartition]:
        return list(self._partitions.values())


def kafka_consumer_config(settings: ConsumerSettings) -> dict:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": settings.auto_offset_reset,
        "max.poll.interval.ms": settings.max_poll_interval_ms,
    }


def persist_and_commit(
    consumer: Consumer,
    conn,
    message,
    record: ObservationRecord,
    assignment: Assignment,
) -> WriteResult:
    result = write_observation(
        conn,
        record,
        topic=message.topic(),
        kafka_partition=message.partition(),
        kafka_offset=message.offset(),
    )
    _service_rebalance(consumer, assignment)
    if not assignment.owns(message):
        raise PartitionRevoked(f"{message.topic()} [{message.partition()}]")
    consumer.commit(message=message, asynchronous=False)
    return result


def run() -> None:
    configure_logging()
    settings = load_consumer_settings()
    database_settings = load_database_settings()
    stop = threading.Event()
    _install_signal_handlers(stop)
    assignment = Assignment()

    consumer = Consumer(kafka_consumer_config(settings))
    consumer.subscribe(
        [settings.topic],
        on_assign=assignment.on_assign,
        on_revoke=assignment.on_revoke,
    )
    conn = connect(database_settings)
    monitor = PipelineMonitor("consumer")
    log_event(
        logger,
        logging.INFO,
        "consumer_started",
        topic=settings.topic,
        group_id=settings.group_id,
        postgres_host=database_settings.host,
        postgres_port=database_settings.port,
        postgres_db=database_settings.dbname,
    )
    try:
        while not stop.is_set():
            message = assignment.pending.pop(0) if assignment.pending else consumer.poll(settings.poll_timeout_seconds)
            if message is None:
                monitor.maybe_check()
                continue
            if message.error():
                log_event(logger, logging.ERROR, "kafka_error", error=message.error())
                continue
            conn = _handle_message(
                consumer,
                conn,
                database_settings,
                settings,
                message,
                stop,
                assignment,
                monitor,
            )
            monitor.maybe_check()
    finally:
        consumer.close()
        if conn is not None and not conn.closed:
            conn.close()
        log_event(logger, logging.INFO, "consumer_stopped")


def _handle_message(consumer, conn, database_settings, settings, message, stop: threading.Event, assignment: Assignment, monitor: PipelineMonitor | None = None):
    try:
        record = parse_event(
            message.value(),
            measurement_units=settings.measurement_units,
            future_tolerance=settings.future_timestamp_tolerance,
        )
    except ValueError as exc:
        log_event(
            logger,
            logging.ERROR,
            "message_rejected",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            reason=exc,
            payload=_payload_excerpt(message.value()),
        )
        if assignment.owns(message):
            consumer.commit(message=message, asynchronous=False)
        return conn

    log_event(
        logger,
        logging.INFO,
        "message_consumed",
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        station_id=record.station_id,
        observation_time=record.observation_time,
        measurement_type=record.measurement_type,
        value=record.value,
        unit=record.unit,
        source=record.source,
        ingested_at=record.ingested_at,
    )
    while not stop.is_set():
        try:
            conn = _ensure_connection(conn, database_settings)
            result = write_observation(
                conn,
                record,
                topic=message.topic(),
                kafka_partition=message.partition(),
                kafka_offset=message.offset(),
            )
        except Exception as exc:
            log_event(
                logger,
                logging.ERROR,
                "database_write_failed",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                error=exc,
                exc_info=True,
            )
            conn = _reconnect(conn, database_settings)
            _service_rebalance(consumer, assignment)
            if not assignment.owns(message):
                _log_revoked(message)
                return conn
            _wait(stop, settings.retry_seconds)
            continue
        _log_write(message, result)
        if monitor is not None:
            monitor.record_consumer_write(datetime.now(timezone.utc))
        try:
            _service_rebalance(consumer, assignment)
            if not assignment.owns(message):
                raise PartitionRevoked(f"{message.topic()} [{message.partition()}]")
            consumer.commit(message=message, asynchronous=False)
        except PartitionRevoked:
            _log_revoked(message)
            return conn
        except Exception as exc:
            log_event(
                logger,
                logging.ERROR,
                "offset_commit_failed",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                error=exc,
                exc_info=True,
            )
            _service_rebalance(consumer, assignment)
            if not assignment.owns(message):
                _log_revoked(message)
                return conn
            _wait(stop, settings.retry_seconds)
            continue
        log_event(
            logger,
            logging.INFO,
            "offset_committed",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )
        return conn
    log_event(
        logger,
        logging.INFO,
        "shutdown_before_commit",
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )
    return conn


def _service_rebalance(consumer, assignment: Assignment) -> None:
    """Let rebalance callbacks run without taking new records from paused partitions."""
    partitions = assignment.topic_partitions()
    if partitions:
        consumer.pause(partitions)
    try:
        while True:
            extra = consumer.poll(0)
            if extra is None:
                return
            assignment.pending.append(extra)
            log_event(
                logger,
                logging.INFO,
                "rebalance_message_queued",
                topic=extra.topic(),
                partition=extra.partition(),
                offset=extra.offset(),
            )
    finally:
        current = assignment.topic_partitions()
        if current:
            consumer.resume(current)


def _payload_excerpt(payload) -> str:
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="replace")
    elif payload is None:
        text = ""
    else:
        text = str(payload)
    text = " ".join(text.split())
    if len(text) > 500:
        return text[:500] + "..."
    return text


def _log_revoked(message) -> None:
    log_event(
        logger,
        logging.WARNING,
        "commit_skipped_partition_revoked",
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )


def _log_write(message, result: WriteResult) -> None:
    log_event(
        logger,
        logging.INFO,
        "database_transaction_committed",
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        raw_inserted=result.raw_inserted,
        canonical_written=result.canonical_written,
    )
    if not result.raw_inserted:
        log_event(
            logger,
            logging.INFO,
            "raw_duplicate",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )
    log_event(
        logger,
        logging.INFO,
        "canonical_updated" if result.canonical_written else "canonical_unchanged",
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )


def _ensure_connection(conn, settings: DatabaseSettings):
    if conn is None or conn.closed:
        return connect(settings)
    return conn


def _reconnect(conn, settings: DatabaseSettings):
    if conn is not None and not conn.closed:
        try:
            conn.rollback()
        except Exception as exc:
            log_event(logger, logging.ERROR, "database_rollback_failed", error=exc, exc_info=True)
        try:
            conn.close()
        except Exception as exc:
            log_event(logger, logging.ERROR, "database_close_failed", error=exc, exc_info=True)
    try:
        return connect(settings)
    except Exception as exc:
        log_event(logger, logging.ERROR, "database_reconnect_failed", error=exc, exc_info=True)
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
            log_event(logger, logging.WARNING, "shutdown_forced", signal=signum)
            raise SystemExit(1)
        log_event(logger, logging.INFO, "shutdown_requested", signal=signum, after="current_message")
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_signal)


if __name__ == "__main__":
    run()
