import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from src.bronze.config import load_consumer_settings
from src.bronze.consumer import PartitionRevoked, _handle_message, kafka_consumer_config, persist_and_commit
from src.bronze.messages import parse_event
from src.database.queries import WriteResult


def event_payload(**overrides) -> bytes:
    body = {
        "station_id": "Y251002001",
        "observation_time": "2026-10-14T04:00:00Z",
        "measurement_type": "H",
        "value": -310.0,
        "unit": "mm",
        "source": "hubeau",
        "ingested_at": "2026-10-14T04:05:00Z",
    }
    body.update(overrides)
    return json.dumps(body).encode()


def parse(payload: bytes, **overrides):
    arguments = {
        "measurement_units": {"H": "mm"},
        "future_tolerance": timedelta(minutes=5),
        "now": datetime(2026, 10, 14, 5, 0, tzinfo=timezone.utc),
    }
    arguments.update(overrides)
    return parse_event(payload, **arguments)


class ParseEventTests(unittest.TestCase):
    def test_parses_producer_event(self) -> None:
        record = parse(event_payload())

        self.assertEqual(record.station_id, "Y251002001")
        self.assertEqual(record.measurement_type, "H")
        self.assertEqual(record.value, -310.0)
        self.assertEqual(record.observation_time, datetime(2026, 10, 14, 4, 0, tzinfo=timezone.utc))

    def test_rejects_invalid_json(self) -> None:
        with self.assertRaises(ValueError):
            parse(b"not-json")

    def test_rejects_missing_field(self) -> None:
        payload = json.loads(event_payload())
        del payload["station_id"]
        with self.assertRaises(ValueError):
            parse(json.dumps(payload).encode())

    def test_rejects_blank_required_strings(self) -> None:
        for field_name in ("station_id", "measurement_type", "unit", "source"):
            with self.subTest(field_name=field_name):
                with self.assertRaises(ValueError) as caught:
                    parse(event_payload(**{field_name: "  "}))
                self.assertIn("blank", str(caught.exception))

    def test_rejects_unsupported_measurement_type(self) -> None:
        with self.assertRaises(ValueError) as caught:
            parse(event_payload(measurement_type="Q", unit="l/s"))
        self.assertIn("unsupported measurement_type", str(caught.exception))

    def test_accepts_a_measurement_type_added_to_the_catalog(self) -> None:
        record = parse(
            event_payload(measurement_type="Q", unit="l/s"),
            measurement_units={"H": "mm", "Q": "l/s"},
        )
        self.assertEqual(record.measurement_type, "Q")
        self.assertEqual(record.unit, "l/s")

    def test_rejects_mismatched_unit(self) -> None:
        with self.assertRaises(ValueError) as caught:
            parse(event_payload(unit="m"))
        self.assertIn("does not match", str(caught.exception))

    def test_rejects_non_numeric_value(self) -> None:
        with self.assertRaises(ValueError) as caught:
            parse(event_payload(value="high"))
        self.assertIn("finite number", str(caught.exception))

    def test_rejects_non_finite_values(self) -> None:
        for token in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(token=token):
                payload = event_payload().decode().replace("-310.0", token).encode()
                with self.assertRaises(ValueError) as caught:
                    parse(payload)
                self.assertIn("finite number", str(caught.exception))

    def test_rejects_malformed_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            parse(event_payload(observation_time="not-a-timestamp"))

    def test_rejects_observation_beyond_the_future_tolerance(self) -> None:
        with self.assertRaises(ValueError) as caught:
            parse(event_payload(observation_time="2026-10-14T05:05:01Z"))
        self.assertIn("future-timestamp tolerance", str(caught.exception))

    def test_accepts_observation_inside_the_future_tolerance(self) -> None:
        record = parse(event_payload(observation_time="2026-10-14T05:05:00Z"))
        self.assertEqual(record.observation_time, datetime(2026, 10, 14, 5, 5, tzinfo=timezone.utc))

    def test_rejects_naive_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            parse(event_payload(observation_time="2026-10-14T04:00:00"))


class PersistAndCommitTests(unittest.TestCase):
    def test_commits_only_after_the_database_write(self) -> None:
        order = []
        consumer = Mock()
        consumer.commit.side_effect = lambda **kwargs: order.append("commit")
        connection = Mock()
        message = Mock()
        record = parse(event_payload())

        def write(_conn, _record, **_kwargs):
            order.append("write")
            return WriteResult(raw_inserted=True, canonical_written=True)

        with patch("src.bronze.consumer._service_rebalance"):
            with patch("src.bronze.consumer.write_observation", write):
                result = persist_and_commit(consumer, connection, message, record, _owns())

        self.assertEqual(order, ["write", "commit"])
        self.assertTrue(result.raw_inserted)
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_does_not_commit_when_the_write_fails(self) -> None:
        consumer = Mock()
        message = Mock()
        record = parse(event_payload())

        with patch("src.bronze.consumer._service_rebalance"):
            with patch(
                "src.bronze.consumer.write_observation",
                side_effect=RuntimeError("database unavailable"),
            ):
                with self.assertRaises(RuntimeError):
                    persist_and_commit(consumer, Mock(), message, record, _owns())

        consumer.commit.assert_not_called()


class CommitFailureTests(unittest.TestCase):
    def test_commit_failure_after_successful_write_propagates(self) -> None:
        consumer = Mock()
        consumer.commit.side_effect = RuntimeError("commit failed")
        message = _message()
        record = parse(event_payload())
        order = []

        def write(_conn, _record, **_kwargs):
            order.append("write")
            return WriteResult(raw_inserted=True, canonical_written=True)

        with patch("src.bronze.consumer._service_rebalance"):
            with patch("src.bronze.consumer.write_observation", write):
                with self.assertRaises(RuntimeError):
                    persist_and_commit(consumer, Mock(), message, record, _owns())

        self.assertEqual(order, ["write"])
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_handle_message_retries_commit_after_the_write_succeeds(self) -> None:
        consumer = Mock()
        attempts = {"commit": 0}

        def commit(message, asynchronous):
            attempts["commit"] += 1
            if attempts["commit"] == 1:
                raise RuntimeError("commit failed")

        consumer.commit.side_effect = commit
        writes = {"count": 0}

        def write(_conn, _record, **_kwargs):
            writes["count"] += 1
            return WriteResult(raw_inserted=True, canonical_written=True)

        connection = Mock()
        connection.closed = False
        with patch("src.bronze.consumer._service_rebalance"):
            with patch("src.bronze.consumer.write_observation", write):
                with patch("src.bronze.consumer._reconnect", side_effect=lambda conn, _settings: conn):
                    _handle_message(
                        consumer,
                        connection,
                        Mock(),
                        _retry_settings(),
                        _message(),
                        threading.Event(),
                        _owns(),
                    )

        self.assertEqual(writes["count"], 2)
        self.assertEqual(attempts["commit"], 2)

    def test_revoked_partition_does_not_commit_after_the_write(self) -> None:
        consumer = Mock()
        message = _message()
        record = parse(event_payload())

        class LostAssignment:
            def owns(self, _message) -> bool:
                return False

            def topic_partitions(self):
                return []

        with patch("src.bronze.consumer._service_rebalance"):
            with patch("src.bronze.consumer.write_observation", return_value=WriteResult(True, True)):
                with self.assertRaises(PartitionRevoked):
                    persist_and_commit(consumer, Mock(), message, record, LostAssignment())

        consumer.commit.assert_not_called()


class RejectionLogTests(unittest.TestCase):
    def test_invalid_message_is_logged_and_its_offset_is_committed(self) -> None:
        consumer = Mock()
        message = _message()
        message.value.return_value = b"not-json"
        settings = _retry_settings()

        with self.assertLogs("src.bronze.consumer", level="ERROR") as logs:
            _handle_message(
                consumer,
                Mock(),
                Mock(),
                settings,
                message,
                threading.Event(),
                _owns(),
            )

        self.assertIn("event=message_rejected", logs.output[0])
        self.assertIn("topic=water.observations.raw", logs.output[0])
        self.assertIn("partition=2", logs.output[0])
        self.assertIn("offset=5", logs.output[0])
        self.assertIn("payload=not-json", logs.output[0])
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_database_write_failure_is_separate_from_a_commit_failure(self) -> None:
        consumer = Mock()
        stop = threading.Event()

        def write(_conn, _record, **_kwargs):
            stop.set()
            raise RuntimeError("database unavailable")

        with patch("src.bronze.consumer._service_rebalance"):
            with patch("src.bronze.consumer.write_observation", write):
                with patch("src.bronze.consumer._reconnect", side_effect=lambda conn, _settings: conn):
                    with self.assertLogs("src.bronze.consumer", level="ERROR") as logs:
                        _handle_message(
                            consumer,
                            Mock(closed=False),
                            Mock(),
                            _retry_settings(),
                            _message(),
                            stop,
                            _owns(),
                        )

        text = " ".join(logs.output)
        self.assertIn("event=database_write_failed", text)
        self.assertIn("offset=5", text)
        self.assertNotIn("event=offset_commit_failed", text)
        consumer.commit.assert_not_called()

    def test_commit_failure_is_separate_from_a_database_write_failure(self) -> None:
        consumer = Mock()
        attempts = {"commit": 0}

        def commit(message, asynchronous):
            attempts["commit"] += 1
            if attempts["commit"] == 1:
                raise RuntimeError("coordinator unavailable")

        consumer.commit.side_effect = commit
        connection = Mock()
        connection.closed = False

        with patch("src.bronze.consumer._service_rebalance"):
            with patch(
                "src.bronze.consumer.write_observation",
                return_value=WriteResult(raw_inserted=False, canonical_written=True),
            ):
                with self.assertLogs("src.bronze.consumer", level="INFO") as logs:
                    _handle_message(
                        consumer,
                        connection,
                        Mock(),
                        _retry_settings(),
                        _message(),
                        threading.Event(),
                        _owns(),
                    )

        text = " ".join(logs.output)
        self.assertIn("event=offset_commit_failed", text)
        self.assertIn("event=database_transaction_committed", text)
        self.assertIn("event=raw_duplicate", text)
        self.assertIn("event=canonical_updated", text)
        self.assertIn("event=offset_committed", text)
        self.assertNotIn("event=database_write_failed", text)

    def test_rebalance_logs_assignment_and_revocation(self) -> None:
        from confluent_kafka import TopicPartition

        from src.bronze.consumer import Assignment

        assignment = Assignment()
        partitions = [TopicPartition("water.observations.raw", 2)]
        with self.assertLogs("src.bronze.consumer", level="INFO") as logs:
            assignment.on_assign(Mock(), partitions)
            assignment.on_revoke(Mock(), partitions)

        text = " ".join(logs.output)
        self.assertIn("event=partitions_assigned", text)
        self.assertIn("event=partitions_revoked", text)
        self.assertIn("partitions=water.observations.raw:2", text)


def _owns():
    assignment = Mock()
    assignment.owns.return_value = True
    return assignment


def _message():
    message = Mock()
    message.topic.return_value = "water.observations.raw"
    message.partition.return_value = 2
    message.offset.return_value = 5
    message.value.return_value = event_payload()
    message.error.return_value = None
    return message


def _retry_settings():
    settings = Mock()
    settings.retry_seconds = 0
    settings.measurement_units = {"H": "mm"}
    settings.future_timestamp_tolerance = timedelta(minutes=5)
    return settings


class ConsumerSettingsTests(unittest.TestCase):
    def test_uses_existing_topic_and_configured_group(self) -> None:
        settings = load_consumer_settings()

        self.assertEqual(settings.topic, "water.observations.raw")
        self.assertEqual(settings.group_id, "water-observations-bronze")
        self.assertGreater(settings.poll_timeout_seconds, 0)
        self.assertFalse(kafka_consumer_config(settings)["enable.auto.commit"])
        self.assertEqual(kafka_consumer_config(settings)["max.poll.interval.ms"], 900000)
        self.assertEqual(settings.measurement_units["H"], "mm")
        self.assertEqual(settings.future_timestamp_tolerance, timedelta(seconds=300))


if __name__ == "__main__":
    unittest.main()
