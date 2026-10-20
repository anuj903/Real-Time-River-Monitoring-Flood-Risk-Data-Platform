import json
import threading
import unittest
from datetime import datetime, timezone
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


class ParseEventTests(unittest.TestCase):
    def test_parses_producer_event(self) -> None:
        record = parse_event(event_payload())

        self.assertEqual(record.station_id, "Y251002001")
        self.assertEqual(record.measurement_type, "H")
        self.assertEqual(record.value, -310.0)
        self.assertEqual(record.observation_time, datetime(2026, 10, 14, 4, 0, tzinfo=timezone.utc))

    def test_rejects_invalid_json(self) -> None:
        with self.assertRaises(ValueError):
            parse_event(b"not-json")

    def test_rejects_missing_field(self) -> None:
        payload = json.loads(event_payload())
        del payload["station_id"]
        with self.assertRaises(ValueError):
            parse_event(json.dumps(payload).encode())

    def test_rejects_naive_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            parse_event(event_payload(observation_time="2026-10-14T04:00:00"))


class PersistAndCommitTests(unittest.TestCase):
    def test_commits_only_after_the_database_write(self) -> None:
        order = []
        consumer = Mock()
        consumer.commit.side_effect = lambda **kwargs: order.append("commit")
        connection = Mock()
        message = Mock()
        record = parse_event(event_payload())

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
        record = parse_event(event_payload())

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
        record = parse_event(event_payload())
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
        record = parse_event(event_payload())

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
    return settings


class ConsumerSettingsTests(unittest.TestCase):
    def test_uses_existing_topic_and_configured_group(self) -> None:
        settings = load_consumer_settings()

        self.assertEqual(settings.topic, "water.observations.raw")
        self.assertEqual(settings.group_id, "water-observations-bronze")
        self.assertGreater(settings.poll_timeout_seconds, 0)
        self.assertFalse(kafka_consumer_config(settings)["enable.auto.commit"])
        self.assertEqual(kafka_consumer_config(settings)["max.poll.interval.ms"], 900000)


if __name__ == "__main__":
    unittest.main()
