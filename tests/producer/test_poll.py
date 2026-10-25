import tempfile
import threading
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from src.producer.checkpoint import load_checkpoint
from src.producer.config import Settings
from src.producer.api_client import HubeauResponseError
from src.producer.producer import PollStopped, poll_once


class PollOnceTests(unittest.TestCase):
    def test_checkpoint_advances_only_after_a_successful_flush(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "producer.json")
            settings = _settings(path)
            producer = FakeProducer()
            stop = threading.Event()
            observed = "2026-10-20T10:00:00Z"

            published = poll_once(_client([_row(observed)]), producer, settings, stop)

            self.assertEqual(published, 1)
            self.assertEqual(producer.flushes, 1)
            stored = load_checkpoint(Path(path), "Y251002001", "H")
            self.assertEqual(stored, datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc))

    def test_failed_publish_does_not_advance_the_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path), max_attempts=1)
            producer = FakeProducer(fail=True)

            with self.assertRaises(RuntimeError):
                poll_once(_client([_row("2026-10-20T10:00:00Z")]), producer, settings, threading.Event())

            self.assertFalse(path.exists())

    def test_failed_fetch_does_not_publish(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"), max_attempts=1)
            producer = FakeProducer()
            client = FakeClient(errors=[httpx.ReadTimeout("timed out")])

            with self.assertRaises(httpx.ReadTimeout):
                poll_once(client, producer, settings, threading.Event())

            self.assertEqual(producer.produced, 0)

    def test_transient_fetch_is_retried_inside_the_poll(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"), max_attempts=2)
            producer = FakeProducer()
            client = FakeClient(
                errors=[httpx.ReadTimeout("timed out")],
                rows=[_row("2026-10-20T11:00:00Z")],
            )

            published = poll_once(client, producer, settings, threading.Event())

            self.assertEqual(published, 1)
            self.assertEqual(len(client.calls), 2)
            self.assertEqual(producer.produced, 1)

    def test_catch_up_requests_from_the_saved_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path))
            checkpoint = datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)
            path.write_text(
                '{"Y251002001:H": "2026-10-18T08:00:00+00:00"}',
                encoding="utf-8",
            )
            client = FakeClient(rows=[_row("2026-10-20T11:00:00Z")])

            poll_once(client, FakeProducer(), settings, threading.Event(), now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc))

            self.assertEqual(client.calls[0]["date_debut_obs"], checkpoint)

    def test_shutdown_during_retry_does_not_publish(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"), max_attempts=3, retry_backoff_seconds=30)
            stop = threading.Event()
            stop.set()
            producer = FakeProducer()

            with self.assertRaises(PollStopped):
                poll_once(_client([_row("2026-10-20T11:00:00Z")]), producer, settings, stop)

            self.assertEqual(producer.produced, 0)

    def test_malformed_source_row_does_not_abort_the_poll(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path))
            producer = FakeProducer()
            rows = [
                {"date_obs": "not-a-timestamp", "resultat_obs": -1, "code_station": "Y251002001", "grandeur_hydro": "H"},
                _row("2026-10-20T10:00:00Z"),
            ]

            with self.assertLogs("src.producer.producer", level="WARNING") as logs:
                published = poll_once(
                    _client(rows),
                    producer,
                    settings,
                    threading.Event(),
                    now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(published, 1)
            self.assertIn("event=source_row_skipped", " ".join(logs.output))
            stored = load_checkpoint(path, "Y251002001", "H")
            self.assertEqual(stored, datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc))

    def test_duplicate_source_rows_publish_the_latest_value_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"))
            producer = FakeProducer()
            rows = [
                _row("2026-10-20T10:00:00Z", resultat_obs=-300.0),
                _row("2026-10-20T10:00:00Z", resultat_obs=-280.0),
            ]

            with self.assertLogs("src.producer.producer", level="WARNING") as logs:
                published = poll_once(
                    _client(rows),
                    producer,
                    settings,
                    threading.Event(),
                    now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(published, 1)
            self.assertEqual(producer.produced, 1)
            self.assertEqual(json.loads(producer.values[0])["value"], -280.0)
            self.assertIn("event=duplicate_source_row", " ".join(logs.output))

    def test_malformed_api_page_does_not_advance_the_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path), max_attempts=2)
            producer = FakeProducer()
            client = FakeClient(errors=[HubeauResponseError("Hub'Eau data must be a list")])

            with self.assertRaises(HubeauResponseError):
                poll_once(client, producer, settings, threading.Event())

            self.assertEqual(producer.produced, 0)
            self.assertEqual(len(client.calls), 1)
            self.assertFalse(path.exists())

    def test_published_poll_logs_counts_and_checkpoint_advancement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"))
            producer = FakeProducer()

            with self.assertLogs("src.producer.producer", level="INFO") as logs:
                poll_once(
                    _client([_row("2026-10-20T10:00:00Z")]),
                    producer,
                    settings,
                    threading.Event(),
                    now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
                )

            text = " ".join(logs.output)
            self.assertIn("event=poll_started", text)
            self.assertIn("event=poll_published", text)
            self.assertIn("pages=1", text)
            self.assertIn("observations_received=1", text)
            self.assertIn("rows_skipped=0", text)
            self.assertIn("duplicate_source_rows=0", text)
            self.assertIn("messages_published=1", text)
            self.assertIn("checkpoint_advanced=true", text)
            self.assertIn("duration_ms=", text)
            self.assertNotIn("event=poll_empty", text)
            self.assertNotIn("event=poll_failed", text)

    def test_empty_poll_is_distinct_from_a_published_poll(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path))

            with self.assertLogs("src.producer.producer", level="INFO") as logs:
                published = poll_once(_client([]), FakeProducer(), settings, threading.Event())

            text = " ".join(logs.output)
            self.assertEqual(published, 0)
            self.assertIn("event=poll_empty", text)
            self.assertIn("messages_published=0", text)
            self.assertIn("checkpoint_advanced=false", text)
            self.assertNotIn("event=poll_published", text)
            self.assertFalse(path.exists())

    def test_failed_poll_is_distinct_from_an_empty_poll(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.json"
            settings = _settings(str(path), max_attempts=1)

            with self.assertLogs("src.producer.producer", level="ERROR") as logs:
                with self.assertRaises(httpx.ReadTimeout):
                    poll_once(
                        FakeClient(errors=[httpx.ReadTimeout("timed out")]),
                        FakeProducer(),
                        settings,
                        threading.Event(),
                    )

            text = " ".join(logs.output)
            self.assertIn("event=poll_failed", text)
            self.assertIn("checkpoint_advanced=false", text)
            self.assertIn("messages_published=0", text)
            self.assertNotIn("event=poll_empty", text)
            self.assertNotIn("event=poll_published", text)
            self.assertFalse(path.exists())

    def test_retry_logs_the_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = _settings(str(Path(directory) / "producer.json"), max_attempts=2)
            client = FakeClient(errors=[httpx.ReadTimeout("timed out")], rows=[_row("2026-10-20T11:00:00Z")])

            with self.assertLogs("src.producer.producer", level="WARNING") as logs:
                poll_once(client, FakeProducer(), settings, threading.Event())

            text = " ".join(logs.output)
            self.assertIn("event=retry", text)
            self.assertIn("operation=hubeau_fetch", text)
            self.assertIn("attempt=1", text)
            self.assertIn("max_attempts=2", text)


def _settings(path: str, **overrides) -> Settings:
    values = dict(
        base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
        station_code="Y251002001",
        grandeur_hydro="H",
        unit="mm",
        lookback_minutes=60,
        page_size=500,
        bootstrap_servers="localhost:9092",
        topic="water.observations.raw",
        poll_interval_seconds=60,
        checkpoint_path=path,
        max_attempts=1,
        retry_backoff_seconds=0,
        max_pages=10,
        catchup_max_age_days=30,
    )
    values.update(overrides)
    return Settings(**values)


def _row(observed: str, **overrides) -> dict:
    row = {
        "code_station": "Y251002001",
        "date_obs": observed,
        "resultat_obs": -300.0,
        "grandeur_hydro": "H",
    }
    row.update(overrides)
    return row


def _client(rows):
    return FakeClient(rows=rows)


class FakeClient:
    def __init__(self, rows=None, errors=None):
        self.rows = rows or []
        self.errors = list(errors or [])
        self.calls = []
        self.pages_fetched = 0

    def fetch_observations(self, **kwargs):
        self.calls.append(kwargs)
        if self.errors:
            self.pages_fetched = 0
            raise self.errors.pop(0)
        self.pages_fetched = 1
        return self.rows


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.produced = 0
        self.flushes = 0
        self.values = []

    def produce(self, **kwargs) -> None:
        self.produced += 1
        self.values.append(kwargs.get("value"))

    def poll(self, _timeout) -> None:
        return None

    def flush(self, timeout) -> int:
        self.flushes += 1
        return 1 if self.fail else 0


if __name__ == "__main__":
    unittest.main()
