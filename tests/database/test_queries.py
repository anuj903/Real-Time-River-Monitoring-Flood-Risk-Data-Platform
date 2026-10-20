import unittest
from datetime import datetime, timezone

from src.database.connection import database_settings_from_env
from src.database.queries import (
    INSERT_OBSERVATION_EVENT,
    UPSERT_OBSERVATION,
    ObservationRecord,
    write_observation,
)


class DatabaseSettingsTests(unittest.TestCase):
    def test_requires_credentials_without_echoing_a_secret(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            database_settings_from_env({"POSTGRES_PORT": "5433", "POSTGRES_DB": "water"})

        message = str(caught.exception)
        self.assertIn("POSTGRES_PASSWORD", message)
        self.assertNotIn("secret-password", message)

    def test_reads_host_port_and_database(self) -> None:
        settings = database_settings_from_env(
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "water",
                "POSTGRES_USER": "water",
                "POSTGRES_PASSWORD": "secret-password",
            }
        )

        self.assertEqual(settings.host, "localhost")
        self.assertEqual(settings.port, 5433)
        self.assertEqual(settings.dbname, "water")
        self.assertNotIn("secret-password", repr(settings))


class WriteObservationTests(unittest.TestCase):
    def test_canonical_upsert_runs_after_a_duplicate_raw_insert(self) -> None:
        connection = FakeConnection(rowcounts=[0, 1])
        record = _record()

        result = write_observation(
            connection,
            record,
            topic="water.observations.raw",
            kafka_partition=2,
            kafka_offset=10,
        )

        self.assertFalse(result.raw_inserted)
        self.assertTrue(result.canonical_written)
        self.assertEqual(len(connection._cursor.statements), 2)
        self.assertIn("ON CONFLICT (topic, kafka_partition, kafka_offset, ingested_at) DO NOTHING", connection._cursor.statements[0][0])
        self.assertIn("ON CONFLICT (station_id, measurement_type, observation_time)", connection._cursor.statements[1][0])
        self.assertTrue(connection.committed)


class ReplayTests(unittest.TestCase):
    def test_replaying_the_same_offset_runs_both_writes_again(self) -> None:
        connection = FakeConnection(rowcounts=[1, 1, 0, 0])
        record = _record()
        kwargs = {
            "topic": "water.observations.raw",
            "kafka_partition": 2,
            "kafka_offset": 10,
        }

        first = write_observation(connection, record, **kwargs)
        second = write_observation(connection, record, **kwargs)

        self.assertTrue(first.raw_inserted)
        self.assertTrue(first.canonical_written)
        self.assertFalse(second.raw_inserted)
        self.assertFalse(second.canonical_written)
        self.assertEqual(len(connection._cursor.statements), 4)
        self.assertEqual(connection._cursor.statements[0][1]["kafka_offset"], 10)
        self.assertEqual(connection._cursor.statements[2][1]["kafka_offset"], 10)

    def test_rolls_back_when_the_canonical_upsert_fails(self) -> None:
        connection = FakeConnection(rowcounts=[1], fail_on_call=2)

        with self.assertRaises(RuntimeError):
            write_observation(
                connection,
                _record(),
                topic="water.observations.raw",
                kafka_partition=2,
                kafka_offset=11,
            )

        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)

    def test_upsert_does_not_overwrite_first_ingested_at(self) -> None:
        update_clause = UPSERT_OBSERVATION.split("DO UPDATE SET", 1)[1]
        self.assertNotIn("first_ingested_at", update_clause)
        self.assertIn("observations.ingested_at < EXCLUDED.ingested_at", UPSERT_OBSERVATION)
        self.assertIn("ON CONFLICT", INSERT_OBSERVATION_EVENT)


class FakeCursor:
    def __init__(self, rowcounts, fail_on_call):
        self._rowcounts = list(rowcounts)
        self._fail_on_call = fail_on_call
        self.statements = []

    def execute(self, sql, params):
        self.statements.append((sql, params))
        if len(self.statements) == self._fail_on_call:
            raise RuntimeError("upsert failed")
        self.rowcount = self._rowcounts.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, rowcounts, fail_on_call=0):
        self._cursor = FakeCursor(rowcounts, fail_on_call)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False


def _record() -> ObservationRecord:
    return ObservationRecord(
        station_id="Y251002001",
        observation_time=datetime(2026, 10, 14, 4, 0, tzinfo=timezone.utc),
        measurement_type="H",
        value=-310.0,
        unit="mm",
        source="hubeau",
        ingested_at=datetime(2026, 10, 14, 4, 5, tzinfo=timezone.utc),
    )


if __name__ == "__main__":
    unittest.main()
