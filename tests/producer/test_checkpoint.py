import unittest
from datetime import datetime, timedelta, timezone

from src.producer.producer import window_start


class WindowStartTests(unittest.TestCase):
    def test_uses_lookback_when_there_is_no_checkpoint(self) -> None:
        now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)

        start = window_start(now, lookback_minutes=60, catchup_max_age_days=30, checkpoint=None)

        self.assertEqual(start, now - timedelta(minutes=60))

    def test_catch_up_starts_at_the_checkpoint(self) -> None:
        now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
        checkpoint = now - timedelta(hours=5)

        start = window_start(now, lookback_minutes=60, catchup_max_age_days=30, checkpoint=checkpoint)

        self.assertEqual(start, checkpoint)

    def test_caps_catch_up_at_the_api_horizon(self) -> None:
        now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
        checkpoint = now - timedelta(days=40)

        start = window_start(now, lookback_minutes=60, catchup_max_age_days=30, checkpoint=checkpoint)

        self.assertEqual(start, now - timedelta(days=30))
