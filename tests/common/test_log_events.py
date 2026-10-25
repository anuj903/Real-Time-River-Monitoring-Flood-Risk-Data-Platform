import logging
import unittest

from src.common.log_events import log_event


class LogEventTests(unittest.TestCase):
    def test_event_line_keeps_context_in_fields(self) -> None:
        logger = logging.getLogger("src.common.log_events")

        with self.assertLogs(logger, level="INFO") as logs:
            log_event(
                logger,
                logging.INFO,
                "poll_published",
                messages_published=2,
                checkpoint_advanced=True,
                password=None,
                error="broker down",
            )

        message = logs.output[0].split(":", 2)[2]
        self.assertTrue(message.startswith("event=poll_published"))
        self.assertIn("messages_published=2", message)
        self.assertIn("checkpoint_advanced=true", message)
        self.assertIn('error="broker down"', message)
        self.assertNotIn("password", message)


if __name__ == "__main__":
    unittest.main()
