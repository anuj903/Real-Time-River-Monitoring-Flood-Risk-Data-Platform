from __future__ import annotations

import logging
from datetime import datetime


def log_event(logger: logging.Logger, level: int, event: str, *, exc_info: bool = False, **fields) -> None:
    """Emit one structured line. Timestamp, severity, and component come from the log record."""
    details = " ".join(f"{key}={_format_value(value)}" for key, value in fields.items() if value is not None)
    message = f"event={event} {details}" if details else f"event={event}"
    logger.log(level, "%s", message, exc_info=exc_info)


def _format_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        text = value.isoformat()
    elif isinstance(value, float):
        text = format(value, "g")
    else:
        text = str(value)
    text = " ".join(text.split())
    if text == "" or any(character in text for character in ' "='):
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return text
