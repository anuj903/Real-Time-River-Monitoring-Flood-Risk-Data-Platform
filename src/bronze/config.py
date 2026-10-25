from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Mapping

import yaml
from dotenv import load_dotenv

from src.common.quality import future_tolerance_from_config, measurement_units_from_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


@dataclass(frozen=True)
class ConsumerSettings:
    bootstrap_servers: str
    topic: str
    group_id: str
    poll_timeout_seconds: float
    retry_seconds: float
    auto_offset_reset: str
    max_poll_interval_ms: int
    measurement_units: Mapping[str, str]
    future_timestamp_tolerance: timedelta


def load_consumer_settings() -> ConsumerSettings:
    load_dotenv(PROJECT_ROOT / ".env")
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    kafka = raw["kafka"]
    consumer = raw["consumer"]
    return ConsumerSettings(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"]),
        topic=kafka["topic"],
        group_id=consumer["group_id"],
        poll_timeout_seconds=float(consumer["poll_timeout_seconds"]),
        retry_seconds=float(consumer["retry_seconds"]),
        auto_offset_reset=consumer["auto_offset_reset"],
        max_poll_interval_ms=int(consumer["max_poll_interval_ms"]),
        measurement_units=measurement_units_from_config(raw),
        future_timestamp_tolerance=future_tolerance_from_config(raw),
    )
