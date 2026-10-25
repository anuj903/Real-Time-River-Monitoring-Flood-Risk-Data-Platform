from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Mapping

import yaml
from dotenv import load_dotenv

from src.common.quality import future_tolerance_from_config, measurement_units_from_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


@dataclass(frozen=True)
class Settings:
    base_url: str
    station_code: str
    grandeur_hydro: str
    unit: str
    lookback_minutes: int
    page_size: int
    bootstrap_servers: str
    topic: str
    poll_interval_seconds: float
    checkpoint_path: str
    max_attempts: int
    retry_backoff_seconds: float
    max_pages: int
    catchup_max_age_days: int
    source: str = "hubeau"
    measurement_units: Mapping[str, str] = field(default_factory=lambda: {"H": "mm"})
    future_timestamp_tolerance: timedelta = timedelta(seconds=300)


def load_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    hubeau = raw["hubeau"]
    kafka = raw["kafka"]
    producer = raw["producer"]
    measurement_units = measurement_units_from_config(raw)
    grandeur_hydro = str(hubeau["grandeur_hydro"]).strip()
    unit = str(hubeau["unit"]).strip()
    if measurement_units.get(grandeur_hydro) != unit:
        raise RuntimeError(
            f"configured unit {unit!r} does not match measurements[{grandeur_hydro!r}]"
        )
    return Settings(
        base_url=hubeau["base_url"],
        station_code=hubeau["station_code"],
        grandeur_hydro=grandeur_hydro,
        unit=unit,
        measurement_units=measurement_units,
        future_timestamp_tolerance=future_tolerance_from_config(raw),
        lookback_minutes=int(hubeau["lookback_minutes"]),
        page_size=int(hubeau["page_size"]),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"]),
        topic=kafka["topic"],
        poll_interval_seconds=float(producer["poll_interval_seconds"]),
        checkpoint_path=producer["checkpoint_path"],
        max_attempts=int(producer["max_attempts"]),
        retry_backoff_seconds=float(producer["retry_backoff_seconds"]),
        max_pages=int(producer["max_pages"]),
        catchup_max_age_days=int(producer["catchup_max_age_days"]),
    )
