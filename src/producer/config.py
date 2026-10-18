import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

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
    source: str = "hubeau"


def load_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    hubeau = raw["hubeau"]
    kafka = raw["kafka"]
    producer = raw["producer"]
    return Settings(
        base_url=hubeau["base_url"],
        station_code=hubeau["station_code"],
        grandeur_hydro=hubeau["grandeur_hydro"],
        unit=hubeau["unit"],
        lookback_minutes=int(hubeau["lookback_minutes"]),
        page_size=int(hubeau["page_size"]),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"]),
        topic=kafka["topic"],
        poll_interval_seconds=float(producer["poll_interval_seconds"]),
    )
