from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def checkpoint_key(station_code: str, grandeur_hydro: str) -> str:
    return f"{station_code}:{grandeur_hydro}"


def load_checkpoint(path: Path, station_code: str, grandeur_hydro: str) -> datetime | None:
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    value = raw.get(checkpoint_key(station_code, grandeur_hydro))
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def save_checkpoint(
    path: Path,
    station_code: str,
    grandeur_hydro: str,
    observation_time: datetime,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw: dict[str, str] = {}
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))
    stamp = observation_time.astimezone(timezone.utc)
    raw[checkpoint_key(station_code, grandeur_hydro)] = stamp.isoformat()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    temporary.replace(path)
