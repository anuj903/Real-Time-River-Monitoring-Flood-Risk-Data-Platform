from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Mapping


def measurement_units_from_config(raw: Mapping) -> dict[str, str]:
    """Return the configured measurement type to unit catalog.

    Adding a type such as discharge is a config change. Callers must not assume
    the catalog contains only water height.
    """
    measurements = raw.get("measurements")
    if not isinstance(measurements, dict) or not measurements:
        raise RuntimeError("config measurements must map at least one measurement type to its unit")

    units: dict[str, str] = {}
    for measurement_type, unit in measurements.items():
        name = str(measurement_type).strip()
        unit_text = str(unit).strip()
        if not name or not unit_text:
            raise RuntimeError("measurement type and unit must be non-blank")
        units[name] = unit_text
    return units


def future_tolerance_from_config(raw: Mapping) -> timedelta:
    quality = raw.get("quality") or {}
    if not isinstance(quality, dict):
        raise RuntimeError("config quality must be a mapping")
    seconds = float(quality.get("future_timestamp_tolerance_seconds", 300))
    if seconds < 0 or not math.isfinite(seconds):
        raise RuntimeError("future_timestamp_tolerance_seconds must be a finite number >= 0")
    return timedelta(seconds=seconds)


def require_timezone(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def require_finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{field_name} must be a finite number")
    return float(value)


def require_measurement_unit(
    measurement_type: str,
    unit: str,
    measurement_units: Mapping[str, str],
) -> None:
    expected = measurement_units.get(measurement_type)
    if expected is None:
        supported = ", ".join(sorted(measurement_units)) or "(none)"
        raise ValueError(f"unsupported measurement_type '{measurement_type}'; supported: {supported}")
    if unit != expected:
        raise ValueError(
            f"unit '{unit}' does not match measurement_type '{measurement_type}' (expected {expected})"
        )


def require_not_future(value: datetime, field_name: str, *, now: datetime, tolerance: timedelta) -> None:
    if value > now + tolerance:
        raise ValueError(f"{field_name} is beyond the future-timestamp tolerance")
