from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Mapping

from pydantic import BaseModel, Field, ValidationError, field_validator

from src.common.quality import require_finite, require_measurement_unit, require_not_future, require_timezone
from src.database.queries import ObservationRecord


class ObservationEvent(BaseModel):
    station_id: str = Field(min_length=1)
    observation_time: datetime
    measurement_type: str = Field(min_length=1)
    value: float
    unit: str = Field(min_length=1)
    source: str = Field(min_length=1)
    ingested_at: datetime

    @field_validator("station_id", "measurement_type", "unit", "source", mode="before")
    @classmethod
    def non_blank_text(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("must be a string")
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped

    @field_validator("value", mode="before")
    @classmethod
    def finite_value(cls, value: object) -> float:
        return require_finite(value, "value")

    @field_validator("observation_time", "ingested_at")
    @classmethod
    def timezone_aware(cls, value: datetime, info) -> datetime:
        return require_timezone(value, info.field_name)


def parse_event(
    payload: bytes | str,
    *,
    measurement_units: Mapping[str, str],
    future_tolerance: timedelta,
    now: datetime | None = None,
) -> ObservationRecord:
    clock = now if now is not None else datetime.now(timezone.utc)
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        body = json.loads(text, parse_constant=_json_constant)
        event = ObservationEvent.model_validate(body)
    except (json.JSONDecodeError, ValidationError, TypeError, UnicodeError, ValueError) as exc:
        raise ValueError(str(exc)) from exc

    require_measurement_unit(event.measurement_type, event.unit, measurement_units)
    require_not_future(event.observation_time, "observation_time", now=clock, tolerance=future_tolerance)
    return ObservationRecord(
        station_id=event.station_id,
        observation_time=event.observation_time,
        measurement_type=event.measurement_type,
        value=event.value,
        unit=event.unit,
        source=event.source,
        ingested_at=event.ingested_at,
    )


def _json_constant(name: str) -> float:
    constants = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
    try:
        return constants[name]
    except KeyError as exc:
        raise ValueError(f"invalid JSON number {name}") from exc
