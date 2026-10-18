from __future__ import annotations

import json
from datetime import datetime

from pydantic import BaseModel, Field, ValidationError

from src.database.queries import ObservationRecord


class ObservationEvent(BaseModel):
    station_id: str = Field(min_length=1)
    observation_time: datetime
    measurement_type: str = Field(min_length=1)
    value: float
    unit: str = Field(min_length=1)
    source: str = Field(min_length=1)
    ingested_at: datetime


def parse_event(payload: bytes | str) -> ObservationRecord:
    try:
        body = json.loads(payload)
        event = ObservationEvent.model_validate(body)
    except (json.JSONDecodeError, ValidationError, TypeError) as exc:
        raise ValueError(str(exc)) from exc
    _require_timezone(event.observation_time, "observation_time")
    _require_timezone(event.ingested_at, "ingested_at")
    return ObservationRecord(
        station_id=event.station_id,
        observation_time=event.observation_time,
        measurement_type=event.measurement_type,
        value=event.value,
        unit=event.unit,
        source=event.source,
        ingested_at=event.ingested_at,
    )


def _require_timezone(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name} must include a timezone")
