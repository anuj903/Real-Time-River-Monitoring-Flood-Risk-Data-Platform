from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

INSERT_OBSERVATION_EVENT = """
INSERT INTO observation_events (
    topic,
    kafka_partition,
    kafka_offset,
    station_id,
    observation_time,
    measurement_type,
    value,
    unit,
    source,
    ingested_at
) VALUES (
    %(topic)s,
    %(kafka_partition)s,
    %(kafka_offset)s,
    %(station_id)s,
    %(observation_time)s,
    %(measurement_type)s,
    %(value)s,
    %(unit)s,
    %(source)s,
    %(ingested_at)s
)
ON CONFLICT (topic, kafka_partition, kafka_offset, ingested_at) DO NOTHING
"""

UPSERT_OBSERVATION = """
INSERT INTO observations (
    station_id,
    measurement_type,
    observation_time,
    value,
    unit,
    source,
    ingested_at,
    first_ingested_at
) VALUES (
    %(station_id)s,
    %(measurement_type)s,
    %(observation_time)s,
    %(value)s,
    %(unit)s,
    %(source)s,
    %(ingested_at)s,
    %(ingested_at)s
)
ON CONFLICT (station_id, measurement_type, observation_time)
DO UPDATE SET
    value = EXCLUDED.value,
    unit = EXCLUDED.unit,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at
WHERE observations.ingested_at < EXCLUDED.ingested_at
"""


@dataclass(frozen=True)
class ObservationRecord:
    station_id: str
    observation_time: datetime
    measurement_type: str
    value: float
    unit: str
    source: str
    ingested_at: datetime


@dataclass(frozen=True)
class WriteResult:
    raw_inserted: bool
    canonical_written: bool


def write_observation(
    conn,
    record: ObservationRecord,
    *,
    topic: str,
    kafka_partition: int,
    kafka_offset: int,
) -> WriteResult:
    """Insert the raw delivery and upsert the canonical row in one transaction."""
    params = {
        "topic": topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "station_id": record.station_id,
        "observation_time": record.observation_time,
        "measurement_type": record.measurement_type,
        "value": record.value,
        "unit": record.unit,
        "source": record.source,
        "ingested_at": record.ingested_at,
    }
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.execute(INSERT_OBSERVATION_EVENT, params)
            raw_inserted = cursor.rowcount == 1
            cursor.execute(UPSERT_OBSERVATION, params)
            canonical_written = cursor.rowcount == 1
    return WriteResult(raw_inserted=raw_inserted, canonical_written=canonical_written)
