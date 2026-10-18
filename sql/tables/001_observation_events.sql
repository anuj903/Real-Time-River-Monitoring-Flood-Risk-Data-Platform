CREATE TABLE IF NOT EXISTS observation_events (
    topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    station_id text NOT NULL,
    observation_time timestamptz NOT NULL,
    measurement_type text NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    source text NOT NULL,
    ingested_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, kafka_partition, kafka_offset, ingested_at)
);

SELECT create_hypertable(
    'observation_events',
    by_range('ingested_at', INTERVAL '7 days'),
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS observation_events_station_observation_time_idx
    ON observation_events (station_id, observation_time);
