CREATE TABLE IF NOT EXISTS observations (
    station_id text NOT NULL,
    measurement_type text NOT NULL,
    observation_time timestamptz NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    source text NOT NULL,
    ingested_at timestamptz NOT NULL,
    first_ingested_at timestamptz NOT NULL,
    PRIMARY KEY (station_id, measurement_type, observation_time)
);

SELECT create_hypertable(
    'observations',
    by_range('observation_time', INTERVAL '7 days'),
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);
