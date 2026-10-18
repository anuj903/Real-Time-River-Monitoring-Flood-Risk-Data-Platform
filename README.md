# Real-Time River Monitoring & Flood Risk Data Platform

**Current stage:** ingestion pipeline working.

A near-real-time data platform that collects river observations from France's Hub'Eau Hydrometry API, publishes them through Apache Kafka, and stores them in TimescaleDB for time-series analysis and later flood-risk monitoring.

```text
Hub'Eau API → Python producer → Kafka → Python consumer → TimescaleDB
```

Flood prediction, risk classification, dashboards, and cloud deployment are later stages. They are not part of the working pipeline.

## Objectives

The engineering goal is to design, run, and eventually deploy a streaming data pipeline: reliable delivery, retries, recovery, deduplication, and time-series storage.

The domain goal is to collect water-level observations, keep history, and later add discharge, trends, and flood-risk indicators.

## Architecture

| Step | Component | Role |
| --- | --- | --- |
| 1 | Hub'Eau Hydrometry API | River observations: water height (`H`) and, later, discharge (`Q`) |
| 2 | Python producer | Fetches readings, normalizes them, publishes JSON |
| 3 | Apache Kafka | Topic `water.observations.raw`; event log between producer and consumer |
| 4 | Python consumer | Validates messages and writes them in one database transaction |
| 5 | `observation_events` | Every Kafka delivery, for audit |
| 6 | `observations` | One canonical row per station, measurement type, and observation time |

Kafka moves and retains events. TimescaleDB is the queryable store. The producer and consumer are separate processes, so one can restart while the other keeps running.

## Status

| Component | Status | Purpose |
| --- | --- | --- |
| Hub'Eau API client | Implemented | Source of river measurements |
| Python producer | Running | Fetches observations and publishes events |
| Kafka in Docker | Running | Buffers and retains messages |
| Python consumer | Running | Reads Kafka and writes TimescaleDB |
| TimescaleDB in Docker | Running | Raw deliveries and canonical observations |
| DBeaver | Working | Schema inspection and SQL |
| Spark Structured Streaming | Planned | Distributed transformations |
| Google Cloud Storage | Planned | Cloud data lake |
| FastAPI | Planned | Query API |
| Plotly Dash | Planned | Monitoring dashboard |
| Airflow | Planned | Backfills and scheduled quality jobs |
| Cloud deployment | Planned | Long-running hosted pipeline |

## Stack

| Technology | Role |
| --- | --- |
| Python | Producer, consumer, and tests |
| Hub'Eau `observations_tr` | External REST source |
| Apache Kafka 4.1.0 (KRaft) | Event streaming |
| `confluent-kafka` | Python Kafka client |
| Docker Compose | Kafka and TimescaleDB |
| PostgreSQL 17 | Relational engine |
| TimescaleDB 2.30.0 | Hypertables and 7-day chunks |
| DBeaver | SQL client |
| `httpx` | API requests |
| `python-dotenv` | Environment variables |
| `psycopg` | TimescaleDB access |

Versions are the ones in this setup, not necessarily the latest releases.

## Data source

| Setting | Value |
| --- | --- |
| Station | `Y251002001`, L'Orb à Bédarieux |
| Measurement | `H` (water height) |
| Unit | `mm` |
| Source | `hubeau` |
| Lookback | Last 60 minutes |
| Poll interval | 60 seconds |

`Q` (discharge, typically litres per second) can be added after the source response and unit handling are confirmed. Hub'Eau's refresh rate is not the same as the station's measurement frequency.

## Kafka event

The producer publishes one JSON object per reading. The message key is `station_id`, so readings for one station stay on the same partition.

| Field | Meaning |
| --- | --- |
| `station_id` | Monitoring station |
| `observation_time` | When the measurement was recorded |
| `measurement_type` | `H` today |
| `value` | Numeric reading |
| `unit` | `mm` |
| `source` | `hubeau` |
| `ingested_at` | When the producer fetched the reading |

Timestamps are timezone-aware and stored as `timestamptz`.

Each poll requests the previous 60 minutes, so the same reading is published again on the next poll with a new `ingested_at`. That overlap is expected. Downstream deduplication keeps a single canonical row.

A Hub'Eau timeout is logged and the next poll continues. There is no durable producer checkpoint yet, so a long stop can leave a gap for readings older than the 60-minute window. Kafka can replay messages it already has. It cannot recover observations the producer never published.

## Kafka

| Setting | Value |
| --- | --- |
| Container | `water-kafka` |
| Image | `apache/kafka:4.1.0` |
| Topic | `water.observations.raw` |
| Partitions | 3 |
| Replication factor | 1 |
| Host port | `9092` |
| Mode | KRaft |

Replication factor 1 fits local development and does not provide broker redundancy.

## TimescaleDB

| Setting | Value |
| --- | --- |
| Container | `water-timescaledb` |
| Image | `timescale/timescaledb:2.30.0-pg17` |
| Host | `localhost` |
| Host port | `5433` |
| Database | `water` |
| User | from `.env` |

Host port `5432` was already taken by a local PostgreSQL server, so Docker publishes TimescaleDB on `5433`. Inside Compose, the database still listens on `5432`.

Keep credentials in `.env`. Do not commit that file.

### `observation_events`

Audit log of each Kafka delivery. Hypertable on `ingested_at`, 7-day chunks.

Primary key: `(topic, kafka_partition, kafka_offset, ingested_at)`. The time column has to be part of the unique key. Secondary index: `(station_id, observation_time)`.

Two Kafka messages for the same physical reading can both be stored here.

### `observations`

Canonical series for queries. Hypertable on `observation_time`, 7-day chunks.

Primary key: `(station_id, measurement_type, observation_time)`.

Upsert rules:

- A newer `ingested_at` updates `value`, `unit`, `source`, and `ingested_at`.
- An older delivery does not replace a newer one.
- `first_ingested_at` is set on the first insert and then left unchanged.
- Repeated deliveries do not create extra canonical rows.

`ingested_at` is when the producer fetched the reading, not when Hub'Eau last revised it.

Compression and retention policies are not enabled yet.

## Consumer

Group id: `water-observations-bronze`. Auto-commit is off.

1. Read a message from `water.observations.raw`.
2. Validate the JSON.
3. In one transaction, insert `observation_events` with `ON CONFLICT DO NOTHING`, then upsert `observations`.
4. Commit the Kafka offset only after that transaction commits.

A duplicate raw insert still runs the canonical upsert. A database failure is retried and the offset is left uncommitted. An invalid payload is logged and its offset is committed so one bad message does not block the partition. Shutdown stops after the current message and does not commit an unfinished write.

This is at-least-once processing with idempotent writes, not end-to-end exactly-once delivery.

## Local setup

Requirements: Docker Desktop, Python 3.9+, and a SQL client such as DBeaver.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
copy .env.example .env
docker compose up -d
```

Apply the schema in this order:

```powershell
$docker = "C:\Users\it\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe"
foreach ($file in @(
  "sql\schemas\001_timescaledb_extension.sql",
  "sql\tables\001_observation_events.sql",
  "sql\tables\002_observations.sql"
)) {
  Get-Content -Raw $file | & $docker exec -i -e PGPASSWORD=water water-timescaledb psql -U water -d water -v ON_ERROR_STOP=1
}
```

`docker` may not be on `PATH`. The executable is often:

`C:\Users\it\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe`

DBeaver connection: host `localhost`, port `5433`, database `water`, user and password from `.env`.

Run the applications from the project root, in separate terminals:

```powershell
.\.venv\Scripts\python.exe -m src.producer.producer
.\.venv\Scripts\python.exe -m src.bronze.consumer
```

Closing a Python terminal stops that process only. Kafka and TimescaleDB keep running until you stop the Compose services.

```sql
SELECT station_id, observation_time, measurement_type, value, unit
FROM observations
ORDER BY observation_time;
```

## Project layout

```text
realtime-water-data-platform/
├── src/
│   ├── producer/          # Hub'Eau client and Kafka producer
│   ├── bronze/            # Kafka consumer
│   ├── database/          # TimescaleDB connection and writes
│   ├── streaming/         # Spark placeholders
│   └── common/
├── config/config.yaml
├── sql/
│   ├── schemas/
│   └── tables/
├── tests/
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -t .
```

The tests cover event parsing, offset commit after a successful write, and the canonical upsert. Crash recovery, pagination, and historical backfill are still open.

## Still open

- Durable producer checkpoints and catch-up after downtime.
- Hub'Eau pagination and historical backfill.
- Kafka volume and retention across a full Docker shutdown.
- Recovery tests for consumer crashes and database outages.
- Metrics and data-quality checks.
- Discharge (`Q`) once units and source behavior are confirmed.

## Planned next

1. Make ingestion recoverable: checkpoints, catch-up, pagination, and tested restarts.
2. Add data-quality checks for timestamps, units, missing values, and unexpected readings.
3. Add a cloud Bronze/Silver/Gold lake when the local contracts are stable.
4. Add Spark only for transformations that need distributed processing.
5. Expose queries with FastAPI and a Plotly Dash view.
6. Use Airflow for backfills and scheduled checks, not as a replacement for the Kafka consumer.
7. Deploy with secrets, persistent storage, lag monitoring, and restart policy.
