<<<<<<< HEAD
# Real-Time Air Quality Monitoring Pipeline

A real-time data engineering pipeline that pulls live PM2.5 air quality readings from the [OpenAQ API](https://openaq.org/) for 8 Indian cities, streams them through Kafka, processes them with Spark Structured Streaming, stores the results in PostgreSQL, orchestrates the whole ingestion step with Apache Airflow, and visualizes everything live in Grafana.

This project was built end-to-end as a hands-on way to learn the standard real-time data engineering stack: **ingestion → messaging → stream processing → storage → orchestration → visualization**.

---

## Table of contents

- [Architecture overview](#architecture-overview)
- [How data flows through the system, step by step](#how-data-flows-through-the-system-step-by-step)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Setup instructions](#setup-instructions)
- [Running the pipeline](#running-the-pipeline)
- [Airflow: how the scheduling works](#airflow-how-the-scheduling-works)
- [Grafana dashboard](#grafana-dashboard)
- [Notable design decisions](#notable-design-decisions)
- [Known limitations](#known-limitations)

---

## Architecture overview

```
                OpenAQ API (live PM2.5 sensor readings)
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │   Ingestion (choose one)               │
        │   • fetch_data.py  (manual/loop mode)  │
        │   • Airflow DAG    (scheduled mode)    │
        └───────────────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Kafka topic        │
                 │   "aqi-readings"     │
                 └─────────────────────┘
                            │
                            ▼
                 ┌─────────────────────────────┐
                 │   Spark Structured Streaming │
                 │   • parses JSON               │
                 │   • flags anomalies           │
                 └─────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   PostgreSQL         │
                 │   table: aqi_readings│
                 └─────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Grafana dashboard  │
                 │   (live PM2.5 chart) │
                 └─────────────────────┘
```

All the infrastructure (Kafka, Zookeeper, PostgreSQL, Grafana, Airflow) runs in Docker containers, managed by a single `docker-compose.yml`.

---

## How data flows through the system, step by step

### 1. Ingestion — getting data out of OpenAQ

`fetch_data.py` calls the OpenAQ API for 8 monitoring stations (R K Puram, Punjabi Bagh, Anand Vihar, Vikas Sadan Gurugram, Zoo Park Hyderabad, Sanjay Palace Agra, Manali Chennai, Sector-125 Noida). For each station:

1. It looks up **every PM2.5 sensor** registered at that location (some stations have more than one — often one old/dead sensor and one currently active one).
2. It queries each sensor for its latest reading and **keeps only the most recent one**, so a dead sensor never silently overrides live data.
3. It packages the result (station name, value, timestamp, etc.) as JSON and sends it to a Kafka topic called `aqi-readings`.

This step can run either as a plain Python loop (`fetch_data.py`, re-fetches every 5 minutes via `time.sleep`) or as an **Airflow DAG** (`dags/aqi_fetch_dag.py`), which does the same job but lets Airflow handle the scheduling instead of a `while True` loop.

### 2. Messaging — Kafka as the buffer

Kafka sits between ingestion and processing so the two are decoupled: the ingestion script doesn't need to know or care what's consuming the data, and Spark doesn't need to know or care how the data was produced. Zookeeper coordinates the Kafka broker (this is the older but still common Kafka setup). A Kafka UI container (`localhost:8080`) lets you inspect the topic and see messages arriving in real time.

### 3. Stream processing — Spark Structured Streaming

`spark_processor.py` subscribes to the `aqi-readings` Kafka topic and, for every micro-batch of incoming messages:

1. Parses the raw JSON bytes into a structured Spark DataFrame.
2. Adds an `is_anomaly` flag — `true` if the PM2.5 value is `≤ 0` or absurdly high (`> 999`), since real PM2.5 readings in Delhi-NCR are never actually zero.
3. Writes the resulting rows into the PostgreSQL `aqi_readings` table using `foreachBatch` + a JDBC connection.

### 4. Storage — PostgreSQL

A single table, `aqi_readings`, holds every processed reading: station name, sensor ID, PM2.5 value, timestamp, the anomaly flag, and when it was processed. This is what both Grafana and any future analysis query against.

### 5. Orchestration — Airflow

Instead of leaving a Python script running forever in a terminal, the ingestion step is wrapped in an Airflow DAG (`aqi_fetch_pipeline`) that Airflow triggers automatically every 5 minutes. See [Airflow: how the scheduling works](#airflow-how-the-scheduling-works) below for the details.

### 6. Visualization — Grafana

Grafana connects directly to the PostgreSQL database and queries `aqi_readings` to draw a live time-series chart, with one line per station, updating as new data arrives.

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python, `requests` | Simple HTTP calls to OpenAQ's REST API |
| Messaging | Apache Kafka + Zookeeper | Decouples ingestion from processing, buffers bursts |
| Stream processing | Apache Spark (Structured Streaming) | Industry-standard for processing unbounded streams |
| Storage | PostgreSQL | Reliable, queryable store for processed results |
| Orchestration | Apache Airflow | Scheduling, retries, and visibility into run history |
| Visualization | Grafana | Live dashboards, no custom front-end needed |
| Containerization | Docker Compose | Runs the whole stack locally with one command |

---

## Project structure

```
aqi-pipeline/
├── docker-compose.yml        # Kafka, Zookeeper, Kafka UI, Postgres, Grafana, Airflow
├── fetch_data.py             # Ingestion script — manual/loop mode
├── spark_processor.py        # Spark Structured Streaming job
├── test_fetch_data.py        # Pytest tests for the ingestion logic (mocked API calls)
├── requirements.txt          # Python dependencies
├── .env.example              # Template for the required environment variable
├── .gitignore
├── dags/
│   └── aqi_fetch_dag.py      # Airflow DAG version of the ingestion step
└── .github/
    └── workflows/
        └── ci.yml            # GitHub Actions: installs deps, runs tests on every push
```

---

## Setup instructions

### Prerequisites

- Docker Desktop
- Python 3.11+
- Java 17+ (required by PySpark — PySpark runs on the JVM)
- An OpenAQ API key ([free registration here](https://explore.openaq.org/register))

### 1. Clone the repo and install Python dependencies

```bash
git clone https://github.com/PriyaS876/aqi-pipeline-.git
cd aqi-pipeline-
pip install -r requirements.txt
```

### 2. Set your API key

Copy `.env.example` to `.env` and fill in your key, or set it directly in your terminal session:

```bash
# Windows (current terminal session only)
set OPENAQ_API_KEY=your_key_here
```

### 3. Start all the infrastructure

```bash
docker compose up -d
```

This starts:
- Kafka + Zookeeper (message broker)
- Kafka UI → `localhost:8080`
- PostgreSQL → port `5433` on the host (mapped from `5432` inside Docker, to avoid clashing with any PostgreSQL already installed on Windows)
- Grafana → `localhost:3000` (default login: `admin` / `admin`)
- Airflow (standalone mode) → `localhost:8081`

### 4. Create the PostgreSQL table

```bash
docker exec -it postgres psql -U aqi_user -d aqi_db
```

```sql
CREATE TABLE aqi_readings (
    id SERIAL PRIMARY KEY,
    station VARCHAR(100),
    location_id VARCHAR(20),
    sensor_id VARCHAR(20),
    parameter VARCHAR(20),
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    timestamp_utc VARCHAR(50),
    fetched_at VARCHAR(50),
    is_anomaly BOOLEAN,
    processed_at TIMESTAMP DEFAULT NOW()
);
```

---

## Running the pipeline

You have two options for the ingestion step — pick one.

### Option A — manual script (simplest for local testing)

```bash
# Terminal 1: start the Spark stream processor
python spark_processor.py

# Terminal 2: start fetching data every 5 minutes
python fetch_data.py
```

### Option B — Airflow-scheduled (closer to how this would run in production)

1. Open `localhost:8081`, log in with `admin` and the password Airflow generates on first start (find it with `docker exec airflow cat /opt/airflow/standalone_admin_password.txt`).
2. Find the `aqi_fetch_pipeline` DAG in the list and toggle it **on**.
3. Airflow will now trigger the ingestion step every 5 minutes automatically — no terminal needs to stay open.
4. In a separate terminal, still run `python spark_processor.py` so the Kafka → Postgres pipeline keeps consuming.

---

## Airflow: how the scheduling works

The DAG lives in `dags/aqi_fetch_dag.py` and is picked up automatically by Airflow because that folder is mounted into the Airflow container via `docker-compose.yml`.

- **`schedule_interval=timedelta(minutes=5)`** — tells Airflow to trigger a new run every 5 minutes.
- **`catchup=False`** — stops Airflow from trying to "backfill" runs for every 5-minute interval since `start_date`; it only cares about new runs going forward.
- **A single `PythonOperator` task** (`fetch_and_send_to_kafka`) does the same job as `fetch_data.py`'s core logic, but is called by Airflow's scheduler instead of a manual loop.
- Because Airflow runs inside its own Docker container, it talks to Kafka using the **internal Docker network address** (`kafka:29092`) rather than `localhost:9092`, which is what you'd use from Windows itself.

You can watch runs happen in real time from the DAG's **Grid** view — each 5-minute run shows up as a colored square (green = success, red = failed), and clicking into a run shows full logs.

---

## Grafana dashboard

1. In Grafana, add a **PostgreSQL** data source with:
   - Host: `postgres:5432` (the internal Docker service name + port — not `localhost:5433`, since Grafana is also running inside Docker)
   - Database: `aqi_db`
   - User / password: `aqi_user` / `aqi_password`
2. Create a new panel with this query, using **Format: Time series**:

```sql
SELECT
  processed_at AS "time",
  value,
  station AS metric
FROM aqi_readings
ORDER BY processed_at
```

This draws one line per station and updates automatically as new data lands in Postgres.

---

## Notable design decisions

- **Multi-sensor resolution during ingestion.** Several OpenAQ stations expose more than one PM2.5 sensor at the same location — often one that stopped reporting years ago and one that's currently live. Early versions of this pipeline picked whichever sensor the API listed first, which sometimes meant pulling in 2018 data instead of today's. The fix: query *all* PM2.5 sensors for a station and keep only the one with the most recent timestamp.
- **Anomaly flagging happens in Spark, not at ingestion.** A PM2.5 reading of exactly `0.0` is almost always a sensor fault rather than genuinely clean air, especially in Delhi-NCR. Rather than dropping these values, Spark flags them (`is_anomaly = true`) so they stay visible in the database and can be filtered or investigated separately.
- **Airflow replaces a bare `while True` loop.** The original ingestion script used `time.sleep(300)` to re-run every 5 minutes. That works, but gives no retry logic, no run history, and no visibility if something silently stops. Wrapping the same logic in an Airflow DAG gives all three for free.
- **Postgres runs on host port 5433, not 5432.** Many Windows machines already have a native PostgreSQL installation listening on 5432 (common if you've ever installed Postgres directly rather than through Docker). Mapping the container to `5433:5432` avoids that port conflict entirely while keeping the container's internal port untouched.
- **The API key is read from an environment variable, not hardcoded.** This keeps the repository safe to make public — anyone running this project supplies their own free OpenAQ key via `.env` rather than reusing one committed to source control.

---

## Known limitations

- Several of the 8 monitoring stations return stale readings (sometimes from 2025, occasionally older) because **all** of their PM2.5 sensors have stopped reporting — this is a genuine data-quality issue in the underlying OpenAQ dataset, not a bug in this pipeline. The multi-sensor fix described above only helps when *at least one* sensor at a station is still live.
- Airflow runs with `SequentialExecutor` and a local SQLite metadata database, which is fine for a single-machine demo but not how you'd run it in production — a real deployment would use `LocalExecutor` or `CeleryExecutor` with Postgres as the metadata store.
- There's no automated retry/backoff on the OpenAQ API calls themselves beyond what Airflow provides at the task level; a flaky API response for one station will just log a message and move on to the next station rather than retrying immediately.
=======
# Real-Time Air Quality Monitoring Pipeline

A real-time data engineering pipeline that pulls live PM2.5 air quality readings from the [OpenAQ API](https://openaq.org/) for 8 Indian cities, streams them through Kafka, processes them with Spark Structured Streaming, stores the results in PostgreSQL, orchestrates the whole ingestion step with Apache Airflow, and visualizes everything live in Grafana.

This project was built end-to-end as a hands-on way to learn the standard real-time data engineering stack: **ingestion → messaging → stream processing → storage → orchestration → visualization**.

---

## Table of contents

- [Architecture overview](#architecture-overview)
- [How data flows through the system, step by step](#how-data-flows-through-the-system-step-by-step)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Setup instructions](#setup-instructions)
- [Running the pipeline](#running-the-pipeline)
- [Airflow: how the scheduling works](#airflow-how-the-scheduling-works)
- [Grafana dashboard](#grafana-dashboard)
- [Notable design decisions](#notable-design-decisions)
- [Known limitations](#known-limitations)

---

## Architecture overview

```
                OpenAQ API (live PM2.5 sensor readings)
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │   Ingestion (choose one)               │
        │   • fetch_data.py  (manual/loop mode)  │
        │   • Airflow DAG    (scheduled mode)    │
        └───────────────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Kafka topic        │
                 │   "aqi-readings"     │
                 └─────────────────────┘
                            │
                            ▼
                 ┌─────────────────────────────┐
                 │   Spark Structured Streaming │
                 │   • parses JSON               │
                 │   • flags anomalies           │
                 └─────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   PostgreSQL         │
                 │   table: aqi_readings│
                 └─────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Grafana dashboard  │
                 │   (live PM2.5 chart) │
                 └─────────────────────┘
```

All the infrastructure (Kafka, Zookeeper, PostgreSQL, Grafana, Airflow) runs in Docker containers, managed by a single `docker-compose.yml`.

---

## How data flows through the system, step by step

### 1. Ingestion — getting data out of OpenAQ

`fetch_data.py` calls the OpenAQ API for 8 monitoring stations (R K Puram, Punjabi Bagh, Anand Vihar, Vikas Sadan Gurugram, Zoo Park Hyderabad, Sanjay Palace Agra, Manali Chennai, Sector-125 Noida). For each station:

1. It looks up **every PM2.5 sensor** registered at that location (some stations have more than one — often one old/dead sensor and one currently active one).
2. It queries each sensor for its latest reading and **keeps only the most recent one**, so a dead sensor never silently overrides live data.
3. It packages the result (station name, value, timestamp, etc.) as JSON and sends it to a Kafka topic called `aqi-readings`.

This step can run either as a plain Python loop (`fetch_data.py`, re-fetches every 5 minutes via `time.sleep`) or as an **Airflow DAG** (`dags/aqi_fetch_dag.py`), which does the same job but lets Airflow handle the scheduling instead of a `while True` loop.

### 2. Messaging — Kafka as the buffer

Kafka sits between ingestion and processing so the two are decoupled: the ingestion script doesn't need to know or care what's consuming the data, and Spark doesn't need to know or care how the data was produced. Zookeeper coordinates the Kafka broker (this is the older but still common Kafka setup). A Kafka UI container (`localhost:8080`) lets you inspect the topic and see messages arriving in real time.

### 3. Stream processing — Spark Structured Streaming

`spark_processor.py` subscribes to the `aqi-readings` Kafka topic and, for every micro-batch of incoming messages:

1. Parses the raw JSON bytes into a structured Spark DataFrame.
2. Adds an `is_anomaly` flag — `true` if the PM2.5 value is `≤ 0` or absurdly high (`> 999`), since real PM2.5 readings in Delhi-NCR are never actually zero.
3. Writes the resulting rows into the PostgreSQL `aqi_readings` table using `foreachBatch` + a JDBC connection.

### 4. Storage — PostgreSQL

A single table, `aqi_readings`, holds every processed reading: station name, sensor ID, PM2.5 value, timestamp, the anomaly flag, and when it was processed. This is what both Grafana and any future analysis query against.

### 5. Orchestration — Airflow

Instead of leaving a Python script running forever in a terminal, the ingestion step is wrapped in an Airflow DAG (`aqi_fetch_pipeline`) that Airflow triggers automatically every 5 minutes. See [Airflow: how the scheduling works](#airflow-how-the-scheduling-works) below for the details.

### 6. Visualization — Grafana

Grafana connects directly to the PostgreSQL database and queries `aqi_readings` to draw a live time-series chart, with one line per station, updating as new data arrives.

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python, `requests` | Simple HTTP calls to OpenAQ's REST API |
| Messaging | Apache Kafka + Zookeeper | Decouples ingestion from processing, buffers bursts |
| Stream processing | Apache Spark (Structured Streaming) | Industry-standard for processing unbounded streams |
| Storage | PostgreSQL | Reliable, queryable store for processed results |
| Orchestration | Apache Airflow | Scheduling, retries, and visibility into run history |
| Visualization | Grafana | Live dashboards, no custom front-end needed |
| Containerization | Docker Compose | Runs the whole stack locally with one command |

---

## Project structure

```
aqi-pipeline/
├── docker-compose.yml        # Kafka, Zookeeper, Kafka UI, Postgres, Grafana, Airflow
├── fetch_data.py             # Ingestion script — manual/loop mode
├── spark_processor.py        # Spark Structured Streaming job
├── test_fetch_data.py        # Pytest tests for the ingestion logic (mocked API calls)
├── requirements.txt          # Python dependencies
├── .env.example              # Template for the required environment variable
├── .gitignore
├── dags/
│   └── aqi_fetch_dag.py      # Airflow DAG version of the ingestion step
└── .github/
    └── workflows/
        └── ci.yml            # GitHub Actions: installs deps, runs tests on every push
```

---
## Viewing the Dashboard (Local Setup)

1. **Start the Docker containers**

docker-compose up -d

   This will start Kafka, Spark, Postgres, and Grafana in the background.

2. **Verify all containers are running**

docker ps

   The `grafana` container should show status "Up".

3. **Open in browser**

http://localhost:3000


4. **Log in**
   - Username: `admin`
   - Password: `admin`
   (On first login you may be prompted to change the password — this can be skipped or set as needed)

5. **Find the dashboard**
   Click "Dashboards" in the sidebar and select the AQI dashboard.


## Setup instructions

### Prerequisites

- Docker Desktop
- Python 3.11+
- Java 17+ (required by PySpark — PySpark runs on the JVM)
- An OpenAQ API key ([free registration here](https://explore.openaq.org/register))

### 1. Clone the repo and install Python dependencies

```bash
git clone https://github.com/PriyaS876/aqi-pipeline-.git
cd aqi-pipeline-
pip install -r requirements.txt
```

### 2. Set your API key

Copy `.env.example` to `.env` and fill in your key, or set it directly in your terminal session:

```bash
# Windows (current terminal session only)
set OPENAQ_API_KEY=your_key_here
```

### 3. Start all the infrastructure

```bash
docker compose up -d
```

This starts:
- Kafka + Zookeeper (message broker)
- Kafka UI → `localhost:8080`
- PostgreSQL → port `5433` on the host (mapped from `5432` inside Docker, to avoid clashing with any PostgreSQL already installed on Windows)
- Grafana → `localhost:3000` (default login: `admin` / `admin`)
- Airflow (standalone mode) → `localhost:8081`

### 4. Create the PostgreSQL table

```bash
docker exec -it postgres psql -U aqi_user -d aqi_db
```

```sql
CREATE TABLE aqi_readings (
    id SERIAL PRIMARY KEY,
    station VARCHAR(100),
    location_id VARCHAR(20),
    sensor_id VARCHAR(20),
    parameter VARCHAR(20),
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    timestamp_utc VARCHAR(50),
    fetched_at VARCHAR(50),
    is_anomaly BOOLEAN,
    processed_at TIMESTAMP DEFAULT NOW()
);
```

---

## Running the pipeline

You have two options for the ingestion step — pick one.

### Option A — manual script (simplest for local testing)

```bash
# Terminal 1: start the Spark stream processor
python spark_processor.py

# Terminal 2: start fetching data every 5 minutes
python fetch_data.py
```

### Option B — Airflow-scheduled (closer to how this would run in production)

1. Open `localhost:8081`, log in with `admin` and the password Airflow generates on first start (find it with `docker exec airflow cat /opt/airflow/standalone_admin_password.txt`).
2. Find the `aqi_fetch_pipeline` DAG in the list and toggle it **on**.
3. Airflow will now trigger the ingestion step every 5 minutes automatically — no terminal needs to stay open.
4. In a separate terminal, still run `python spark_processor.py` so the Kafka → Postgres pipeline keeps consuming.

---

## Airflow: how the scheduling works

The DAG lives in `dags/aqi_fetch_dag.py` and is picked up automatically by Airflow because that folder is mounted into the Airflow container via `docker-compose.yml`.

- **`schedule_interval=timedelta(minutes=5)`** — tells Airflow to trigger a new run every 5 minutes.
- **`catchup=False`** — stops Airflow from trying to "backfill" runs for every 5-minute interval since `start_date`; it only cares about new runs going forward.
- **A single `PythonOperator` task** (`fetch_and_send_to_kafka`) does the same job as `fetch_data.py`'s core logic, but is called by Airflow's scheduler instead of a manual loop.
- Because Airflow runs inside its own Docker container, it talks to Kafka using the **internal Docker network address** (`kafka:29092`) rather than `localhost:9092`, which is what you'd use from Windows itself.

You can watch runs happen in real time from the DAG's **Grid** view — each 5-minute run shows up as a colored square (green = success, red = failed), and clicking into a run shows full logs.

---

## Grafana dashboard

1. In Grafana, add a **PostgreSQL** data source with:
   - Host: `postgres:5432` (the internal Docker service name + port — not `localhost:5433`, since Grafana is also running inside Docker)
   - Database: `aqi_db`
   - User / password: `aqi_user` / `aqi_password`
2. Create a new panel with this query, using **Format: Time series**:

```sql
SELECT
  processed_at AS "time",
  value,
  station AS metric
FROM aqi_readings
ORDER BY processed_at
```

This draws one line per station and updates automatically as new data lands in Postgres.

---

## Notable design decisions

- **Multi-sensor resolution during ingestion.** Several OpenAQ stations expose more than one PM2.5 sensor at the same location — often one that stopped reporting years ago and one that's currently live. Early versions of this pipeline picked whichever sensor the API listed first, which sometimes meant pulling in 2018 data instead of today's. The fix: query *all* PM2.5 sensors for a station and keep only the one with the most recent timestamp.
- **Anomaly flagging happens in Spark, not at ingestion.** A PM2.5 reading of exactly `0.0` is almost always a sensor fault rather than genuinely clean air, especially in Delhi-NCR. Rather than dropping these values, Spark flags them (`is_anomaly = true`) so they stay visible in the database and can be filtered or investigated separately.
- **Airflow replaces a bare `while True` loop.** The original ingestion script used `time.sleep(300)` to re-run every 5 minutes. That works, but gives no retry logic, no run history, and no visibility if something silently stops. Wrapping the same logic in an Airflow DAG gives all three for free.
- **Postgres runs on host port 5433, not 5432.** Many Windows machines already have a native PostgreSQL installation listening on 5432 (common if you've ever installed Postgres directly rather than through Docker). Mapping the container to `5433:5432` avoids that port conflict entirely while keeping the container's internal port untouched.
- **The API key is read from an environment variable, not hardcoded.** This keeps the repository safe to make public — anyone running this project supplies their own free OpenAQ key via `.env` rather than reusing one committed to source control.

---

## Known limitations

- Several of the 8 monitoring stations return stale readings (sometimes from 2025, occasionally older) because **all** of their PM2.5 sensors have stopped reporting — this is a genuine data-quality issue in the underlying OpenAQ dataset, not a bug in this pipeline. The multi-sensor fix described above only helps when *at least one* sensor at a station is still live.
- Airflow runs with `SequentialExecutor` and a local SQLite metadata database, which is fine for a single-machine demo but not how you'd run it in production — a real deployment would use `LocalExecutor` or `CeleryExecutor` with Postgres as the metadata store.
- There's no automated retry/backoff on the OpenAQ API calls themselves beyond what Airflow provides at the task level; a flaky API response for one station will just log a message and move on to the next station rather than retrying immediately.
>>>>>>> e5b629625431e316d78bd11ce88e3bae0e574df5
