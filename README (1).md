# Job Market Analytics Pipeline

A real-time data engineering pipeline that generates simulated job-posting
events, streams them through Kafka, validates and processes them with Spark
Structured Streaming, stores the results in PostgreSQL, and uses Airflow to
schedule recurring data-quality snapshots on top of that data.

## The business problem

Imagine a company monitoring the job market and continuously collecting
postings (title, company, location, salary, posted date). They want to
answer:

- How much of the incoming data is invalid, and why is it being rejected?
- Can new job postings be processed as they arrive, rather than in an
  end-of-day batch?
- What roles, locations, and salary trends are showing up in the market?

This project simulates the data engineering system that would support that.

## Architecture

```
Producer (main.py)
      │  Faker + weighted job titles/locations/salaries,
      │  ~10% of records deliberately corrupted
      ▼
    Kafka (topic: job-postings)
      │  durable buffer, decouples producer from processing speed
      ▼
Spark Structured Streaming (spark_kafka.py)
      │  parses JSON → validates → splits into two streams
      ├──── valid ────► PostgreSQL: valid_jobs
      └──── invalid ──► PostgreSQL: invalid_jobs (with error_reason)
                                   │
                                   ▼
                     Airflow DAG: job_market_analytics (hourly)
                                   │  reads valid_jobs / invalid_jobs
                                   ▼
                     PostgreSQL: data_quality_snapshots
                                 error_reason_snapshots
```

The streaming path (producer → Kafka → Spark → Postgres) runs continuously.
Airflow does **not** ingest or move job records; it runs short, scheduled
batch jobs that summarize what the streaming path has already stored.

| Component | Role |
|---|---|
| Python (`main.py`) | Producer: generates job events (runs on the host, not in Docker) |
| Faker | Simulated data source (stand-in for a real job board) |
| Kafka | Event buffer: decouples producer and consumer speed |
| Spark Structured Streaming | Continuous parsing, validation, transformation |
| PostgreSQL | Storage for valid/invalid records, snapshot tables, and Airflow metadata |
| Airflow | Scheduled batch orchestration around the streaming pipeline |
| Docker Compose | Runs Kafka, Postgres, Spark, and Airflow as containers |

## Repository layout

```
.
├── main.py                       # producer (run on host)
├── spark_kafka.py                # streaming job (mounted into the Spark container)
├── docker-compose.yml            # kafka, postgres, spark, airflow
└── dags/
    └── job_market_analytics.py   # Airflow DAG
```

## Running it

Prerequisites: Docker Desktop, Python 3 with `faker` and `kafka-python`
installed for the producer.

1. **Create the external Postgres volume** (the compose file declares it as
   `external`, so it must exist first):
   ```
   docker volume create postgres-data
   ```
2. **Create the `dags` folder** if it doesn't exist (it is bind-mounted into
   the Airflow container):
   ```
   mkdir dags
   ```
3. **Start the services:**
   ```
   docker compose up -d
   ```
4. **Create the Airflow metadata database** (Postgres' `POSTGRES_DB` only
   creates one database, so the second is created manually):
   ```
   docker exec -it job-postgres psql -U postgres -c "CREATE DATABASE airflow;"
   ```
5. **Create the pipeline tables** in the `job_market` database. The upsert
   in `spark_kafka.py` needs these to exist, including the unique constraint
   on `job_id` that `ON CONFLICT` relies on:
   ```sql
   CREATE TABLE IF NOT EXISTS valid_jobs (
       id          SERIAL PRIMARY KEY,
       job_title   TEXT,
       company     TEXT,
       location    TEXT,
       salary      INTEGER,
       posted_date DATE,
       created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       job_id      TEXT,
       CONSTRAINT valid_jobs_job_id_unique UNIQUE (job_id)
   );

   CREATE TABLE IF NOT EXISTS invalid_jobs (
       id           SERIAL PRIMARY KEY,
       job_title    TEXT,
       company      TEXT,
       location     TEXT,
       salary       INTEGER,
       posted_date  DATE,
       is_valid     BOOLEAN,
       error_reason TEXT,
       created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       job_id       TEXT,
       CONSTRAINT invalid_jobs_job_id_unique UNIQUE (job_id)
   );
   ```
   Run it with, for example:
   ```
   docker exec -it job-postgres psql -U postgres -d job_market
   ```
6. **Restart Spark** so it starts against the tables, then **start the
   producer:**
   ```
   docker compose restart spark
   python main.py
   ```
7. **Open Airflow** at `http://localhost:8080`, log in as `admin`, switch the
   `job_market_analytics` DAG on, and trigger a run. Airflow 3 generates a
   random admin password each time the container is created; find it in the
   container logs (`docker logs job-airflow`).

## Data validation rules

Every record is checked against 5 rules in `spark_kafka.py`. A failing
record is routed to `invalid_jobs` with a comma-separated `error_reason`
explaining exactly why:

| Rule | Condition |
|---|---|
| `missing_required_field` | Any of the 5 core fields is null |
| `salary_too_low` | salary < 1,000 |
| `salary_too_high` | salary > 999,999 |
| `future_date` | posted_date is after today |
| `old_posted_date` | posted_date is more than 365 days old |

## Airflow orchestration

The DAG `job_market_analytics` (`dags/job_market_analytics.py`) runs hourly
and has three tasks:

1. `create_tables`: creates the two snapshot tables if they don't exist.
2. `snapshot_data_quality`: counts rows in `valid_jobs` and `invalid_jobs`
   and stores the totals and invalid percentage in `data_quality_snapshots`.
3. `snapshot_error_reasons`: counts rejected rows per `error_reason` and
   stores them in `error_reason_snapshots`.

Task 1 runs first; tasks 2 and 3 are independent and run in parallel
(`create_tables >> [snapshot_data_quality, snapshot_error_reasons]`).

Design decisions:

- **Why Airflow sits beside the pipeline, not inside it.** Airflow
  orchestrates finite, scheduled tasks. The streaming job never finishes, so
  it stays outside Airflow and keeps running independently.
- **`catchup=False`.** The tasks snapshot the tables as they are now, so
  replaying missed past hours would only write near-identical rows.
- **Idempotent writes.** Each snapshot row is keyed on the run's
  `logical_date` and written with `ON CONFLICT ... DO UPDATE`, so retrying a
  run overwrites its own row instead of duplicating it. This is the same
  principle as the `job_id` upsert in the streaming path.
- **Retries.** Each task retries twice, one minute apart.
- **Connection management.** The DAG connects through an Airflow connection
  (`job_market_db`) defined by the `AIRFLOW_CONN_JOB_MARKET_DB` environment
  variable in `docker-compose.yml`, so no credentials appear in DAG code.
- **Executor and deployment.** Airflow runs in `standalone` mode with
  `LocalExecutor`, as a single container, with its metadata stored in a
  separate `airflow` database inside the existing Postgres container. This
  matches the project's scale; a production setup would separate the
  scheduler, API server, and workers.

**Verified:** a manual run completed with all three tasks succeeding.
`data_quality_snapshots` recorded 7,455 valid and 876 invalid records
(10.51% invalid, consistent with the ~10% corruption rate), and the five
`error_reason_snapshots` counts summed to exactly 876, matching the invalid
total.

## Debugging log: issues found and fixed

This section documents real problems found while building this project,
how they were diagnosed, and how they were fixed.

### 1. `invalid_jobs` was structurally always empty

**Symptom:** despite having 5 validation rules, `invalid_jobs` had 0 rows,
even after significant data volume.

**Root cause:** `main.py` only ever generated data that satisfied all 5
rules by construction (salaries always in-range, dates always within the
last year, no field ever null). The validation logic in Spark was correct;
it simply never had anything to catch.

**Fix:** added a `BAD_DATA_PROBABILITY` (10%) to `main.py`. When triggered,
one of 5 corruption functions is chosen at random and deliberately breaks
exactly one field, matching exactly one Spark validation rule. This keeps
each invalid record attributable to a single cause and produces a roughly
even distribution across all 5 failure types.

**Verified:** after running with the fix, `invalid_jobs` showed all 5
`error_reason` values represented, each in a similar range (41 to 49
out of 228 total at the time), matching the intended ~10% corruption rate.

### 2. No idempotency: duplicate writes were possible

**Problem:** the original write path used
`batch_df.write.jdbc(..., mode="append")`. If Spark ever reprocessed a
micro-batch (e.g. a crash between writing to Postgres and committing the
checkpoint), the same rows would be inserted again with no way to detect
the duplication.

**Root cause:** job records had no unique identity at all. Nothing
distinguished one posting from another, or a record from a reprocessed
copy of itself.

**Fix, in three parts:**
1. `main.py` now generates a UUID (`job_id`) once per job, before any
   corruption logic runs, so the ID travels with the record through Kafka
   and Spark unchanged.
2. `job_schema` in `spark_kafka.py` was updated to include `job_id`
   (Spark's `from_json` silently drops fields not listed in the schema,
   so this step was required for the ID to survive parsing).
3. Both Postgres tables got a `job_id TEXT` column with a `UNIQUE`
   constraint. The write functions were rewritten from a plain JDBC
   append to a `psycopg2`-based upsert using
   `INSERT ... ON CONFLICT (job_id) DO NOTHING`, run per-partition via
   `foreachPartition`.

**Note:** this required adding `psycopg2-binary` to the Spark container,
installed via the compose file's `command:` before `spark-submit` runs,
since the base image doesn't include a Python Postgres driver.

**Verified:** manually inserting a row with an already-existing `job_id`
returned `INSERT 0 0` (zero rows written, no error), confirming
duplicates are silently and safely rejected.

### 3. `docker-compose.yml` had two reproducibility gaps

- `kafka-data` was declared as a named volume but never mounted anywhere,
  so Kafka had no real persistence configured at all.
- The `job_market` database was never declared via `POSTGRES_DB`; it only
  existed because it had been created manually at some point. A fresh
  clone of this repo, on a fresh volume, would not have this database.

**Fix:** added `POSTGRES_DB: job_market` and `POSTGRES_USER: postgres` to
the postgres service's environment, and mounted `kafka-data` (see the next
issue for why the exact path took extra work to get right).

### 4. Kafka data did not survive a full container recreation

A good example of why "looks right" and "verified working" are different
things.

**First attempt:** mounted `kafka-data` at `/tmp/kraft-combined-logs`,
based on general documentation about Kafka's default KRaft log directory.
Also pinned a fixed `CLUSTER_ID`, reasoning that a randomly generated
cluster ID on each restart would cause the broker to treat existing data
as belonging to a different, unrecognized cluster.

**Test:** `docker compose down` (removes containers, not volumes) →
`docker compose up -d` → list topics. The topic was gone.

**Investigation:** rather than guessing further, read the actual files
inside the running container:
- `/etc/kafka/docker/run` delegates to `configure` then `launch`.
- `/etc/kafka/docker/configure` confirmed `CLUSTER_ID` is required and
  validated (it was already being read correctly).
- `/etc/kafka/docker/launch` showed that storage formatting **does**
  check for "already formatted" and skips destructive re-formatting if so.
  This ruled out both of the first two hypotheses (cluster ID mismatch
  and blind reformatting).
- The generated `/opt/kafka/config/server.properties` had no `log.dirs`
  entry at all.
- The live broker's startup logs showed:
  ```
  Loading logs from log dirs ArrayBuffer(/tmp/kafka-logs)
  No logs found to be loaded in /tmp/kafka-logs
  ```
  and the full config dump confirmed `log.dir = /tmp/kafka-logs`.

**Actual root cause:** this specific image's generated config falls back
to Kafka's single-directory default, `/tmp/kafka-logs`, not
`/tmp/kraft-combined-logs`, which is a different default associated with
other Kafka tooling and documentation. The volume had been mounted at a
path the broker was never actually using.

**Fix:** changed the mount to `kafka-data:/tmp/kafka-logs`.

**Verified:** produced data, then ran the destructive test again
(`docker compose down` → `docker compose up -d` → list topics). The
`job-postings` topic survived.

**Lesson:** the `CLUSTER_ID` pin and the volume mount were both
reasonable pieces of Kafka/KRaft knowledge, but neither was the actual
bug. The fix came from reading the running container's own logs and
config rather than reasoning from general documentation about how similar
images usually behave.

### 5. Airflow setup issues

- **Compose file rejected with a YAML error.** The saved file had a stray
  title line (`Docker compose · YML`) as its first line, picked up by
  copying displayed text instead of the file itself. Deleting that line
  fixed it. Lesson: when a parser reports an error at line 3 of a file you
  didn't expect to be wrong, print the file and read it.
- **Login credentials "stopped working".** Airflow 3 generates a random
  admin password when the container is created, so recreating the container
  (which any compose change that affects it does) produces a new one. The
  current password is in the container logs.
- **Slow first page load.** The first UI load after container creation took
  long enough that the browser timed out. Checking `docker compose ps` and
  the container logs showed the container was healthy and the DAG parsed
  with 0 errors; a fresh browser window and patience resolved it.
- **Airflow 3 vs older tutorials.** Many online examples target Airflow 2.
  Details that differ in 3.x include the health components shown in the UI
  (the DAG processor is a separate component), the DAG import
  (`from airflow.sdk import DAG`), and the database setting living under
  `AIRFLOW__DATABASE__...` rather than `AIRFLOW__CORE__...`.

## Known limitations

- **Simulated data.** Faker-generated data with rules I wrote myself is
  good for demonstrating architecture, but it doesn't reproduce real-world
  messiness (inconsistent encodings, duplicates across sources, mixed
  currencies).
- **Table definitions are not in version control.** `valid_jobs` and
  `invalid_jobs` were created by hand during early development. The setup
  SQL in this README reconstructs them; a proper init script (mounted into
  `/docker-entrypoint-initdb.d/`) would make setup reproducible.
- **Credentials and keys are hardcoded** in `docker-compose.yml` and
  `spark_kafka.py` for local development. They should move to environment
  files or a secrets mechanism before this goes anywhere shared.
- **Single Kafka partition and single-node setup.** Fine at this scale; a
  larger deployment would use multiple partitions and consumers.
- **Airflow in `standalone` mode** with `LocalExecutor` is a development
  setup, not a production topology.
- **Snapshot queries run `COUNT(*)` over the full tables.** This is fine
  now, but would need incremental or indexed approaches as the tables grow
  into the tens of millions of rows.
- **No automated tests or monitoring yet** for the validation logic or
  pipeline throughput.
- **The Airflow DAG does not manage the streaming job.** If Spark or the
  producer stops, nothing alerts; the snapshots would simply stop changing.

## Current status

- Core streaming pipeline (Kafka → Spark → Postgres): working, idempotent,
  and persistent across restarts.
- Data quality: all 5 validation rules are exercised by the producer.
- Airflow: running, with a scheduled DAG producing hourly data-quality
  snapshots.

## Planned next steps

1. Let the pipeline run so the hourly snapshots build a real time series.
2. Build the SQL analytics layer (jobs by location, salary by role, most
   common titles) and add it to the DAG as additional tasks.
3. Connect Power BI to PostgreSQL for visualization.
4. Add tests for the validation logic and move table definitions into an
   init script.
