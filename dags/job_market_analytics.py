from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Connection id matches the AIRFLOW_CONN_JOB_MARKET_DB env var in docker-compose.yml
# (Airflow strips the prefix and lowercases it: job_market_db)
CONN_ID = "job_market_db"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="job_market_analytics",
    description="Hourly data-quality snapshots of the job-postings pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["job-market", "analytics"],
) as dag:

    # Task 1: make sure the output tables exist (safe to run every time)
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS data_quality_snapshots (
                snapshot_ts   TIMESTAMPTZ NOT NULL,
                valid_count   BIGINT NOT NULL,
                invalid_count BIGINT NOT NULL,
                invalid_pct   NUMERIC(5, 2) NOT NULL,
                PRIMARY KEY (snapshot_ts)
            );

            CREATE TABLE IF NOT EXISTS error_reason_snapshots (
                snapshot_ts  TIMESTAMPTZ NOT NULL,
                error_reason TEXT NOT NULL,
                job_count    BIGINT NOT NULL,
                PRIMARY KEY (snapshot_ts, error_reason)
            );
        """,
    )

    # Task 2: overall valid vs invalid counts
    # {{ logical_date }} is the run's timestamp, so retrying a run rewrites
    # the same row instead of adding a duplicate (same idea as our job_id upsert)
    snapshot_data_quality = SQLExecuteQueryOperator(
        task_id="snapshot_data_quality",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO data_quality_snapshots
                (snapshot_ts, valid_count, invalid_count, invalid_pct)
            SELECT
                '{{ logical_date }}'::timestamptz,
                v.cnt,
                i.cnt,
                ROUND(100.0 * i.cnt / NULLIF(v.cnt + i.cnt, 0), 2)
            FROM (SELECT COUNT(*) AS cnt FROM valid_jobs)   v,
                 (SELECT COUNT(*) AS cnt FROM invalid_jobs) i
            ON CONFLICT (snapshot_ts) DO UPDATE SET
                valid_count   = EXCLUDED.valid_count,
                invalid_count = EXCLUDED.invalid_count,
                invalid_pct   = EXCLUDED.invalid_pct;
        """,
    )

    # Task 3: why records were rejected
    snapshot_error_reasons = SQLExecuteQueryOperator(
        task_id="snapshot_error_reasons",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO error_reason_snapshots
                (snapshot_ts, error_reason, job_count)
            SELECT
                '{{ logical_date }}'::timestamptz,
                error_reason,
                COUNT(*)
            FROM invalid_jobs
            GROUP BY error_reason
            ON CONFLICT (snapshot_ts, error_reason) DO UPDATE SET
                job_count = EXCLUDED.job_count;
        """,
    )

    # Task 1 must finish first; tasks 2 and 3 are independent, so they run in parallel
    create_tables >> [snapshot_data_quality, snapshot_error_reasons]
