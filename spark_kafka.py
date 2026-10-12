from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_date,
    current_date,
    date_sub,
    concat_ws,
    when,
    lit,
    array
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
import psycopg2
from psycopg2.extras import execute_values

# --------------------------------------------------
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("JobMarketKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. PostgreSQL configuration
# --------------------------------------------------

POSTGRES_URL = "jdbc:postgresql://postgres:5432/job_market"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# psycopg2 (used for upserts) takes plain connection params, not a JDBC URL
PG_CONN_PARAMS = {
    "host": "postgres",
    "port": 5432,
    "dbname": "job_market",
    "user": "postgres",
    "password": "admin123"
}

# --------------------------------------------------
# 3. Define the expected Kafka message structure
# --------------------------------------------------

job_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("posted_date", StringType(), True)
])

# --------------------------------------------------
# 4. Read job postings from Kafka
# --------------------------------------------------

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "job-postings")
    .option("startingOffsets", "earliest")
    .load()
)

# --------------------------------------------------
# 5. Convert Kafka value from binary → string
# --------------------------------------------------

messages = df.selectExpr(
    "CAST(value AS STRING) AS json"
)

# --------------------------------------------------
# 6. Parse JSON into columns
# --------------------------------------------------

jobs_df = (
    messages
    .select(
        from_json(col("json"), job_schema).alias("job")
    )
    .select("job.*")
    .withColumn(
        "posted_date",
        to_date(col("posted_date"))
    )
)

# --------------------------------------------------
# 7. Define individual data-quality checks
# --------------------------------------------------

missing_required_fields = (
    col("job_title").isNull()
    | col("company").isNull()
    | col("location").isNull()
    | col("salary").isNull()
    | col("posted_date").isNull()
)

salary_too_low = (
    col("salary").isNotNull()
    & (col("salary") < 1000)
)

salary_too_high = (
    col("salary").isNotNull()
    & (col("salary") > 999999)
)

future_date = (
    col("posted_date").isNotNull()
    & (col("posted_date") > current_date())
)

old_date = (
    col("posted_date").isNotNull()
    & (col("posted_date") < date_sub(current_date(), 365))
)

# --------------------------------------------------
# 8. Create error reasons
# --------------------------------------------------

validated_df = (
    jobs_df

    .withColumn(
        "error_reason",
        concat_ws(
            ",",
            array(
                when(
                    missing_required_fields,
                    lit("missing_required_field")
                ),

                when(
                    salary_too_low,
                    lit("salary_too_low")
                ),

                when(
                    salary_too_high,
                    lit("salary_too_high")
                ),

                when(
                    future_date,
                    lit("future_date")
                ),

                when(
                    old_date,
                    lit("old_posted_date")
                )
            )
        )
    )

    .withColumn(
        "is_valid",
        col("error_reason") == ""
    )
)

# --------------------------------------------------
# 9. Separate valid and invalid records
# --------------------------------------------------

clean_jobs_df = (
    validated_df
    .filter(col("is_valid") == True)
    .drop("is_valid", "error_reason")
)

invalid_jobs_df = (
    validated_df
    .filter(col("is_valid") == False)
)

# --------------------------------------------------
# 10. Write VALID records to PostgreSQL (idempotent upsert)
# --------------------------------------------------

def upsert_valid_partition(rows):
    rows = list(rows)
    if not rows:
        return

    conn = psycopg2.connect(**PG_CONN_PARAMS)
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO valid_jobs
                    (job_id, job_title, company, location, salary, posted_date)
                VALUES %s
                ON CONFLICT (job_id) DO NOTHING
                """,
                [
                    (r.job_id, r.job_title, r.company, r.location, r.salary, r.posted_date)
                    for r in rows
                ]
            )
        conn.commit()
    finally:
        conn.close()


def write_valid_jobs(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_df.foreachPartition(upsert_valid_partition)

# --------------------------------------------------
# 11. Write INVALID records to PostgreSQL (idempotent upsert)
# --------------------------------------------------

def upsert_invalid_partition(rows):
    rows = list(rows)
    if not rows:
        return

    conn = psycopg2.connect(**PG_CONN_PARAMS)
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO invalid_jobs
                    (job_id, job_title, company, location, salary, posted_date, is_valid, error_reason)
                VALUES %s
                ON CONFLICT (job_id) DO NOTHING
                """,
                [
                    (r.job_id, r.job_title, r.company, r.location, r.salary, r.posted_date, r.is_valid, r.error_reason)
                    for r in rows
                ]
            )
        conn.commit()
    finally:
        conn.close()


def write_invalid_jobs(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_df.foreachPartition(upsert_invalid_partition)

# --------------------------------------------------
# 12. Start PostgreSQL streaming writes
# --------------------------------------------------

valid_query = (
    clean_jobs_df
    .writeStream
    .foreachBatch(write_valid_jobs)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/valid_jobs"
    )
    .queryName("valid_jobs")
    .start()
)

invalid_query = (
    invalid_jobs_df
    .writeStream
    .foreachBatch(write_invalid_jobs)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/invalid_jobs"
    )
    .queryName("invalid_jobs")
    .start()
)

# --------------------------------------------------
# 13. Keep the streaming application running
# --------------------------------------------------

spark.streams.awaitAnyTermination()