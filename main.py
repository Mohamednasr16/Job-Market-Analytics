import json
import time
import random
import uuid
from datetime import date, timedelta
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
)

# --------------------------------------------------
# Job titles and their relative popularity
# --------------------------------------------------

job_titles = [
    "Data Engineer",
    "Data Scientist",
    "Data Analyst",
    "ML Engineer",
    "AI Engineer",
    "Software Engineer",
    "Backend Developer",
    "Frontend Developer",
    "Full Stack Developer",
    "DevOps Engineer",
    "Cloud Engineer",
    "Database Administrator",
    "BI Analyst",
    "Business Analyst",
    "Cybersecurity Analyst"
]

job_weights = [
    8,   # Data Engineer
    6,   # Data Scientist
    10,  # Data Analyst
    5,   # ML Engineer
    4,   # AI Engineer
    10,  # Software Engineer
    8,   # Backend Developer
    6,   # Frontend Developer
    8,   # Full Stack Developer
    6,   # DevOps Engineer
    5,   # Cloud Engineer
    3,   # Database Administrator
    7,   # BI Analyst
    8,   # Business Analyst
    4    # Cybersecurity Analyst
]

# --------------------------------------------------
# Locations
# --------------------------------------------------

locations = [
    "Cairo",
    "Giza",
    "Alexandria",
    "Dubai",
    "Abu Dhabi",
    "Riyadh",
    "Jeddah",
    "Doha",
    "London",
    "Manchester",
    "Berlin",
    "Paris",
    "Amsterdam",
    "Toronto",
    "New York"
]

# --------------------------------------------------
# Salary ranges by job title
# --------------------------------------------------

salary_ranges = {
    "Data Engineer": (60000, 110000),
    "Data Scientist": (65000, 120000),
    "Data Analyst": (45000, 85000),
    "ML Engineer": (70000, 130000),
    "AI Engineer": (75000, 140000),
    "Software Engineer": (55000, 105000),
    "Backend Developer": (50000, 95000),
    "Frontend Developer": (45000, 90000),
    "Full Stack Developer": (55000, 100000),
    "DevOps Engineer": (65000, 115000),
    "Cloud Engineer": (70000, 120000),
    "Database Administrator": (50000, 90000),
    "BI Analyst": (50000, 90000),
    "Business Analyst": (45000, 85000),
    "Cybersecurity Analyst": (60000, 110000)
}

# --------------------------------------------------
# Deliberately invalid data injection
# --------------------------------------------------
#
# Our Spark job (spark_kafka.py) has 5 validation rules, but a producer
# that only ever emits clean data means invalid_jobs stays empty forever.
# So a small percentage of records are intentionally broken here, one
# rule at a time, so every failure category actually gets exercised.

BAD_DATA_PROBABILITY = 0.10  # ~10% of jobs are deliberately invalid


def corrupt_missing_field(job):
    field = random.choice(
        ["job_title", "company", "location", "salary", "posted_date"]
    )
    job[field] = None
    return job


def corrupt_salary_too_low(job):
    job["salary"] = random.randint(100, 900)
    return job


def corrupt_salary_too_high(job):
    job["salary"] = random.randint(1_000_000, 5_000_000)
    return job


def corrupt_future_date(job):
    future = date.today() + timedelta(days=random.randint(1, 60))
    job["posted_date"] = future.isoformat()
    return job


def corrupt_old_date(job):
    old = date.today() - timedelta(days=random.randint(366, 1000))
    job["posted_date"] = old.isoformat()
    return job


CORRUPTIONS = [
    corrupt_missing_field,
    corrupt_salary_too_low,
    corrupt_salary_too_high,
    corrupt_future_date,
    corrupt_old_date,
]


def maybe_corrupt(job):
    if random.random() < BAD_DATA_PROBABILITY:
        corruption_fn = random.choice(CORRUPTIONS)
        job = corruption_fn(job)
    return job


# --------------------------------------------------
# Generate jobs continuously
# --------------------------------------------------

while True:

    # Select a job using weighted probabilities
    job_title = random.choices(
        job_titles,
        weights=job_weights,
        k=1
    )[0]

    min_salary, max_salary = salary_ranges[job_title]

    job = {
        "job_id": str(uuid.uuid4()),
        "job_title": job_title,
        "company": fake.company(),
        "location": random.choice(locations),
        "salary": random.randint(min_salary, max_salary),
        "posted_date": fake.date_between(
            start_date="-1y",
            end_date="today"
        ).isoformat()
    }

    job = maybe_corrupt(job)

    message = json.dumps(job).encode("utf-8")

    producer.send(
        "job-postings",
        message
    )

    producer.flush()

    print("Job sent:", job)

    time.sleep(0.1)