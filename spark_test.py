from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JobMarketTest") \
    .getOrCreate()

print("Spark version:", spark.version)

data = [
    ("Data Engineer", 35000),
    ("Data Analyst", 25000),
    ("Data Scientist", 50000)
]

df = spark.createDataFrame(
    data,
    ["job_title", "salary"]
)

df.show()

spark.stop()