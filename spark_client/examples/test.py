import time
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkPostgresExample") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Sample DataFrame
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
], ["id", "name"])

df.show()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/telco_db"
connection_properties = {
    "user": "telco",
    "password": "Telco12345",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to Postgres
df.write \
    .jdbc(jdbc_url, "users", mode="append", properties=connection_properties)

print("✅ Data written to Postgres table 'users'")

# Read back from Postgres to verify
df_read = spark.read \
    .jdbc(jdbc_url, "users", properties=connection_properties)

print("📥 Data read back from Postgres:")
df_read.show()

# Keep Spark UI alive for debugging
print("Sleeping to keep Spark UI alive at http://<host>:4040 ...")
time.sleep(300)

spark.stop()

