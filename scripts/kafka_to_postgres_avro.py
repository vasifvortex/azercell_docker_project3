from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import requests
import json

# -------------------------------
# Helper: Fetch latest Avro schema from registry
# -------------------------------
def get_latest_avro_schema(subject, registry_url):
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.json()['schema'])

# -------------------------------
# Schema Registry configuration
# -------------------------------
schema_registry_url = "http://localhost:8083"  # Replace with actual registry
topic_name = "test_topic"
subject_name = f"{topic_name}-value"

# Get Avro schema
avro_schema = get_latest_avro_schema(subject_name, schema_registry_url)

# -------------------------------
# Spark session
# -------------------------------
spark = SparkSession.builder \
    .appName("KafkaAvroToPostgres") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0",
        "org.postgresql:postgresql:42.6.0",
        "org.scala-lang:scala-library:2.12.18"  # Added Scala library explicitly
    ])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Read Kafka stream
# -------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Strip 5-byte Confluent wire format header
df_stripped = df_raw.withColumn("avro_payload", expr("substring(value, 6, length(value)-5)"))

# Deserialize Avro messages
df_parsed = df_stripped.select(
    from_avro("avro_payload", json.dumps(avro_schema), {"schema.registry.url": schema_registry_url}).alias("data")
).select("data.*")

# -------------------------------
# Write to PostgreSQL using foreachBatch
# -------------------------------
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/telco_db") \
        .option("dbtable", "user_events") \
        .option("user", "telco") \
        .option("password", "Telco12345") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# -------------------------------
# Start streaming query
# -------------------------------
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/checkpoints/kafka_to_postgres") \
    .start()

query.awaitTermination()

