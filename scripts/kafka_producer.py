#!/usr/bin/env python3
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import json

# -----------------------------
# Kafka + Schema Registry config
# -----------------------------
producer_config = {
    'bootstrap.servers': 'localhost:9094,localhost:9095,localhost:9096',  # Mapped Kafka ports
    'schema.registry.url': 'http://localhost:8083'                         # Mapped Schema Registry port
}

# -----------------------------
# Define Avro schema
# -----------------------------
value_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
"""
value_schema = avro.loads(value_schema_str)

# -----------------------------
# Create AvroProducer
# -----------------------------
producer = AvroProducer(producer_config, default_value_schema=value_schema)

# -----------------------------
# Produce example messages
# -----------------------------
topic_name = 'test_topic'

for i in range(1, 6):
    message = {'id': i, 'name': f'user{i}'}
    producer.produce(topic=topic_name, value=message)
    print(f"Produced message: {message}")

producer.flush()
print("All messages sent to Kafka (Avro format).")

