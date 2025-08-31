from confluent_kafka import avro, Producer
from confluent_kafka.avro import AvroProducer

# Schema
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

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}

producer = AvroProducer(producer_config, default_value_schema=value_schema)

# Produce example messages
for i in range(1, 6):
    producer.produce(topic='test_topic', value={'id': i, 'name': f'user{i}'})

producer.flush()
print("Produced 5 messages to Kafka (Avro format).")
