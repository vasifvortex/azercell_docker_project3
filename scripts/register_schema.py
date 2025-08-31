import requests
import json

# Schema Registry URL
schema_registry_url = "http://localhost:8081"

# Subject under which schema will be registered
subject = "test_topic-value"

# Define the Avro schema as a string
avro_schema = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"}
    ]
}

# Payload for the Schema Registry API
payload = {
    "schema": json.dumps(avro_schema)
}

# Headers
headers = {
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}

# Register schema
response = requests.post(
    f"{schema_registry_url}/subjects/{subject}/versions",
    headers=headers,
    data=json.dumps(payload)
)

# Output result
print("Status Code:", response.status_code)
print("Response:", response.json())

