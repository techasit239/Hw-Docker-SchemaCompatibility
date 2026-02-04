from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer

topic = "CTSCAN-ROOM"

avro_schema_str = """
{
  "namespace": "example.avro",
  "type": "record",
  "name": "CTSCAN_ROOM",
  "fields": [
    {"name": "HN", "type": "int"},
    {"name": "Name", "type": "string"},
    {"name": "Surname", "type": "string"},
    {"name": "Room_Number", "type": "int"},
    {"name": "CTSCAN_Type", "type": "int"}
  ]
}
"""

sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema_str
)

producer_conf = {
    'bootstrap.servers': 'localhost:8097,localhost:8098,localhost:8099',
    'key.serializer': StringSerializer(),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

data = {
    "HN": 45678,
    "Name": "Sahaphum",
    "Surname": "Ketkaew",
    "Room_Number": 1,
    "CTSCAN_Type": 1
}

producer.produce(topic=topic, key="c001", value=data)
producer.flush()

print("✅ Avro message produced and schema registered")
