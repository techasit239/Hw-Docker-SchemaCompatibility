import argparse
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka import SerializingProducer


def build_sample_data(version: int) -> dict:
    """
    Build a sample employee record.
    v1: full_name, factory, position, dropoff_point
    v2: v1 + insurance (nullable), phone (nullable)
    v3: (your v3 removed dropoff_point) -> full_name, factory, position, insurance, phone
    """
    base = {
        "full_name": "Koi Techasit",
        "factory": "Factory-A",
        "position": "Operator",
    }

    if version == 1:
        base["dropoff_point"] = "Gate 3"
        return base

    if version == 2:
        base["dropoff_point"] = "Gate 3"
        base["insurance"] = "Group AIA"
        base["phone"] = "0812345678"
        return base

    if version == 3:
        # v3 in your registry is the one without dropoff_point
        base["insurance"] = None
        base["phone"] = "0812345678"
        return base

    raise ValueError("Unsupported schema version. Use 1, 2, or 3.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="employee-shuttle-queue")
    parser.add_argument("--subject", default="employee-shuttle-queue-value")
    parser.add_argument("--version", type=int, default=1, help="Schema version to use (1/2/3)")
    parser.add_argument("--key", default="emp001")
    parser.add_argument("--bootstrap", default="localhost:8097,localhost:8098,localhost:8099")
    parser.add_argument("--sr", default="http://localhost:8081")
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": args.sr})

    meta = schema_registry_client.get_version(args.subject, version=args.version)
    schema_str = meta.schema.schema_str
    print(f"Producer using subject={args.subject}, schema_version={meta.version}")

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        conf={"auto.register.schemas": False}  # use existing schema only (clean experiment)
    )

    producer_conf = {
        "bootstrap.servers": args.bootstrap,
        "key.serializer": StringSerializer(),
        "value.serializer": avro_serializer,
    }

    producer = SerializingProducer(producer_conf)

    value = build_sample_data(args.version)
    producer.produce(topic=args.topic, key=args.key, value=value)
    producer.flush()
    print(f"Produced to topic={args.topic}, key={args.key}, value={value}")


if __name__ == "__main__":
    main()
