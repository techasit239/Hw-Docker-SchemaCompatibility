import argparse
from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka import DeserializingConsumer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="employee-shuttle-queue")
    parser.add_argument("--subject", default="employee-shuttle-queue-value")
    parser.add_argument("--reader-version", type=int, default=0,
                        help="Reader schema version (0 = use writer schema only).")
    parser.add_argument("--group", default="employee-avro-group")
    parser.add_argument("--bootstrap", default="localhost:8097,localhost:8098,localhost:8099")
    parser.add_argument("--sr", default="http://localhost:8081")
    parser.add_argument("--offset", default="earliest", choices=["earliest", "latest"])
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": args.sr})

    # If reader-version is provided, we pass it as reader schema to demonstrate evolution
    reader_schema_str = None
    if args.reader_version and args.reader_version > 0:
        meta = schema_registry_client.get_version(args.subject, version=args.reader_version)
        reader_schema_str = meta.schema.schema_str
        print(f"Consumer using READER schema: subject={args.subject}, version={meta.version}")
    else:
        print("Consumer using WRITER schema (no reader schema override)")

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str=reader_schema_str
    )

    consumer_conf = {
        "bootstrap.servers": args.bootstrap,
        "key.deserializer": StringDeserializer(),
        "value.deserializer": avro_deserializer,
        "group.id": args.group,
        "auto.offset.reset": args.offset,
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([args.topic])

    print(f"Waiting messages on topic={args.topic} ... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = msg.value()
            print(f"Key={msg.key()} Value={data}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
