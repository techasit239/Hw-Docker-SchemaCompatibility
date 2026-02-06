import argparse
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def build_sample_data(version: int) -> dict:
    """
    สร้างข้อมูลตัวอย่างให้ตรงกับ Schema ทั้ง 8 เวอร์ชัน
    เพื่อให้สามารถทดสอบได้ครบทุก Scenario (Real & Extra)
    """
    
    # ข้อมูลพื้นฐานที่ใช้ร่วมกัน (ชื่อ, ตำแหน่ง)
    base = {
        "full_name": "Koi Techasit",
        "position": "Operator",
    }

    # === Scenario 1: Base Version ===
    if version == 1:
        base["factory"] = "Factory-A"       # Type: String
        base["dropoff_point"] = "Gate 3"
        return base

    # === Scenario 2: Add Optional Fields ===
    if version == 2:
        base["factory"] = "Factory-A"       # Type: String
        base["dropoff_point"] = "Gate 3"
        base["insurance"] = "Group AIA"
        base["phone"] = "081-234-5678"
        return base

    # === Scenario 3: Remove Field ===
    if version == 3:
        base["factory"] = "Factory-A"       # Type: String
        # dropoff_point ถูกตัดออกไป
        base["insurance"] = None
        base["phone"] = "081-234-5678"    
        return base

    # === Scenario 4: Type Promotion (Int -> Float) ===
    # Schema v4 กำหนด factory เป็น float
    if version == 4:
        base["factory"] = 50.5              # Type: Float (เพื่อทดสอบการอ่านค่า)
        base["dropoff_point"] = "Gate 3"
        return base

    # === Scenario 5: Type Precision (Float -> Int) ===
    # Schema v5 กำหนด factory เป็น int
    if version == 5:
        base["factory"] = 50                # Type: Int
        base["dropoff_point"] = "Gate 3"
        return base

    # === Scenario 6: Type Mismatch (Int -> String) ===
    # Schema v6 กำหนด factory เป็น string (กลับมาเป็น string เพื่อทดสอบ Error กับ v5)
    if version == 6:
        base["factory"] = "Factory-String"  # Type: String
        base["dropoff_point"] = "Gate 3"    
        return base

    # === Scenario 7: Add Required Field ===
    # Schema v7 เพิ่ม citizen_id เป็น Required
    if version == 7:
        base["factory"] = "Factory-A"       # Type: String
        base["dropoff_point"] = "Gate 3"
        base["citizen_id"] = "1-1004-00000-00-0" # Required field ใหม่
        return base


    # === Scenario 8: Rename Field (factory -> plant) ===
    if version == 8:
        # เปลี่ยนชื่อ key ใน dict ให้ตรงกับ schema ใหม่
        base["plant"] = "Plant-A"  # เปลี่ยนจาก factory เป็น plant
        base["dropoff_point"] = "Gate 3"
        return base


    raise ValueError(f"❌ Unsupported schema version: {version}. Please use 1-8.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="employee-shuttle-queue")
    parser.add_argument("--subject", default="employee-shuttle-queue-value")
    # เปลี่ยน default help ให้เป็น 1-8
    parser.add_argument("--version", type=int, default=1, help="Schema version to use (1-8)")
    parser.add_argument("--key", default="emp001")
    parser.add_argument("--bootstrap", default="localhost:8097,localhost:8098,localhost:8099")
    parser.add_argument("--sr", default="http://localhost:8081")
    args = parser.parse_args()

    # 1. เชื่อมต่อ Schema Registry
    schema_registry_client = SchemaRegistryClient({"url": args.sr})

    try:
        # 2. พยายามดึง Schema ตาม Version ที่ระบุ
        meta = schema_registry_client.get_version(args.subject, version=args.version)
        schema_str = meta.schema.schema_str
        print(f"✅ Found Schema! Producer using subject={args.subject}, version={meta.version}")
    except Exception as e:
        print(f"❌ Error: Could not find Schema Version {args.version} on server.")
        print("   Did you run 'setup_complete_schemas.py' ?")
        print(f"   Details: {e}")
        return

    # 3. ตั้งค่า Serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        conf={"auto.register.schemas": False}  # Clean experiment: ใช้ Schema ที่มีอยู่แล้วเท่านั้น
    )

    producer_conf = {
        "bootstrap.servers": args.bootstrap,
        "key.serializer": StringSerializer(),
        "value.serializer": avro_serializer,
    }

    producer = SerializingProducer(producer_conf)

    # 4. สร้างและส่งข้อมูล
    try:
        value = build_sample_data(args.version)
        producer.produce(topic=args.topic, key=args.key, value=value)
        producer.flush()
        print(f"🚀 Produced to topic={args.topic} | Version={args.version}")
        print(f"📦 Data: {value}")
    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"❌ Produce Error: {e}")


if __name__ == "__main__":
    main()