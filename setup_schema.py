import requests
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

def setup_complete_schemas():
    # Configuration
    sr_url = 'http://localhost:8081'
    subject_name = "employee-shuttle-queue-value"
    client = SchemaRegistryClient({'url': sr_url})

    print(f"🚀 Starting Comprehensive Setup (v1-v7) for: {subject_name}\n")

    # ⚠️ Step 0: Force Compatibility to NONE (เพื่อให้ลงทะเบียน Schema ผิดๆ ได้)
    try:
        requests.put(
            f"{sr_url}/config/{subject_name}", 
            json={"compatibility": "NONE"}, 
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        print("🔓 Set Compatibility to NONE (Allows all schema types).")
    except:
        pass

    # ---------------------------------------------------------
    # 📝 Schema Definitions (v1 - v7)
    # ---------------------------------------------------------
    
    # v1: Base
    s_v1 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"string"},
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"}
    ]}"""

    # v2: Add Optional
    s_v2 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"string"},
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"},
        {"name":"insurance","type":["null","string"],"default":null},
        {"name":"phone","type":["null","string"],"default":null}
    ]}"""

    # v3: Remove Field
    s_v3 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"string"},
        {"name":"position","type":"string"},
        {"name":"insurance","type":["null","string"],"default":null},
        {"name":"phone","type":["null","string"],"default":null}
    ]}"""

    # v4: Factory -> Float
    s_v4 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"float"}, 
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"}
    ]}"""

    # v5: Factory -> Int
    s_v5 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"int"},
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"}
    ]}"""

    # v6: Factory -> String (To test Int <-> String mismatch against v5)
    s_v6 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"string"},
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"}
    ]}"""

    # v7: Add Required (Citizen ID)
    s_v7 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"factory","type":"string"},
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"},
        {"name":"citizen_id","type":"string"}
    ]}"""

    # v8: Rename Field 'factory' -> 'plant' (Breaking Change for FULL)
    s_v8 = """{"type":"record","name":"Employee","namespace":"com.employee","fields":[
        {"name":"full_name","type":"string"},
        {"name":"plant","type":"string"}, 
        {"name":"position","type":"string"},
        {"name":"dropoff_point","type":"string"}
    ]}"""

    # Register Loop
    schemas = [s_v1, s_v2, s_v3, s_v4, s_v5, s_v6, s_v7, s_v8]
    
    print("\n⏳ Registering Schemas...")
    for i, s_str in enumerate(schemas, 1):
        try:
            sid = client.register_schema(subject_name, Schema(s_str, "AVRO"))
            print(f"   ✅ Registered v{i}: ID = {sid}")
        except Exception as e:
            print(f"   ❌ Failed v{i}: {e}")

if __name__ == "__main__":
    setup_complete_schemas()