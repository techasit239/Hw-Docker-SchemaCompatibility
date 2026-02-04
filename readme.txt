-- Install Package Python -- 

pip install confluent_kafka
pip install authlib
pip install fastavro

-- Run Program --

1. cd [... path directory to the path where docker-compose.yml is located ..]
2. run command : docker-compose up -d
3. run python ( first_time ) :  python producer_avro_first_run.py
4. Verify that CTSCAN-ROOM-value is registered in the Schema Registry.