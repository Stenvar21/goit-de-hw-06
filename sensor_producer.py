import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = str(random.randint(1000, 9999))
topic_name = "sten_building_sensors"

print(f"Датчик {sensor_id} запущено. Відправка даних...")

try:
    while True:
        data = {
            "id": sensor_id,
            "temperature": random.uniform(10, 45),
            "humidity": random.uniform(20, 90),
            "timestamp": datetime.now().isoformat()
        }
        producer.send(topic_name, value=data)
        print(f"Надіслано: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Зупинка...")
finally:
    producer.close()