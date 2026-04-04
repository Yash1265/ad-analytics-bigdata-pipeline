from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "campaign_id": random.randint(101, 105),
        "clicks": random.randint(1, 50),
        "spend": round(random.uniform(10, 100), 2)
    }

    producer.send('ad-events', data)
    print("Sent:", data)

    time.sleep(1)
