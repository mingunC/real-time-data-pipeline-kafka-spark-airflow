from kafka import KafkaProducer
import json
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_locations = ['Toronto', 'Vancouver', 'Seoul', 'New York', 'Los Angels']

for _ in range(10):
    sample_data = {
        "location": random.choice(sample_locations),
        "temperature": round(random.uniform(-10, 35), 2),
        "humidity": round(random.uniform(0.2, 0.9), 2)
    }
    producer.send("weather_events", value=sample_data)

producer.flush()
print(" Completed the sending of messages!")