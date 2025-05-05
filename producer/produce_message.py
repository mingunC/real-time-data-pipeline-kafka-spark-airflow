from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_data = {
    "location": "Toronto",
    "temperature": 21.5,
    "humidity": 0.75
}

producer.send("weather_events", value=sample_data)
producer.flush()
print(" Completed the sending of the message!")