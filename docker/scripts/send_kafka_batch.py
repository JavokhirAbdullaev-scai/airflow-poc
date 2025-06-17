from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime, timedelta
import os

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Update with your Kafka broker
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'vehicle-topic-v1')



# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate 1000 events
for _ in range(1000):
    event = {
        'timestamp': int((datetime.now() - timedelta(seconds=random.randint(0, 3600))).timestamp()),
        'camera_id': str(uuid.uuid4()),
        'bbox': [random.randint(100, 1200), random.randint(100, 800), random.randint(1200, 1400), random.randint(800, 1000)],
        'license_plate': {
            'text': random.choice(['ABC123', 'XYZ789', '']),
            'arabic_text': '',
            'confidence': round(random.uniform(0, 1), 2),
            'text_confidence': round(random.uniform(0, 1), 2),
            'bbox': [random.randint(100, 500), random.randint(100, 300), random.randint(500, 700), random.randint(300, 400)]
        },
        'car_make': {
            'make': random.choice(['gacmotor', 'toyota', 'honda']),
            'model': random.choice(['ga4', 'camry', 'civic']),
            'confidence': round(random.uniform(0, 1), 2)
        },
        'color': {
            'name': random.choice(['black', 'white', 'red'])
        },
        'dwell_time': round(random.uniform(1, 10), 2)
    }
    producer.send(KAFKA_TOPIC, event)
    time.sleep(0.01)  # Small delay to avoid overwhelming broker

producer.flush()
producer.close()
print("Sent 1000 events to Kafka topic:", KAFKA_TOPIC)