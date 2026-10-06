import json
import os
import sys
import warnings

# Suppress deprecation warnings for cleaner console output
warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError, KafkaError

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sensor-data'
GROUP_ID = 'sensor-monitoring-group'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_DATA_FILE = os.path.join(BASE_DIR, 'dashboard', 'latest_reading.json')


def update_dashboard_data(reading, total_count):
    """Updates latest_reading.json for the optional live web dashboard."""
    try:
        data = dict(reading)
        data['total_messages'] = total_count
        os.makedirs(os.path.dirname(DASHBOARD_DATA_FILE), exist_ok=True)
        with open(DASHBOARD_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def start_consumer():
    """Subscribes to 'sensor-data' topic and listens for real-time sensor messages."""
    print(f"Connecting Kafka Consumer to {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"Successfully connected! Subscribed to topic: '{TOPIC_NAME}'")
        print("Waiting for incoming temperature sensor readings...\n")
        print("=" * 50)
    except (KafkaConnectionError, KafkaError, Exception) as e:
        print(f"\n[ERROR] Could not connect to Kafka broker at {KAFKA_BROKER}.")
        print("Please verify that Apache Kafka is running on port 9092.")
        print(f"Details: {e}")
        sys.exit(1)

    message_count = 0
    try:
        for message in consumer:
            reading = message.value
            message_count += 1

            # Display received message in specified format
            print("\n---------------------------------")
            print("New Sensor Reading")
            print(f"Sensor ID   : {reading.get('sensor_id')}")
            print(f"Location    : {reading.get('location')}")
            print(f"Temperature : {reading.get('temperature')} °C")
            print(f"Humidity    : {reading.get('humidity')} %")
            print(f"Status      : {reading.get('status')}")
            print(f"Timestamp   : {reading.get('timestamp')}")
            print("---------------------------------")

            # Persist latest reading for dashboard polling
            update_dashboard_data(reading, message_count)

    except KeyboardInterrupt:
        print("\n\n[INFO] Consumer stopped by user.")
    finally:
        consumer.close()
        print("Kafka Consumer closed gracefully.")


if __name__ == '__main__':
    start_consumer()
