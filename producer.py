import csv
import json
import os
import sys
import time
import warnings

# Suppress deprecation warnings for cleaner console output
warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError, KafkaError

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sensor-data'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_DIR, 'dataset', 'sensor_data.csv')


def create_producer():
    """Initializes and returns a KafkaProducer connected to localhost:9092."""
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka broker!\n")
        return producer
    except (KafkaConnectionError, KafkaError, Exception) as e:
        print(f"\n[ERROR] Could not connect to Kafka broker at {KAFKA_BROKER}.")
        print("Please verify that Apache Kafka service is up and running on port 9092.")
        print(f"Details: {e}")
        sys.exit(1)


def load_sensor_data(file_path):
    """Reads sensor dataset from CSV file and formats numerical data types."""
    if not os.path.exists(file_path):
        print(f"[ERROR] Dataset file not found at path: '{file_path}'")
        sys.exit(1)

    readings = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            readings.append({
                "timestamp": row["timestamp"].strip(),
                "sensor_id": row["sensor_id"].strip(),
                "location": row["location"].strip(),
                "temperature": float(row["temperature"]),
                "humidity": int(row["humidity"]),
                "status": row["status"].strip()
            })
    return readings


def stream_sensor_data():
    """Continuously reads data records and publishes them to the Kafka topic every 2 seconds."""
    producer = create_producer()
    readings = load_sensor_data(DATASET_PATH)

    print(f"Starting Sensor Data Streaming to Topic: '{TOPIC_NAME}'")
    print(f"Loaded {len(readings)} records. Streaming interval: 2.0 seconds.")
    print("Press Ctrl+C to stop streaming.")
    print("=" * 50)

    msg_count = 0
    try:
        while True:
            for reading in readings:
                producer.send(TOPIC_NAME, value=reading)
                producer.flush()
                msg_count += 1

                formatted_json = json.dumps(reading, indent=2)
                print(f"\nSending message #{msg_count}:")
                print(formatted_json)

                time.sleep(2)
    except KeyboardInterrupt:
        print("\n\n[INFO] Streaming interrupted by user.")
    finally:
        producer.close()
        print("Kafka Producer shutdown complete.")


if __name__ == '__main__':
    stream_sensor_data()
