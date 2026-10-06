import csv
import json
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_DIR, 'dataset', 'sensor_data.csv')
DASHBOARD_DATA_FILE = os.path.join(BASE_DIR, 'dashboard', 'latest_reading.json')


def load_sensor_data(file_path):
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


def update_dashboard(reading, count):
    data = dict(reading)
    data['total_messages'] = count
    os.makedirs(os.path.dirname(DASHBOARD_DATA_FILE), exist_ok=True)
    with open(DASHBOARD_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def main():
    readings = load_sensor_data(DATASET_PATH)
    print("=" * 60)
    print("🔥 REAL-TIME DEMO SIMULATOR ACTIVE 🔥")
    print("Updating 'dashboard/latest_reading.json' every 2 seconds...")
    print("Open http://localhost:8000 in your browser to watch live updates!")
    print("Press Ctrl+C to stop.")
    print("=" * 60)

    count = 0
    try:
        while True:
            for reading in readings:
                count += 1
                update_dashboard(reading, count)

                print(f"\n[Message #{count}] Sent reading:")
                print(f"  Sensor ID   : {reading['sensor_id']}")
                print(f"  Location    : {reading['location']}")
                print(f"  Temperature : {reading['temperature']} °C")
                print(f"  Humidity    : {reading['humidity']} %")
                print(f"  Status      : {reading['status']}")
                print(f"  Timestamp   : {reading['timestamp']}")

                time.sleep(2)
    except KeyboardInterrupt:
        print("\n\n[INFO] Simulation stopped.")


if __name__ == '__main__':
    main()
