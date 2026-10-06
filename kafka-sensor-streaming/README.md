# Real-Time Sensor Data Streaming Using Apache Kafka

A beginner-friendly academic mini-project demonstrating real-time data streaming using **Apache Kafka** and **Python**.

This project simulates a IoT temperature sensor producing streaming readings sent to an Apache Kafka topic. A Kafka consumer receives and displays the readings in real-time, with an optional live web dashboard.

---

## 📌 1. What is Apache Kafka?

**Apache Kafka** is an open-source distributed event streaming platform used for high-performance data pipelines, streaming analytics, and data integration.

### Core Kafka Concepts Demonstrated:

* **Kafka Broker**: The server that receives, stores, and serves messages. By default, it runs on `localhost:9092`.
* **Kafka Producer**: An application (`producer.py`) that generates and publishes data/events to Kafka.
* **Kafka Topic**: A named stream/channel (`sensor-data`) where messages are stored and organized.
* **Kafka Consumer**: An application (`consumer.py`) that subscribes to a topic to read and process messages in real-time.

---

## 🏗️ 2. Project Architecture

```text
+----------------------------+
| Temperature Sensor         |
| (dataset/sensor_data.csv)  |
+----------------------------+
              │
              ▼
+----------------------------+
| Kafka Producer             |
| (producer.py)              |
+----------------------------+
              │
              │ JSON Messages (Every 2s)
              ▼
+----------------------------+
| Kafka Broker               |
| Topic: sensor-data         |
| (localhost:9092)           |
+----------------------------+
              │
              │ Subscribe & Fetch
              ▼
+----------------------------+
| Kafka Consumer             |
| (consumer.py)              |
+----------------------------+
              │
      ┌───────┴────────┐
      ▼                ▼
+-----------+   +──────────────────+
|  Console  |   | Simple Dashboard |
|  Output   |   | (dashboard/)     |
+-----------+   +──────────────────+
```

---

## 📁 3. Project Structure

```text
kafka-sensor-streaming/
│
├── producer.py              # Kafka Producer script (publishes sensor readings)
├── consumer.py              # Kafka Consumer script (listens for sensor readings)
├── requirements.txt        # Python dependency list
├── dataset/
│   └── sensor_data.csv     # Sample sensor readings dataset
├── dashboard/               # Optional web UI dashboard
│   ├── index.html
│   ├── style.css
│   └── script.js
└── README.md                # Project documentation & execution guide
```

---

## 🚀 4. Step-by-Step Setup & Run Guide

### Prerequisites
* **Python 3.7+** installed
* **Apache Kafka** installed locally (or via Docker / Homebrew)

---

### Step 1: Install Python Dependencies

Open your terminal and navigate to the project directory:

```bash
cd kafka-sensor-streaming
pip install -r requirements.txt
```

*(Or inside a virtual environment):*
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### Step 2: Start Apache Kafka

#### Option A: Using Homebrew (macOS)
```bash
# Start Zookeeper (if using Kafka Zookeeper mode)
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

# In a new terminal tab, start Kafka Broker
kafka-server-start /usr/local/etc/kafka/server.properties
```

#### Option B: Using KRaft mode (Kafka 3.x+ without Zookeeper)
```bash
# Generate Cluster ID & format log directory
KAFKA_CLUSTER_ID="$(kafka-storage generate-cluster-id)"
kafka-storage format -t $KAFKA_CLUSTER_ID -c /usr/local/etc/kafka/kraft/server.properties

# Start Kafka Server
kafka-server-start /usr/local/etc/kafka/kraft/server.properties
```

#### Option C: Using Docker (Quickest)
```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

---

### Step 3: Create the Kafka Topic (Optional)

Kafka automatically creates topics when messages are produced, but you can also create `sensor-data` manually:

```bash
kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

To list existing topics:
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

---

### Step 4: Run the Kafka Consumer

Open a **new terminal** tab/window and start the consumer first so it is ready to receive incoming messages:

```bash
python3 consumer.py
```

---

### Step 5: Run the Kafka Producer

Open **another terminal** tab/window and run the producer:

```bash
python3 producer.py
```

The producer will read `dataset/sensor_data.csv` line by line and stream each JSON message to Kafka every 2 seconds.

---

### Step 6: View Optional Web Dashboard (Optional)

If you wish to view the live graphical dashboard:

```bash
cd dashboard
python3 -m http.server 8000
```

Now open your browser and navigate to: `http://localhost:8000`

---

## 📊 5. Expected Output

### Producer Terminal Output:
```text
Connecting to Kafka broker at localhost:9092...
Successfully connected to Kafka broker!

Starting Sensor Data Streaming to Topic: 'sensor-data'
Loaded 20 records. Streaming interval: 2.0 seconds.
Press Ctrl+C to stop streaming.
==================================================

Sending message #1:
{
  "timestamp": "2026-10-06 10:00:01",
  "sensor_id": "S001",
  "location": "Room-A",
  "temperature": 27.4,
  "humidity": 61,
  "status": "NORMAL"
}
```

### Consumer Terminal Output:
```text
Connecting Kafka Consumer to localhost:9092...
Successfully connected! Subscribed to topic: 'sensor-data'
Waiting for incoming temperature sensor readings...
==================================================

---------------------------------
New Sensor Reading

Sensor ID   : S001
Location    : Room-A
Temperature : 27.4 °C
Humidity    : 61 %
Status      : NORMAL
Timestamp   : 2026-10-06 10:00:01
---------------------------------
```

---

## 🔍 6. How It Works (Producer → Topic → Consumer)

1. **Producer (`producer.py`)**: Reads the dataset row by row, formats it into JSON, and sends it as bytes over a TCP socket to Kafka topic `sensor-data`.
2. **Kafka Topic (`sensor-data`)**: Acts as a distributed immutable append-only log storing published sensor records.
3. **Consumer (`consumer.py`)**: Subscribes to `sensor-data`, continuously fetches new unread messages, deserializes the JSON string, and prints out telemetry readings in real-time.
