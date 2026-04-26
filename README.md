# Kafka Clickstream Simulation Project

##  Overview

This project simulates a **real-time clickstream pipeline** using Apache Kafka.

* A **Producer** generates user activity events (page views, clicks, purchases)
* A **Consumer** reads and processes these events
* Kafka runs via Docker
* Kafka UI is used for visualization

This mimics how real-world systems track user behavior on websites/apps.

---

##  Architecture

```
Producer (Python) ---> Kafka Topic (clickstream) ---> Consumer (Python)
                                 |
                              Kafka UI
```

---

##  Tech Stack

* Python
* Apache Kafka
* Docker & Docker Compose
* confluent-kafka (Python client)

---

##  Project Structure

```
.
├── producer.py
├── consumer.py
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🚀 Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/your-username/clickstream-kafka.git
cd clickstream-kafka
```

---

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate     # Linux/Mac
# OR
venv\Scripts\activate        # Windows
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Start Kafka using Docker

```bash
docker-compose up -d
```

Wait ~30–40 seconds for Kafka to become healthy.

---

### 5. Run Producer

```bash
python producer.py
```

This will continuously send random clickstream events.

---

### 6. Run Consumer (in another terminal)

```bash
python consumer.py
```

You should see events being consumed in real time.

---

## Kafka UI

Access Kafka UI at:

```
http://localhost:8080
```

* View topic: `clickstream`
* Inspect messages
* Monitor consumer groups

---

##  Sample Event

```json
{
  "user_id": "user1",
  "event": "click",
  "page": "/products",
  "time": "2026-04-26T12:00:00Z"
}
```

---

## 🧠Key Concepts Demonstrated

* Event-driven architecture
* Producer–Consumer model
* Kafka topics & partitions
* Consumer groups
* Real-time data streaming
* Offset management

---

##  Important Notes

* Kafka runs inside Docker → use `kafka:9092` as bootstrap server
* Do NOT change it to `localhost:9092` inside Python scripts
* Topic `clickstream` is auto-created
* Virtual environment (`venv/`) is intentionally excluded from Git

---

##  Stopping Services

```bash
docker-compose down
```

