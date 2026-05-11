# Kafka Clickstream Streaming Pipeline

Real-time clickstream analytics pipeline built using:

- Apache Kafka
- Spark Structured Streaming
- Python
- Docker

The project simulates realistic e-commerce user behavior and processes streaming events for:

- Cart abandonment detection
- Purchase session analytics
- Event-time streaming
- Session windows
- Watermarking

---

# Architecture

```text
Producer --> Kafka(clickstream) --> Spark Consumers
                    |
                    +--> Kafka UI
```

---

# Features

## Realistic Clickstream Simulation

The producer generates events like:

- `page_view`
- `search`
- `product_view`
- `add_to_cart`
- `purchase`

It also simulates:

- Late events
- Duplicate events
- Stateful user journeys
- Event-time delays

Source: :contentReference[oaicite:0]{index=0}

---

# Streaming Pipelines

## 1. Cart Abandonment Detection

Detects users who:

- Added products to cart
- Did not purchase
- Became inactive

Uses:

- Watermarking
- Session windows
- Stateful aggregation

Outputs to Kafka topic:

```text
abandoned_carts
```

Source: :contentReference[oaicite:1]{index=1}

---

## 2. User Purchase Summary

Processes purchase events and:

- Joins with product metadata
- Computes total spending
- Tracks purchased products per session

Outputs parquet summaries.

Source: :contentReference[oaicite:2]{index=2}

---

# Tech Stack

| Component | Technology |
|---|---|
| Streaming Broker | Kafka |
| Stream Processing | Spark Structured Streaming |
| Producer | Python |
| Consumers | Python + Spark |
| Containerization | Docker |
| Monitoring | Kafka UI |

---

# Project Structure

```text
.
├── producer.py
├── admin.py
├── docker-compose.yml
├── requirements.txt
├── products.csv
│
├── consumers/
│   ├── abandoned_carts_consumer.py
│   ├── cart_abandonment.py
│   ├── user_purchase_summary.py
│   └── funnel_analysis.py
│
├── Dockerfile
├── spark.Dockerfile
└── README.md
```

---

# Event Schema

```json
{
  "event_id": 1,
  "user_id": 44,
  "event_type": "add_to_cart",
  "product_id": 12,
  "time_generated": "2026-05-11T12:00:00Z",
  "time_received": "2026-05-11T12:00:03Z"
}
```

Schema source: :contentReference[oaicite:3]{index=3}

---

# Setup

## Start the pipeline

```bash
docker compose up --build
```

This starts:

- Kafka
- Kafka UI
- Producer
- Spark consumers
- Admin service

Docker Compose source: :contentReference[oaicite:4]{index=4}

---

# Kafka UI

Open:

```text
http://localhost:8080
```

You can:

- Inspect topics
- Monitor messages
- View consumer groups
- Debug offsets

---

# Kafka Topics

## clickstream

Raw clickstream events.

## abandoned_carts

Derived stream for abandoned carts.

Configured with:

- Topic compaction
- Retention policies
- Fast cleanup

Source: :contentReference[oaicite:5]{index=5}

---

# Important Streaming Concepts

## Watermarking

```python
withWatermark("event_time", "5 seconds")
```

## Session Windows

```python
session_window(col("event_time"), "5 seconds")
```

## Exactly-Once Producer Safety

```python
"acks": "all",
"enable.idempotence": True
```

Producer config source: :contentReference[oaicite:6]{index=6}

---

# Requirements

```text
confluent-kafka==2.14.0
```

Source: :contentReference[oaicite:7]{index=7}

---

# Notes

- Kafka broker inside Docker:
  
```text
kafka:9092
```

- Kafka UI:
  
```text
localhost:8080
```

- Spark runs inside Docker containers.

---