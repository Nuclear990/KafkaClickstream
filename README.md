# E-commerce Clickstream Pipeline

The project simulates realistic e-commerce user activity and processes streaming events for:

- Cart abandonment detection
- Purchase session analytics
- Stateful streaming pipelines

---

# Architecture

```text
                +-------------------+
                |   producer.py     |
                | Clickstream Sim   |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Kafka Topic       |
                |   clickstream     |
                +---------+---------+
                          |
          +---------------+----------------+
          |                                |
          v                                v

+----------------------+      +---------------------------+
| cart_abandonment.py  |      | user_purchase_summary.py |
| Spark Streaming Job  |      | Spark Streaming Job      |
+----------+-----------+      +-------------+-------------+
           |                                  |
           v                                  v

+----------------------+      +---------------------------+
| abandoned_carts      |      | parquet output            |
| Kafka Topic          |      | purchase summaries        |
+----------+-----------+      +---------------------------+
           |
           v

+-------------------------------+
| abandoned_carts_consumer.py   |
| Python Debug Consumer         |
+-------------------------------+
```

---

# Tech Stack

| Component | Technology |
|---|---|
| Streaming Broker | Kafka |
| Stream Processing | Spark Structured Streaming |
| Producer | Python |
| Consumers | Python + Spark |
| Containerization | Docker |

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

Supported event types:

- `page_view`
- `search`
- `product_view`
- `add_to_cart`
- `purchase`

---

# Pipelines

## Cart Abandonment Pipeline

Detects users who added products to cart but did not complete a purchase.

File:

```text
consumers/cart_abandonment.py
```

---

## User Purchase Summary Pipeline

Processes purchase events and generates session-based purchase summaries.

File:

```text
consumers/user_purchase_summary.py
```

---

# Running the Project

Start all services:

```bash
docker compose up --build
```

This starts:

- Kafka
- Producer
- Spark consumers
- Topic admin service

---

# Requirements

```text
confluent-kafka==2.14.0
```

---