# E-commerce Clickstream Pipeline

The project simulates realistic e-commerce user activity and processes streaming events for:

- Cart abandonment detection
- Purchase session analytics
- Product and category performance tracking
- Stateful funnel analysis

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
               +--------------------------+---+------------------+-----------+
               |                          |                      |           |
               v                          v                      v           v

+----------------------+  +-------------------------+  +--------------------+  +---------------------+
| cart_abandonment.py  |  | user_purchase_summary.py|  | product_summary.py |  | funnel_analysis.py  |
| Spark Streaming Job  |  | Spark Streaming Job     |  | Spark Streaming Job|  | Spark Streaming Job |
+----------+-----------+  +------------+------------+  +---------+----------+  | (stateful ASP)      |
           |                           |                          |             +----------+----------+
           v                           v                          v                        |
                                                                                           v
+----------------------+  +-------------------------+  +--------------------+  +---------------------+
| abandoned_carts      |  | parquet output          |  | parquet output     |  | parquet output      |
| Kafka Topic          |  | purchase summaries      |  | product + category |  | funnel metrics      |
+----------+-----------+  +-------------------------+  | summaries          |  +---------------------+
           |                                           +--------------------+
           v

+--------------------------+
| abandoned_carts_consumer |
| Python Debug Consumer    |
+--------------------------+
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
│   ├── product_summary.py
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

Detects users who added products to cart but did not complete a purchase within a session window. Publishes abandonment events to a dedicated Kafka topic consumed downstream by a CRM or notification system.

File:

```text
consumers/cart_abandonment.py
```

Output: `abandoned_carts` Kafka topic — `user_id`, `product_ids`, `ts`

---

## User Purchase Summary Pipeline

Processes purchase events and generates session-based purchase summaries enriched with product metadata. Writes results to Parquet for downstream batch processing.

File:

```text
consumers/user_purchase_summary.py
```

Output: `/tmp/user_purchase_summary` — `user_id`, `session_start`, `session_end`, `purchased_products`, `total_amount`

---

## Product Summary Pipeline

Joins purchase events with product metadata to produce two aggregations per micro-batch: a product-level summary and a category-level rollup. Both are written to separate Parquet directories.

File:

```text
consumers/product_summary.py
```

Output:
- `/tmp/product_summary` — `product_id`, `name`, `category`, `num_purchases`, `revenue`
- `/tmp/category_summary` — `category`, `num_products`, `num_purchases`, `revenue`

---

## Funnel Analysis Pipeline

Tracks each user's highest reached funnel stage using arbitrary stateful processing (ASP). Computes per-batch drop-off and conversion rates across all four funnel stages. Sessions expire after 30 minutes of inactivity.

File:

```text
consumers/funnel_analysis.py
```

Funnel stages:

| Stage | Event Types |
|---|---|
| 1 | `page_view`, `search` |
| 2 | `product_view` |
| 3 | `add_to_cart` |
| 4 | `purchase` |

Output: console — `funnel_stage`, `user_count`, `dropoff_rate_pct`, `conversion_rate_pct`

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
- Streamlit Dashboard

---
