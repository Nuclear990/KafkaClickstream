# E-commerce Clickstream Pipeline

The project simulates realistic e-commerce user activity and processes streaming events for:

* Cart abandonment detection
* Purchase session analytics
* Product and category performance tracking
* Stateful funnel analysis
* Gold-layer KPI aggregation for dashboards

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
| cart_abandonment.py  |  | user_purchase_summary.py|  | products_summary.py|  | funnel_analysis.py  |
| Spark Streaming Job  |  | Spark Streaming Job     |  | Spark Streaming Job|  | Spark Streaming Job |
+----------+-----------+  +------------+------------+  +---------+----------+  | (stateful ASP)      |
           |                           |                          |             +----------+----------+
           v                           v                          v                        |
                                                                                           v
+----------------------+  +-------------------------+  +--------------------+  +---------------------+
| abandoned_carts      |  | parquet output          |  | parquet output     |  | parquet output      |
| Kafka Topic          |  | user summaries          |  | product + category |  | funnel metrics      |
+----------+-----------+  +------------+------------+  | summaries          |  +---------------------+
           |                           |               +--------------------+
           v                           v
+-------------------------------+   +------------------+
| abandoned_carts_consumer.py   |   | user_gold.py    |
| Python Debug Consumer         |   | Gold Aggregates |
+-------------------------------+   +--------+---------+
                                             |
                                             v
                                  +----------------------+
                                  | Streamlit Dashboard  |
                                  | user_dashboard.py    |
                                  +----------------------+
```

---

# Tech Stack

| Component         | Technology                 |
| ----------------- | -------------------------- |
| Streaming Broker  | Kafka                      |
| Stream Processing | Spark Structured Streaming |
| Producer          | Python                     |
| Consumers         | Python + Spark             |
| Dashboard         | Streamlit                  |
| Containerization  | Docker                     |

---

# Project Structure

```text
.
├── producer.py
├── admin.py
├── docker-compose.yml
├── requirements.txt
├── reset_data.sh
│
├── consumers/
│   ├── abandoned_carts_consumer.py
│   ├── cart_abandonment.py
│   ├── user_purchase_summary.py
│   ├── products_summary.py
│   └── funnel_analysis.py
│
├── streamlit/
│   ├── user_dashboard.py
│   └── streamlit.Dockerfile
│
├── user_gold.py
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

* `page_view`
* `search`
* `product_view`
* `add_to_cart`
* `purchase`

---

# Pipelines

## Cart Abandonment Pipeline

Detects users who added products to cart but did not complete a purchase within a session window. Publishes abandonment events to a dedicated Kafka topic.

File:

```text
consumers/cart_abandonment.py
```

Output: `abandoned_carts` Kafka topic — `user_id`, `product_ids`, `ts`

---

## User Purchase Summary Pipeline

Processes purchase events and generates session-based purchase summaries. Results are written to parquet for downstream aggregation.

File:

```text
consumers/user_purchase_summary.py
```

Output:

```text
/app/data/output/users
```

Columns:

* `user_id`
* `session_start`
* `session_end`
* `total_amount`

---

## Product Summary Pipeline

Joins purchase events with product metadata to produce product-level and category-level aggregations.

File:

```text
consumers/products_summary.py
```

Outputs:

```text
/app/data/output/product
/app/data/output/category
```

---

## Funnel Analysis Pipeline

Tracks each user's highest reached funnel stage using arbitrary stateful processing (ASP). Computes drop-off and conversion rates across all funnel stages.

File:

```text
consumers/funnel_analysis.py
```

Funnel stages:

| Stage | Event Types           |
| ----- | --------------------- |
| 1     | `page_view`, `search` |
| 2     | `product_view`        |
| 3     | `add_to_cart`         |
| 4     | `purchase`            |

Output:

```text
/app/data/output/funnel
```

---

## Gold Metrics Pipeline

Consumes silver-layer user summaries and produces dashboard-ready KPI aggregates.

File:

```text
user_gold.py
```

Generated metrics:

* Average Order Value (AOV)
* Average Purchase Session Duration
* Session Spend Distribution Histogram

Outputs:

```text
/app/data/gold_output/users/averages
/app/data/gold_output/users/spend_distribution
```

---

# Dashboard

Streamlit dashboard visualizing gold-layer metrics.

File:

```text
streamlit/user_dashboard.py
```

Features:

* Average Order Value KPI
* Average Session Duration KPI
* Session Spend Distribution Chart
* Last 2 hours rolling view

Access:

```text
http://localhost:8501
```

---

# Running the Project

Start all services:

```bash
docker compose up --build
```

This starts:

* Kafka
* Topic admin service
* Producer
* Spark streaming jobs
* Gold aggregation pipeline
* Streamlit dashboard

---

# Resetting Data

Clear checkpoints and parquet outputs:

```bash
./reset_data.sh
```

---

