# Kafka + PySpark Retail Streaming Pipeline

Real-time retail order processing pipeline using Apache Kafka and PySpark Structured Streaming — running entirely on your laptop via Docker. No cloud account needed.

A Python producer generates fake retail orders every second. A PySpark streaming job reads from Kafka, transforms the data, and writes partitioned Parquet files every 30 seconds.

---

## Architecture

```
retail_producer.py
(generates 1 order/sec)
        │
        ▼
Kafka Topic: retail-transactions
(running in Docker)
        │
        ▼
PySpark Structured Streaming
- Parse JSON from Kafka
- Filter failed orders
- Add revenue_tier, discount_category
- Add processing timestamp
        │
        ▼
output/parquet/
(partitioned by country/category)
```

---

## Tech Stack

- **Apache Kafka** — message broker (via Docker)
- **PySpark Structured Streaming** — real-time processing
- **Docker + Docker Compose** — local infrastructure
- **Python / Faker** — synthetic order generation
- **Parquet** — columnar output storage

---

## Quick Start

### 1. Start Kafka
```bash
docker-compose up -d
# Kafka UI available at http://localhost:8090
```

### 2. Install Python packages
```bash
pip install -r requirements.txt
```

### 3. Start the producer (Terminal 1)
```bash
python producer/retail_producer.py
```

### 4. Start Spark streaming (Terminal 2)
```bash
python consumer/spark_streaming_consumer.py
```

### 5. Stop everything
```bash
# Ctrl+C in both terminals, then:
docker-compose down
```

---

## Sample Output

**Producer:**
```
✅ [   42] ORD-A3F8B2C1 | Laptop           |  $ 808.99 | completed  | US
✅ [   43] ORD-D9E1F4A2 | Running Shoes    |  $  80.99 | completed  | UK
```

**Spark batch summary:**
```
Batch 3 | 28 records
┌───────────────┬──────┬─────────┐
│category       │orders│revenue  │
├───────────────┼──────┼─────────┤
│Electronics    │8     │5,421.92 │
│Apparel        │7     │621.43   │
│Furniture      │4     │1,199.96 │
└───────────────┴──────┴─────────┘
✅ Batch 3 written to output/parquet/
```

---

## Project Structure

```
project2-kafka-spark-retail/
├── producer/
│   └── retail_producer.py         # Kafka producer
├── consumer/
│   └── spark_streaming_consumer.py # PySpark streaming job
├── output/                        # Created at runtime
│   ├── parquet/                   # Processed Parquet files
│   └── checkpoints/               # Spark checkpoints
├── docker-compose.yml             # Kafka + Zookeeper + UI
├── requirements.txt
└── README.md
```
