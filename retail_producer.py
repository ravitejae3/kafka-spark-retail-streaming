"""
retail_producer.py
==================
Simulates a retail system sending real-time order events to Kafka.
Generates a new fake order every second and sends it to the
'retail-transactions' Kafka topic.

Usage:
    python producer/retail_producer.py

Leave this running in Terminal 1 while the Spark consumer runs in Terminal 2.
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
random.seed()  # different data each run

# ── CONFIG ─────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC_NAME    = "retail-transactions"
SEND_INTERVAL = 1.0   # seconds between messages
# ───────────────────────────────────────────────────────

PRODUCTS = [
    {"name": "Laptop",        "category": "Electronics",  "base_price": 899.99},
    {"name": "Smartphone",    "category": "Electronics",  "base_price": 699.99},
    {"name": "Headphones",    "category": "Electronics",  "base_price": 149.99},
    {"name": "Running Shoes", "category": "Apparel",      "base_price": 89.99},
    {"name": "T-Shirt",       "category": "Apparel",      "base_price": 29.99},
    {"name": "Jeans",         "category": "Apparel",      "base_price": 59.99},
    {"name": "Coffee Maker",  "category": "Home",         "base_price": 49.99},
    {"name": "Blender",       "category": "Home",         "base_price": 39.99},
    {"name": "Desk Chair",    "category": "Furniture",    "base_price": 299.99},
    {"name": "Bookshelf",     "category": "Furniture",    "base_price": 149.99},
    {"name": "Novel",         "category": "Books",        "base_price": 14.99},
    {"name": "Cookbook",      "category": "Books",        "base_price": 24.99},
    {"name": "Yoga Mat",      "category": "Sports",       "base_price": 34.99},
    {"name": "Dumbbells",     "category": "Sports",       "base_price": 59.99},
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
COUNTRIES       = ["US", "US", "US", "US", "UK", "CA", "AU", "IN"]  # US weighted


def generate_order():
    """Create a single realistic retail order event."""
    product    = random.choice(PRODUCTS)
    quantity   = random.randint(1, 5)
    discount   = round(random.uniform(0, 0.3), 2)  # 0–30% discount
    unit_price = round(product["base_price"] * (1 - discount), 2)
    total      = round(unit_price * quantity, 2)

    # Occasionally simulate failed/returned orders for realistic data
    status = random.choices(
        ["completed", "completed", "completed", "completed", "pending", "failed", "returned"],
        weights=[60, 60, 60, 60, 10, 5, 5]
    )[0]

    return {
        "order_id":        f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id":     f"CUST-{random.randint(1000, 9999)}",
        "customer_name":   fake.name(),
        "product_name":    product["name"],
        "category":        product["category"],
        "quantity":        quantity,
        "unit_price":      unit_price,
        "discount_pct":    discount,
        "total_amount":    total,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "status":          status,
        "country":         random.choice(COUNTRIES),
        "city":            fake.city(),
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def connect_producer():
    """Try to connect to Kafka, retry if not ready."""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # wait for all replicas to confirm
                retries=3,
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready yet (attempt {attempt+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka. Is docker-compose running?")


def main():
    print("🛒 Retail Order Event Producer")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Topic:  {TOPIC_NAME}")
    print(f"   Rate:   1 message / {SEND_INTERVAL}s")
    print("-" * 50)

    producer   = connect_producer()
    sent_count = 0

    try:
        while True:
            order = generate_order()

            producer.send(TOPIC_NAME, value=order)
            sent_count += 1

            print(
                f"✅ [{sent_count:>5}] "
                f"{order['order_id']} | "
                f"{order['product_name']:<16} | "
                f"${order['total_amount']:>8.2f} | "
                f"{order['status']:<10} | "
                f"{order['country']}"
            )

            # Flush every 10 messages
            if sent_count % 10 == 0:
                producer.flush()

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n⏹  Stopped. Total messages sent: {sent_count:,}")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
