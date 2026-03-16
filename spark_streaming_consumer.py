"""
spark_streaming_consumer.py
============================
PySpark Structured Streaming job that reads retail orders from Kafka,
transforms them, and writes processed Parquet files every 30 seconds.

This is the same pattern used in production Walmart-style retail pipelines.

Usage:
    python consumer/spark_streaming_consumer.py

Run this in Terminal 2 while the producer runs in Terminal 1.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, TimestampType
)

# ── CONFIG ─────────────────────────────────────────────
KAFKA_BROKER     = "localhost:9092"
KAFKA_TOPIC      = "retail-transactions"
OUTPUT_PATH      = "output/parquet"
CHECKPOINT_PATH  = "output/checkpoints"
TRIGGER_SECONDS  = 30    # process a batch every 30 seconds
# ───────────────────────────────────────────────────────

# Schema for the incoming JSON messages from Kafka
ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),    True),
    StructField("customer_id",     StringType(),    True),
    StructField("customer_name",   StringType(),    True),
    StructField("product_name",    StringType(),    True),
    StructField("category",        StringType(),    True),
    StructField("quantity",        IntegerType(),   True),
    StructField("unit_price",      FloatType(),     True),
    StructField("discount_pct",    FloatType(),     True),
    StructField("total_amount",    FloatType(),     True),
    StructField("payment_method",  StringType(),    True),
    StructField("status",          StringType(),    True),
    StructField("country",         StringType(),    True),
    StructField("city",            StringType(),    True),
    StructField("event_timestamp", StringType(),    True),
])


def create_spark_session():
    """Initialize Spark with the Kafka connector package."""
    print("⚡ Starting Spark session...")
    return (
        SparkSession.builder
        .appName("RetailStreamingPipeline")
        .master("local[*]")   # use all CPU cores
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_from_kafka(spark):
    """Create a streaming DataFrame from the Kafka topic."""
    print(f"📨 Connecting to Kafka topic: {KAFKA_TOPIC}")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform(raw_df):
    """
    Parse the JSON payload and apply transformations:
    - Parse raw Kafka bytes → structured columns
    - Filter out failed orders
    - Add revenue_tier (High/Medium/Low)
    - Add processing_time column
    - Round amounts to 2 decimal places
    """
    # Kafka delivers messages as bytes — parse the JSON value
    parsed = raw_df.select(
        F.from_json(
            F.col("value").cast("string"),
            ORDER_SCHEMA
        ).alias("data"),
        F.col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Filter out failed orders (only process completed/pending)
    filtered = parsed.filter(
        F.col("status").isin("completed", "pending", "returned")
    )

    # Add revenue tier
    enriched = filtered.withColumn(
        "revenue_tier",
        F.when(F.col("total_amount") >= 500,  "High")
         .when(F.col("total_amount") >= 100,  "Medium")
         .otherwise("Low")
    )

    # Add discount category
    enriched = enriched.withColumn(
        "discount_category",
        F.when(F.col("discount_pct") >= 0.20, "Heavy Discount")
         .when(F.col("discount_pct") >= 0.10, "Moderate Discount")
         .otherwise("Low Discount")
    )

    # Add processing timestamp
    enriched = enriched.withColumn("processed_at", F.current_timestamp())

    # Parse event_timestamp to proper TimestampType
    enriched = enriched.withColumn(
        "event_ts",
        F.to_timestamp(F.col("event_timestamp"))
    )

    return enriched


def process_batch(batch_df, batch_id):
    """Called for every micro-batch. Logs stats and writes Parquet."""
    count = batch_df.count()
    if count == 0:
        return

    # Log batch summary to console
    print(f"\n{'─'*60}")
    print(f"📦 Batch {batch_id} | {count} records")

    # Show category breakdown for this batch
    batch_df.groupBy("category").agg(
        F.count("order_id").alias("orders"),
        F.round(F.sum("total_amount"), 2).alias("revenue")
    ).orderBy(F.desc("revenue")).show(5, truncate=False)

    # Write to Parquet, partitioned by country and category
    (
        batch_df.write
        .mode("append")
        .partitionBy("country", "category")
        .parquet(OUTPUT_PATH)
    )

    print(f"✅ Batch {batch_id} written to {OUTPUT_PATH}/")


def main():
    # Create output directories
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # reduce noisy Spark logs

    print("\n🚀 Retail Streaming Pipeline Started")
    print(f"   Reading from: Kafka topic '{KAFKA_TOPIC}'")
    print(f"   Writing to:   {OUTPUT_PATH}/")
    print(f"   Batch size:   every {TRIGGER_SECONDS}s")
    print(f"   Kafka UI:     http://localhost:8090")
    print("-" * 60)

    # Read → Transform → Write
    raw_df      = read_from_kafka(spark)
    enriched_df = transform(raw_df)

    query = (
        enriched_df.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print(f"\n⏳ Waiting for data from Kafka...")
    print("   (Make sure the producer is running in Terminal 1)")
    print("   Press Ctrl+C to stop.\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹  Streaming stopped.")
        query.stop()
        spark.stop()
        print(f"✅ Parquet files saved to: {OUTPUT_PATH}/")
        print("   You can now open them with pandas: pd.read_parquet('output/parquet/')")


if __name__ == "__main__":
    main()
