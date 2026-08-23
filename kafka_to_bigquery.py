"""
kafka_to_bigquery.py
Pure Python Kafka consumer → BigQuery
Replaces spark_consumer.py with zero JVM dependencies
Same data pipeline, simpler implementation
"""
import json
import os
import logging
from datetime import datetime
from kafka import KafkaConsumer
from google.cloud import bigquery
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_to_bq")

PROJECT   = os.getenv("GCP_PROJECT", "demand-intelligence-504514")
DATASET   = "ecommerce_events"
BATCH_SIZE = 100    # write to BigQuery every 100 messages

bq = bigquery.Client(project=PROJECT)

def validate_order(event: dict) -> bool:
    """Schema validation — same logic as PySpark StructType check."""
    required = ["order_id", "product_id", "category",
                "final_price", "region", "timestamp"]
    return all(event.get(f) for f in required)

def write_batch(rows: list, table_id: str):
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    job = bq.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job.result()
    log.info(f"✅ Wrote {len(rows)} rows → {table_id}")

def consume(topics: list):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="bq-consumer-group",
        enable_auto_commit=True,
    )

    orders_buffer  = []
    pricing_buffer = []
    total          = 0

    log.info(f"🚀 Consuming {topics} → BigQuery | batch size={BATCH_SIZE}")

    try:
        for msg in consumer:
            event    = msg.value
            topic    = msg.topic
            total   += 1

            if topic == "orders-topic":
                if validate_order(event):
                    orders_buffer.append(event)
                else:
                    log.warning(f"Bad event skipped: {event.get('order_id')}")

            elif topic == "pricing-topic":
                # Add derived column inline — was done in Spark before
                event["is_undercut"] = event.get("price_diff", 0) < 0
                pricing_buffer.append(event)

            # Flush buffers when batch size reached
            if len(orders_buffer) >= BATCH_SIZE:
                write_batch(orders_buffer,
                    f"{PROJECT}.{DATASET}.orders")
                orders_buffer = []

            if len(pricing_buffer) >= BATCH_SIZE:
                write_batch(pricing_buffer,
                    f"{PROJECT}.{DATASET}.pricing_events")
                pricing_buffer = []

            if total % 50 == 0:
                log.info(f"Consumed {total} messages | "
                         f"orders buffer: {len(orders_buffer)} | "
                         f"pricing buffer: {len(pricing_buffer)}")

    except KeyboardInterrupt:
        # Flush remaining on exit
        if orders_buffer:
            write_batch(orders_buffer,
                f"{PROJECT}.{DATASET}.orders")
        if pricing_buffer:
            write_batch(pricing_buffer,
                f"{PROJECT}.{DATASET}.pricing_events")
        log.info(f"⏹ Stopped. Total messages consumed: {total}")
        consumer.close()

if __name__ == "__main__":
    consume(["orders-topic", "pricing-topic"])