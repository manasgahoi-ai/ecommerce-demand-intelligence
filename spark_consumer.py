"""
PySpark Structured Streaming Consumer
Electronics E-Commerce Demand Intelligence Platform

What this does:
  1. Connects to local Kafka (orders-topic + pricing-topic)
  2. Parses and validates JSON event schemas
  3. Writes clean micro-batches into BigQuery every 15 seconds
  4. Uses checkpointing — safe to restart, no data loss or duplicates

Run:
  python spark_consumer.py --mode orders     # consume orders only
  python spark_consumer.py --mode pricing    # consume pricing only
  python spark_consumer.py --mode both       # consume both (default)

Prerequisites:
  pip install pyspark==3.5.0 google-cloud-bigquery db-dtypes
  Java 11 or 17 must be installed (java -version to check)
  GOOGLE_APPLICATION_CREDENTIALS env var must point to credentials.json
"""
import os

import argparse
import logging
from datetime import datetime
from google.cloud import bigquery

from pyspark import __version__ as PYSPARK_VERSION
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("spark_consumer")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# Keep all tuneable values in one place
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    "gcp_project":        os.getenv("GCP_PROJECT", "demand-intelligence-504514"),
    "kafka_bootstrap":    os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    # checkpoint_dir MUST live on a POSIX-compliant filesystem. When running
    # under WSL from /mnt/e/... (Windows /mnt), Hadoop's RawLocalFileSystem
    # calls chmod 700 on the checkpoint root and NTFS denies it with
    # "chmod: Operation not permitted (os error 1)". Use /tmp by default and
    # let CHECKPOINT_DIR env var override for native-Linux or container runs.
    "checkpoint_dir":     os.getenv("CHECKPOINT_DIR", "/tmp/demand-intelligence/checkpoints"),
    "bq_dataset":         "ecommerce_events",
    "bq_orders_table":    "orders",
    "bq_pricing_table":   "pricing_events",
    "trigger_seconds":    15,                                                # micro-batch every 15s
    "kafka_start_offset": "latest",                                          # 'latest' = only new events
                                                                             # 'earliest' = all history
}


# ─────────────────────────────────────────────────────────────────────────────
# EVENT SCHEMAS
# Must exactly match the JSON fields produced by synthetic_generator.py
# PySpark uses these to parse the raw Kafka bytes into typed columns
# ─────────────────────────────────────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("event_type",        StringType()),
    StructField("order_id",          StringType()),
    StructField("product_id",        StringType()),
    StructField("product_name",      StringType()),
    StructField("category",          StringType()),
    StructField("brand",             StringType()),
    StructField("price_tier",        StringType()),
    StructField("base_price",        DoubleType()),
    StructField("final_price",       DoubleType()),
    StructField("discount_percent",  DoubleType()),
    StructField("quantity",          IntegerType()),
    StructField("total_value",       DoubleType()),
    StructField("region",            StringType()),
    StructField("city",              StringType()),
    StructField("payment_method",    StringType()),
    StructField("customer_segment",  StringType()),
    StructField("is_returned",       BooleanType()),
    StructField("timestamp",         StringType()),   # parsed to TIMESTAMP below
])

PRICING_SCHEMA = StructType([
    StructField("event_type",        StringType()),
    StructField("product_id",        StringType()),
    StructField("product_name",      StringType()),
    StructField("category",          StringType()),
    StructField("brand",             StringType()),
    StructField("competitor",        StringType()),
    StructField("competitor_price",  DoubleType()),
    StructField("our_price",         DoubleType()),
    StructField("price_diff",        DoubleType()),
    StructField("price_diff_pct",    DoubleType()),
    StructField("in_stock",          BooleanType()),
    StructField("timestamp",         StringType()),
])


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# The Kafka connector JAR is downloaded automatically on first run (~30 secs)
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    log.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("DemandIntelligence-StreamConsumer")
        # Only Kafka connector needed — BigQuery writes are handled by the
        # Python google-cloud-bigquery client inside foreachBatch, so no
        # Spark-BigQuery connector JAR is required (and pulling it in
        # conflicts with PySpark's bundled Scala library).
        #
        # IMPORTANT: the connector artifact version must EXACTLY match the
        # Spark version that PySpark bundles under pyspark/jars/. With
        # pyspark==4.2.0 installed locally the bundle is spark-sql_2.13-4.2.0,
        # so the connector is spark-sql-kafka-0-10_2.13:4.2.0. Using any
        # older (e.g. 3.5.0) version raises:
        #   NoSuchMethodError: SerializedOffset.<init>(String)
        # because Spark 4.x moved that class's package path.
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:%s" % PYSPARK_VERSION
        )
        .config("spark.sql.streaming.metricsEnabled", "true")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready.")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY WRITER
# Uses foreachBatch pattern — called by Spark on each micro-batch
# foreachBatch gives you a regular DataFrame, so you can use any Python
# library to write it — we use the official google-cloud-bigquery client
# ─────────────────────────────────────────────────────────────────────────────
def make_bq_writer(table_id: str):
    """
    Returns a function that writes a DataFrame micro-batch to BigQuery.
    table_id format: "project.dataset.table"

    Why foreachBatch instead of the Spark BigQuery connector?
    The official connector needs a GCS temp bucket for staging — extra setup.
    foreachBatch + Python client works locally with zero extra infrastructure
    while demonstrating the same architectural pattern.
    """
    bq_client = bigquery.Client(project=CONFIG["gcp_project"])

    def write_to_bq(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            log.info(f"[Batch {batch_id}] Empty batch, skipping.")
            return

        row_count = batch_df.count()
        log.info(f"[Batch {batch_id}] Writing {row_count} rows to {table_id}")

        pandas_df = batch_df.toPandas()

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        job = bq_client.load_table_from_dataframe(
            pandas_df, table_id, job_config=job_config
        )
        job.result()
        log.info(f"[Batch {batch_id}] ✅ {row_count} rows written to {table_id}")

    return write_to_bq


# ─────────────────────────────────────────────────────────────────────────────
# STREAM PROCESSORS
# One function per topic — each validates schema then calls the BQ writer
# ─────────────────────────────────────────────────────────────────────────────

def process_orders_stream(spark: SparkSession):
    """
    Reads orders-topic → parses JSON → validates → writes to BigQuery orders table.

    Pipeline:
      raw bytes (Kafka) → JSON string → StructType columns → type conversions
      → null validation → BigQuery
    """
    project = CONFIG["gcp_project"]
    dataset  = CONFIG["bq_dataset"]
    table    = CONFIG["bq_orders_table"]
    table_id = f"{project}.{dataset}.{table}"

    log.info(f"Starting orders stream → {table_id}")

    # Step 1: Read raw bytes from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONFIG["kafka_bootstrap"])
        .option("subscribe", "orders-topic")
        .option("startingOffsets", CONFIG["kafka_start_offset"])
        .option("failOnDataLoss", "false")    # don't crash if Kafka loses data
        .load()
    )

    # Step 2: Parse JSON — Kafka gives us raw bytes, cast to string first
    # from_json converts the JSON string into typed columns using our schema
    parsed = (
        raw_stream
        .select(
            from_json(
                col("value").cast("string"),    # raw bytes → string
                ORDERS_SCHEMA                   # string → typed struct
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp")   # Kafka metadata
        )
        .select("data.*", "kafka_timestamp")    # flatten struct into columns
    )

    # Step 3: Type conversions and validation
    clean = (
        parsed
        # Convert ISO timestamp string to proper TIMESTAMP type
        .withColumn("timestamp", to_timestamp(col("timestamp")))

        # Flag rows with critical nulls — don't drop them, mark them for review
        # This lets you see data quality issues without losing events
        .withColumn(
            "has_quality_issues",
            when(
                col("order_id").isNull() |
                col("product_id").isNull() |
                col("timestamp").isNull(),
                lit(True)
            ).otherwise(lit(False))
        )

        # Drop Kafka metadata column before writing to BQ
        .drop("kafka_timestamp")
    )

    # Step 4: Log quality issues (don't block the stream)
    good_rows = clean.filter(col("has_quality_issues") == False).drop("has_quality_issues")
    bad_rows  = clean.filter(col("has_quality_issues") == True)

    # Step 5: Write good rows to BigQuery every N seconds
    checkpoint = f"{CONFIG['checkpoint_dir']}/orders"
    bq_writer  = make_bq_writer(table_id)

    query = (
        good_rows.writeStream
        .foreachBatch(bq_writer)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{CONFIG['trigger_seconds']} seconds")
        .start()
    )

    log.info("Orders stream running. Writing to BigQuery every "
             f"{CONFIG['trigger_seconds']}s.")
    return query


def process_pricing_stream(spark: SparkSession):
    """
    Reads pricing-topic → parses JSON → writes to BigQuery pricing_events table.
    """
    project  = CONFIG["gcp_project"]
    dataset  = CONFIG["bq_dataset"]
    table    = CONFIG["bq_pricing_table"]
    table_id = f"{project}.{dataset}.{table}"

    log.info(f"Starting pricing stream → {table_id}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONFIG["kafka_bootstrap"])
        .option("subscribe", "pricing-topic")
        .option("startingOffsets", CONFIG["kafka_start_offset"])
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            from_json(
                col("value").cast("string"),
                PRICING_SCHEMA
            ).alias("data")
        )
        .select("data.*")
    )

    clean = (
        parsed
        .withColumn("timestamp", to_timestamp(col("timestamp")))

        # Derived column: are we losing on price?
        # Useful ML feature — adds signal without changing source data
        .withColumn(
            "is_undercut",
            when(col("price_diff") < 0, lit(True)).otherwise(lit(False))
        )
    )

    checkpoint = f"{CONFIG['checkpoint_dir']}/pricing"
    bq_writer  = make_bq_writer(table_id)

    query = (
        clean.writeStream
        .foreachBatch(bq_writer)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{CONFIG['trigger_seconds']} seconds")
        .start()
    )

    log.info("Pricing stream running.")
    return query


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PySpark Structured Streaming Consumer"
    )
    parser.add_argument(
        "--mode",
        choices=["orders", "pricing", "both"],
        default="both",
        help="Which topics to consume (default: both)"
    )
    args = parser.parse_args()

    spark   = create_spark_session()
    queries = []

    if args.mode in ("orders", "both"):
        queries.append(process_orders_stream(spark))

    if args.mode in ("pricing", "both"):
        queries.append(process_pricing_stream(spark))

    log.info(f"\n{'='*55}")
    log.info(f"  {len(queries)} stream(s) running | mode={args.mode}")
    log.info(f"  Kafka:    {CONFIG['kafka_bootstrap']}")
    log.info(f"  BigQuery: {CONFIG['gcp_project']}.{CONFIG['bq_dataset']}")
    log.info(f"  Trigger:  every {CONFIG['trigger_seconds']}s")
    log.info(f"  Press Ctrl+C to stop")
    log.info(f"{'='*55}\n")

    # Keep alive — wait for all streams to finish (they run forever)
    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping streams...")
        for q in queries:
            q.stop()
        spark.stop()
        log.info("All streams stopped cleanly.")


if __name__ == "__main__":
    main()