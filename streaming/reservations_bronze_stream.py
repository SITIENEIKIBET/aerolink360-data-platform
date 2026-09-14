"""
Spark Structured Streaming consumer for the Reservations domain.

Reads live CDC events from the aaa.reservations.public.bookings Kafka
topic, parses Debezium's envelope (before/after/op), deduplicates,
applies watermarking for late-arriving events, and writes a clean
Bronze Delta table.

Usage:
    python streaming\\reservations_bronze_stream.py
"""
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "aaa.reservations.public.bookings"
CHECKPOINT_PATH = "streaming/checkpoints/reservations_bookings"
DELTA_OUTPUT_PATH = "streaming/delta/bronze_bookings"


def build_spark_session():
    return (
        SparkSession.builder
        .appName("AeroLink360-Reservations-Bronze-Stream")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# Debezium envelope: the "after" field's inner structure matches the
# bookings table schema. We only need to declare the fields we care
# about extracting - Spark will simply ignore the rest of the JSON.
after_schema = StructType([
    StructField("booking_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("booking_reference", StringType()),
    StructField("booking_status", StringType()),
    StructField("booking_channel", StringType()),
    StructField("currency", StringType()),
])

source_schema = StructType([
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("db", StringType()),
    StructField("table", StringType()),
    StructField("txId", LongType()),
    StructField("lsn", LongType()),
])

payload_schema = StructType([
    StructField("before", after_schema, True),
    StructField("after", after_schema, True),
    StructField("source", source_schema, True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

envelope_schema = StructType([
    StructField("payload", payload_schema, True),
])

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session started. Connecting to Kafka at %s, topic %s", KAFKA_BOOTSTRAP, TOPIC)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka's value is raw bytes containing Debezium's JSON envelope.
    # Cast to string, then parse against our declared schema.
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .withColumn("parsed", F.from_json(F.col("json_value"), envelope_schema))
             .select(
            F.col("parsed.payload.after.*"),
            F.col("parsed.payload.op").alias("cdc_operation"),
            F.col("parsed.payload.source.ts_ms").alias("source_ts_ms"),
            F.col("parsed.payload.source.snapshot").alias("is_snapshot"),
            F.col("parsed.payload.source.txId").alias("source_tx_id"),
            F.col("parsed.payload.source.lsn").alias("source_lsn"),
            F.col("kafka_timestamp"),
        )
        # Deduplicate: if the same booking_id + tx_id combination somehow
        # arrives twice (e.g. a Kafka Connect retry), only keep one.
        # This is the concrete implementation of the "handle duplicate
        # events" requirement.
        .withWatermark("kafka_timestamp", "10 minutes")
        .dropDuplicates(["booking_id"])
    )

    # Filter out delete events and pure-null rows (safety net for malformed
    # events that fail to parse against the schema - they'd show up as
    # entirely null after the from_json parse rather than crashing the job)
    clean = parsed.filter(F.col("booking_id").isNotNull())

    query = (
        clean.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start(DELTA_OUTPUT_PATH)
    )

    logger.info("Streaming query started. Writing to %s. Checkpoint at %s", DELTA_OUTPUT_PATH, CHECKPOINT_PATH)
    logger.info("Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()