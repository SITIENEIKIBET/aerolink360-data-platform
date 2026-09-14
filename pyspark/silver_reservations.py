"""
Silver layer transformation for Reservations: reads Bronze bookings,
applies business rule validation and referential checks, writes
Silver + Quarantine Delta tables. Batch job (run after streaming
Bronze ingestion has caught up), not itself a streaming job.

Usage:
    python pyspark\\silver_reservations.py
"""
import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

VALID_STATUSES = ["PENDING", "CONFIRMED", "CANCELLED", "REFUNDED"]


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Silver-Reservations")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load("streaming/delta/bronze_bookings")

    # Keep only the latest event per booking_id (CDC stream may have
    # multiple versions of the same booking over time - Silver wants
    # current state, not full history at this stage)
    from pyspark.sql.window import Window
    latest_window = Window.partitionBy("booking_id").orderBy(F.col("source_ts_ms").desc())
    latest = (
        bronze
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    logger.info("Bronze bookings (all CDC versions): %d rows", bronze.count())
    logger.info("Latest state per booking: %d rows", latest.count())

    # ---- Business rule validation ----
    checked = (
        latest
        .withColumn("has_valid_status", F.col("booking_status").isin(VALID_STATUSES))
        .withColumn("has_valid_reference", F.col("booking_reference").isNotNull())
    )

    valid = checked.filter(F.col("has_valid_status") & F.col("has_valid_reference"))
    quarantined = checked.filter(~(F.col("has_valid_status") & F.col("has_valid_reference")))

    silver = valid.select(
        "booking_id", "customer_id", "booking_reference", "booking_status",
        "booking_channel", "currency", "source_ts_ms",
    ).withColumn("_silver_processed_at", F.current_timestamp())

    quarantine = quarantined.withColumn(
        "quarantine_reason",
        F.when(~F.col("has_valid_status"), F.lit("invalid booking_status value"))
         .otherwise(F.lit("missing booking_reference"))
    ).withColumn("_silver_processed_at", F.current_timestamp())

    silver.write.format("delta").mode("overwrite").save("streaming/delta/silver_bookings")
    quarantine.write.format("delta").mode("overwrite").save("streaming/delta/silver_quarantine_bookings")

    logger.info("Silver bookings: %d rows", silver.count())
    logger.info("Quarantined bookings: %d rows", quarantine.count())

    spark.stop()


if __name__ == "__main__":
    main()