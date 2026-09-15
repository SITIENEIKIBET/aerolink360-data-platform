"""
Silver layer for Flight Operations: reads Bronze flights, validates
business rules (per master prompt Section 10 examples), writes
Silver + Quarantine.
"""
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

VALID_STATUSES = ["SCHEDULED", "BOARDING", "DEPARTED", "ARRIVED", "DELAYED", "CANCELLED", "DIVERTED"]


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Silver-FlightOps")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load("streaming/delta/bronze_flights")

    latest_window = Window.partitionBy("flight_id").orderBy(F.col("source_ts_ms").desc())
    latest = bronze.withColumn("_rn", F.row_number().over(latest_window)).filter(F.col("_rn") == 1).drop("_rn")

    logger.info("Bronze flights (all CDC versions): %d rows", bronze.count())
    logger.info("Latest state per flight: %d rows", latest.count())

    checked = (
        latest
        .withColumn("has_valid_status", F.col("flight_status").isin(VALID_STATUSES))
        .withColumn("has_valid_airports",
                    F.col("origin_airport").isNotNull() & F.col("destination_airport").isNotNull() &
                    (F.col("origin_airport") != F.col("destination_airport")))
    )

    valid = checked.filter(F.col("has_valid_status") & F.col("has_valid_airports"))
    quarantined = checked.filter(~(F.col("has_valid_status") & F.col("has_valid_airports")))

    silver = valid.select(
        "flight_id", "schedule_id", "origin_airport", "destination_airport",
        "flight_status", "aircraft_registration", "source_ts_ms",
    ).withColumn("_silver_processed_at", F.current_timestamp())

    quarantine = quarantined.withColumn(
        "quarantine_reason",
        F.when(~F.col("has_valid_status"), F.lit("invalid flight_status value"))
         .otherwise(F.lit("missing or identical origin/destination airport"))
    ).withColumn("_silver_processed_at", F.current_timestamp())

    silver.write.format("delta").mode("overwrite").save("streaming/delta/silver_flights")
    quarantine.write.format("delta").mode("overwrite").save("streaming/delta/silver_quarantine_flights")

    logger.info("Silver flights: %d rows", silver.count())
    logger.info("Quarantined flights: %d rows", quarantine.count())
    spark.stop()


if __name__ == "__main__":
    main()