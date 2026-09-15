"""
Silver layer for Maintenance: reads Bronze aircraft, validates business
rules, writes Silver + Quarantine.
"""
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Silver-Maintenance")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load("streaming/delta/bronze_aircraft")

    latest_window = Window.partitionBy("aircraft_registration").orderBy(F.col("source_ts_ms").desc())
    latest = bronze.withColumn("_rn", F.row_number().over(latest_window)).filter(F.col("_rn") == 1).drop("_rn")

    logger.info("Bronze aircraft (all CDC versions): %d rows", bronze.count())
    logger.info("Latest state per aircraft: %d rows", latest.count())

    # Business rule: a grounded aircraft must have a reason recorded
    checked = (
        latest
        .withColumn("has_valid_hours",
                    (F.col("total_flight_hours").isNull()) | (F.col("total_flight_hours") >= 0))
        .withColumn("has_valid_grounding",
                    (~F.col("is_grounded")) | (F.col("is_grounded") & F.col("grounded_reason").isNotNull()))
    )

    valid = checked.filter(F.col("has_valid_hours") & F.col("has_valid_grounding"))
    quarantined = checked.filter(~(F.col("has_valid_hours") & F.col("has_valid_grounding")))

    silver = valid.select(
        "aircraft_registration", "model_id", "total_flight_hours",
        "total_flight_cycles", "is_grounded", "grounded_reason", "source_ts_ms",
    ).withColumn("_silver_processed_at", F.current_timestamp())

    quarantine = quarantined.withColumn(
        "quarantine_reason",
        F.when(~F.col("has_valid_hours"), F.lit("negative total_flight_hours"))
         .otherwise(F.lit("grounded aircraft missing grounded_reason"))
    ).withColumn("_silver_processed_at", F.current_timestamp())

    silver.write.format("delta").mode("overwrite").save("streaming/delta/silver_aircraft")
    quarantine.write.format("delta").mode("overwrite").save("streaming/delta/silver_quarantine_aircraft")

    logger.info("Silver aircraft: %d rows", silver.count())
    logger.info("Quarantined aircraft: %d rows", quarantine.count())
    spark.stop()


if __name__ == "__main__":
    main()