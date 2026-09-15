"""
Silver layer for Loyalty transactions: reads Bronze ledger (append-only,
no CDC "latest state" concept needed - every transaction is a permanent
fact), validates business rules, writes Silver + Quarantine.
"""
import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

VALID_TXN_TYPES = ["EARN", "REDEEM", "EXPIRE", "ADJUSTMENT"]


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Silver-Loyalty")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load("streaming/delta/bronze_loyalty_transactions")
    logger.info("Bronze loyalty transactions: %d rows", bronze.count())

    checked = (
        bronze
        .withColumn("has_valid_type", F.col("transaction_type").isin(VALID_TXN_TYPES))
        .withColumn("has_valid_amount", F.col("miles_amount") > 0)
    )

    valid = checked.filter(F.col("has_valid_type") & F.col("has_valid_amount"))
    quarantined = checked.filter(~(F.col("has_valid_type") & F.col("has_valid_amount")))

    silver = valid.select(
        "transaction_id", "loyalty_account_id", "transaction_type",
        "miles_amount", "reference_type", "source_ts_ms",
    ).withColumn("_silver_processed_at", F.current_timestamp())

    quarantine = quarantined.withColumn(
        "quarantine_reason",
        F.when(~F.col("has_valid_type"), F.lit("invalid transaction_type value"))
         .otherwise(F.lit("non-positive miles_amount"))
    ).withColumn("_silver_processed_at", F.current_timestamp())

    silver.write.format("delta").mode("overwrite").save("streaming/delta/silver_loyalty_transactions")
    quarantine.write.format("delta").mode("overwrite").save("streaming/delta/silver_quarantine_loyalty_transactions")

    logger.info("Silver loyalty transactions: %d rows", silver.count())
    logger.info("Quarantined loyalty transactions: %d rows", quarantine.count())
    spark.stop()


if __name__ == "__main__":
    main()