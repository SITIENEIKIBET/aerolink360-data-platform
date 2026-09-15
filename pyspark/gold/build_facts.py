"""
Gold layer: builds fact tables from Silver, joined to Gold dimensions
for surrogate key resolution.
"""
import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Gold-Facts")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_fact_bookings(spark):
    logger.info("Building fact_bookings...")
    silver_bookings = spark.read.format("delta").load("streaming/delta/silver_bookings")
    dim_customer = spark.read.format("delta").load("streaming/delta/dim_customer")

    fact = (
        silver_bookings.alias("b")
        .join(dim_customer.alias("c"), F.col("b.customer_id") == F.col("c.customer_id"), "left")
        .select(
            F.col("b.booking_id"), F.col("c.customer_key"), F.col("b.booking_reference"),
            F.col("b.booking_status"), F.col("b.booking_channel"), F.col("b.currency"),
            F.col("b.source_ts_ms"),
        )
    )
    fact.write.format("delta").mode("overwrite").save("streaming/delta/fact_bookings")
    logger.info("fact_bookings: %d rows (nulls in customer_key indicate unmatched customer): %d",
                fact.count(), fact.filter(F.col("customer_key").isNull()).count())


def build_fact_flight_operations(spark):
    logger.info("Building fact_flight_operations...")
    silver_flights = spark.read.format("delta").load("streaming/delta/silver_flights")
    dim_airport = spark.read.format("delta").load("streaming/delta/dim_airport")
    dim_aircraft = spark.read.format("delta").load("streaming/delta/dim_aircraft")

    fact = (
        silver_flights.alias("f")
        .join(dim_airport.alias("o"), F.col("f.origin_airport") == F.col("o.airport_code"), "left")
        .join(dim_airport.alias("d"), F.col("f.destination_airport") == F.col("d.airport_code"), "left")
        .join(dim_aircraft.alias("a"), F.col("f.aircraft_registration") == F.col("a.aircraft_registration"), "left")
        .select(
            F.col("f.flight_id"), F.col("o.airport_key").alias("origin_airport_key"),
            F.col("d.airport_key").alias("destination_airport_key"),
            F.col("f.aircraft_registration"), F.col("f.flight_status"), F.col("f.source_ts_ms"),
        )
    )
    fact.write.format("delta").mode("overwrite").save("streaming/delta/fact_flight_operations")
    logger.info("fact_flight_operations: %d rows", fact.count())


def build_fact_loyalty_transactions(spark):
    logger.info("Building fact_loyalty_transactions...")
    silver_loyalty = spark.read.format("delta").load("streaming/delta/silver_loyalty_transactions")
    fact = silver_loyalty.select(
        "transaction_id", "loyalty_account_id", "transaction_type", "miles_amount", "source_ts_ms",
    )
    fact.write.format("delta").mode("overwrite").save("streaming/delta/fact_loyalty_transactions")
    logger.info("fact_loyalty_transactions: %d rows", fact.count())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_fact_bookings(spark)
    build_fact_flight_operations(spark)
    build_fact_loyalty_transactions(spark)

    spark.stop()


if __name__ == "__main__":
    main()