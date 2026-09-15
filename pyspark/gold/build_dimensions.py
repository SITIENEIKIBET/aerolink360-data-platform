"""
Gold layer: builds reference dimensions via batch JDBC reads from
Postgres (per ADR-012) plus a generated dim_date.
"""
import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

JDBC_DRIVER = "org.postgresql.Driver"
PG_PASSWORD = "AeroLink@2026!"  # matches POSTGRES_PASSWORD from .env


def build_spark():
    return (
        SparkSession.builder
        .appName("AeroLink360-Gold-Dimensions")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_pg_table(spark, port, dbname, table):
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://localhost:{port}/{dbname}")
        .option("dbtable", table)
        .option("user", "aaa_admin")
        .option("password", PG_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .load()
    )


def build_dim_date(spark, start="2025-01-01", end="2026-12-31"):
    logger.info("Building dim_date...")
    dim_date = (
        spark.sql(f"SELECT sequence(to_date('{start}'), to_date('{end}'), interval 1 day) as date")
        .withColumn("date", F.explode("date"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
    )
    dim_date.write.format("delta").mode("overwrite").save("streaming/delta/dim_date")
    logger.info("dim_date: %d rows", dim_date.count())


def build_dim_airport(spark):
    logger.info("Building dim_airport...")
    airports = read_pg_table(spark, 5434, "flightops", "airports")
    dim_airport = (
        airports
        .withColumn("airport_key", F.monotonically_increasing_id().cast("int"))
        .select("airport_key", "airport_code", "airport_name", "city", "country", "timezone")
    )
    dim_airport.write.format("delta").mode("overwrite").save("streaming/delta/dim_airport")
    logger.info("dim_airport: %d rows", dim_airport.count())
def build_dim_customer(spark):
    logger.info("Building dim_customer...")
    customers = read_pg_table(spark, 5433, "reservations", "customers")
    dim_customer = (
        customers
        .withColumn("customer_key", F.monotonically_increasing_id().cast("int"))
        .select("customer_key", "customer_id", "first_name", "last_name",
                "email", "nationality", "date_of_birth")
    )
    dim_customer.write.format("delta").mode("overwrite").save("streaming/delta/dim_customer")
    logger.info("dim_customer: %d rows", dim_customer.count())


def build_dim_fare_class(spark):
    logger.info("Building dim_fare_class...")
    fare_classes = read_pg_table(spark, 5433, "reservations", "fare_classes")
    fare_rules = read_pg_table(spark, 5433, "reservations", "fare_rules")
    dim_fare_class = (
        fare_classes.alias("fc")
        .join(fare_rules.alias("fr"), F.col("fc.fare_class_id") == F.col("fr.fare_class_id"), "left")
        .select(
            F.col("fc.fare_class_id"), F.col("fc.class_code"), F.col("fc.class_name"),
            F.col("fr.refundable"), F.col("fr.change_fee"), F.col("fr.baggage_allowance_kg"),
        )
        .withColumn("fare_class_key", F.monotonically_increasing_id().cast("int"))
    )
    dim_fare_class.write.format("delta").mode("overwrite").save("streaming/delta/dim_fare_class")
    logger.info("dim_fare_class: %d rows", dim_fare_class.count())


def build_dim_aircraft_model(spark):
    logger.info("Building dim_aircraft_model...")
    models = read_pg_table(spark, 5435, "maintenance", "aircraft_models")
    dim_model = models.withColumnRenamed("model_id", "aircraft_model_key")
    dim_model.write.format("delta").mode("overwrite").save("streaming/delta/dim_aircraft_model")
    logger.info("dim_aircraft_model: %d rows", dim_model.count())


def build_dim_aircraft(spark):
    """
    CDC-sourced dimension: joins Silver aircraft (from streaming) with
    the batch-loaded dim_aircraft_model for a denormalized, BI-ready
    aircraft dimension.
    """
    logger.info("Building dim_aircraft (from Silver + dim_aircraft_model)...")
    silver_aircraft = spark.read.format("delta").load("streaming/delta/silver_aircraft")
    dim_model = spark.read.format("delta").load("streaming/delta/dim_aircraft_model")

    dim_aircraft = (
        silver_aircraft.alias("a")
        .join(dim_model.alias("m"), F.col("a.model_id") == F.col("m.aircraft_model_key"), "left")
        .select(
            F.col("a.aircraft_registration"), F.col("m.model_name"), F.col("m.manufacturer"),
            F.col("a.total_flight_hours"), F.col("a.total_flight_cycles"),
            F.col("a.is_grounded"), F.col("a.grounded_reason"),
        )
    )
    dim_aircraft.write.format("delta").mode("overwrite").save("streaming/delta/dim_aircraft")
    logger.info("dim_aircraft: %d rows", dim_aircraft.count())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_dim_date(spark)
    build_dim_airport(spark)
    build_dim_customer(spark)
    build_dim_fare_class(spark)
    build_dim_aircraft_model(spark)
    build_dim_aircraft(spark)

    spark.stop()


if __name__ == "__main__":
    main()