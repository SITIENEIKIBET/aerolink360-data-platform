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


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_dim_date(spark)
    build_dim_airport(spark)

    spark.stop()


if __name__ == "__main__":
    main()