"""
Spark Structured Streaming consumer for Flight Operations.
Reads aaa.flightops.public.flights CDC events, writes clean Bronze Delta.
"""
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "aaa.flightops.public.flights"
CHECKPOINT_PATH = "streaming/checkpoints/flightops_flights"
DELTA_OUTPUT_PATH = "streaming/delta/bronze_flights"

after_schema = StructType([
    StructField("flight_id", StringType()),
    StructField("schedule_id", IntegerType()),
    StructField("origin_airport", StringType()),
    StructField("destination_airport", StringType()),
    StructField("flight_status", StringType()),
    StructField("aircraft_registration", StringType()),
])
source_schema = StructType([
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("txId", LongType()),
])
payload_schema = StructType([
    StructField("after", after_schema, True),
    StructField("source", source_schema, True),
    StructField("op", StringType()),
])
envelope_schema = StructType([StructField("payload", payload_schema, True)])


def main():
    spark = (
        SparkSession.builder
        .appName("AeroLink360-FlightOps-Bronze-Stream")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Connecting to Kafka at %s, topic %s", KAFKA_BOOTSTRAP, TOPIC)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .withColumn("parsed", F.from_json(F.col("json_value"), envelope_schema))
        .select(
            F.col("parsed.payload.after.*"),
            F.col("parsed.payload.op").alias("cdc_operation"),
            F.col("parsed.payload.source.ts_ms").alias("source_ts_ms"),
            F.col("parsed.payload.source.snapshot").alias("is_snapshot"),
            F.col("kafka_timestamp"),
        )
        .withWatermark("kafka_timestamp", "10 minutes")
        .dropDuplicates(["flight_id"])
    )

    clean = parsed.filter(F.col("flight_id").isNotNull())

    query = (
        clean.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start(DELTA_OUTPUT_PATH)
    )
    logger.info("Streaming query started. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()