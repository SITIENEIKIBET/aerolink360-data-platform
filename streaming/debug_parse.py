"""
One-shot diagnostic: reads a handful of raw messages from Kafka and shows
both the raw JSON and the result of our parsing logic, side by side.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = (
    SparkSession.builder
    .appName("Debug-Parse")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Batch read, not streaming - just grab whatever's there right now
raw = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "aaa.reservations.public.bookings")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as json_value")
)

print("=== RAW JSON (first message) ===")
print(raw.first()["json_value"][:1500])

after_schema = StructType([
    StructField("booking_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("booking_reference", StringType()),
    StructField("booking_status", StringType()),
])
source_schema = StructType([
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("txId", LongType()),
])
payload_schema = StructType([
    StructField("before", after_schema, True),
    StructField("after", after_schema, True),
    StructField("source", source_schema, True),
    StructField("op", StringType()),
])
envelope_schema = StructType([
    StructField("payload", payload_schema, True),
])

parsed = raw.withColumn("parsed", F.from_json(F.col("json_value"), envelope_schema))

print("\n=== PARSED RESULT (first 5 rows) ===")
parsed.select("parsed.payload.after.*", "parsed.payload.op").show(5, truncate=False)

print(f"\nTotal raw messages: {raw.count()}")
print(f"Rows where booking_id parsed successfully: {parsed.filter(F.col('parsed.payload.after.booking_id').isNotNull()).count()}")

spark.stop()