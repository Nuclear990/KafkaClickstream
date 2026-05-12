from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

import shutil
import os

# Cleanup (DEV ONLY)

output_path = "/tmp/output"
checkpoint_path = "/tmp/checkpoint"

shutil.rmtree(output_path, ignore_errors=True)
shutil.rmtree(checkpoint_path, ignore_errors=True)

# Spark Session

spark = SparkSession.builder \
    .appName("ClickstreamConsumer") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .load()

# Schema

schema = StructType([
    StructField("event_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("product_id", IntegerType()),
    StructField("time_generated", TimestampType()),
    StructField("time_received", TimestampType())
])

# Parse JSON

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Event Time + Watermark

df = df.withColumn(
    "event_time",
    to_timestamp("time_generated")
)

df = df.withWatermark(
    "event_time",
    "5 seconds"
)

# Flags

df = df.withColumn(
    "is_purchase",
    when(
        col("event_type") == "purchase",
        1
    ).otherwise(0)
)

# Session Window Aggregation

session_df = df.groupBy(
    col("user_id"),
    session_window(
        col("event_time"),
        "5 seconds"
    )
).agg(
    collect_set(
        when(
            col("event_type") == "add_to_cart",
            col("product_id")
        )
    ).alias("cart_products"),

    max("is_purchase").alias("has_purchased")
)

# Cart Abandonment Logic

kafka_df = session_df.select(
    col("user_id")
        .cast("string")
        .alias("key"),

    to_json(
        struct(
            col("user_id"),

            col("cart_products")
                .alias("product_ids"),

            col("session_window.end")
                .alias("ts")
        )
    ).alias("value")
).filter(
    (size(col("cart_products")) > 0)
    &
    (col("has_purchased") == 0)
)

# Kafka Sink

kafka_query = kafka_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "abandoned_carts") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="5 seconds") \
    .start()

# Console Sink

console_query = kafka_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Await Termination

spark.streams.awaitAnyTermination()